package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var pWorkerDesignString = `address: %s
privatekey: %s
network_id: mitum contest; Sat 26 Dec 2020 05:29:13 AM KST
storage:
    base: ./data/worker/%s
network:
  bind: 0.0.0.0:%d
  publish: %s
  tls_insecure: true
`

var errCommandWorkerDone = util.NewIDError("command worker done")

type CommandWorker struct { //nolint:govet //...
	baseCommand
	managerCI quicstream.ConnInfo
	pws       util.LockedMap[base.Height, *ProcessWorker]
	bdbs      map[base.Height]isaac.BlockWriteDatabase
	ctxCancel func(error)
	stvsPool  util.GCache[string, []StateMergeValue]
}

func (cmd *CommandWorker) Run(pctx context.Context) error {
	switch i, pps, err := cmd.prepare(pctx); {
	case err != nil:
		return err
	default:
		defer func() {
			_, _ = pps.Close(pctx)
		}()

		pctx = i
	}

	return cmd.run(pctx)
}

func (cmd *CommandWorker) prepare(pctx context.Context) (context.Context, *ps.PS, error) {
	if err := cmd.baseCommand.prepare(pctx); err != nil {
		return nil, nil, err
	}

	var priv base.Privatekey

	switch i, err := base.NewMPrivatekeyFromSeed(strings.Repeat(cmd.ConnInfo.String(), 33)); {
	case err != nil:
		return nil, nil, err
	default:
		priv = i
	}

	port := cmd.ConnInfo.UDPAddr().Port
	address := base.NewStringAddress(fmt.Sprintf("%d", port))

	designString := fmt.Sprintf(
		pWorkerDesignString,
		address,
		priv,
		address,
		port,
		cmd.ConnInfo,
	)

	cmd.Log().Info().Str("string", designString).Msg("design")

	pps, pctx := preparePS(
		pctx,
		"cmd-worker",
		designString,
		cmd.Operations,
	)
	if err := cmd.pps(pps, pctx); err != nil {
		return nil, nil, err
	}

	switch i, err := pps.Run(pctx); {
	case err != nil:
		return pctx, nil, err
	default:
		pctx = i
	}

	if err := cmd.loadDesign(pctx); err != nil {
		return nil, nil, err
	}

	cmd.bdbs = map[base.Height]isaac.BlockWriteDatabase{}
	cmd.stvsPool = util.NewLFUGCache[string, []StateMergeValue](int(cmd.Operations) * 4)

	return pctx, pps, nil
}

func (cmd *CommandWorker) run(pctx context.Context) error {
	var ctx context.Context

	ctx, cmd.ctxCancel = context.WithCancelCause(pctx)

	cmd.pws = util.NewSingleLockedMap[base.Height, *ProcessWorker]()

	<-ctx.Done()

	switch err := context.Cause(ctx); {
	case !errors.Is(err, errCommandWorkerDone):
		return err
	default:
		return nil
	}
}

func (cmd *CommandWorker) pps(pps *ps.PS, pctx context.Context) error {
	_ = pps.POK(launch.PNameStates).
		PostAddOK("worker-network-handlers", cmd.pNetworkHandlers)

	return nil
}

func (cmd *CommandWorker) pNetworkHandlers(pctx context.Context) (context.Context, error) {
	var gerror error

	ensureHandlerAdd(pctx, &gerror, handlerNameByHint(WorkerCancelHeaderHint), cmd.handlerCancelFunc, nil)
	ensureHandlerAdd(pctx, &gerror, handlerNameByHint(WorkerDoneHeaderHint), cmd.handlerDoneFunc, nil)
	ensureHandlerAdd(pctx, &gerror, handlerNameByHint(WorkerReadyHeaderHint), cmd.handlerReadyFunc, nil)
	ensureHandlerAdd(pctx, &gerror, handlerNameByHint(WorkerNewOperationsHeaderHint), cmd.handlerNewOperationsFunc, nil)
	ensureHandlerAdd(pctx, &gerror, handlerNameByHint(WorkerInsertOpHeaderHint), cmd.handlerInsertOpFunc, nil)
	ensureHandlerAdd(pctx, &gerror, handlerNameByHint(WorkerInsertValuesDoneHeaderHint), cmd.handlerInsertValuesDoneFunc, nil)
	ensureHandlerAdd(pctx, &gerror, handlerNameByHint(WorkerPhaseDoneHeaderHint), cmd.handlerPhaseDoneFunc, nil)
	ensureHandlerAdd(pctx, &gerror, handlerNameByHint(WorkerSyncStatesHeaderHint), cmd.handlerSyncStates, nil)
	ensureHandlerAdd(pctx, &gerror, handlerNameByHint(WorkerSetStatesHeaderHint), cmd.handlerSetStates, nil)
	ensureHandlerAdd(pctx, &gerror, handlerNameByHint(WorkerSetOperationsHeaderHint), cmd.handlerSetOperations, nil)
	ensureHandlerAdd(pctx, &gerror, handlerNameByHint(WorkerSetStateMergeValuesHeaderHint), cmd.handlerSetStateMergeValues, nil)

	return pctx, gerror
}

func (cmd *CommandWorker) handlerCancelFunc(pctx context.Context) (quicstreamheader.Handler[WorkerCancelHeader], error) {
	return newWorkerHandlerCancelFunc(
		func(height base.Height) (func(), error) {
			i, _ := cmd.pws.Value(height)
			if i == nil {
				return nil, util.ErrNotFound.Errorf("ProcessWorker")
			}

			return i.Cancel, nil
		},
	)
}

func (cmd *CommandWorker) handlerDoneFunc(
	pctx context.Context,
) (quicstreamheader.Handler[WorkerDoneHeader], error) {
	return newWorkerHandlerWorkerDoneFunc(
		func(height base.Height) (func() error, error) {
			i, _ := cmd.pws.Value(height)
			if i == nil {
				return nil, util.ErrNotFound.Errorf("handlerDone")
			}

			return func() error {
				if err := i.Done(); err != nil {
					cmd.ctxCancel(err)

					return err
				}

				if height == 1 {
					go func() {
						<-time.After(time.Second)

						cmd.ctxCancel(errCommandWorkerDone)
					}()
				}

				return nil
			}, nil
		},
	)
}

func (cmd *CommandWorker) handlerReadyFunc(pctx context.Context) (quicstreamheader.Handler[WorkerReadyHeader], error) {
	var design launch.NodeDesign
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		launch.DesignContextKey, &design,
		launch.CenterDatabaseContextKey, &db,
	); err != nil {
		return nil, err
	}

	return newWorkerHandlerReadyFunc(
		func(args processWorkerReadyArgs) error {
			if cmd.managerCI.UDPAddr() == nil {
				switch ci, err := ParseConnInfo(args.Manager); {
				case err != nil:
					return err
				default:
					cmd.managerCI = ci
				}
			}

			_, _, err := cmd.pws.Set(args.Height, func(_ *ProcessWorker, found bool) (*ProcessWorker, error) {
				if found {
					return nil, util.ErrFound.Errorf("already worker ready, %d", args.Height)
				}

				return cmd.newWorker(
					design,
					args,
					db,
					launch.LocalFSDataDirectory(design.Storage.Base),
					cmd.encs.Default(),
				)
			})

			return err
		},
	)
}

func (cmd *CommandWorker) handlerNewOperationsFunc(pctx context.Context) (quicstreamheader.Handler[WorkerNewOperationsHeader], error) {
	return newWorkerHandlerNewOperationsFunc(cmd.Log().Debug, func(height base.Height) (
		workerNewOperationsFunc,
		func() error,
		func(error),
		error,
	) {
		switch w, found := cmd.pws.Value(height); {
		case !found:
			return nil, nil, nil, util.ErrNotFound.Errorf("ProcessWorker not found, %d", height)
		default:
			return w.NewOperations()
		}
	})
}

func (cmd *CommandWorker) handlerInsertOpFunc(pctx context.Context) (quicstreamheader.Handler[WorkerInsertOpHeader], error) {
	return newWorkerHandlerInsertOpFunc(func(height base.Height) (
		processWorkerInsertValues,
		error,
	) {
		switch w, found := cmd.pws.Value(height); {
		case !found:
			return nil, util.ErrNotFound.Errorf("ProcessWorker not found, %d", height)
		default:
			return w.InsertValues, nil
		}
	})
}

func (cmd *CommandWorker) handlerInsertValuesDoneFunc(pctx context.Context) (quicstreamheader.Handler[WorkerInsertValuesDoneHeader], error) {
	return newWorkerHandlerInsertValuesDoneFunc(func(height base.Height) (f func(worker string) error, _ error) {
		switch w, found := cmd.pws.Value(height); {
		case !found:
			return nil, util.ErrNotFound.Errorf("ProcessWorker not found, %d", height)
		default:
			return w.WorkerInsertValuesDone, nil
		}
	})
}

func (cmd *CommandWorker) handlerPhaseDoneFunc(pctx context.Context) (quicstreamheader.Handler[WorkerPhaseDoneHeader], error) {
	return newWorkerHandlerPhaseDoneFunc(func(height base.Height) (f func(worker string, phase int, mergedOps, unmergedOps []uint64) error, _ error) {
		switch w, found := cmd.pws.Value(height); {
		case !found:
			return nil, util.ErrNotFound.Errorf("ProcessWorker not found, %d", height)
		default:
			return w.WorkerPhaseDone, nil
		}
	})
}

func (cmd *CommandWorker) handlerSyncStates(pctx context.Context) (quicstreamheader.Handler[WorkerSyncStatesHeader], error) {
	return newWorkerHandlerSyncStatesFunc(
		func(ctx context.Context, _ base.Height) error {
			return cmd.requestWorkers(
				ctx,
				"handlerSyncStates",
				newWorkerBrokerSetStatesFunc(
					func(f func(base.State) error) error {
						var gerr error

						cmd.statesPool.Traverse(func(_ string, v base.State) bool {
							gerr = f(v)

							return gerr == nil
						})

						return nil
					},
				),
			)
		},
	)
}

func (cmd *CommandWorker) handlerSetStates(pctx context.Context) (quicstreamheader.Handler[WorkerSetStatesHeader], error) {
	return newWorkerHandlerSetStatesFunc(func(st base.State) error {
		cmd.statesPool.Set(st.Key(), st, 0)

		return nil
	})
}

func (cmd *CommandWorker) handlerSetOperations(pctx context.Context) (quicstreamheader.Handler[WorkerSetOperationsHeader], error) {
	return newWorkerHandlerSetOperationsFunc(func(op base.Operation) error {
		cmd.opsPool.Set(op.Hash().String(), op, 0)

		return nil
	})
}

func (cmd *CommandWorker) handlerSetStateMergeValues(pctx context.Context) (quicstreamheader.Handler[WorkerSetStateMergeValuesHeader], error) {
	return newWorkerHandlerSetStateMergeValuesFunc(func(op util.Hash, vs []StateMergeValue) error {
		cmd.stvsPool.Set(op.String(), vs, 0)

		return nil
	})
}

func (cmd *CommandWorker) newWorker(
	design launch.NodeDesign,
	margs processWorkerReadyArgs,
	db isaac.Database,
	baseroot string,
	enc encoder.Encoder,
) (pm *ProcessWorker, _ error) {
	height := margs.Height
	local := cmd.localCI.String()

	if err := util.TraverseSliceV1(margs.Workers, func(_ int, worker string) (bool, error) {
		if cmd.cis.Exists(worker) {
			return true, nil
		}

		var ci quicstream.ConnInfo

		switch i, err := ParseConnInfo(worker); {
		case err != nil:
			return false, err
		case network.EqualConnInfo(i, cmd.localCI):
			return true, nil
		default:
			ci = i
		}

		_ = cmd.cis.SetValue(worker, ci)

		return true, nil
	}); err != nil {
		return nil, err
	}

	var bdb isaac.BlockWriteDatabase

	switch i, err := db.NewBlockWriteDatabase(height); {
	case err != nil:
		return nil, err
	default:
		bdb = i
		cmd.bdbs[height] = bdb
	}

	fsroot := filepath.Join(baseroot, height.String())
	if err := os.MkdirAll(fsroot, 0o700); err != nil {
		return nil, errors.WithMessage(err, "create fs root directory")
	}

	saveOperationFuncs := newWriterSaveOperationsFuncs(
		bdb,
		fsroot,
		enc,
		"operations.json.gz",
	)

	saveStatesFuncs := newWriterSaveStatesFuncs(
		bdb,
		fsroot,
		enc,
		"states.json.gz",
	)

	args := NewProcessWorkerArgs(design.NetworkID)
	args.NOps = margs.NOps
	args.GetOperationFunc = func(ctx context.Context, opindex uint64, operationhash util.Hash) (Operation, bool, error) {
		switch i, found := cmd.opsPool.Get(operationhash.String()); {
		case !found:
			return nil, false, nil
		default:
			return i.(Operation), true, nil
		}
	}
	args.GetStateFunc = func(_ context.Context, key string) (base.State, bool, error) {
		i, found := cmd.statesPool.Get(key)

		return i, found, nil
	}
	args.NewJobWorker = func(ctx context.Context) (util.JobWorker, error) {
		return util.NewBaseJobWorker(ctx, cmd.c)
	}

	nworkers := uint64(len(margs.Workers))
	nshared := uint64(margs.NStateShare)

	args.IsEnoughWorkers = func(n uint64) bool {
		return n > nworkers-nshared
	}

	args.StateOwners = cmd.remoteStateOwnersFunc(height)

	switch stateOwnersf, err := cmd.djb2StateOwnersFunc(margs.Workers, margs.NStateShare); {
	case err != nil:
		return nil, err
	default:
		args.StateOwners = stateOwnersf
	}

	args.BroadcastInsertOpFunc = func(ctx context.Context, worker string, opindex uint64, fact util.Hash, values []StateMergeValue, owners map[string][]string) error {
		return cmd.requestWorker(
			ctx,
			"BroadcastInsertOp",
			worker,
			newWorkerBrokerInsertOpFunc(
				height,
				cmd.localCI.String(),
				cmd.encs.Default(),
				opindex,
				fact,
				values,
				owners,
			),
		)
	}
	args.BroadcastInsertValuesDoneFunc = func(ctx context.Context) error {
		return cmd.requestWorkersErrCallback(
			ctx,
			"BroadcastInsertValuesDone",
			newWorkerBrokerInsertValuesDoneFunc(
				height,
				cmd.localCI.String(),
			),
			func(worker string, err error) {
				cmd.Log().Error().Err(err).Str("worker", worker).Msg("BroadcastInsertValuesDone")
			},
		)
	}
	args.BroadcastPhaseDoneFunc = func(ctx context.Context, phase int, mergedOps, unmergedOps []uint64) error {
		return cmd.requestWorkersErrCallback(
			ctx,
			"BroadcastPhaseDone",
			newWorkerBrokerPhaseDoneFunc(
				height,
				cmd.localCI.String(),
				phase,
				mergedOps,
				unmergedOps,
			),
			func(worker string, err error) {
				cmd.Log().Error().Err(err).Str("worker", worker).Msg("BroadcastPhaseDone")
			},
		)
	}

	args.BroadcastCanceled = func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()

		return cmd.request(
			ctx,
			"BroadcastCanceled",
			newManagerBrokerWorkerCanceledFunc(
				height,
				cmd.localCI.String(),
			),
			cmd.managerCI,
		)
	}
	args.SaveOperationFuncs = saveOperationFuncs
	args.SaveStateFuncs = saveStatesFuncs

	processingDoneLog := func(*zerolog.Event, string) {}

	args.WhenNewOperationsFunc = func() {
		processingDoneLog = logging.TimeElapsed()
	}

	args.UpdateMergeDoneFunc = func(ctx context.Context) (
		func(base.State) error,
		func(uint64, string) error,
		func() error,
		error,
	) {
		processingDoneLog(cmd.Log().Info(), "processing done")

		statech := make(chan base.State)
		reasonch := make(chan [2]interface{})

		_, done, _ := NewContextDoneFunc(
			ctx,
			func(ctx context.Context) error {
				updateState := func(f func(stateKey string, stateHash util.Hash) error) error {
					for {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case st, notclosed := <-statech:
							if !notclosed {
								return nil
							}

							if err := f(st.Key(), st.Hash()); err != nil {
								return err
							}
						}
					}
				}
				updateReason := func(f func(opindex uint64, reason string) error) error {
					for {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case i, notclosed := <-reasonch:
							if !notclosed {
								return nil
							}

							if err := f(i[0].(uint64), i[1].(string)); err != nil {
								return err
							}
						}
					}
				}

				return cmd.request(
					ctx,
					"UpdateMergeDone",
					newManagerBrokerWorkerUpdateMergedFunc(
						height,
						cmd.localCI.String(),
						updateState,
						updateReason,
					),
					cmd.managerCI,
				)
			},
		)

		updateState := func(st base.State) error {
			cmd.statesPool.Set(st.Key(), st, 0)
			statech <- st

			return nil
		}
		updateReason := func(opindex uint64, reason string) error {
			reasonch <- [2]interface{}{opindex, reason}

			return nil
		}

		go func() {
			_ = done()
		}()

		return updateState, updateReason, func() error {
			close(statech)
			close(reasonch)

			return done()
		}, nil
	}
	args.UpdateStoreDoneFunc = func(ctx context.Context, chunks map[base.BlockItemType]string) error {
		if err := bdb.Write(); err != nil {
			return err
		}

		return cmd.request(
			ctx,
			"UpdateStoreDone",
			newManagerBrokerWorkerStoreDoneFunc(
				height,
				cmd.localCI.String(),
				chunks,
			),
			cmd.managerCI,
		)
	}
	args.FullOps = margs.FullOps
	args.IsPreProcessed = margs.IsPreProcessed
	args.GetStateMergeValuesFunc = func(_ uint64, op, _ util.Hash) ([]StateMergeValue, bool, error) {
		i, found := cmd.stvsPool.Get(op.String())

		return i, found, nil
	}

	cmd.Log().Debug().Interface("args", margs).Msg("new worker ready")

	switch i, err := NewProcessWorker(
		context.Background(),
		args,
		local,
		height,
	); {
	case err != nil:
		return nil, err
	default:
		pm = i
		pm.SetLogging(cmd.Logging)
	}

	return pm, nil
}

func (cmd *CommandWorker) djb2StateOwnersFunc(workers []string, share uint8) (
	func(context.Context, string) ([]string, error),
	error,
) {
	f, err := djb2StateOwnersFunc(workers, share)
	if err != nil {
		return nil, err
	}

	return func(_ context.Context, stateKey string) ([]string, error) {
		return f(stateKey)
	}, nil
}

func (cmd *CommandWorker) remoteStateOwnersFunc(height base.Height) func(context.Context, string) ([]string, error) {
	return func(ctx context.Context, stateKey string) (owners []string, _ error) {
		if err := cmd.request(ctx, "StateOwners", newManagerBrokerStateOwnersFunc(
			height,
			cmd.localCI.String(),
			stateKey,
			func(s []string) error {
				owners = s

				return nil
			}),
			cmd.managerCI,
		); err != nil {
			return nil, err
		}

		return owners, nil
	}
}
