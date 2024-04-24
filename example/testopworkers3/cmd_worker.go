package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/ps"
)

var pWorkerDesignString = `address: %s
privatekey: %s
network_id: mitum contest; Sat 26 Dec 2020 05:29:13 AM KST
storage:
    base: ./data/worker/%s
network:
  bind: 0.0.0.0:%d
  publish: localhost:%d
  tls_insecure: true
`

type CommandWorker struct { //nolint:govet //...
	baseCommand
	Port           int `arg:"" name:"network port"`
	managerCI      quicstream.ConnInfo
	pws            util.LockedMap[base.Height, *ProcessWorker]
	localCI        quicstream.ConnInfo
	workerCIs      util.LockedMap[string, quicstream.ConnInfo]
	connectionPool *quicstream.ConnectionPool
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

	cmd.Log().Debug().
		Int("port", cmd.Port).
		Msg("flags")

	if cmd.Port <= 1024 {
		return nil, nil, errors.Errorf("invalid port, %d", cmd.Port)
	}

	var priv base.Privatekey

	switch i, err := base.NewMPrivatekeyFromSeed(strings.Repeat(string(util.Int64ToBytes(int64(cmd.Port))), 33)); {
	case err != nil:
		return nil, nil, err
	default:
		priv = i
	}

	address := base.NewStringAddress(fmt.Sprintf("w%02d", cmd.Port))

	designString := fmt.Sprintf(
		pWorkerDesignString,
		address,
		priv,
		address,
		cmd.Port,
		cmd.Port,
	)

	cmd.Log().Info().Str("string", designString).Msg("design")

	pps, pctx := preparePS(
		pctx,
		"cmd-worker",
		designString,
		cmd.Operations,
	)

	switch i, err := pps.Run(pctx); {
	case err != nil:
		return pctx, nil, err
	default:
		pctx = i
	}

	if err := cmd.loadDesign(pctx); err != nil {
		return nil, nil, err
	}

	if err := util.LoadFromContextOK(pctx,
		launch.ConnectionPoolContextKey, &cmd.connectionPool,
	); err != nil {
		return nil, nil, err
	}

	cmd.Log().Debug().Interface("main_process", pps.Verbose()).Msg("processed")

	return pctx, pps, nil
}

func (cmd *CommandWorker) run(pctx context.Context) error {
	var design launch.NodeDesign

	if err := util.LoadFromContextOK(pctx,
		launch.DesignContextKey, &design,
	); err != nil {
		return err
	}

	cmd.pws = util.NewSingleLockedMap[base.Height, *ProcessWorker]()

	switch i, err := util.NewShardedMap[string, quicstream.ConnInfo](1<<9, nil); {
	case err != nil:
		return err
	default:
		cmd.localCI = design.Network.PublishConnInfo()
		cmd.workerCIs = i

		_ = cmd.workerCIs.SetValue("local", cmd.localCI)
		_ = cmd.workerCIs.SetValue(cmd.localCI.String(), cmd.localCI)
	}

	return nil
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

func (cmd *CommandWorker) handlerDoneFunc(pctx context.Context) (quicstreamheader.Handler[WorkerDoneHeader], error) {
	return newWorkerHandlerWorkerDoneFunc(
		func(height base.Height) (func() error, error) {
			i, _ := cmd.pws.Value(height)
			if i == nil {
				return nil, util.ErrNotFound.Errorf("ProcessWorker")
			}

			return func() error { return nil }, nil
		},
	)
}

func (cmd *CommandWorker) handlerReadyFunc(pctx context.Context) (quicstreamheader.Handler[WorkerReadyHeader], error) {
	var design launch.NodeDesign
	var db isaac.Database
	var pool *isaacdatabase.TempPool
	var client isaac.NetworkClient

	if err := util.LoadFromContextOK(pctx,
		launch.DesignContextKey, &design,
		launch.CenterDatabaseContextKey, &db,
		launch.PoolDatabaseContextKey, &pool,
		launch.QuicstreamClientContextKey, &client,
	); err != nil {
		return nil, err
	}

	return newWorkerHandlerReadyFunc(
		func(args processWorkerReadyArgs) error {
			if cmd.managerCI.Addr() == nil {
				switch ci, err := quicstream.NewConnInfoFromFullString(args.Manager); {
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
					pool,
					client,
				)
			})

			return err
		},
	)
}

func (cmd *CommandWorker) handlerNewOperationsFunc(pctx context.Context) (quicstreamheader.Handler[WorkerNewOperationsHeader], error) {
	return func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header WorkerNewOperationsHeader,
	) (context.Context, error) {
		handler, err := newWorkerHandlerNewOperationsFunc(func(height base.Height) (
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
		if err != nil {
			return ctx, err
		}

		return handler(ctx, addr, broker, header)
	}, nil
}

func (cmd *CommandWorker) handlerInsertOpFunc(pctx context.Context) (quicstreamheader.Handler[WorkerInsertOpHeader], error) {
	return func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header WorkerInsertOpHeader,
	) (context.Context, error) {
		handler, err := newWorkerHandlerInsertOpFunc(func(height base.Height) (
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
		if err != nil {
			return ctx, err
		}

		return handler(ctx, addr, broker, header)
	}, nil
}

func (cmd *CommandWorker) handlerInsertValuesDoneFunc(pctx context.Context) (quicstreamheader.Handler[WorkerInsertValuesDoneHeader], error) {
	return func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header WorkerInsertValuesDoneHeader,
	) (context.Context, error) {
		handler, err := newWorkerHandlerInsertValuesDoneFunc(func(height base.Height) (f func(worker string) error, _ error) {
			switch w, found := cmd.pws.Value(height); {
			case !found:
				return nil, util.ErrNotFound.Errorf("ProcessWorker not found, %d", height)
			default:
				return w.WorkerInsertValuesDone, nil
			}
		})
		if err != nil {
			return ctx, err
		}

		return handler(ctx, addr, broker, header)
	}, nil
}

func (cmd *CommandWorker) handlerPhaseDoneFunc(pctx context.Context) (quicstreamheader.Handler[WorkerPhaseDoneHeader], error) {
	return func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header WorkerPhaseDoneHeader,
	) (context.Context, error) {
		handler, err := newWorkerHandlerPhaseDoneFunc(func(height base.Height) (f func(worker string, phase int, mergedOps, unmergedOps []uint64) error, _ error) {
			switch w, found := cmd.pws.Value(height); {
			case !found:
				return nil, util.ErrNotFound.Errorf("ProcessWorker not found, %d", height)
			default:
				return w.WorkerPhaseDone, nil
			}
		})
		if err != nil {
			return ctx, err
		}

		return handler(ctx, addr, broker, header)
	}, nil
}

func (cmd *CommandWorker) newWorker(
	design launch.NodeDesign,
	margs processWorkerReadyArgs,
	db isaac.Database,
	baseroot string,
	enc encoder.Encoder,
	pool *isaacdatabase.TempPool,
	client isaac.NetworkClient,
) (*ProcessWorker, error) {
	var local string
	if i, _ := cmd.workerCIs.Value("local"); true {
		local = i.String()
	}

	if err := util.TraverseSliceV1(margs.Workers, func(_ int, worker string) (bool, error) {
		if cmd.workerCIs.Exists(worker) {
			return true, nil
		}

		ci, err := quicstream.NewConnInfoFromFullString(worker)
		if err != nil {
			return false, err
		}

		_ = cmd.workerCIs.SetValue(worker, ci)

		return true, nil
	}); err != nil {
		return nil, err
	}

	var bdb isaac.BlockWriteDatabase

	switch i, err := db.NewBlockWriteDatabase(margs.Height); {
	case err != nil:
		return nil, err
	default:
		bdb = i
	}

	fsroot := filepath.Join(baseroot, margs.Height.String())
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
	args.GetOperationFunc = func(ctx context.Context, operationhash util.Hash) (Operation, bool, error) {
		switch i, found, err := pool.Operation(ctx, operationhash); {
		case err != nil:
			return nil, false, err
		case !found:
			return nil, false, nil
		default:
			return i.(Operation), true, nil
		}
	}
	// FIXME sync states with other workers
	args.GetStateFunc = func(_ context.Context, key string) (base.State, bool, error) {
		return db.State(key)
	}
	args.NewJobWorker = func(ctx context.Context) (util.JobWorker, error) {
		return util.NewBaseJobWorker(ctx, cmd.c)
	}

	nworkers := uint64(len(margs.Workers))
	nshared := uint64(margs.NStateShare)

	args.IsEnoughWorkers = func(n uint64) bool {
		return n > nworkers-nshared
	}
	args.StateOwners = func(ctx context.Context, stateKey string) (owners []string, _ error) {
		if err := cmd.requestWorkers(ctx, newManagerBrokerStateOwnersFunc(
			margs.Height,
			cmd.localCI.String(),
			stateKey,
			func(s []string) error {
				owners = s

				return nil
			},
		)); err != nil {
			return nil, err
		}

		return owners, nil
	}
	args.BroadcastInsertOpFunc = func(ctx context.Context, worker string, opindex uint64, fact util.Hash, values []StateMergeValue, owners map[string][]string) error {
		return cmd.requestWorker(
			ctx,
			worker,
			newWorkerBrokerInsertOpFunc(
				margs.Height,
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
		return cmd.requestWorkers(
			ctx,
			newWorkerBrokerInsertValuesDoneFunc(
				margs.Height,
				cmd.localCI.String(),
			),
		)
	}
	args.BroadcastPhaseDoneFunc = func(ctx context.Context, phase int, mergedOps, unmergedOps []uint64) error {
		return cmd.requestWorkers(
			ctx,
			newWorkerBrokerPhaseDoneFunc(
				margs.Height,
				cmd.localCI.String(),
				phase,
				mergedOps,
				unmergedOps,
			),
		)
	}

	args.BroadcastCanceled = func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()

		return cmd.requestManager(
			ctx,
			newManagerBrokerWorkerCanceledFunc(
				margs.Height,
				cmd.localCI.String(),
			),
		)
	}
	args.SaveOperationFuncs = saveOperationFuncs
	args.SaveStateFuncs = saveStatesFuncs
	args.UpdateMergeDoneFunc = func(ctx context.Context) (
		func(base.State) error,
		func(uint64, string) error,
		func() error,
		error,
	) {
		statech := make(chan base.State)
		reasonch := make(chan [2]interface{})

		_, done, cancel := NewContextDoneFunc(
			ctx,
			func(ctx context.Context) error {
				updateState := func(f func(stateKey string, stateHash util.Hash) error) error {
					for {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case st := <-statech:
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
						case i := <-reasonch:
							if err := f(i[0].(uint64), i[1].(string)); err != nil {
								return err
							}
						}
					}
				}

				return cmd.requestManager(
					ctx,
					newManagerBrokerWorkerUpdateMergedFunc(
						margs.Height,
						cmd.localCI.String(),
						updateState,
						updateReason,
					),
				)
			},
		)

		defer cancel(nil)

		updateState := func(st base.State) error {
			statech <- st

			return nil
		}
		updateReason := func(opindex uint64, reason string) error {
			reasonch <- [2]interface{}{opindex, reason}

			return nil
		}

		return updateState, updateReason, func() error {
			close(statech)
			close(reasonch)

			return done()
		}, nil
	}
	args.UpdateStoreDoneFunc = func(ctx context.Context, chunks map[base.BlockItemType]string) error {
		return cmd.requestManager(
			ctx,
			newManagerBrokerWorkerStoreDoneFunc(
				margs.Height,
				cmd.localCI.String(),
				chunks,
			),
		)
	}

	return NewProcessWorker(
		context.Background(),
		args,
		local,
		margs.Height,
	)
}

func (cmd *CommandWorker) requestWorkers(
	ctx context.Context,
	brokerf quicstreamheader.BrokerFunc,
) error {
	openStream := openStreamFunc(cmd.encs, cmd.connectionPool.Dial)

	_ = cmd.workerCIs.Traverse(func(worker string, ci quicstream.ConnInfo) bool {
		switch {
		case worker == "local",
			worker == cmd.localCI.String():
			return true
		}

		if err := requestHandlerFunc(ctx, openStream, brokerf, ci); err != nil {
			cmd.Log().Error().Err(err).Str("worker", worker).Interface("conn_info", ci).Msg("request failed")
		}

		return true
	})

	return nil
}

func (cmd *CommandWorker) requestWorker(
	ctx context.Context,
	worker string,
	brokerf quicstreamheader.BrokerFunc,
) error {
	if worker == "local" {
		return errors.Errorf("local is not remote worker")
	}

	var ci quicstream.ConnInfo

	switch i, found := cmd.workerCIs.Value(worker); {
	case !found:
		return util.ErrNotFound.Errorf("unknown worker ci, %q", worker)
	case cmd.localCI.String() == i.String():
		return errors.Errorf("local is not remote worker")
	default:
		ci = i
	}

	openStream := openStreamFunc(cmd.encs, cmd.connectionPool.Dial)

	return requestHandlerFunc(ctx, openStream, brokerf, ci)
}

func (cmd *CommandWorker) requestManager(
	ctx context.Context,
	brokerf quicstreamheader.BrokerFunc,
) error {
	if cmd.managerCI.Addr() == nil {
		return errors.Errorf("manager ci not yet known")
	}

	openStream := openStreamFunc(cmd.encs, cmd.connectionPool.Dial)

	return requestHandlerFunc(ctx, openStream, brokerf, cmd.managerCI)
}
