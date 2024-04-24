package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

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
	Port      int `arg:"" name:"network port"`
	workers   util.LockedMap[base.Height, *ProcessWorker]
	workerCIs util.LockedMap[string, quicstream.ConnInfo]
}

func (cmd *CommandWorker) Run(pctx context.Context) error {
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &cmd.Logging); err != nil {
		return err
	}

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

	address := base.NewStringAddress(fmt.Sprintf("worker-%04d", cmd.Port))

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

	cmd.workers = util.NewSingleLockedMap[base.Height, *ProcessWorker]()

	switch i, err := util.NewShardedMap[string, quicstream.ConnInfo](1<<9, nil); {
	case err != nil:
		return err
	default:
		cmd.workerCIs = i

		_ = cmd.workerCIs.SetValue("local", design.Network.PublishConnInfo())
		_ = cmd.workerCIs.SetValue(design.Network.PublishConnInfo().String(), design.Network.PublishConnInfo())
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
			i, _ := cmd.workers.Value(height)
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
			i, _ := cmd.workers.Value(height)
			if i == nil {
				return nil, util.ErrNotFound.Errorf("ProcessWorker")
			}

			return func() error { return nil }, nil
		},
	)
}

func (cmd *CommandWorker) handlerReadyFunc(pctx context.Context) (quicstreamheader.Handler[WorkerReadyHeader], error) {
	var encs *encoder.Encoders
	var design launch.NodeDesign
	var db isaac.Database
	var pool *isaacdatabase.TempPool
	var client isaac.NetworkClient

	if err := util.LoadFromContextOK(pctx,
		launch.EncodersContextKey, &encs,
		launch.DesignContextKey, &design,
		launch.CenterDatabaseContextKey, &db,
		launch.PoolDatabaseContextKey, &pool,
		launch.QuicstreamClientContextKey, &client,
	); err != nil {
		return nil, err
	}

	return newWorkerHandlerReadyFunc(
		func(args processWorkerReadyArgs) error {
			_, _, err := cmd.workers.Set(args.Height, func(_ *ProcessWorker, found bool) (*ProcessWorker, error) {
				if found {
					return nil, util.ErrFound.Errorf("already worker ready, %d", args.Height)
				}

				return cmd.newWorker(
					design,
					args,
					db,
					launch.LocalFSDataDirectory(design.Storage.Base),
					encs.Default(),
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
			switch w, found := cmd.workers.Value(height); {
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
			switch w, found := cmd.workers.Value(height); {
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
			switch w, found := cmd.workers.Value(height); {
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
			switch w, found := cmd.workers.Value(height); {
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

	requestRemotes := cmd.remotesFunc(client)

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
	args.StateOwners = func(ctx context.Context, key string) (owners []string, _ error) {
		// FIXME request manager StateOwners
		if err := requestRemotes(ctx, func(ctx context.Context, client isaac.NetworkClient, ci quicstream.ConnInfo) error {
			// switch i, err := client.DOPStateOwners(ctx, ci, key); {
			// case err != nil:
			//	return err
			// default:
			//	owners = i

			return nil
			//}
		}); err != nil {
			return nil, err
		}

		return owners, nil
	}
	args.BroadcastInsertOpFunc = func(ctx context.Context, worker string, opindex uint64, fact util.Hash, values []StateMergeValue, owners map[string][]string) error {
		// FIXME request workers except local
		return nil
	}
	args.BroadcastInsertValuesDoneFunc = func(context.Context) error {
		// FIXME request workers except local
		return nil
	}
	args.BroadcastPhaseDoneFunc = func(_ context.Context, phase int, mergedOps, unmergedOps []uint64) error {
		return nil
	}

	args.BroadcastCanceled = func() error {
		// FIXME request manager
		return nil
	}
	args.SaveOperationFuncs = saveOperationFuncs
	args.SaveStateFuncs = saveStatesFuncs
	args.UpdateMergeDoneFunc = func(context.Context) (
		func(base.State) error,
		func(uint64, string) error,
		func() error,
		error,
	) {
		// FIXME request to manager UpdateMerged
		return nil, nil, nil, nil
	}
	args.UpdateStoreDoneFunc = func(_ context.Context, chunks map[base.BlockItemType]string) error {
		// FIXME	request manager WorkerStoreDone

		return nil
	}

	return NewProcessWorker(
		context.Background(),
		args,
		local,
		margs.Height,
	)
}

func (cmd *CommandWorker) remotesFunc(
	client isaac.NetworkClient,
) func(context.Context, func(_ context.Context, client isaac.NetworkClient, ci quicstream.ConnInfo) error) error {
	var local string
	if i, _ := cmd.workerCIs.Value("local"); true {
		local = i.String()
	}

	return func(ctx context.Context, f func(context.Context, isaac.NetworkClient, quicstream.ConnInfo) error) error {
		var ew util.JobWorker

		switch i, err := util.NewBaseJobWorker(ctx, int64(cmd.workerCIs.Len())); {
		case err != nil:
			return err
		default:
			ew = i
		}

		_ = cmd.workerCIs.Traverse(func(worker string, ci quicstream.ConnInfo) bool {
			if worker == local {
				return true
			}

			if err := ew.NewJob(func(ctx context.Context, _ uint64) error {
				if err := f(ctx, client, ci); err != nil {
					cmd.Log().Error().Err(err).Stringer("remote", ci).Msg("failed to request to remote")
				}

				return nil
			}); err != nil {
				return false
			}

			return true
		})

		ew.Done()

		return ew.Wait()
	}
}
