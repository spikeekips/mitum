package main

import (
	"context"
	"iter"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/quicstream"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type workerCommand struct {
	baseCommand
	Manager string `arg:""`

	local            string
	workerOptions    *dop5.WorkerOptions
	manager          *net.UDPAddr
	processStartedAt *atomic.Value
}

func (cmd *workerCommand) Run(log *zerolog.Logger) error {
	cmd.local = cmd.Bind

	if err := cmd.baseCommand.prepare(cmd.local, log); err != nil {
		return err
	}
	defer cmd.baseCommand.closef()

	if err := cmd.prepare(); err != nil {
		return err
	}

	cmd.log.Debug().Msg("prepared")

	var wait dutil.ErrorFunc

	switch w, err := cmd.ready(); {
	case err != nil:
		return err
	default:
		wait = w
	}

	return wait()
}

func (cmd *workerCommand) prepare() error {
	switch addr, err := net.ResolveUDPAddr("udp", cmd.Manager); {
	case err != nil:
		return err
	case addr.Port < 1:
		return errors.Errorf("invalid manager addr, %q", cmd.Manager)
	default:
		cmd.manager = addr
	}

	cmd.workerOptions = dop5.NewWorkerOptions(cmd.local)
	cmd.processStartedAt = new(atomic.Value)

	return nil
}

func (cmd *workerCommand) ready() (dutil.ErrorFunc, error) {
	done, wait := dutil.NewContextDone(cmd.ctx)

	cmd.mustAddHandler("_manager_stop_worker", acceptHandlerStopWorker(func() error {
		return done(nil)
	}))

	cmd.mustAddHandler("manager_start_worker", acceptHandlerStartWorker(func(
		workID string,
		height base.Height,
		workers, opsWorkers []string,
		opsShare, statesShare, maxOpsSize uint64,
	) error {
		wait, err := cmd.startWorker(
			dutil.SetContextValueLogger(cmd.ctx, cmd.log),
			workID,
			height,
			workers, opsWorkers,
			opsShare, statesShare, maxOpsSize,
		)
		if err != nil {
			return err
		}

		go func() {
			err := wait()
			if err != nil {
				panic(err)
			}
		}()

		return nil
	}))

	if err := cmd.pingManager(); err != nil {
		return nil, err
	}

	return wait, nil
}

func (cmd *workerCommand) startWorker(
	ctx context.Context,
	workID string,
	height base.Height,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
	maxOpsSize uint64,
) (dutil.ErrorFunc, error) {
	workCanceled := cmd.newFuncReportCancelWork(workID)

	var initialInsert dop5.WorkerInitialInsertFunc
	var workerInsertSMVs dop5.WorkerProcessWorkerInsertSMVsFunc
	var workerUnmerged dop5.WorkerUnmergedFunc
	var wait dop5.WorkerWaitFunc
	var workerClose dutil.InErrorFunc[error]

	switch ii, wi, wu, w, c, err := dop5.NewFuncWorker(
		ctx,
		cmd.workerOptions,
		cmd.getState,
		cmd.newFuncCreateState(height),
		workCanceled,
		workers, opsWorkers,
		opsShare, statesShare,
		maxOpsSize,
	); {
	case err != nil:
		return nil, err
	default:
		defer c(nil)

		initialInsert, workerInsertSMVs, workerUnmerged, wait, workerClose = ii, wi, wu, w, c
	}

	addHandler, _, handler, closeHandler := quicstream.NewFuncPrefixStreamHandler()

	cmd.mustAddHandler(workID, handler)

	mustAddWorkHandler := newFuncMustAddHandler(addHandler)

	var maxItems uint64

	mustAddWorkHandler("manager_send_ops", cmd.newHandlerInitialInsert(
		workID, workers, statesShare,
		func(i uint64) {
			atomic.AddUint64(&maxItems, i)
		},
		initialInsert,
	).StreamHandler(cmd.newAcceptSubStream))

	mustAddWorkHandler("worker_distribute_smvs", acceptHandlerWorkerDistributeSMVs(
		cmd.enc,
		workerInsertSMVs,
	).StreamHandler(cmd.newAcceptSubStream))

	mustAddWorkHandler("worker_unmerged", acceptHandlerWorkerUnmerged(workerUnmerged))

	workClosedDone, workClosedWait := dutil.NewContextDone(ctx)
	mustAddWorkHandler("manager_close_work", acceptHandlerCloseWork(func() error {
		if err := workClosedDone(workerClose(nil)); err != nil {
			return err
		}

		return workClosedWait()
	}))

	mustAddWorkHandler("_manager_wait_worker_closed", acceptHandlerWaitWorkerClosed(workClosedWait))
	mustAddWorkHandler("manager_cancel_work", acceptHandlerCancelWork(func(err error) error {
		return workClosedDone(workerClose(err))
	}))

	return func() error {
			defer cmd.removeHandler([]byte(workID))
			defer closeHandler()

			return workClosedDone(wait(
				cmd.newFuncBroadcastUnmerged(workID),
				cmd.newFuncReportResult(workID),
				cmd.newFuncSaveStates(1<<9),
			))
		},
		nil
}

func (cmd *workerCommand) getState(stateKey string) (st base.State, _ bool, _ error) {
	found, err := redisGetState(
		cmd.ctx,
		cmd.enc,
		cmd.rdb,
		stateKey,
		&st,
	)

	return st, found, err
}

func (cmd *workerCommand) newFuncCreateState(height base.Height) dop5.CreateStateFunc {
	return func(stateKey string, value base.StateValue, prev util.Hash, ops []util.Hash) (
		base.State,
		error,
	) {
		return dop5.NewBaseState(
				height,
				stateKey,
				value,
				prev,
				ops,
			),
			nil
	}
}

func (cmd *workerCommand) newFuncReportCancelWork(workID string) dutil.InErrorFunc[error] {
	request := cmd.requestWork(workID, "manager_worker_error")

	return func(err error) error {
		_, rerr := request(cmd.ctx, cmd.manager, nil, nil, requestHandlerWorkerErr(cmd.local, err))

		return rerr
	}
}

func (cmd *workerCommand) newHandlerInitialInsert(
	workID string,
	workers []string,
	statesShare uint64,
	maxItemsf func(uint64),
	initialInsert dop5.WorkerInitialInsertFunc,
) quicstream.AcceptSubStreamHandler {
	return acceptHandlerSendOps(
		func(maxItems uint64, seq iter.Seq2[uint64, [2]util.Hash]) error {
			cmd.processStartedAt.Store(time.Now())

			maxItemsf(maxItems)

			maxItemsPerWorker := maxItems / uint64(len(workers))

			return initialInsert(
				newFuncGetStateOwnersMurmur3(workers, statesShare),
				cmd.newFuncWorkerDistributeSMVs(workID, maxItemsPerWorker),
				cmd.getOperationFact,
				seq,
				cmd.newFuncReportStatesOwners(workID),
			)
		},
	)
}

func (cmd *workerCommand) newFuncWorkerDistributeSMVs(workID string, maxItems uint64) dop5.NewWorkerDistributeSMVsFunc {
	request := cmd.requestWorkWorker(workID, "worker_distribute_smvs")

	return func(ctx context.Context, worker string) (
		dop5.WorkerProcessInsertSMVsFunc,
		dutil.In2ErrorFunc[[]string, []string],
		error,
	) {
		insert, owners, handler := requestHandlerWorkerDistributeSMVs(ctx, cmd.enc,
			worker, maxItems, defaultSubStreamItems, defaultMaxSubStreamsSize)

		requestDone, requestWait := dutil.NewContextDone(ctx)

		go func() {
			_, err := request(ctx, worker, nil, nil,
				handler.StreamHandler(cmd.openSubStreams))
			_ = requestDone(err)
		}()

		return insert, func(subs, affected []string) error {
			if err := owners(subs, affected); err != nil {
				return err
			}

			return requestWait()
		}, nil
	}
}

func (cmd *workerCommand) getOperationFact(ctx context.Context, opindex uint64, oph, facth util.Hash) (
	fact dop5.OperationFact,
	_ bool,
	_ string,
	_ error,
) {
	found, err := redisGetOperationFact(
		ctx,
		cmd.enc,
		cmd.rdb,
		facth,
		&fact,
	)

	return fact, found, "", err
}

func (cmd *workerCommand) newFuncReportStatesOwners(workID string) dop5.WorkerReportStatesOwnersFunc {
	request := cmd.requestWork(workID, "manager_worker_owners")

	return func(ctx context.Context, owners []string) error {
		_, err := request(ctx, cmd.manager, nil, nil, requestHandlerWorkerOwners(cmd.local, owners))

		return err
	}
}

func (cmd *workerCommand) newFuncBroadcastUnmerged(workID string) dop5.WorkerBroadcastUnmerged {
	request := cmd.requestWorkWorker(workID, "worker_unmerged")

	return func(
		ctx context.Context,
		worker string,
		unmerged []uint64,
	) error {
		_, err := request(ctx, worker, nil, nil, requestHandlerWorkerUnmerged(cmd.local, unmerged))

		return err
	}
}

func (cmd *workerCommand) newFuncReportResult(workID string) dop5.WorkerReportResultFunc {
	request := cmd.requestWork(workID, "manager_worker_result")

	return func(
		ctx context.Context,
		reasons map[uint64]string,
		statesSize uint64,
		seq dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
	) error {
		cmd.log.Info().Stringer("elapsed", time.Since(cmd.processStartedAt.Load().(time.Time))).Msg("processed")
		defer dutil.ElapsedLogEvent(cmd.log.Info(), "report result")()

		handler := requestHandlerWorkerResult(
			cmd.local,
			reasons,
			statesSize,
			seq,
			defaultSubStreamItems, defaultMaxSubStreamsSize,
		).StreamHandler(cmd.openSubStreams)

		_, err := request(ctx, cmd.manager, nil, nil, handler)

		return err
	}
}

func (cmd *workerCommand) pingManager() error {
	handler := requestHandlerWorkerPingManager(cmd.local, time.Second)

	connected := make(chan error)

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	reset := sync.OnceFunc(func() {
		ticker.Reset(time.Millisecond * 333)
	})

	go func() {
		for range ticker.C {
			_, err := cmd.request("_worker_ping_manager")(
				cmd.ctx,
				cmd.manager,
				nil,
				defaultQuicConfig(),
				handler,
			)
			cmd.log.Debug().Err(err).Msg("ping manager")

			if err == nil {
				close(connected)

				return
			}

			reset()
		}
	}()

	return <-connected
}

func (cmd *workerCommand) newFuncSaveStates(size uint64) dop5.WorkerSaveStatesFunc {
	return func(sts []base.State) error {
		return redisSaveStates(
			cmd.ctx,
			cmd.enc,
			cmd.rdb,
			1<<9, 1<<9,
			sts,
		)
	}
}
