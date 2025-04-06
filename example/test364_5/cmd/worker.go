package main

import (
	"context"
	"fmt"
	"iter"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/quicstream"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	stateCacheGetFunc = func(stateKey string) (_ base.State, found bool)
	stateCacheSetFunc = func(base.State)
	newStateCacheFunc = func(workers []string, statesShare uint64, _ base.Height) (
		get stateCacheGetFunc, set stateCacheSetFunc, lazySet stateCacheSetFunc)
)

type workerCommand struct { //betteralign:ignore
	baseCommand
	Manager    string `arg:""`
	CapOpsSize uint64 `default:"200000"`

	local           string
	workerOptions   *dop5.WorkerOptions
	manager         *net.UDPAddr
	workerProcessAt time.Time
	getStateCache   newStateCacheFunc
	factPool        dutil.LockedMap[string, dop5.OperationFact]
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

	cmd.log.Debug().Msg("cmd/prepared")

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

	cmd.getStateCache, _ = newFuncWorkersAwareStateCache(
		cmd.ctx,
		int(cmd.CapOpsSize),
		1<<9, 1<<9, // NOTE will be enough :)
	)

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
			if err := wait(); err != nil {
				panic(err)
			}
		}()

		return nil
	}))

	createFactPool := sync.OnceValue(func() (err error) {
		cmd.factPool, err = dutil.NewLockedMap[string, dop5.OperationFact](nil, cmd.CapOpsSize)

		return err
	})

	cmd.mustAddHandler("_worker_preload_ops", acceptHandlerPreloadOps(
		cmd.enc,
		func(fact dop5.OperationFact) error {
			if err := createFactPool(); err != nil {
				return err
			}

			fmt.Println(">", fact.Hash())
			_ = cmd.factPool.SetValue(fact.Hash().String(), fact)

			return nil
		},
	).StreamHandler(cmd.newAcceptSubStream))

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
	l := cmd.log.With().
		Str("work_id", workID).
		Interface("height", height).
		Logger()

	l.Debug().
		Strs("workers", workers).
		Strs("ops_workers", opsWorkers).
		Uint64("ops_share", opsShare).
		Uint64("states_share", statesShare).
		Uint64("max_ops_size", maxOpsSize).
		Msg("cmd/worker started")

	if cmd.workerAddrs.Len() < 1 {
		if err := cmd.parseWorkersAddr(workers); err != nil {
			return nil, err
		}
	}

	getState, setStates := cmd.newFuncGetState(workers, statesShare, height)

	defer dutil.ElapsedLogMsg(l.Debug, "cmd/worker ready")()

	workCanceled := cmd.newFuncReportCancelWork(workID)

	var initialInsert dop5.WorkerInitialInsertFunc
	var workerInsertSMVs dop5.WorkerProcessWorkerInsertSMVsFunc
	var workerUnmerged dop5.WorkerUnmergedFunc
	var wait dop5.WorkerWaitFunc
	var workerClose dutil.InErrorFunc[error]

	switch ii, wi, wu, w, c, err := dop5.NewFuncWorker(
		ctx,
		cmd.workerOptions,
		getState,
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

	mustAddWorkHandler := cmd.newFuncMustAddHandler(addHandler)

	mustAddWorkHandler("manager_send_ops", cmd.newHandlerInitialInsert(
		workID, workers, statesShare,
		initialInsert,
	).StreamHandler(cmd.newAcceptSubStream))

	mustAddWorkHandler("worker_distribute_smvs", acceptHandlerWorkerDistributeSMVs(
		cmd.enc,
		workerInsertSMVs,
	).StreamHandler(cmd.newAcceptSubStream))

	mustAddWorkHandler("worker_unmerged", acceptHandlerWorkerUnmerged(workerUnmerged))

	workClosedDone, workClosedWait := dutil.NewContextDone(ctx)
	mustAddWorkHandler("manager_close_work", acceptHandlerCloseWork(func() error {
		if err := workerClose(nil); err != nil {
			return workClosedDone(err)
		}

		return workClosedWait()
	}))

	mustAddWorkHandler("_manager_wait_worker_closed", acceptHandlerWaitWorkerClosed(workClosedWait))
	mustAddWorkHandler("manager_cancel_work", acceptHandlerCancelWork(func(err error) error {
		return workClosedDone(workerClose(err))
	}))

	return func() error {
			switch f, err := cmd.pprof(fmt.Sprintf("worker-work-%s", workID)); {
			case err != nil:
				return err
			default:
				defer f()
			}

			defer cmd.removeHandler([]byte(workID))
			defer closeHandler()

			return workClosedDone(wait(
				cmd.newFuncBroadcastUnmerged(workID),
				cmd.newFuncReportResult(workID),
				cmd.newFuncSaveStates(setStates),
			))
		},
		nil
}

func (cmd *workerCommand) newFuncGetState(workers []string, statesShare uint64, height base.Height) (
	dop5.GetStateFunc,
	func([]base.State),
) {
	get, set, lazySet := cmd.getStateCache(workers, statesShare, height)

	return func(stateKey string) (st base.State, _ bool, _ error) {
			if i, found := get(stateKey); found {
				if i != nil {
					return i.(base.State), true, nil
				}

				return nil, false, nil
			}

			found, err := cmd.db.getState(cmd.ctx, stateKey, &st)
			if err == nil && found {
				lazySet(st)
			}

			return st, found, err
		},
		func(sts []base.State) {
			for st := range slices.Values(sts) {
				set(st)
			}
		}
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
	initialInsert dop5.WorkerInitialInsertFunc,
) quicstream.AcceptSubStreamHandler {
	return acceptHandlerSendOps(
		func(maxItems uint64, seq iter.Seq2[uint64, [2]util.Hash]) error {
			cmd.workerProcessAt = time.Now()

			cmd.log.Debug().Uint64("max_items", maxItems).Msg("cmd/initial insert started")

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

func (cmd *workerCommand) newFuncWorkerDistributeSMVs(workID string, maxItemsPerWorker uint64) dop5.NewWorkerDistributeSMVsFunc {
	request := cmd.requestWorkWorker(workID, "worker_distribute_smvs")

	return func(ctx context.Context, worker string) (
		dop5.WorkerProcessInsertSMVsFunc,
		dutil.In2ErrorFunc[[]string, []string],
		error,
	) {
		insert, owners, handler := requestHandlerWorkerDistributeSMVs(ctx, cmd.enc,
			cmd.local, maxItemsPerWorker, defaultSubStreamItems, defaultMaxSubStreamsSize)

		requestDone, requestWait := dutil.NewContextDone(ctx)

		l := dutil.ElapsedLogMsg(func() *zerolog.Event {
			return cmd.log.Debug().
				Uint64("max_items_per_worker", maxItemsPerWorker).
				Str("worker", worker)
		}, "cmd/distribute smvs done")

		go func() {
			_, err := request(ctx, worker, nil, nil,
				handler.StreamHandler(cmd.openSubStreams))
			_ = requestDone(err)
		}()

		return insert, func(subs, affected []string) error {
			defer l()

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
	if cmd.factPool != nil {
		i, found := cmd.factPool.Value(facth.String())

		return i, found, "", nil
	}

	found, err := cmd.db.getOperationFact(ctx, facth, &fact)

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
		cmd.log.Info().
			Stringer("elapsed", time.Since(cmd.workerProcessAt)).
			Msg("cmd/process done")

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
			cmd.log.Debug().Err(err).Msg("cmd/ping manager")

			if err == nil {
				close(connected)

				return
			}

			reset()
		}
	}()

	return <-connected
}

func (cmd *workerCommand) newFuncSaveStates(setStates func([]base.State)) dop5.WorkerSaveStatesFunc {
	return func(sts []base.State) error {
		defer dutil.ElapsedLogMsg(func() *zerolog.Event {
			return cmd.log.Debug().
				Int("size", len(sts))
		}, "cmd/save states")()

		setStates(sts)

		return cmd.db.saveStates(cmd.ctx, 1<<9, 1<<9, sts)
	}
}

func newFuncWorkersAwareStateCache(ctx context.Context, cacheSize int, bufSize, outBufSize uint64) (
	newStateCacheFunc,
	func(),
) {
	get, set, lazySet, closef := dutil.NewLazyCache[string, base.State, int64](
		ctx, cacheSize, bufSize, outBufSize, func(st base.State) int64 {
			return int64(st.Height())
		})

	var last int64
	var lastWorkers [2]any /* {workers []string, statesShare uint64} */
	var cur [3]any
	var isOld func(h base.Height) bool

	getCur := func() (stateCacheGetFunc, stateCacheSetFunc, stateCacheSetFunc) {
		return cur[0].(func(string) (base.State, bool)),
			cur[1].(func(base.State)),
			cur[2].(func(base.State))
	}

	return func(workers []string, statesShare uint64, height base.Height) (stateCacheGetFunc, stateCacheSetFunc, stateCacheSetFunc) {
			switch {
			case lastWorkers[0] == nil:
			case lastWorkers[1].(uint64) != statesShare:
			case !slices.Equal(lastWorkers[0].([]string), workers):
			default:
				return getCur()
			}

			lastWorkers = [2]any{workers, statesShare}
			prev := last

			switch iheight := int64(height); {
			case last > 0 && iheight <= last:
				panic(errors.Errorf("lower height, %d < %d", iheight, last))
			default:
				last = iheight
			}

			switch {
			case height == 0 || prev == 0:
				isOld = func(base.Height) bool { return false }
			default:
				isOld = func(h base.Height) bool {
					return int64(h) < prev
				}
			}

			hget := func(stateKey string) (base.State, bool) {
				st, found := get(stateKey)
				if found && isOld(st.Height()) {
					return nil, false
				}

				return st, found
			}
			hset := func(st base.State) {
				set(st.Key(), st, 0)
			}
			hlazySet := func(st base.State) {
				lazySet(st.Key(), st, 0)
			}

			cur = [3]any{hget, hset, hlazySet}

			return hget, hset, hlazySet
		},
		closef
}
