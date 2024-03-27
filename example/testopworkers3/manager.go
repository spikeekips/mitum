package testopsworkers

import (
	"context"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var (
	errProcessManagerWorkersMergeDone = util.NewIDError("process manager workers merge done")
	errProcessManagerWorkersStoreDone = util.NewIDError("process manager workers store done")
)

type (
	opOwnersFunc            func(opindex uint64, op, fact util.Hash) ([]string, error)
	stateOwnersFunc         func(stateKey string) ([]string, error)
	workerNewOperationsFunc func(opindex uint64, op, fact util.Hash) error
)

type ProcessManagerArgs struct {
	NOpShare            uint8
	NStateShare         uint8
	NewJobWorker        func(context.Context) (util.JobWorker, error)
	WorkerNewOperations func(_ context.Context, worker string) (
		_ workerNewOperationsFunc,
		done func() error,
		cancel func(),
		_ error,
	)
	IsEnoughWorkers    func(workers uint64) bool // NOTE include local worker
	NewOpOwnersFunc    func(workers []string, share uint8) (opOwnersFunc, error)
	NewStateOwnersFunc func(workers []string, share uint8) (stateOwnersFunc, error)
	MapSize            uint64
}

func NewProcessManagerArgs() *ProcessManagerArgs {
	return &ProcessManagerArgs{
		NOpShare:    1,
		NStateShare: 1,
		NewJobWorker: func(ctx context.Context) (util.JobWorker, error) {
			return util.NewBaseJobWorker(ctx, 1<<6)
		},
		WorkerNewOperations: func(context.Context, string) (
			workerNewOperationsFunc,
			func() error,
			func(),
			error,
		) {
			return nil, nil, nil, util.ErrNotImplemented.Errorf("WorkerNewOperations")
		},
		NewOpOwnersFunc:    serialOpOwnersFunc,
		NewStateOwnersFunc: serialStateOwnersFunc,
		MapSize:            1 << 9,
	}
}

type ProcessManager struct {
	*logging.Logging
	args                  *ProcessManagerArgs
	workers               []string
	ctx                   func() context.Context
	ctxCancel             func(error)
	mergeDoneCtx          func() context.Context
	mergeDoneCtxCancel    func(error)
	storeDoneCtx          func() context.Context
	storeDoneCtxCancel    func(error)
	ops                   [][2]util.Hash /* {operation hash, fact hash} */
	workersMergeDone      *util.Locked[map[ /* worker */ string] /* phase */ struct{}]
	workersStoreDone      *util.Locked[map[ /* worker */ string]struct{}]
	mergeJobWorker        util.JobWorker
	storeJobWorker        util.JobWorker
	statesValues          util.LockedMap[ /* state key */ string, [2]interface{} /* {phase, base.StateValue} */]
	collectPhaseMergedOps collectPhaseMergedOpsFunc
	collectPhaseReason    collectPhaseReasonFunc
	collectPhaseResult    collectPhaseResultFunc
	newOpOwnersFunc       opOwnersFunc
	stateOwners           util.LockedMap[ /* state key */ string /* workers */, []string]
	newStateOwnersFunc    func(string) ([]string, error)
	cleanf                func()
}

func NewProcessManager(
	ctx context.Context,
	args *ProcessManagerArgs,
	workers []string,
	ops [][2]util.Hash,
) (*ProcessManager, error) {
	if len(workers) < 1 {
		return nil, errors.Errorf("empty workers")
	}

	switch {
	case args.NOpShare < 1:
		return nil, errors.Errorf("zero operation share")
	case int(args.NOpShare) > len(workers):
		args.NOpShare = uint8(len(workers))
	}

	switch {
	case args.NStateShare < 1:
		return nil, errors.Errorf("zero state share")
	case int(args.NStateShare) > len(workers):
		args.NStateShare = uint8(len(workers))
	}

	switch i, err := args.NewJobWorker(ctx); {
	case err != nil:
		return nil, err
	default:
		i.Close()
	}

	pmctx, ctxCancel := context.WithCancelCause(ctx)
	mergeDoneCtx, mergeDoneCtxCancel := context.WithCancelCause(pmctx)
	storeDoneCtx, storeDoneCtxCancel := context.WithCancelCause(pmctx)

	pm := &ProcessManager{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "process-manager")
		}),
		args:               args,
		workers:            workers,
		ctx:                func() context.Context { return pmctx },
		ctxCancel:          ctxCancel,
		mergeDoneCtx:       func() context.Context { return mergeDoneCtx },
		mergeDoneCtxCancel: mergeDoneCtxCancel,
		storeDoneCtx:       func() context.Context { return storeDoneCtx },
		storeDoneCtxCancel: storeDoneCtxCancel,
		ops:                ops,
	}

	if len(ops) < 1 {
		pm.workers = pm.workers[:args.NStateShare]
	}

	if pm.args.IsEnoughWorkers == nil {
		i := uint64(len(pm.workers)) - uint64(pm.args.NStateShare)

		pm.args.IsEnoughWorkers = func(n uint64) bool {
			return n > i
		}
	}

	pm.workersMergeDone = util.NewLocked[map[string]struct{}](map[string]struct{}{})
	pm.workersStoreDone = util.NewLocked[map[string]struct{}](map[string]struct{}{})

	pm.mergeJobWorker, _ = args.NewJobWorker(pmctx)
	pm.storeJobWorker, _ = args.NewJobWorker(pmctx)

	switch i, err := util.NewShardedMap[string, [2]interface{}](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		pm.statesValues = i
	}

	var collectClose func()

	switch i, j, k, m, err := newCollectPhaseMergedFunc(args.MapSize); {
	case err != nil:
		return nil, err
	default:
		pm.collectPhaseMergedOps = i
		pm.collectPhaseReason = j
		pm.collectPhaseResult = k
		collectClose = m
	}

	switch i, err := util.NewShardedMap[string, []string](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		pm.stateOwners = i
	}

	switch i, err := args.NewStateOwnersFunc(pm.workers, args.NStateShare); {
	case err != nil:
		return nil, err
	default:
		pm.newStateOwnersFunc = newStateOwnersFunc(i)
	}

	switch i, err := args.NewOpOwnersFunc(pm.workers, args.NOpShare); {
	case err != nil:
		return nil, err
	default:
		pm.newOpOwnersFunc = newOpOwnersFunc(i)
	}

	pm.cleanf = func() {
		pm.ctxCancel(nil)

		pm.mergeJobWorker.Close()
		pm.storeJobWorker.Close()

		pm.stateOwners.Close()
		pm.workersMergeDone.EmptyValue()
		pm.workersStoreDone.EmptyValue()

		collectClose()
	}

	return pm, nil
}

func (pm *ProcessManager) Cancel() {
	pm.ctxCancel(nil)
	pm.cleanf()
}

func (pm *ProcessManager) StateOwners(stateKey string) (owners []string, _ error) {
	if err := pm.stateOwners.GetOrCreate(
		stateKey,
		func(i []string, _ bool) error {
			owners = i

			return nil
		},
		func() ([]string, error) {
			return pm.newStateOwnersFunc(stateKey)
		},
	); err != nil {
		return nil, err
	}

	return owners, nil
}

func (pm *ProcessManager) Process() error {
	if err := pm.process(); err != nil {
		pm.cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) WorkerMergeDone(worker string) error {
	if err := pm.workerMergeDone(worker); err != nil {
		pm.cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) WorkerStoreDone(worker string) error {
	if err := pm.workerStoreDone(worker); err != nil {
		pm.cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) ManifestDone() (base.Manifest, error) {
	switch i, err := pm.manifestDone(); {
	case err != nil:
		pm.cancel(err)

		return nil, err
	default:
		return i, nil
	}
}

func (pm *ProcessManager) StoreDone() error {
	if err := pm.storeDone(); err != nil {
		pm.cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) UpdatedStateValue(
	_ context.Context,
	worker string,
	phase int,
	stateKey string,
	value base.StateValue,
	mergedOps, unmergedOps []uint64,
	maxUpdatedValues uint64,
) error {
	if err := pm.mergeJobWorker.NewJob(func(context.Context, uint64) error {
		if _, _, _, err := pm.statesValues.SetOrRemove(
			stateKey,
			func(prev [2]interface{}, found bool) (v [2]interface{}, _ bool, _ error) {
				if value == nil {
					return v, true, nil // nil value, remove
				}

				if !found {
					return [2]interface{}{phase, value}, false, nil
				}

				if phase <= prev[0].(int) {
					return v, false, util.ErrLockedSetIgnore
				}

				return [2]interface{}{phase, value}, false, nil
			},
		); err != nil {
			return err
		}

		return pm.collectPhaseMergedOps(phase, stateKey, mergedOps, unmergedOps, maxUpdatedValues)
	}); err != nil {
		pm.cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) UpdatedProcessReason(
	_ context.Context,
	worker string,
	phase int,
	opindex uint64,
	reason string,
) error {
	return pm.mergeJobWorker.NewJob(func(context.Context, uint64) error {
		return pm.collectPhaseReason(phase, opindex, reason)
	})
}

func (pm *ProcessManager) cancel(err error) {
	pm.ctxCancel(err)

	pm.Cancel()
}

func (pm *ProcessManager) process() error {
	var ew util.JobWorker

	switch i, err := pm.args.NewJobWorker(pm.ctx()); {
	case err != nil:
		return err
	default:
		defer i.Close()

		ew = i
	}

	switch {
	case len(pm.ops) < 1:
		if err := pm.newEmptyNewOperationFunc(ew.NewJob); err != nil {
			return err
		}
	default:
		if err := pm.newNonEmptyNewOperationFunc(ew.NewJob); err != nil {
			return err
		}
	}

	ew.Done()

	if err := ew.Wait(); err != nil {
		return err
	}

	return nil
}

func (pm *ProcessManager) manifestDone() (base.Manifest, error) {
	<-pm.mergeDoneCtx().Done()

	switch err := context.Cause(pm.mergeDoneCtx()); {
	case errors.Is(err, errProcessManagerWorkersMergeDone):
	case err != nil:
		return nil, err
	}

	pm.mergeJobWorker.Done()

	if err := pm.mergeJobWorker.Wait(); err != nil {
		return nil, err
	}

	pm.Log().Debug().Msg("merge done")

	mergedOps, unmergedOps := pm.collectPhaseResult()

	pm.Log().Debug().
		Interface("merged_ops", mergedOps).
		Interface("unmerged_ops", unmergedOps).
		Msg("merge result")

	// FIXME start to save block files

	return nil, nil
}

func (pm *ProcessManager) storeDone() error {
	defer pm.Cancel()

	<-pm.storeDoneCtx().Done()

	switch err := context.Cause(pm.storeDoneCtx()); {
	case errors.Is(err, errProcessManagerWorkersStoreDone):
	case err != nil:
		pm.cancel(err)

		return err
	}

	pm.storeJobWorker.Done()

	if err := pm.storeJobWorker.Wait(); err != nil {
		pm.cancel(err)

		return err
	}

	pm.Log().Debug().Msg("store done")

	return nil
}

func (pm *ProcessManager) workerMergeDone(worker string) error {
	l := pm.Log().With().Str("worker", worker).Logger()

	var count uint64

	if _, err := pm.workersMergeDone.Set(func(m map[string]struct{}, _ bool) (map[string]struct{}, error) {
		if m == nil {
			return nil, util.ErrLockedSetIgnore // NOTE already done
		}

		if _, workerFound := m[worker]; workerFound {
			return nil, errors.Errorf("worker, %q already merge done", worker)
		}

		m[worker] = struct{}{}
		count = uint64(len(m))

		return m, nil
	}); err != nil {
		return err
	}

	if count < 1 {
		l.Debug().Msg("already merge done")

		return nil
	}

	if pm.args.IsEnoughWorkers(count) {
		l.Debug().Msg("enough workers merge done")

		pm.mergeDoneCtxCancel(errProcessManagerWorkersMergeDone)

		// FIXME make states tree
		// FIXME create manifest
	}

	return nil
}

func (pm *ProcessManager) workerStoreDone(worker string) error {
	l := pm.Log().With().Str("worker", worker).Logger()

	var count uint64

	if _, err := pm.workersStoreDone.Set(func(m map[string]struct{}, _ bool) (map[string]struct{}, error) {
		if m == nil {
			return nil, util.ErrLockedSetIgnore // NOTE already done
		}

		if _, workerFound := m[worker]; workerFound {
			return nil, errors.Errorf("worker, %q already store done", worker)
		}

		m[worker] = struct{}{}
		count = uint64(len(m))

		return m, nil
	}); err != nil {
		return err
	}

	if count < 1 {
		l.Debug().Msg("already store done")

		return nil
	}

	if pm.args.IsEnoughWorkers(count) {
		l.Debug().Msg("enough workers store done")

		pm.mergeDoneCtxCancel(errProcessManagerWorkersStoreDone)
	}

	return nil
}

func (pm *ProcessManager) newOperationFunc(
	newJob func(util.ContextWorkerCallback) error,
	f func(func(string) (workerNewOperationsFunc, error)) error,
) error {
	chs := map[string]chan [3]interface{}{}

	defer func() {
		for i := range chs {
			close(chs[i])
		}

		clear(chs)
	}()

	newWorker := func(worker string) (workerNewOperationsFunc, error) {
		gerr := util.EmptyLocked[error]()
		var ch chan [3]interface{}

		switch i, found := chs[worker]; {
		case !found:
			ch = make(chan [3]interface{}, pm.args.MapSize)
			chs[worker] = ch

			if err := newJob(func(ctx context.Context, _ uint64) error {
				var newop workerNewOperationsFunc
				var done func() error
				var cancel func()

				switch i, j, k, err := pm.args.WorkerNewOperations(ctx, worker); {
				case err != nil:
					_ = gerr.SetValue(err)
				default:
					newop = i
					done = j
					cancel = k
				}

				for i := range ch {
					if e, _ := gerr.Value(); e != nil {
						cancel()

						continue
					}

					opindex := i[0].(uint64)
					op := i[1].(util.Hash)
					fact := i[2].(util.Hash)

					if err := newop(opindex, op, fact); err != nil {
						_ = gerr.SetValue(err)
					}
				}

				if err, _ := gerr.Value(); err != nil {
					return err
				}

				return done()
			}); err != nil {
				return nil, err
			}
		default:
			ch = i
		}

		return func(opindex uint64, op, fact util.Hash) error {
			if err, _ := gerr.Value(); err != nil {
				return err
			}

			ch <- [3]interface{}{opindex, op, fact}

			return nil
		}, nil
	}

	return f(newWorker)
}

func (pm *ProcessManager) newEmptyNewOperationFunc(
	newJob func(util.ContextWorkerCallback) error,
) error {
	return pm.newOperationFunc(
		newJob,
		func(f func(string) (workerNewOperationsFunc, error)) error {
			return util.TraverseSliceV1(pm.workers, func(_ int, worker string) (bool, error) {
				_, err := f(worker)

				return true, err
			})
		},
	)
}

func (pm *ProcessManager) newNonEmptyNewOperationFunc(
	newJob func(util.ContextWorkerCallback) error,
) error {
	return pm.newOperationFunc(
		newJob,
		func(f func(string) (workerNewOperationsFunc, error)) error {
			return util.TraverseSliceV1(pm.ops, func(i int, v [2]util.Hash) (bool, error) {
				opindex, op, fact := uint64(i), v[0], v[1]

				switch owners, err := pm.newOpOwnersFunc(opindex, op, fact); {
				case err != nil:
					return false, err
				default:
					return true, util.TraverseSliceV1(owners, func(_ int, owner string) (bool, error) {
						switch i, err := f(owner); {
						case err != nil:
							return false, err
						default:
							return true, i(opindex, op, fact)
						}
					})
				}
			})
		},
	)
}

func serialOpOwnersFunc(workers []string, share uint8) (opOwnersFunc, error) {
	w := uint64(len(workers))
	if w < 1 {
		return nil, errors.Errorf("empty workers")
	}

	n := uint64(share)
	if n < 1 {
		return nil, errors.Errorf("zero share")
	}

	if n > w {
		n = w
	}

	return func(opindex uint64, _, _ util.Hash) ([]string, error) {
		add, done := util.CompactAppendSlice[string](int(n))

		for i := uint64(0); i < n; i++ {
			add(workers[((opindex*n)+i)%w])
		}

		return done(), nil
	}, nil
}

func serialStateOwnersFunc(workers []string, share uint8) (stateOwnersFunc, error) {
	w := uint64(len(workers))
	if w < 1 {
		return nil, errors.Errorf("empty workers")
	}

	n := uint64(share)
	if n < 1 {
		return nil, errors.Errorf("zero share")
	}

	if n > w {
		n = w
	}

	last := util.NewLocked(uint64(0))

	return func(stateKey string) (owners []string, _ error) {
		_, _ = last.Set(func(l uint64, _ bool) (uint64, error) {
			add, done := util.CompactAppendSlice[string](int(n))

			var i uint64

			for i = 0; i < n; i++ {
				add(workers[(l+i)%w])
			}

			owners = done()

			return l + i, nil
		})

		return owners, nil
	}, nil
}

func newOpOwnersFunc(f opOwnersFunc) opOwnersFunc {
	return func(opindex uint64, op, fact util.Hash) ([]string, error) {
		switch owners, err := f(opindex, op, fact); {
		case err != nil:
			return nil, err
		case len(owners) < 1:
			return nil, util.ErrNotFound.Errorf("op owners")
		default:
			return owners, nil
		}
	}
}

func newStateOwnersFunc(f stateOwnersFunc) stateOwnersFunc {
	return func(stateKey string) ([]string, error) {
		switch owners, err := f(stateKey); {
		case err != nil:
			return nil, err
		case len(owners) < 1:
			return nil, util.ErrNotFound.Errorf("state owners")
		default:
			return owners, nil
		}
	}
}
