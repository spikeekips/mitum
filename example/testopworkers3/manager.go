package testopsworkers

import (
	"cmp"
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
)

var (
	errProcessManagerWorkersMergeDone       = util.NewIDError("process manager workers merge done")
	errProcessManagerWorkersStoreDone       = util.NewIDError("process manager workers store done")
	errProcessManagerWorkersWorkersCanceled = util.NewIDError("process manager; workers canceled")
	errProcessManagerWorkersDone            = util.NewIDError("process manager done")
)

type (
	opOwnersFunc            func(opindex uint64, op, fact util.Hash) ([]string, error)
	stateOwnersFunc         func(stateKey string) ([]string, error)
	workerNewOperationsFunc func(opindex uint64, op, fact util.Hash) error
)

type processWorkerPrepareArgs struct {
	Height      base.Height
	Workers     []string
	NStateShare uint8
}

type ProcessManagerArgs struct {
	Workers             []string
	NOpShare            uint8
	NStateShare         uint8
	NewJobWorker        func(context.Context) (util.JobWorker, error)
	WorkerCancel        func(_ context.Context, worker string) error
	WorkerDone          func(_ context.Context, worker string) error
	WorkerNewOperations func(_ context.Context, worker string, _ processWorkerPrepareArgs) (
		_ workerNewOperationsFunc,
		done func() error,
		cancel func(),
		_ error,
	)
	IsEnoughWorkers     func(workers uint64) bool // NOTE include local worker
	NewOpOwnersFunc     func(workers []string, share uint8) (opOwnersFunc, error)
	NewStateOwnersFunc  func(workers []string, share uint8) (stateOwnersFunc, error)
	MapSize             uint64
	MaxStatesSize       uint64
	WorkerCancelTimeout time.Duration
}

func NewProcessManagerArgs(workers []string) *ProcessManagerArgs {
	return &ProcessManagerArgs{
		Workers:     workers,
		NOpShare:    1,
		NStateShare: 1,
		NewJobWorker: func(ctx context.Context) (util.JobWorker, error) {
			return util.NewBaseJobWorker(ctx, 1<<6)
		},
		WorkerCancel: func(context.Context, string) error {
			panic(util.ErrNotImplemented.Errorf("WorkerCancel"))
		},
		WorkerNewOperations: func(context.Context, string, processWorkerPrepareArgs) (
			workerNewOperationsFunc,
			func() error,
			func(),
			error,
		) {
			return nil, nil, nil, util.ErrNotImplemented.Errorf("WorkerNewOperations")
		},
		NewOpOwnersFunc:     serialOpOwnersFunc,
		NewStateOwnersFunc:  serialStateOwnersFunc,
		MapSize:             1 << 9,
		MaxStatesSize:       1 << 18,
		WorkerCancelTimeout: time.Second * 3,
	}
}

type ProcessManager struct {
	*logging.Logging
	args               *ProcessManagerArgs
	previousManifest   util.Hash
	previousSuffrage   util.Hash
	proposal           base.ProposalSignFact
	ctx                func() context.Context
	ctxCancel          func(error)
	mergeDoneCtx       func() context.Context
	mergeDoneCtxCancel func(error)
	storeDoneCtx       func() context.Context
	storeDoneCtxCancel func(error)
	ops                [][2]util.Hash /* {operation hash, fact hash} */
	workersMergeDone   *util.Locked[map[ /* worker */ string] /* phase */ struct{}]
	workersStoreDone   *util.Locked[map[ /* worker */ string]struct{}]
	mergeJobWorker     util.JobWorker
	storeJobWorker     util.JobWorker
	newOpOwnersFunc    opOwnersFunc
	stateOwners        util.LockedMap[ /* state key */ string /* workers */, []string]
	newStateOwnersFunc func(string) ([]string, error)
	cleanf             func()
	states             util.LockedMap[ /* state key */ string /* state hash */, util.Hash]
	reasons            util.LockedMap[ /* opindex */ uint64 /* reason */, string]
	addStateKeys       func(...string) bool
	doneStateKeys      func() []string
	addFailedWorker    func(worker string) bool
	isWorkersFailed    func() error
	isFailedWorker     func(worker string) bool
}

func NewProcessManager(
	ctx context.Context,
	args *ProcessManagerArgs,
	previousManifest, previousSuffrage util.Hash,
	proposal base.ProposalSignFact,
) (*ProcessManager, error) {
	ops := proposal.ProposalFact().Operations()

	if err := validateProcessManagerArgs(args, len(ops) < 1); err != nil {
		return nil, err
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
		previousManifest:   previousManifest,
		previousSuffrage:   previousSuffrage,
		proposal:           proposal,
		ctx:                func() context.Context { return pmctx },
		ctxCancel:          ctxCancel,
		mergeDoneCtx:       func() context.Context { return mergeDoneCtx },
		mergeDoneCtxCancel: mergeDoneCtxCancel,
		storeDoneCtx:       func() context.Context { return storeDoneCtx },
		storeDoneCtxCancel: storeDoneCtxCancel,
		ops:                ops,
	}

	pm.workersMergeDone = util.NewLocked[map[string]struct{}](map[string]struct{}{})
	pm.workersStoreDone = util.NewLocked[map[string]struct{}](map[string]struct{}{})

	pm.mergeJobWorker, _ = args.NewJobWorker(pmctx)
	pm.storeJobWorker, _ = args.NewJobWorker(pmctx)

	switch i, err := util.NewShardedMap[string, []string](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		pm.stateOwners = i
	}

	switch i, err := util.NewShardedMap[string, util.Hash](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		pm.states = i
	}

	switch i, err := util.NewShardedMap[uint64, string](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		pm.reasons = i
	}

	switch i, err := args.NewStateOwnersFunc(pm.args.Workers, args.NStateShare); {
	case err != nil:
		return nil, err
	default:
		pm.newStateOwnersFunc = newStateOwnersFunc(i)
	}

	switch i, err := args.NewOpOwnersFunc(pm.args.Workers, args.NOpShare); {
	case err != nil:
		return nil, err
	default:
		pm.newOpOwnersFunc = newOpOwnersFunc(i)
	}

	// FIXME remove MaxStatesSize; use to grow CompactAppendSlice
	pm.addStateKeys, pm.doneStateKeys = util.SortCompactAppendSlice[string](
		int(pm.args.MaxStatesSize),
		cmp.Less[string],
	)

	clearFailedWorkers := pm.failedWorkers()

	pm.cleanf = func() {
		pm.ctxCancel(nil)

		clearFailedWorkers()

		pm.mergeJobWorker.Close()
		pm.storeJobWorker.Close()
		pm.stateOwners.Close()
		pm.states.Close()
		pm.workersMergeDone.EmptyValue()
		pm.workersStoreDone.EmptyValue()
		_ = pm.doneStateKeys()
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

func (pm *ProcessManager) WorkerStoreDone(worker string) error {
	if err := pm.workerStoreDone(worker); err != nil {
		pm.cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) ManifestDone() (base.Manifest, error) {
	if err := pm.mergeDone(); err != nil {
		pm.cancel(err)

		return nil, err
	}

	return pm.newManifest()
}

func (pm *ProcessManager) StoreDone() error {
	if err := pm.storeDone(); err != nil {
		pm.cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) Done() error {
	defer pm.Cancel()

	if err := pm.mergeDone(); err != nil {
		pm.ctxCancel(err)

		return err
	}

	if err := pm.storeDone(); err != nil {
		pm.ctxCancel(err)

		return err
	}

	if err := pm.workersDone(); err != nil {
		pm.ctxCancel(err)

		return err
	}

	// FIXME
	pm.ctxCancel(errProcessManagerWorkersDone)

	return nil
}

func (pm *ProcessManager) UpdateMerged(worker string) (
	updateState func(stateKey string, stateHash util.Hash) error,
	updateReason func(opindex uint64, reason string) error,
	done func() error,
	_ error,
) {
	return pm.updateState,
		pm.updatedReason,
		func() error {
			return pm.workerMergeDone(worker)
		},
		nil
}

func (pm *ProcessManager) WorkerCanceled(worker string) {
	if pm.addFailedWorker(worker) {
		if err := pm.isWorkersFailed(); err != nil {
			pm.ctxCancel(errProcessManagerWorkersWorkersCanceled)
		}
	}
}

func (pm *ProcessManager) updateState(stateKey string, stateHash util.Hash) error {
	if err := pm.mergeJobWorker.NewJob(func(context.Context, uint64) error {
		if _, _, err := pm.states.Set(
			stateKey,
			func(util.Hash, bool) (util.Hash, error) {
				pm.addStateKeys(stateKey)

				return stateHash, nil
			},
		); err != nil {
			return err
		}

		return nil
	}); err != nil {
		pm.cancel(err)

		return err
	}

	return nil
}

func (pm *ProcessManager) updatedReason(
	opindex uint64,
	reason string,
) error {
	return pm.mergeJobWorker.NewJob(func(context.Context, uint64) error {
		_, _, err := pm.reasons.Set(opindex, func(_ string, found bool) (string, error) {
			if !found {
				return reason, nil
			}

			if len(reason) < 1 {
				return "", util.ErrLockedSetIgnore
			}

			return reason, nil
		})

		return err
	})
}

func (pm *ProcessManager) cancel(err error) {
	pm.ctxCancel(err)

	pm.Cancel()
}

func (pm *ProcessManager) process() error {
	go func() {
		<-pm.ctx().Done()

		switch err := context.Cause(pm.ctx()); {
		case errors.Is(err, errProcessManagerWorkersDone),
			errors.Is(err, errProcessManagerWorkersWorkersCanceled):
		default:
			pm.cancelWorkers()
		}
	}()

	switch i := len(pm.ops); {
	case i < 1:
		pm.mergeJobWorker.Done()
	default:
		if err := pm.newNonEmptyNewOperationFunc(); err != nil {
			pm.ctxCancel(err)

			return err
		}
	}

	if err := pm.mergeJobWorker.Wait(); err != nil {
		return err
	}

	pm.mergeDoneCtxCancel(errProcessManagerWorkersMergeDone)

	pm.Log().Debug().Msg("merge done")

	// FIXME start to save block files
	if err := pm.storeJobWorker.Wait(); err != nil {
		pm.cancel(err)

		return err
	}

	pm.storeDoneCtxCancel(errProcessManagerWorkersStoreDone)

	pm.Log().Debug().Msg("store done")

	return nil
}

func (pm *ProcessManager) mergeDone() error {
	<-pm.mergeDoneCtx().Done()

	switch err := context.Cause(pm.mergeDoneCtx()); {
	case errors.Is(err, errProcessManagerWorkersMergeDone):
	case err != nil:
		return err
	}

	return nil
}

func (pm *ProcessManager) storeDone() error {
	<-pm.storeDoneCtx().Done()

	switch err := context.Cause(pm.storeDoneCtx()); {
	case errors.Is(err, errProcessManagerWorkersStoreDone):
	case err != nil:
		pm.cancel(err)

		return err
	}

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

		pm.mergeJobWorker.Done()
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

		pm.storeJobWorker.Done()
	}

	return nil
}

func (pm *ProcessManager) newOperationFunc(
	newJob func(util.ContextWorkerCallback) error,
	errf func(worker string),
	f func(func(string) (workerNewOperationsFunc, error)) error,
) error {
	args := processWorkerPrepareArgs{
		Height:      pm.proposal.Point().Height(),
		Workers:     pm.args.Workers,
		NStateShare: pm.args.NStateShare,
	}

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
				cancel := func() {}

				switch i, j, k, err := pm.args.WorkerNewOperations(ctx, worker, args); {
				case err != nil:
					_ = gerr.SetValue(err)
					errf(worker)
				default:
					newop = i
					done = j
					cancel = k
				}

				for i := range ch {
					if err, _ := gerr.Value(); err != nil {
						cancel()

						continue
					}

					opindex := i[0].(uint64)
					op := i[1].(util.Hash)
					fact := i[2].(util.Hash)

					if err := newop(opindex, op, fact); err != nil {
						_ = gerr.SetValue(err)
						errf(worker)
					}
				}

				if err, _ := gerr.Value(); err != nil {
					return nil
				}

				if err := done(); err != nil {
					_ = gerr.SetValue(err)
					errf(worker)
				}

				return nil
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

func (pm *ProcessManager) newNonEmptyNewOperationFunc() error {
	var ew util.JobWorker

	switch i, err := pm.args.NewJobWorker(pm.ctx()); {
	case err != nil:
		return err
	default:
		defer i.Close()

		ew = i
	}

	if err := pm.newOperationFunc(
		ew.NewJob,
		func(worker string) {
			_ = pm.addFailedWorker(worker)
		},
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
							if pm.addFailedWorker(owner) {
								return true, pm.isWorkersFailed()
							}

							return true, nil
						default:
							if err := i(opindex, op, fact); err != nil {
								if pm.addFailedWorker(owner) {
									return true, pm.isWorkersFailed()
								}

								return true, nil
							}
						}

						return true, nil
					})
				}
			})
		},
	); err != nil {
		return err
	}

	ew.Done()

	if err := ew.Wait(); err != nil {
		return err
	}

	return pm.isWorkersFailed()
}

func (pm *ProcessManager) newManifest() (base.Manifest, error) {
	var statesTree, opsTree util.Hash

	switch i, err := pm.newStatesTree(); {
	case err != nil:
		return nil, err
	default:
		statesTree = i
	}

	switch i, err := pm.newOpsTree(); {
	case err != nil:
		return nil, err
	default:
		opsTree = i
	}

	var suffrage, prev util.Hash

	if pm.proposal.Point().Height() > base.GenesisHeight {
		suffrage = pm.previousSuffrage
		prev = pm.previousManifest
	}

	manifest := isaac.NewManifest(
		pm.proposal.Point().Height(),
		prev,
		pm.proposal.Fact().Hash(),
		opsTree,
		statesTree,
		suffrage,
		pm.proposal.ProposalFact().ProposedAt(),
	)

	pm.Log().Debug().
		Interface("manifest", manifest).
		Msg("manifest created")

	return manifest, nil
}

func (pm *ProcessManager) newStatesTree() (util.Hash, error) {
	keys := pm.doneStateKeys()
	if len(keys) < 1 {
		return nil, nil
	}

	var tg *fixedtree.Writer

	switch i, err := fixedtree.NewWriter(base.StateFixedtreeHint, uint64(len(keys))); {
	case err != nil:
		return nil, err
	default:
		tg = i
	}

	// FIXME suffrage proof

	if err := util.TraverseSliceV1(keys, func(i int, stateKey string) (bool, error) {
		switch h, found := pm.states.Value(stateKey); {
		case !found:
			return false, util.ErrNotFound.Errorf("state hash, %q", stateKey)
		default:
			return true, tg.Add(uint64(i), fixedtree.NewBaseNode(h.String()))
		}
	}); err != nil {
		return nil, err
	}

	switch tr, err := tg.Tree(); {
	case err != nil:
		return nil, err
	case tr.Len() < 1:
		return nil, nil
	default:
		// FIXME store tree

		return tr.Root(), nil
	}
}

func (pm *ProcessManager) newOpsTree() (util.Hash, error) {
	if len(pm.ops) < 1 {
		return nil, nil
	}

	var tg *fixedtree.Writer

	switch i, err := fixedtree.NewWriter(base.StateFixedtreeHint, uint64(len(pm.ops))); {
	case err != nil:
		return nil, err
	default:
		tg = i
	}

	if err := util.TraverseSliceV1(pm.ops, func(i int, v [2]util.Hash) (bool, error) {
		opindex := uint64(i)
		_, fact := v[0], v[1]

		var node base.OperationFixedtreeNode

		switch reason, found := pm.reasons.Value(opindex); {
		case found:
			node = base.NewNotInStateOperationFixedtreeNode(fact, reason)
		default:
			node = base.NewInStateOperationFixedtreeNode(fact, "")
		}

		return true, tg.Add(opindex, node)
	}); err != nil {
		return nil, err
	}

	switch tr, err := tg.Tree(); {
	case err != nil:
		return nil, err
	case tr.Len() < 1:
		return nil, nil
	default:
		// FIXME store tree

		return tr.Root(), nil
	}
}

func (pm *ProcessManager) cancelWorkers() {
	var ew util.JobWorker

	switch i, err := pm.args.NewJobWorker(context.Background()); {
	case err != nil:
		return
	default:
		defer i.Close()

		ew = i
	}

	_ = util.TraverseSliceV1(pm.args.Workers, func(_ int, worker string) (bool, error) {
		_ = ew.NewJob(func(ctx context.Context, _ uint64) error {
			wctx, cancel := context.WithTimeout(ctx, pm.args.WorkerCancelTimeout)
			defer cancel()

			if err := pm.args.WorkerCancel(wctx, worker); err != nil {
				pm.Log().Error().Err(err).Str("worker", worker).Msg("failed to cancel worker")
			}

			return nil
		})

		return true, nil
	})

	ew.Done()

	if err := ew.Wait(); err != nil {
		pm.Log().Error().Err(err).Msg("failed to cancel workers")
	}
}

func (pm *ProcessManager) failedWorkers() func() {
	failedWorkers := util.NewLocked(map[string]struct{}{})

	pm.addFailedWorker = func(worker string) (added bool) {
		_, _ = failedWorkers.Set(func(m map[string]struct{}, _ bool) (map[string]struct{}, error) {
			if m == nil { // NOTE ProcessManager cleaned
				return nil, util.ErrLockedSetIgnore
			}

			if _, found := m[worker]; found {
				return nil, util.ErrLockedSetIgnore
			}

			m[worker] = struct{}{}
			added = true

			return m, nil
		})

		return added
	}

	share := uint64(pm.args.NOpShare)

	if i := uint64(pm.args.NStateShare); i < share {
		share = i
	}

	pm.isWorkersFailed = func() error {
		return failedWorkers.Get(func(m map[string]struct{}, _ bool) error {
			if i := uint64(len(m)); i >= share {
				return errors.Errorf("over share workers failed, %d", i)
			}

			return nil
		})
	}

	pm.isFailedWorker = func(worker string) (found bool) {
		_ = failedWorkers.Get(func(m map[string]struct{}, _ bool) error {
			_, found = m[worker]

			return nil
		})

		return found
	}

	return func() {
		_ = failedWorkers.EmptyValue()
	}
}

func (pm *ProcessManager) workersDone() error {
	var ew util.JobWorker

	switch i, err := pm.args.NewJobWorker(pm.ctx()); {
	case err != nil:
		return err
	default:
		defer i.Close()

		ew = i
	}

	if err := util.TraverseSliceV1(pm.args.Workers, func(_ int, worker string) (bool, error) {
		if pm.isFailedWorker(worker) {
			return true, nil
		}

		return true, ew.NewJob(func(ctx context.Context, _ uint64) error {
			if err := pm.args.WorkerDone(ctx, worker); err != nil {
				if pm.addFailedWorker(worker) {
					return pm.isWorkersFailed()
				}
			}

			return nil
		})
	}); err != nil {
		return err
	}

	ew.Done()

	if err := ew.Wait(); err != nil {
		return err
	}

	return nil
}

func validateProcessManagerArgs(
	args *ProcessManagerArgs,
	emptyOps bool,
) error {
	if len(args.Workers) < 1 {
		return errors.Errorf("empty workers")
	}

	switch {
	case args.NOpShare < 1:
		return errors.Errorf("zero operation share")
	case int(args.NOpShare) > len(args.Workers):
		args.NOpShare = uint8(len(args.Workers))
	}

	switch {
	case args.NStateShare < 1:
		return errors.Errorf("zero state share")
	case int(args.NStateShare) > len(args.Workers):
		args.NStateShare = uint8(len(args.Workers))
	}

	if emptyOps {
		args.Workers = args.Workers[:args.NStateShare]
	}

	if args.IsEnoughWorkers == nil {
		args.IsEnoughWorkers = newIsEnoughWorkers(uint64(len(args.Workers)), uint64(args.NStateShare))
	}

	return nil
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

func newIsEnoughWorkers(workers, nshare uint64) func(uint64) bool {
	i := workers - nshare

	return func(n uint64) bool {
		return n > i
	}
}
