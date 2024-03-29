package testopsworkers

import (
	"cmp"
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var (
	errProcessWorkerMergeDone = util.NewIDError("ProcessWorker merge done")
	errProcessWorkerDone      = util.NewIDError("ProcessWorker done")
)

type Operation interface {
	base.Operation
	ProcessNew(context.Context) (
		[]StateMergeValue, base.OperationProcessReasonError, error,
	)
}

type OperationProcessorArgs struct {
	NetworkID                base.NetworkID
	NewJobWorker             func(context.Context) (util.JobWorker, error)
	GetOperationFunc         func(_ context.Context, operation util.Hash) (Operation, bool, error)
	IsInStateOperationFunc   func(_ context.Context, fact util.Hash) (bool, error)
	StoreOperationFunc       func(opindex uint64, op Operation) error
	UpdateReasonsFunc        func(opindex uint64, reason base.OperationProcessReasonError) error
	MergeStateMergeValueFunc func(opindex uint64, fact util.Hash, _ []StateMergeValue) error
}

func NewOperationProcessorArgs(networkID base.NetworkID) *OperationProcessorArgs {
	return &OperationProcessorArgs{
		NetworkID: networkID,
		NewJobWorker: func(ctx context.Context) (util.JobWorker, error) {
			return util.NewBaseJobWorker(ctx, 1<<6)
		},
		GetOperationFunc:         func(context.Context, util.Hash) (Operation, bool, error) { return nil, false, nil },
		IsInStateOperationFunc:   func(context.Context, util.Hash) (bool, error) { return false, nil },
		StoreOperationFunc:       func(uint64, Operation) error { return nil },
		UpdateReasonsFunc:        func(uint64, base.OperationProcessReasonError) error { return nil },
		MergeStateMergeValueFunc: func(uint64, util.Hash, []StateMergeValue) error { return nil },
	}
}

type OperationProcessor struct {
	*logging.Logging
	name      string
	args      *OperationProcessorArgs
	ctx       func() context.Context
	ctxcancel func(error)
	ew        util.JobWorker
}

func NewOperationProcessor(
	ctx context.Context,
	args *OperationProcessorArgs,
) (*OperationProcessor, error) {
	pctx, pcancel := context.WithCancelCause(ctx)

	opsr := &OperationProcessor{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "operation-processor")
		}),
		args:      args,
		ctx:       func() context.Context { return pctx },
		ctxcancel: pcancel,
	}

	switch i, err := args.NewJobWorker(pctx); {
	case err != nil:
		return nil, err
	default:
		opsr.ew = i
	}

	return opsr, nil
}

func (opsr *OperationProcessor) Cancel() {
	opsr.ctxcancel(nil)
}

func (opsr *OperationProcessor) Process() (
	process func(opindex uint64, operation, fact util.Hash) error,
	done func(),
	cancel func(error),
	_ error,
) {
	if err := opsr.ctx().Err(); err != nil {
		return nil, nil, nil, err
	}

	return func(opindex uint64, op, fact util.Hash) error {
			return opsr.ew.NewJob(func(ctx context.Context, _ uint64) error {
				var reason base.OperationProcessReasonError

				switch err := opsr.process(ctx, opindex, op, fact); {
				case errors.As(err, &reason):
					return opsr.args.UpdateReasonsFunc(opindex, reason)
				case err != nil:
					opsr.ctxcancel(err)

					return err
				default:
					return nil
				}
			})
		},
		opsr.ew.Done,
		func(err error) {
			opsr.ctxcancel(err)

			opsr.ew.Cancel()
		},
		nil
}

func (opsr *OperationProcessor) Wait() error {
	if err := context.Cause(opsr.ctx()); err != nil {
		return err
	}

	return opsr.ew.Wait()
}

func (opsr *OperationProcessor) process(ctx context.Context, opindex uint64, op, fact util.Hash) error {
	var opb Operation

	switch found, err := opsr.args.IsInStateOperationFunc(ctx, fact); {
	case err != nil:
		return err
	case found:
		return base.NewBaseOperationProcessReasonError("already in state")
	}

	switch i, err := opsr.getOperation(ctx, op); {
	case err != nil:
		return err
	default:
		opb = i

		if err := opsr.args.StoreOperationFunc(opindex, opb); err != nil {
			return err
		}
	}

	switch vs, reason, err := opb.ProcessNew(ctx); {
	case err != nil:
		return err
	case reason != nil:
		return reason
	case len(vs) < 1:
		return base.NewBaseOperationProcessReasonError("empty values")
	default:
		return opsr.args.MergeStateMergeValueFunc(opindex, fact, vs)
	}
}

func (opsr *OperationProcessor) getOperation(ctx context.Context, op util.Hash) (Operation, error) {
	switch op, found, err := opsr.args.GetOperationFunc(ctx, op); {
	case err != nil:
		return nil, err
	case !found:
		return nil, base.NewBaseOperationProcessReasonError("not found")
	default:
		if err := op.IsValid(opsr.args.NetworkID); err != nil {
			return nil, base.NewBaseOperationProcessReasonError("%+v", err)
		}

		return op, nil
	}
}

func (opsr *OperationProcessor) SetName(s string) {
	opsr.name = s

	_ = opsr.SetLogger(opsr.Log().With().Str("name", s).Logger())
}

type ProcessWorkerArgs struct {
	NetworkID                     base.NetworkID
	GetOperationFunc              func(_ context.Context, operation util.Hash) (Operation, bool, error)
	IsInStateOperationFunc        func(_ context.Context, fact util.Hash) (bool, error)
	GetStateFunc                  func(_ context.Context, key string) (base.State, bool, error)
	NewJobWorker                  func(context.Context) (util.JobWorker, error)
	IsEnoughWorkers               func(n uint64) bool
	StateOwners                   func(_ context.Context, stateKey string) (owners []string, _ error)
	BroadcastInsertOpFunc         func(_ context.Context, worker string, opindex uint64, fact util.Hash, _ []StateMergeValue) error
	BroadcastInsertValuesDoneFunc func(context.Context) error
	BroadcastPhaseDoneFunc        func(_ context.Context, phase int, mergedOps, unmergedOps []uint64) error
	StoreOperationFunc            func(opindex uint64, op Operation) error
	StoreStateFunc                func(base.State) error
	UpdateMergeDoneFunc           func(context.Context) (
		updateState func(base.State) error,
		updateReason func(opindex uint64, reason string) error,
		done func() error,
		_ error,
	)
	UpdateStoreDoneFunc func(context.Context) error
	MapSize             uint64
}

func NewProcessWorkerArgs(networkID base.NetworkID) *ProcessWorkerArgs {
	return &ProcessWorkerArgs{
		NetworkID:              networkID,
		GetOperationFunc:       func(context.Context, util.Hash) (Operation, bool, error) { return nil, false, nil },
		IsInStateOperationFunc: func(context.Context, util.Hash) (bool, error) { return false, nil },
		GetStateFunc:           func(context.Context, string) (base.State, bool, error) { return nil, false, nil },
		NewJobWorker: func(ctx context.Context) (util.JobWorker, error) {
			return util.NewBaseJobWorker(ctx, 1<<6)
		},
		IsEnoughWorkers: func(uint64) bool { return false },
		StateOwners: func(context.Context, string) ([]string, error) {
			return nil, util.ErrNotImplemented.Errorf("StateOwners")
		},
		BroadcastInsertOpFunc:         func(context.Context, string, uint64, util.Hash, []StateMergeValue) error { return nil },
		BroadcastInsertValuesDoneFunc: func(context.Context) error { return nil },
		BroadcastPhaseDoneFunc:        func(context.Context, int, []uint64, []uint64) error { return nil },
		StoreOperationFunc:            func(uint64, Operation) error { return nil },
		StoreStateFunc:                func(base.State) error { return nil },
		UpdateMergeDoneFunc: func(context.Context) (
			updateState func(base.State) error,
			updateReason func(opindex uint64, reason string) error,
			done func() error,
			_ error,
		) {
			return nil, nil, nil, util.ErrNotImplemented.Errorf("UpdateMergeDoneFunc")
		},
		UpdateStoreDoneFunc: func(context.Context) error {
			return util.ErrNotImplemented.Errorf("UpdateStoreDoneFunc")
		},
		MapSize: 1 << 9,
	}
}

type ProcessWorker struct {
	*logging.Logging
	args                  *ProcessWorkerArgs
	local                 string
	height                base.Height
	ctx                   func() context.Context
	ctxCancel             func(error)
	mergeCtx              func() context.Context
	mergeCtxCancel        func(error)
	preNewOperations      func() (*OperationProcessor, func() error, error)
	merger                func() (*StatesMergerWorker, error)
	mergeDelayedWorker    util.JobWorker
	delayedWorker         util.JobWorker
	cleanf                func()
	opOwners              util.LockedMap[ /* opindex */ uint64 /* worker */, map[string]struct{}]
	stateOwners           util.LockedMap[ /* state key */ string /* worker */, []string]
	states                util.LockedMap[string, base.State]
	mergedStateMergers    util.LockedMap[string, StateMerger]
	collectPhaseMergedOps collectPhaseMergedOpsFunc
	collectPhaseReason    collectPhaseReasonFunc
	collectPhaseResult    collectPhaseResultFunc
	ophs                  util.LockedMap[uint64, util.Hash]
}

func NewProcessWorker(
	ctx context.Context,
	args *ProcessWorkerArgs,
	local string,
	height base.Height,
) (*ProcessWorker, error) {
	pctx, cancel := context.WithCancelCause(ctx)
	mergeCtx, mergeCtxCancel := context.WithCancelCause(pctx)

	pw := &ProcessWorker{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "processor-worker").Str("local", local)
		}),
		args:           args,
		local:          local,
		height:         height,
		ctx:            func() context.Context { return pctx },
		ctxCancel:      cancel,
		mergeCtx:       func() context.Context { return mergeCtx },
		mergeCtxCancel: mergeCtxCancel,
	}

	pw.args.StateOwners = func(context.Context, string) ([]string, error) { return []string{local}, nil }

	var opsr *OperationProcessor
	var opsrOnce sync.Once

	pw.preNewOperations = func() (*OperationProcessor, func() error, error) {
		var opsrDone func() error
		var err error

		opsrOnce.Do(func() {
			opsr, opsrDone, err = pw.newOperationProcessor()
		})

		return opsr, opsrDone, err
	}

	var merger *StatesMergerWorker
	var mergerOnce sync.Once

	pw.merger = func() (*StatesMergerWorker, error) {
		var err error

		mergerOnce.Do(func() {
			merger, err = pw.newMerger()
			if err == nil {
				_ = merger.SetLogging(pw.Logging)
			}
		})

		return merger, err
	}

	switch i, err := pw.args.NewJobWorker(mergeCtx); {
	case err != nil:
		return nil, err
	default:
		pw.mergeDelayedWorker = i
	}

	switch i, err := pw.args.NewJobWorker(pctx); {
	case err != nil:
		return nil, err
	default:
		pw.delayedWorker = i
	}

	switch i, err := util.NewShardedMap[uint64, map[string]struct{}](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		pw.opOwners = i
	}

	switch i, err := util.NewShardedMap[string, []string](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		pw.stateOwners = i
	}

	switch i, err := util.NewShardedMap[string, base.State](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		pw.states = i
	}

	switch i, err := util.NewShardedMap[string, StateMerger](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		pw.mergedStateMergers = i
	}

	switch i, err := util.NewShardedMap[uint64, util.Hash](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		pw.ophs = i
	}

	var collectClose func()

	switch i, j, k, m, err := newCollectPhaseMergedFunc(args.MapSize); {
	case err != nil:
		return nil, err
	default:
		pw.collectPhaseMergedOps = i
		pw.collectPhaseReason = j
		pw.collectPhaseResult = k
		collectClose = m
	}

	pw.cleanf = func() {
		if opsr != nil {
			opsr.Cancel()
		}

		if merger != nil {
			merger.Cancel()
		}

		collectClose()

		pw.mergeDelayedWorker.Close()
		pw.delayedWorker.Close()
		pw.opOwners.Close()
		pw.stateOwners.Close()
		pw.states.Close()
		pw.mergedStateMergers.Close()
		pw.ophs.Close()
	}

	return pw, nil
}

func (pw *ProcessWorker) Cancel() {
	pw.ctxCancel(nil)

	pw.cleanf()
}

func (pw *ProcessWorker) NewOperations() (
	process func(opindex uint64, op, fact util.Hash) error,
	done func(),
	cancel func(error),
	_ error,
) {
	var opsr *OperationProcessor
	var opsrDone func() error

	switch i, j, err := pw.preNewOperations(); {
	case err != nil:
		return nil, nil, nil, err
	case i == nil:
		return nil, nil, nil, errors.Errorf("NewOperations already called")
	default:
		opsr = i
		opsrDone = j
	}

	switch p, d, c, err := opsr.Process(); {
	case err != nil:
		pw.ctxCancel(err)

		return nil, nil, nil, err
	default:
		return p, func() {
				d()

				go func() {
					if err := opsrDone(); err != nil {
						pw.ctxCancel(err)
					}

					if err := pw.wait(); err != nil {
						pw.ctxCancel(err)
					}
				}()
			},
			c, nil
	}
}

func (pw *ProcessWorker) InsertValues(worker string, opindex uint64, fact util.Hash, values []StateMergeValue) error {
	_ = pw.ophs.SetValue(opindex, fact)

	if err := pw.setStateMergeValuesOwners(opindex, values, nil); err != nil {
		pw.ctxCancel(err)

		return err
	}

	if err := pw.mergerInsertOp(opindex, values); err != nil {
		pw.ctxCancel(err)

		return err
	}

	return nil
}

func (pw *ProcessWorker) WorkerInsertValuesDone(worker string) error {
	if err := pw.workerInsertValuesDone(worker); err != nil {
		pw.ctxCancel(err)

		return err
	}

	return nil
}

func (pw *ProcessWorker) WorkerPhaseDone(worker string, phase int, mergedOps, unmergedOps []uint64) error {
	var merger *StatesMergerWorker

	switch i, err := pw.merger(); {
	case err != nil:
		pw.ctxCancel(err)

		return err
	default:
		merger = i
	}

	var mops, uops []uint64

	switch i, err := pw.filterOps(mergedOps); {
	case err != nil:
		return err
	default:
		mops = i
	}

	switch i, err := pw.filterOps(unmergedOps); {
	case err != nil:
		return err
	default:
		uops = i
	}

	if err := merger.WorkerPhaseDone(
		worker, phase, mops, uops,
		len(mergedOps) < 1 && len(unmergedOps) < 1,
	); err != nil {
		pw.ctxCancel(err)

		return err
	}

	return nil
}

func (pw *ProcessWorker) MergeWait() error {
	<-pw.mergeCtx().Done()

	switch err := context.Cause(pw.mergeCtx()); {
	case errors.Is(err, errProcessWorkerMergeDone):
		return nil
	default:
		return err
	}
}

func (pw *ProcessWorker) Wait() error {
	<-pw.ctx().Done()

	switch err := context.Cause(pw.ctx()); {
	case errors.Is(err, errProcessWorkerDone):
		return nil
	default:
		return err
	}
}

func (pw *ProcessWorker) wait() error {
	if err := context.Cause(pw.mergeCtx()); err != nil {
		return err
	}

	switch merger, err := pw.merger(); {
	case err != nil:
		return err
	default:
		if err := merger.Done(); err != nil {
			return err
		}
	}

	if err := pw.mergeResult(); err != nil {
		return err
	}

	pw.mergeCtxCancel(errProcessWorkerMergeDone)

	pw.delayedWorker.Done()

	if err := pw.delayedWorker.Wait(); err != nil {
		return err
	}

	// FIXME wait until
	// - merger done
	// - store block files

	if err := pw.args.UpdateStoreDoneFunc(pw.ctx()); err != nil {
		return err
	}

	pw.ctxCancel(errProcessWorkerDone)

	return nil
}

func (pw *ProcessWorker) workerInsertValuesDone(worker string) error {
	switch merger, err := pw.merger(); {
	case err != nil:
		return err
	case worker == pw.local:
		if err := merger.InsertDone(); err != nil {
			return err
		}
	default:
		if err := merger.WorkerInsertDone(worker); err != nil {
			return err
		}
	}

	return nil
}

func (pw *ProcessWorker) newMerger() (*StatesMergerWorker, error) {
	args := NewStatesMergerWorkerArgs()

	args.GetStateValue = pw.getStateValue
	args.NewJobWorker = pw.args.NewJobWorker
	args.IsEnoughWorkers = pw.args.IsEnoughWorkers
	args.BroadcastPhaseDoneFunc = pw.broadcastPhaseDone
	args.UpdatedValuesFunc = pw.updatedValues
	args.UpdateReasonsFunc = pw.updateReasons
	args.MapSize = pw.args.MapSize

	switch i, err := NewStatesMergerWorker(pw.ctx(), args); {
	case err != nil:
		return nil, err
	default:
		_ = i.SetLogging(pw.Logging)

		return i, nil
	}
}

func (pw *ProcessWorker) mergerInsertOp(opindex uint64, values []StateMergeValue) error {
	switch merger, err := pw.merger(); {
	case err != nil:
		return err
	default:
		return merger.InsertOp(opindex, values)
	}
}

func (pw *ProcessWorker) newOperationProcessor() (
	opsr *OperationProcessor,
	done func() error,
	_ error,
) {
	if _, err := pw.merger(); err != nil {
		return nil, nil, err
	}

	args := NewOperationProcessorArgs(pw.args.NetworkID)

	args.NewJobWorker = pw.args.NewJobWorker
	args.GetOperationFunc = pw.args.GetOperationFunc
	args.IsInStateOperationFunc = pw.args.IsInStateOperationFunc
	args.StoreOperationFunc = pw.storeOperation
	args.UpdateReasonsFunc = pw.opsrUpdateReasons
	args.MergeStateMergeValueFunc = pw.mergeStateMergeValue

	switch i, err := NewOperationProcessor(pw.ctx(), args); {
	case err != nil:
		return nil, nil, err
	default:
		_ = i.SetLogging(pw.Logging)

		opsr = i
	}

	_ = opsr.SetLogging(pw.Logging)

	return opsr, func() error {
		if err := opsr.Wait(); err != nil {
			return err
		}

		if err := pw.workerInsertValuesDone(pw.local); err != nil {
			return err
		}

		if err := pw.args.BroadcastInsertValuesDoneFunc(pw.ctx()); err != nil {
			return err
		}

		return nil
	}, nil
}

func (pw *ProcessWorker) storeOperation(opindex uint64, op Operation) error {
	return pw.delayedWorker.NewJob(func(ctx context.Context, _ uint64) error {
		return pw.args.StoreOperationFunc(opindex, op)
	})
}

func (pw *ProcessWorker) updatedValues(
	ctx context.Context,
	phase int,
	stateKey string,
	merger StateMerger,
	mergedOps, unmergedOps []uint64,
	maxUpdatedValues uint64,
) error {
	return pw.mergeDelayedWorker.NewJob(func(context.Context, uint64) error {
		if merger != nil {
			_ = pw.mergedStateMergers.SetValue(stateKey, merger)
		}

		return pw.collectPhaseMergedOps(phase, stateKey, mergedOps, unmergedOps, maxUpdatedValues)
	})
}

func (pw *ProcessWorker) opsrUpdateReasons(opindex uint64, reason base.OperationProcessReasonError) error {
	return pw.mergeDelayedWorker.NewJob(func(context.Context, uint64) error {
		return pw.collectPhaseReason(-1, opindex, reason.OperationProcessFailedReason())
	})
}

func (pw *ProcessWorker) updateReasons(ctx context.Context, phase int, opindex uint64, reason string) error {
	return pw.mergeDelayedWorker.NewJob(func(context.Context, uint64) error {
		return pw.collectPhaseReason(phase, opindex, reason)
	})
}

func (pw *ProcessWorker) mergeStateMergeValue(opindex uint64, fact util.Hash, values []StateMergeValue) error {
	var owners map[string][]StateMergeValue

	switch i, err := pw.filterStateMergeValuesOwners(opindex, values); {
	case err != nil:
		return err
	default:
		owners = i
	}

	var ew util.JobWorker

	switch i, err := pw.args.NewJobWorker(pw.ctx()); {
	case err != nil:
		return err
	default:
		defer i.Close()

		ew = i
	}

	for owner := range owners {
		owner := owner
		filtered := owners[owner]
		if len(filtered) < 1 {
			continue
		}

		if err := ew.NewJob(func(ctx context.Context, _ uint64) error {
			switch {
			case owner == pw.local:
				if !pw.ophs.Exists(opindex) {
					_ = pw.ophs.SetValue(opindex, fact)
				}

				return pw.mergerInsertOp(opindex, filtered)
			default:
				return pw.args.BroadcastInsertOpFunc(pw.ctx(), owner, opindex, fact, filtered)
			}
		}); err != nil {
			return err
		}
	}

	ew.Done()

	return ew.Wait()
}

func (pw *ProcessWorker) broadcastPhaseDone(ctx context.Context, phase int, mergedOps, unmergedOps []uint64) error {
	return pw.args.BroadcastPhaseDoneFunc(ctx, phase, mergedOps, unmergedOps)
}

func (pw *ProcessWorker) setStateMergeValuesOwners(
	opindex uint64,
	values []StateMergeValue,
	f func(worker string, _ StateMergeValue),
) error {
	if _, _, err := pw.opOwners.Set(opindex, func(m map[string]struct{}, found bool) (map[string]struct{}, error) {
		if found {
			return nil, util.ErrLockedSetIgnore
		}

		var ew util.JobWorker

		switch i, err := pw.args.NewJobWorker(pw.ctx()); {
		case err != nil:
			return nil, err
		default:
			defer i.Close()

			ew = i
		}

		lm := util.NewSingleLockedMap[string, struct{}]()
		defer lm.Close()

		if err := util.TraverseSliceV1(values, func(_ int, v StateMergeValue) (bool, error) {
			return true, ew.NewJob(func(ctx context.Context, _ uint64) error {
				switch owners, err := pw.stateMergeValueOwners(ctx, v); {
				case err != nil:
					return err
				default:
					for i := range owners {
						owner := owners[i]
						_, _, _ = lm.Set(owner, func(_ struct{}, found bool) (struct{}, error) {
							if found {
								return struct{}{}, util.ErrLockedSetIgnore
							}

							return struct{}{}, nil
						})

						if f != nil {
							f(owner, v)
						}
					}

					return nil
				}
			})

			return true, nil
		}); err != nil {
			return nil, err
		}

		ew.Done()

		if err := ew.Wait(); err != nil {
			return nil, err
		}

		return lm.Map(), nil
	}); err != nil {
		return err
	}

	return nil
}

func (pw *ProcessWorker) filterStateMergeValuesOwners(opindex uint64, values []StateMergeValue) (
	map[ /* state owner */ string][]StateMergeValue,
	error,
) {
	filtered := util.NewSingleLockedMap[string, []StateMergeValue]()
	defer filtered.Close()

	if err := pw.setStateMergeValuesOwners(
		opindex,
		values,
		func(owner string, v StateMergeValue) {
			_, _, _ = filtered.Set(owner, func(i []StateMergeValue, _ bool) ([]StateMergeValue, error) {
				i = append(i, v)

				return i, nil
			})
		},
	); err != nil {
		return nil, err
	}

	return filtered.Map(), nil
}

func (pw *ProcessWorker) stateMergeValueOwners(ctx context.Context, value StateMergeValue) (owners []string, _ error) {
	if err := pw.stateOwners.GetOrCreate(
		value.Key(),
		func(m []string, _ bool) error {
			owners = m

			return nil
		},
		func() ([]string, error) {
			return pw.args.StateOwners(ctx, value.Key())
		},
	); err != nil {
		return nil, err
	}

	return owners, nil
}

func (pw *ProcessWorker) filterOps(ops []uint64) ([]uint64, error) {
	add, done := util.CompactAppendSlice[uint64](len(ops))

	if err := util.TraverseSliceV1(ops, func(_ int, opindex uint64) (bool, error) {
		switch m, found := pw.opOwners.Value(opindex); {
		case !found:
			return true, nil
		default:
			if _, found := m[pw.local]; found {
				add(opindex)
			}

			return true, nil
		}
	}); err != nil {
		return nil, err
	}

	return done(), nil
}

func (pw *ProcessWorker) getStateValue(ctx context.Context, stateKey string) (value base.StateValue, found bool, _ error) {
	if _, _, err := pw.states.Set(stateKey, func(i base.State, stfound bool) (base.State, error) {
		if stfound {
			found = true
			value = i.Value()

			return nil, util.ErrLockedSetIgnore
		}

		switch i, stfound, err := pw.args.GetStateFunc(ctx, stateKey); {
		case err != nil:
			return nil, err
		case !stfound:
			return nil, util.ErrLockedSetIgnore
		default:
			found = true
			value = i.Value()

			return i, nil
		}
	}); err != nil {
		return nil, false, err
	}

	return value, found, nil
}

func (pw *ProcessWorker) mergeResult() error {
	pw.mergeDelayedWorker.Done()

	if err := pw.mergeDelayedWorker.Wait(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(pw.mergeCtx())
	defer cancel()

	var updateState func(base.State) error
	var updateReason func(uint64, string) error
	var done func() error

	switch i, j, k, err := pw.args.UpdateMergeDoneFunc(ctx); {
	case err != nil:
		return err
	default:
		updateState = i
		updateReason = j
		done = k
	}

	rm, ru := pw.collectPhaseResult()
	if len(rm) < 1 && len(ru) < 1 {
		return done()
	}

	var ew util.JobWorker

	switch i, err := pw.args.NewJobWorker(ctx); {
	case err != nil:
		return err
	default:
		defer i.Close()

		ew = i
	}

	for stateKey := range rm {
		stateKey := stateKey
		mergedOps := rm[stateKey]

		if err := ew.NewJob(func(context.Context, uint64) error {
			switch i, err := pw.newState(stateKey, mergedOps); {
			case err != nil:
				return err
			default:
				if err := pw.delayedWorker.NewJob(func(context.Context, uint64) error {
					return pw.args.StoreStateFunc(i)
				}); err != nil {
					return err
				}

				return updateState(i)
			}
		}); err != nil {
			return err
		}
	}

	for opindex := range ru {
		opindex := opindex
		reason := ru[opindex]

		if err := ew.NewJob(func(context.Context, uint64) error {
			return updateReason(opindex, reason)
		}); err != nil {
			return err
		}
	}

	ew.Done()

	if err := ew.Wait(); err != nil {
		return err
	}

	return done()
}

func (pw *ProcessWorker) newState(
	stateKey string,
	mergedOp []uint64,
) (base.State, error) {
	var merger StateMerger

	switch i, found := pw.mergedStateMergers.Value(stateKey); {
	case !found:
		return nil, util.ErrNotFound.Errorf("merged state value, %q", stateKey)
	default:
		merger = i
	}

	var value base.StateValue

	switch i, err := merger.Close(); {
	case err != nil:
		return nil, err
	default:
		value = i
	}

	var prev util.Hash

	if i, found := pw.states.Value(stateKey); found {
		prev = i.Hash()
	}

	addFacts, doneFacts := util.CompactAppendSlice[util.Hash](len(mergedOp))
	if err := util.TraverseSliceV1(mergedOp, func(_ int, opindex uint64) (bool, error) {
		i, found := pw.ophs.Value(opindex)
		if !found {
			return false, util.ErrNotFound.Errorf("operation fact hash; opindex=%d", opindex)
		}

		_ = addFacts(i)

		return true, nil
	}); err != nil {
		return nil, err
	}

	return base.NewBaseState(pw.height, stateKey, value, prev, doneFacts()), nil
}

type (
	collectPhaseMergedOpsFunc func(
		phase int,
		stateKey string,
		mergedOps, unmergedOps []uint64,
		maxUpdatedValues uint64,
	) error
	collectPhaseReasonFunc func(phase int, opindex uint64, reason string) error
	collectPhaseResultFunc func() (
		mergedOps map[ /* state key */ string][]uint64,
		unmergedOps map[ /* opindex */ uint64] /* reason */ string,
	)
)

func newCollectPhaseMergedFunc(mapSize uint64) (
	collectPhaseMergedOpsFunc,
	collectPhaseReasonFunc,
	collectPhaseResultFunc,
	func(),
	error,
) {
	m, err := util.NewShardedMap[string, [2]interface{}](mapSize, nil)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	um, err := util.NewShardedMap[uint64, [2]interface{}](mapSize, nil)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	collectMergedOps := newCollectPhaseMergedOpsFunc(m, um)
	collectReaon := newCollectPhaseReasonFunc(um)
	result := newCollectPhaseResultFunc(m, um)

	return collectMergedOps, collectReaon, result, func() {
		m.Close()
		um.Close()
	}, nil
}

func newCollectPhaseMergedOpsFunc(
	m util.LockedMap[string, [2]interface{}],
	um util.LockedMap[uint64, [2]interface{}],
) collectPhaseMergedOpsFunc {
	return func(
		phase int,
		stateKey string,
		mergedOps, unmergedOps []uint64,
		maxUpdatedValues uint64,
	) error {
		_, _, err := m.Set(stateKey, func(prev [2]interface{}, found bool) (v [2]interface{}, _ error) {
			_ = util.TraverseSliceV1(unmergedOps, func(_ int, opindex uint64) (bool, error) {
				_, _, _ = um.Set(opindex, func(prev [2]interface{}, found bool) ([2]interface{}, error) {
					if found {
						return [2]interface{}{}, util.ErrLockedSetIgnore
					}

					return [2]interface{}{phase, ""}, nil
				})

				return true, nil
			})

			if !found {
				return [2]interface{}{phase, mergedOps}, nil
			}

			prevPhase, prevMerged := prev[0].(int), prev[1].([]uint64)
			if phase <= prevPhase {
				return v, util.ErrLockedSetIgnore
			}

			_ = util.TraverseSliceV1(mergedOps, func(_ int, opindex uint64) (bool, error) {
				_ = um.RemoveValue(opindex)

				return true, nil
			})

			updated := util.Filter2Slices(prevMerged, unmergedOps, func(a, b uint64) bool {
				return a == b
			})

			// NOTE without sort, the generated base.State will have different hash
			add, done := util.SortUniqCompactAppendSlice(len(updated)+len(mergedOps), cmp.Less[uint64])
			add(updated...)
			add(mergedOps...)

			return [2]interface{}{phase, done()}, nil
		})

		return err
	}
}

func newCollectPhaseReasonFunc(um util.LockedMap[uint64, [2]interface{}]) collectPhaseReasonFunc {
	return func(phase int, opindex uint64, reason string) error {
		_, _, _ = um.Set(opindex, func(prev [2]interface{}, found bool) (v [2]interface{}, _ error) {
			if !found {
				return [2]interface{}{phase, reason}, nil
			}

			switch prevReason := prev[1].(string); {
			case len(reason) < 1 || len(prevReason) > 0:
				return v, util.ErrLockedSetIgnore
			case len(prevReason) > 0 && phase <= prev[0].(int):
				return v, util.ErrLockedSetIgnore
			}

			return [2]interface{}{phase, reason}, nil
		})

		return nil
	}
}

func newCollectPhaseResultFunc(
	m util.LockedMap[string, [2]interface{}],
	um util.LockedMap[uint64, [2]interface{}],
) collectPhaseResultFunc {
	return func() (map[string][]uint64, map[uint64]string) {
		defer m.Close()
		defer um.Close()

		var reasons map[uint64]string

		if um.Len() > 0 {
			reasons = map[uint64]string{}
			_ = um.Traverse(func(opindex uint64, v [2]interface{}) bool {
				reasons[opindex] = v[1].(string)

				return true
			})
		}

		if m.Len() < 1 {
			return nil, reasons
		}

		s := map[string][]uint64{}

		_ = m.Traverse(func(key string, i [2]interface{}) bool {
			if ops := i[1].([]uint64); len(ops) > 0 {
				s[key] = ops
			}

			return true
		})

		return s, reasons
	}
}
