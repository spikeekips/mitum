package testopsworkers

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var errProcessWorkerMergeDone = util.NewIDError("ProcessWorker merge done")

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
	MergeStateMergeValueFunc func(opindex uint64, _ []StateMergeValue) error
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
		MergeStateMergeValueFunc: func(uint64, []StateMergeValue) error { return nil },
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
		return opsr.args.MergeStateMergeValueFunc(opindex, vs)
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
	GetStateValue                 func(_ context.Context, key string) (base.StateValue, bool, error)
	NewJobWorker                  func(context.Context) (util.JobWorker, error)
	IsEnoughWorkers               func(n uint64) bool
	StateOwners                   func(_ context.Context, stateKey string) (owners []string, _ error)
	BroadcastInsertOpFunc         func(_ context.Context, worker string, opindex uint64, _ []StateMergeValue) error
	BroadcastInsertValuesDoneFunc func(context.Context) error
	BroadcastPhaseDoneFunc        func(_ context.Context, phase int, mergedOps, unmergedOps []uint64) error
	StoreOperationFunc            func(opindex uint64, op Operation) error
	UpdatedValuesFunc             statesMergerDoneUpdatedValuesFunc
	UpdateReasonsFunc             statesMergerDoneUpdatedReasonsFunc
	MapSize                       uint64
}

func NewProcessWorkerArgs(networkID base.NetworkID) *ProcessWorkerArgs {
	return &ProcessWorkerArgs{
		NetworkID:              networkID,
		GetOperationFunc:       func(context.Context, util.Hash) (Operation, bool, error) { return nil, false, nil },
		IsInStateOperationFunc: func(context.Context, util.Hash) (bool, error) { return false, nil },
		GetStateValue:          func(context.Context, string) (base.StateValue, bool, error) { return nil, false, nil },
		NewJobWorker: func(ctx context.Context) (util.JobWorker, error) {
			return util.NewBaseJobWorker(ctx, 1<<6)
		},
		IsEnoughWorkers: func(uint64) bool { return false },
		StateOwners: func(context.Context, string) ([]string, error) {
			return nil, util.ErrNotImplemented.Errorf("StateOwners")
		},
		BroadcastInsertOpFunc:         func(context.Context, string, uint64, []StateMergeValue) error { return nil },
		BroadcastInsertValuesDoneFunc: func(context.Context) error { return nil },
		BroadcastPhaseDoneFunc:        func(context.Context, int, []uint64, []uint64) error { return nil },
		StoreOperationFunc:            func(uint64, Operation) error { return nil },
		UpdatedValuesFunc:             func(context.Context, int, string, StateMerger, []uint64, []uint64, uint64) error { return nil },
		UpdateReasonsFunc:             func(context.Context, int, uint64, string) error { return nil },
		MapSize:                       1 << 9,
	}
}

type ProcessWorker struct {
	*logging.Logging
	local            string
	args             *ProcessWorkerArgs
	ctx              func() context.Context
	ctxCancel        func(error)
	mergeCtx         func() context.Context
	mergeCtxCancel   func(error)
	preNewOperations func() (*OperationProcessor, func() error, error)
	merger           func() (*StatesMergerWorker, error)
	delayedWorker    util.JobWorker
	cleanf           func()
	opOwners         util.LockedMap[ /* opindex */ uint64 /* worker */, map[string]struct{}]
	stateOwners      util.LockedMap[ /* state key */ string /* worker */, []string]
}

func NewProcessWorker(ctx context.Context, args *ProcessWorkerArgs, local string) (*ProcessWorker, error) {
	pctx, cancel := context.WithCancelCause(ctx)
	mergeCtx, mergeCtxCancel := context.WithCancelCause(pctx)

	pw := &ProcessWorker{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "processor-worker").Str("local", local)
		}),
		args:           args,
		local:          local,
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

	pw.cleanf = func() {
		if opsr != nil {
			opsr.Cancel()
		}

		if merger != nil {
			merger.Cancel()
		}

		pw.delayedWorker.Close()
		pw.opOwners.Close()
		pw.stateOwners.Close()
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
		return p,
			func() {
				d()

				go func() {
					if err := opsrDone(); err != nil {
						pw.ctxCancel(err)
					}
				}()
			},
			c, nil
	}
}

func (pw *ProcessWorker) InsertValues(worker string, opindex uint64, values []StateMergeValue) error {
	switch merger, err := pw.merger(); {
	case err != nil:
		pw.ctxCancel(err)

		return err
	default:
		if err := pw.setStateMergeValuesOwners(opindex, values, nil); err != nil {
			pw.ctxCancel(err)

			return err
		}

		if err := merger.InsertOp(opindex, values); err != nil {
			pw.ctxCancel(err)

			return err
		}

		return nil
	}
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
	// FIXME collect state keys and hashes

	if err := context.Cause(pw.mergeCtx()); err != nil {
		return err
	}

	switch merger, err := pw.merger(); {
	case err != nil:
		pw.ctxCancel(err)

		return err
	default:
		if err := merger.Done(); err != nil {
			pw.ctxCancel(err)

			return err
		}

		pw.mergeCtxCancel(errProcessWorkerMergeDone)

		return nil
	}
}

func (pw *ProcessWorker) Wait() error {
	// FIXME wait until
	// - merger done
	// - store block files

	<-pw.mergeCtx().Done()

	switch err := context.Cause(pw.mergeCtx()); {
	case errors.Is(err, errProcessWorkerMergeDone):
	case err != nil:
		return err
	}

	pw.delayedWorker.Done()

	if err := pw.delayedWorker.Wait(); err != nil {
		pw.ctxCancel(err)

		return err
	}

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

	args.GetStateValue = pw.args.GetStateValue
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
	return pw.args.UpdatedValuesFunc(ctx, phase, stateKey, merger, mergedOps, unmergedOps, maxUpdatedValues)
}

func (pw *ProcessWorker) opsrUpdateReasons(opindex uint64, reason base.OperationProcessReasonError) error {
	return pw.args.UpdateReasonsFunc(pw.ctx(), -1, opindex, reason.OperationProcessFailedReason())
}

func (pw *ProcessWorker) updateReasons(ctx context.Context, phase int, opindex uint64, reason string) error {
	return pw.args.UpdateReasonsFunc(ctx, phase, opindex, reason)
}

func (pw *ProcessWorker) mergeStateMergeValue(opindex uint64, values []StateMergeValue) error {
	var merger *StatesMergerWorker

	switch i, err := pw.merger(); {
	case err != nil:
		return err
	default:
		merger = i
	}

	var owners map[string][]StateMergeValue

	switch i, err := pw.filterStateMergeValuesOwners(opindex, values); {
	case err != nil:
		return err
	default:
		owners = i
	}

	// FIXME util.JobWorker

	for owner := range owners {
		filtered := owners[owner]

		switch {
		case owner == pw.local:
			if err := merger.InsertOp(opindex, filtered); err != nil {
				return err
			}
		default:
			if err := pw.args.BroadcastInsertOpFunc(pw.ctx(), owner, opindex, filtered); err != nil {
				return err
			}
		}
	}

	return nil
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
