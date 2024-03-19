package testopsworkers

import (
	"context"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
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

		// FIXME non-blocking store
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
	NetworkID              base.NetworkID
	GetOperationFunc       func(_ context.Context, operation util.Hash) (Operation, bool, error)
	IsInStateOperationFunc func(_ context.Context, fact util.Hash) (bool, error)
	GetStateValue          func(key string) (base.StateValue, bool, error)
	NewJobWorker           func(context.Context) (util.JobWorker, error)
	IsEnoughWorkers        func(n uint64) bool
	BroadcastPhaseDoneFunc func(phase int, mergedOps, unmergedOps []uint64) error
	MapSize                uint64
}

func NewProcessWorkerArgs(networkID base.NetworkID) *ProcessWorkerArgs {
	return &ProcessWorkerArgs{
		NetworkID:              networkID,
		GetOperationFunc:       func(context.Context, util.Hash) (Operation, bool, error) { return nil, false, nil },
		IsInStateOperationFunc: func(context.Context, util.Hash) (bool, error) { return false, nil },
		GetStateValue:          func(string) (base.StateValue, bool, error) { return nil, false, nil },
		NewJobWorker: func(ctx context.Context) (util.JobWorker, error) {
			return util.NewBaseJobWorker(ctx, 1<<6)
		},
		IsEnoughWorkers:        func(uint64) bool { return false },
		BroadcastPhaseDoneFunc: func(int, []uint64, []uint64) error { return nil },
		MapSize:                1 << 9,
	}
}

type ProcessWorker struct {
	*logging.Logging
	workerName string
	args       *ProcessWorkerArgs
	ctx        func() context.Context
	ctxcancel  func(error)
	opsr       *OperationProcessor
	merger     *StatesMergerWorker
	status     *util.Locked[uint8 /* 1: insert done */]
	mergeDone  map[ /* worker*/ string]struct{}
}

func NewProcessWorker(ctx context.Context, args *ProcessWorkerArgs) (*ProcessWorker, error) {
	pctx, cancel := context.WithCancelCause(ctx)

	pw := &ProcessWorker{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "processor-worker")
		}),
		args:      args,
		ctx:       func() context.Context { return pctx },
		ctxcancel: cancel,
		status:    util.NewLocked(uint8(0)),
		mergeDone: map[string]struct{}{},
	}

	if err := pw.newMerger(); err != nil {
		return nil, err
	}

	if err := pw.newOperationProcessor(); err != nil {
		return nil, err
	}

	return pw, nil
}

func (pw *ProcessWorker) Cancel() {
	pw.ctxcancel(nil)
}

func (pw *ProcessWorker) NewOperations() (
	process func(opindex uint64, op, fact util.Hash) error,
	done func(),
	cancel func(error),
	_ error,
) {
	if status, _ := pw.status.Value(); status > 0 {
		return nil, nil, nil, errors.Errorf("already insert done")
	}

	switch p, d, c, err := pw.opsr.Process(); {
	case err != nil:
		pw.ctxcancel(err)

		return nil, nil, nil, err
	default:
		return p,
			func() {
				d()

				go func() {
					if err := pw.opsr.Wait(); err != nil {
						pw.ctxcancel(err)

						return
					}

					if _, err := pw.workerInsertValuesDone(localWorker); err != nil {
						pw.ctxcancel(err)
					}
				}()
			},
			c, nil
	}
}

func (pw *ProcessWorker) InsertValues(worker string, opindex uint64, values []StateMergeValue) error {
	if status, _ := pw.status.Value(); status > 0 {
		return errors.Errorf("already insert done")
	}

	return pw.merger.InsertOp(opindex, values)
}

func (pw *ProcessWorker) WorkerInsertValuesDone(worker string) error {
	if status, _ := pw.status.Value(); status > 0 {
		return nil
	}

	if _, err := pw.workerInsertValuesDone(worker); err != nil {
		pw.ctxcancel(err)

		return err
	}

	return nil
}

func (pw *ProcessWorker) WorkerPhaseDone(worker string, phase int, mergedOps, unmergedOps []uint64, isEmpty bool) error {
	return pw.merger.WorkerPhaseDone(worker, phase, mergedOps, unmergedOps, isEmpty)
}

func (pw *ProcessWorker) MergeWait() error {
	// FIXME collect state keys and hashes

	return pw.merger.Done()
}

func (pw *ProcessWorker) Wait() error {
	// FIXME wait until
	// - merger done
	// - store block files
	return nil
}

func (pw *ProcessWorker) SetWorkerName(s string) {
	pw.workerName = s

	_ = pw.SetLogger(pw.Log().With().Str("worker_name", s).Logger())
}

func (pw *ProcessWorker) workerInsertValuesDone(worker string) (isEnough bool, _ error) {
	var localDone bool

	_, _ = pw.status.Set(func(status uint8, isEmpty bool) (uint8, error) {
		if status > 0 {
			return 0, util.ErrLockedSetIgnore
		}

		if _, found := pw.mergeDone[worker]; found {
			return 0, util.ErrLockedSetIgnore
		}

		pw.mergeDone[worker] = struct{}{}

		if _, localDone = pw.mergeDone[localWorker]; localDone {
			isEnough = pw.args.IsEnoughWorkers(uint64(len(pw.mergeDone)))
		}

		clear(pw.mergeDone)

		return 1, nil
	})

	if isEnough {
		if err := pw.merger.InsertDone(); err != nil {
			return isEnough, err
		}
	}

	return isEnough, nil
}

func (pw *ProcessWorker) newMerger() error {
	args := NewStatesMergerWorkerArgs()

	args.GetStateValue = pw.args.GetStateValue
	args.NewJobWorker = pw.args.NewJobWorker
	args.IsEnoughWorkers = pw.args.IsEnoughWorkers
	args.BroadcastPhaseDoneFunc = pw.args.BroadcastPhaseDoneFunc
	args.UpdatedValuesFunc = pw.updatedValues
	args.UpdateReasonsFunc = pw.updateReasons
	args.MapSize = pw.args.MapSize

	switch i, err := NewStatesMergerWorker(pw.ctx(), args); {
	case err != nil:
		return err
	default:
		pw.merger = i

		return nil
	}
}

func (pw *ProcessWorker) newOperationProcessor() error {
	args := NewOperationProcessorArgs(pw.args.NetworkID)

	args.NewJobWorker = pw.args.NewJobWorker
	args.GetOperationFunc = pw.args.GetOperationFunc
	args.IsInStateOperationFunc = pw.args.IsInStateOperationFunc
	args.StoreOperationFunc = pw.storeOperation
	args.UpdateReasonsFunc = pw.opsrUpdateReasons
	args.MergeStateMergeValueFunc = pw.merger.InsertOp

	switch i, err := NewOperationProcessor(pw.ctx(), args); {
	case err != nil:
		return err
	default:
		pw.opsr = i
	}

	return nil
}

func (pw *ProcessWorker) storeOperation(opindex uint64, op Operation) error {
	return nil
}

func (pw *ProcessWorker) updatedValues(
	phase int,
	stateKey string,
	merger StateMerger,
	mergedOps, unmergedOps []uint64,
	maxUpdatedValues uint64,
) error {
	return nil
}

func (pw *ProcessWorker) opsrUpdateReasons(opindex uint64, reason base.OperationProcessReasonError) error {
	return nil
}

func (pw *ProcessWorker) updateReasons(phase int, opindex uint64, reason string) error {
	return nil
}
