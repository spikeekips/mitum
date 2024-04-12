package main

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var localWorker = "local"

var (
	errStatesMergerWorkerDone             = util.NewIDError("states merger worker done")
	errStatesMergerWorkerAlreadyPhaseDone = util.NewIDError("worker already phase done")
)

type (
	statesMergerDoneUpdatedValuesFunc func(
		_ context.Context,
		phase int,
		stateKey string,
		merger StateMerger,
		mergedOps, unmergedOps []uint64,
		maxUpdatedValues uint64,
	) error
	statesMergerDoneUpdatedReasonsFunc func(
		_ context.Context,
		phase int,
		opindex uint64,
		reason string,
	) error
)

type StatesMergerWorkerArgs struct {
	GetStateValue          func(_ context.Context, key string) (base.StateValue, bool, error)
	NewJobWorker           func(context.Context) (util.JobWorker, error)
	IsEnoughWorkers        func(workers uint64) bool // NOTE include local worker
	BroadcastPhaseDoneFunc func(_ context.Context, phase int, mergedOps, unmergedOps []uint64) error
	UpdatedValuesFunc      statesMergerDoneUpdatedValuesFunc
	UpdateReasonsFunc      statesMergerDoneUpdatedReasonsFunc
	MapSize                uint64
}

func NewStatesMergerWorkerArgs() *StatesMergerWorkerArgs {
	return &StatesMergerWorkerArgs{
		GetStateValue: func(context.Context, string) (base.StateValue, bool, error) { return nil, false, nil },
		NewJobWorker: func(ctx context.Context) (util.JobWorker, error) {
			return util.NewBaseJobWorker(ctx, 1<<6)
		},
		IsEnoughWorkers:        func(uint64) bool { return false },
		BroadcastPhaseDoneFunc: func(context.Context, int, []uint64, []uint64) error { return nil },
		UpdatedValuesFunc: func(context.Context, int, string, StateMerger, []uint64, []uint64, uint64) error {
			return nil
		},
		UpdateReasonsFunc: func(context.Context, int, uint64, string) error { return nil },
		MapSize:           1 << 9,
	}
}

type StatesMergerWorker struct {
	*logging.Logging
	args             *StatesMergerWorkerArgs
	ctx              func() context.Context
	ctxcancel        func(error)
	mergers          util.LockedMap[string /* state key */, StateMerger]
	valuespool       util.LockedMap[uint64 /* opindex */, []StateMergeValue]
	clean            func()
	workerName       string // NOTE for testing
	phaseStatus      util.LockedMap[int /* phase */, uint8 /* 1: local done, 2: merge done enough */]
	phaseDoneRecords util.LockedMap[int /* phase */, map[string][3]interface{} /* {mergedOps, unmergedOps []uint8, is empty bool} */]
	phaseOps         util.LockedMap[string /* phase + opindex */, xMergeFlag /* {phase, xMergeFlag} */]
	psms             util.LockedMap[int /* phase */, *phaseStatesMerger]
	phaseDoneOps     util.LockedMap[uint64 /* opindex */ /* {phase, xMergeFlag} */, [2]interface{}]
}

func NewStatesMergerWorker(
	ctx context.Context,
	args *StatesMergerWorkerArgs,
) (*StatesMergerWorker, error) {
	gctx, gctxcancel := context.WithCancelCause(ctx)

	dw := &StatesMergerWorker{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "states-merger")
		}),
		args:      args,
		ctx:       func() context.Context { return gctx },
		ctxcancel: gctxcancel,
	}

	switch i, err := util.NewShardedMap[string, StateMerger](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		dw.mergers = i
	}

	switch i, err := util.NewShardedMap[uint64, []StateMergeValue](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		dw.valuespool = i
	}

	switch i, err := util.NewShardedMap[int, uint8](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		dw.phaseStatus = i
	}

	switch i, err := util.NewShardedMap[int, map[string][3]interface{}](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		dw.phaseDoneRecords = i
	}

	switch i, err := util.NewShardedMap[int, *phaseStatesMerger](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		dw.psms = i
	}

	switch i, err := util.NewShardedMap[string, xMergeFlag](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		dw.phaseOps = i
	}

	switch i, err := util.NewShardedMap[uint64, [2]interface{}](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		dw.phaseDoneOps = i
	}

	var cleanOnce sync.Once

	dw.clean = func() {
		cleanOnce.Do(func() {
			dw.cleanf()
		})
	}

	return dw, nil
}

func (dw *StatesMergerWorker) InsertOp(opindex uint64, values []StateMergeValue) error {
	switch _, created, err := dw.valuespool.Set(opindex, func(_ []StateMergeValue, found bool) ([]StateMergeValue, error) {
		if found {
			return nil, util.ErrLockedSetIgnore
		}

		return values, nil
	}); {
	case err != nil:
		return err
	case !created:
		return util.ErrFound.Errorf("known opindex, %d", opindex)
	}

	return dw.phaseStatus.Get(-1, func(status uint8, found bool) error {
		if status > 1 {
			return errStatesMergerWorkerAlreadyPhaseDone.Errorf("insert")
		}

		if err := dw.mergeOps(localWorker, 0, []uint64{opindex}, nil); err != nil {
			dw.ctxcancel(err)

			return err
		}

		return nil
	})
}

func (dw *StatesMergerWorker) InsertDone() error {
	return dw.workerInsertDone(localWorker, -1, nil, nil, true)
}

func (dw *StatesMergerWorker) WorkerInsertDone(worker string) error {
	switch err := dw.workerInsertDone(worker, -1, nil, nil, true); {
	case errors.Is(err, errStatesMergerWorkerAlreadyPhaseDone):
		return nil
	default:
		return err
	}
}

func (dw *StatesMergerWorker) WorkerPhaseDone(worker string, phase int, mergedOps, unmergedOps []uint64, isEmpty bool) error {
	switch err := dw.workerPhaseDone(worker, phase, mergedOps, unmergedOps, isEmpty); {
	case errors.Is(err, errStatesMergerWorkerAlreadyPhaseDone):
		return nil
	default:
		return err
	}
}

func (dw *StatesMergerWorker) Cancel() {
	dw.Log().Debug().Msg("cancel")

	dw.ctxcancel(nil)

	_ = dw.phaseStatus.Traverse(func(phase int, status uint8) bool {
		if status > 0 {
			return true
		}

		if psm, found := dw.psms.Value(phase); found && status < 1 {
			psm.Cancel()
		}

		return true
	})

	dw.clean()
}

func (dw *StatesMergerWorker) Done() error {
	defer dw.clean()

	<-dw.ctx().Done()

	switch err := context.Cause(dw.ctx()); {
	case errors.Is(err, errStatesMergerWorkerDone):
		dw.Log().Debug().Msg("done")

		return nil
	default:
		return err
	}
}

func (dw *StatesMergerWorker) cleanf() {
	dw.mergers.Close()
	dw.valuespool.Close()
	dw.phaseStatus.Close()
	dw.phaseDoneRecords.Close()
	dw.phaseOps.Close()
	dw.psms.Close()
	dw.phaseDoneOps.Close()
}

func (dw *StatesMergerWorker) localPhaseDone(phase int, psm *phaseStatesMerger) error {
	var mergedOps, unmergedOps []uint64

	if psm != nil {
		var updatef phaseStatesMergerDoneUpdateFunc
		var err error

		if mergedOps, unmergedOps, updatef, err = psm.Done(); err != nil {
			return err
		}

		if phase == 0 {
			addMergedOps, doneMergedOps := util.CompactAppendSlice[uint64](dw.valuespool.Len())
			_ = dw.valuespool.Traverse(func(opindex uint64, _ []StateMergeValue) bool {
				if slices.Index(unmergedOps, opindex) < 0 {
					addMergedOps(opindex)
				}

				return true
			})

			mergedOps = doneMergedOps()
		}

		_ = dw.phaseStatus.Get(phase, func(uint8, bool) error {
			util.TraverseSliceV1(mergedOps, func(_ int, opindex uint64) (bool, error) {
				dw.phaseDoneOps.SetValue(opindex, [2]interface{}{phase, mergeFlag})

				return true, nil
			})

			util.TraverseSliceV1(unmergedOps, func(_ int, opindex uint64) (bool, error) {
				dw.phaseDoneOps.SetValue(opindex, [2]interface{}{phase, unmergeFlag})

				return true, nil
			})

			return nil
		})

		dw.updateDone(phase, updatef)
	}

	if err := dw.args.BroadcastPhaseDoneFunc(dw.ctx(), phase, mergedOps, unmergedOps); err != nil {
		return err
	}

	return dw.workerPhaseDone(
		localWorker,
		phase,
		mergedOps,
		unmergedOps,
		len(mergedOps) < 1 && len(unmergedOps) < 1,
	)
}

func (dw *StatesMergerWorker) emptyLocalPhaseDone(phase int) error {
	if err := dw.args.BroadcastPhaseDoneFunc(dw.ctx(), phase, nil, nil); err != nil {
		return err
	}

	return dw.workerPhaseDone(localWorker, phase, nil, nil, true)
}

func (dw *StatesMergerWorker) psm(phase int) (psm *phaseStatesMerger, _ error) {
	if err := dw.psms.GetOrCreate(
		phase,
		func(i *phaseStatesMerger, _ bool) error {
			psm = i

			return nil
		},
		func() (*phaseStatesMerger, error) {
			var p *phaseStatesMerger

			switch i, err := newPhaseStatesMerger(
				dw.ctx(),
				dw.mergers.Value,
				dw.valuespool.Value,
				dw.args.GetStateValue,
				dw.args.NewJobWorker,
				dw.args.MapSize,
			); {
			case err != nil:
				return nil, err
			default:
				p = i
			}

			_ = p.SetLogging(dw.Logging)
			p.SetWorkerName(fmt.Sprintf("%s/%d", dw.workerName, phase))

			go func() {
				if err := dw.localPhaseDone(phase, p); err != nil {
					dw.ctxcancel(err)
				}
			}()

			return p, nil
		},
	); err != nil {
		return nil, err
	}

	return psm, nil
}

func (dw *StatesMergerWorker) setPhaseDoneRecord(worker string, phase int, mergedOps, unmergedOps []uint64, isEmpty bool) (
	isMergeDoneEnough, isEmptyEnough bool,
	_ error,
) {
	if len(mergedOps) > 0 || len(unmergedOps) > 0 {
		isEmpty = false
	}

	localMergeDone := worker == localWorker
	localDoneEmpty := worker == localWorker && isEmpty

	var nm, ne uint64

	if _, _, err := dw.phaseDoneRecords.Set(phase, func(m map[string][3]interface{}, found bool) (map[string][3]interface{}, error) {
		if !found {
			nm = 1
			if isEmpty {
				ne = 1
			}

			return map[string][3]interface{}{worker: {mergedOps, unmergedOps, isEmpty}}, nil
		}

		switch _, found := m[worker]; {
		case found:
			return nil, util.ErrFound.Errorf("worker=%q phase=%d", worker, phase)
		default:
			m[worker] = [3]interface{}{mergedOps, unmergedOps, isEmpty}
		}

		for worker := range m {
			if !localMergeDone && worker == localWorker {
				localMergeDone = true
			}

			nm++

			if i := m[worker][2]; i != nil && i.(bool) {
				if !localDoneEmpty && worker == localWorker {
					localDoneEmpty = true
				}

				ne++
			}
		}

		return m, nil
	}); err != nil {
		return false, false, err
	}

	if !localMergeDone {
		return false, false, nil
	}

	isMergeDoneEnough = dw.args.IsEnoughWorkers(nm)

	if !localDoneEmpty {
		return isMergeDoneEnough, false, nil
	}

	isEmptyEnough = dw.args.IsEnoughWorkers(ne)
	if isEmptyEnough {
		return false, true, nil
	}

	return isMergeDoneEnough, isEmptyEnough, nil
}

func (dw *StatesMergerWorker) dumpPhaseDoneRecords(phase int) (mergedOps, unmergedOps []uint64) {
	addMergedOps, doneMergedOps := util.UniqCompactAppendSlice[uint64](dw.valuespool.Len())
	addUnmergedOps, doneUnmergedOps := util.UniqCompactAppendSlice[uint64](dw.valuespool.Len())

	_ = dw.phaseDoneRecords.Get(phase, func(m map[string][3]interface{}, found bool) error {
		if !found {
			return nil
		}

		for worker := range m {
			i := m[worker]

			if j := i[0]; j != nil {
				_ = util.TraverseSliceV1(j.([]uint64), func(_ int, opindex uint64) (bool, error) {
					addMergedOps(opindex)

					return true, nil
				})
			}

			if j := i[1]; j != nil {
				_ = util.TraverseSliceV1(j.([]uint64), func(_ int, opindex uint64) (bool, error) {
					addUnmergedOps(opindex)

					return true, nil
				})
			}
		}

		return nil
	})

	return doneMergedOps(), doneUnmergedOps()
}

func (dw *StatesMergerWorker) workerInsertDone(worker string, phase int, mergedOps, unmergedOps []uint64, isEmpty bool) error {
	_, _, err := dw.phaseStatus.Set(phase, func(status uint8, found bool) (uint8, error) {
		if status > 1 {
			return 0, errStatesMergerWorkerAlreadyPhaseDone.Errorf(worker)
		}

		switch isMergeDoneEnough, isEmptyEnough, err := dw.isEnoughWorkerPhaseDone(worker, status, -1, nil, nil, true); {
		case err != nil:
			dw.ctxcancel(err)

			return 0, err
		case isEmptyEnough, isMergeDoneEnough:
			if err := dw.mergeDone(phase + 1); err != nil {
				return 0, err
			}

			return 2, nil
		case worker == localWorker:
			return 1, nil
		default:
			return 0, util.ErrLockedSetIgnore
		}
	})

	return err
}

func (dw *StatesMergerWorker) workerPhaseDone(worker string, phase int, mergedOps, unmergedOps []uint64, isEmpty bool) error {
	dw.Log().Debug().
		Str("worker", worker).
		Int("phase", phase).
		Msg("worker phase done")

	_, _, err := dw.phaseStatus.Set(phase, func(status uint8, found bool) (uint8, error) {
		if status > 1 {
			return 0, errStatesMergerWorkerAlreadyPhaseDone.Errorf(worker)
		}

		switch isMergeDoneEnough, isEmptyEnough, err := dw.isEnoughWorkerPhaseDone(worker, status, phase, mergedOps, unmergedOps, isEmpty); {
		case err != nil:
			dw.ctxcancel(err)

			return 0, err
		case isEmptyEnough:
			dw.ctxcancel(errStatesMergerWorkerDone)

			return 2, nil
		case status < 2 && isMergeDoneEnough:
			if err := dw.mergeDone(phase + 1); err != nil {
				return 0, err
			}

			return 1, nil
		case worker == localWorker:
			return 1, nil
		default:
			return 0, util.ErrLockedSetIgnore
		}
	})

	return err
}

func (dw *StatesMergerWorker) isEnoughWorkerPhaseDone(worker string, status uint8, phase int, mergedOps, unmergedOps []uint64, isEmpty bool) (
	isMergeDoneEnough, isEmptyEnough bool,
	_ error,
) {
	l := dw.Log().With().
		Str("worker", worker).
		Int("phase", phase).
		Uint8("status", status).
		Bool("is_empty", isEmpty).
		Int("merged_ops", len(mergedOps)).
		Int("unmerged_ops", len(unmergedOps)).
		Logger()

	if status < 1 && worker == localWorker {
		l.Debug().Msg("local worker done; dump stored records")

		dmergedOps, dunmergedOps := dw.dumpPhaseDoneRecords(phase)

		if err := dw.mergeOps(worker, phase+1, dmergedOps, dunmergedOps); err != nil {
			return false, false, err
		}
	}

	if status > 0 {
		if err := dw.mergeOps(worker, phase+1, mergedOps, unmergedOps); err != nil {
			return false, false, err
		}
	}

	var err error

	if isMergeDoneEnough, isEmptyEnough, err = dw.setPhaseDoneRecord(worker, phase, mergedOps, unmergedOps, isEmpty); err != nil {
		return false, false, err
	}

	l.Debug().
		Bool("is_merge_done_enough", isMergeDoneEnough).
		Bool("is_empty_enough", isEmptyEnough).
		Msg("is enough worker in phase done?")

	return isMergeDoneEnough, isEmptyEnough, nil
}

func (dw *StatesMergerWorker) mergeOps(worker string, phase int, mergedOps, unmergedOps []uint64) error {
	l := dw.Log().With().
		Str("worker", worker).
		Int("phase", phase).
		Logger()

	l.Debug().
		Uints64("merged_ops", mergedOps).
		Uints64("unmerged_ops", unmergedOps).
		Msg("trying to merge/unmerge")

	if len(mergedOps) < 1 && len(unmergedOps) < 1 {
		return nil
	}

	filterOpsFunc := func(xmf xMergeFlag) func(uint64) bool {
		return func(opindex uint64) bool {
			var found bool

			dw.phaseOps.Set(fmt.Sprintf("%d-%d", phase, opindex), func(prev xMergeFlag, opfound bool) (xMergeFlag, error) {
				if opfound {
					found = true

					return 0, util.ErrLockedSetIgnore
				}

				return xmf, nil
			})

			return !found
		}
	}

	var doneMergedOps, doneUnmergedOps []uint64

	switch i, err := dw.xMergeOps(phase, mergedOps, mergeFlag, filterOpsFunc(mergeFlag)); {
	case err != nil:
		return err
	default:
		doneMergedOps = i
	}

	switch i, err := dw.xMergeOps(phase, unmergedOps, unmergeFlag, filterOpsFunc(unmergeFlag)); {
	case err != nil:
		return err
	default:
		doneUnmergedOps = i
	}

	l.Debug().
		Uints64("merged_ops", doneMergedOps).
		Uints64("unmerged_ops", doneUnmergedOps).
		Msg("merged/unmerged")

	return nil
}

func (dw *StatesMergerWorker) mergeDone(phase int) error {
	l := dw.Log().With().Int("phase", phase).Logger()

	psm, found := dw.psms.Value(phase)
	if !found {
		l.Debug().Msg("merge done, but psm not found; will local phase done")

		go func() {
			if err := dw.emptyLocalPhaseDone(phase); err != nil {
				dw.ctxcancel(err)
			}
		}()

		return nil
	}

	l.Debug().Msg("merge done")

	return psm.MergeDone()
}

func (dw *StatesMergerWorker) xMergeOps(
	phase int,
	ops []uint64,
	xmf xMergeFlag,
	filterOp func(opindex uint64) bool,
) ([]uint64, error) {
	if len(ops) < 1 {
		return nil, nil
	}

	var psmOnce sync.Once
	var psm *phaseStatesMerger
	var gerr error

	getPsm := func() (*phaseStatesMerger, error) {
		psmOnce.Do(func() {
			switch i, err := dw.psm(phase); {
			case err != nil:
				gerr = err
			default:
				psm = i
			}
		})

		return psm, gerr
	}

	addOps, doneOps := util.CompactAppendSlice[uint64](len(ops))

	if err := util.TraverseSliceV1(ops, func(_ int, opindex uint64) (bool, error) {
		if i, found := dw.phaseDoneOps.Value(opindex); found && i[1].(xMergeFlag) == xmf {
			return true, nil
		}

		if !filterOp(opindex) {
			return true, nil
		}

		addOps(opindex)

		switch psm, err := getPsm(); {
		case err != nil:
			return false, err
		case xmf == mergeFlag:
			return true, psm.Merge(opindex)
		default:
			return true, psm.Unmerge(opindex)
		}
	}); err != nil {
		return nil, err
	}

	return doneOps(), nil
}

func (dw *StatesMergerWorker) updateDone(phase int, f phaseStatesMergerDoneUpdateFunc) error {
	return f(
		dw.updateValues(phase),
		dw.updateReason(phase),
	)
}

func (dw *StatesMergerWorker) updateValues(phase int) phaseStatesMergerDoneUpdatedValuesFunc {
	return func(stateKey string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
		_ = dw.mergers.SetValue(stateKey, merger)

		return dw.args.UpdatedValuesFunc(dw.ctx(), phase, stateKey, merger, mergedOps, unmergedOps, maxUpdatedValues)
	}
}

func (dw *StatesMergerWorker) updateReason(phase int) phaseStatesMergerDoneUpdatedReasonsFunc {
	return func(opindex uint64, reason string) error {
		return dw.args.UpdateReasonsFunc(dw.ctx(), phase, opindex, reason)
	}
}
