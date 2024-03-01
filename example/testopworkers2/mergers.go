package testopsworkers

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var localStatesMergerWorker = "local"

var distributedStatesMergerWorkerDone = util.NewIDError("states merger worker done")

type (
	distributedStatesMergerDoneUpdatedValuesFunc  func(phase int, stateKey string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error
	distributedStatesMergerDoneUpdatedReasonsFunc func(phase int, opindex uint64, reason string) error
)

type DistributedStatesMergerWorkerArgs struct {
	GetStateValue          func(key string) (base.StateValue, bool, error)
	NewDistributedWorker   func(context.Context) (distributedWorker, error)
	IsEnoughWorkers        func(workers uint64) bool // NOTE include local worker
	BroadcastPhaseDoneFunc func(phase int, unmergedOps []uint64) error
	UpdatedValuesFunc      distributedStatesMergerDoneUpdatedValuesFunc
	UpdateReasonsFunc      distributedStatesMergerDoneUpdatedReasonsFunc
	MapSize                uint64
}

func NewDistributedStatesMergerWorkerArgs() *DistributedStatesMergerWorkerArgs {
	return &DistributedStatesMergerWorkerArgs{
		GetStateValue: func(string) (base.StateValue, bool, error) { return nil, false, nil },
		NewDistributedWorker: func(ctx context.Context) (distributedWorker, error) {
			return util.NewErrgroupWorker(ctx, 1<<6)
		},
		IsEnoughWorkers:        func(uint64) bool { return false },
		BroadcastPhaseDoneFunc: func(int, []uint64) error { return nil },
		UpdatedValuesFunc:      func(int, string, StateMerger, []uint64, []uint64, uint64) error { return nil }, // FIXME add phase; base.StateValue, not merger
		UpdateReasonsFunc:      func(int, uint64, string) error { return nil },                                  // FIXME add phase
		MapSize:                1 << 9,
	}
}

type DistributedStatesMergerWorker struct {
	*logging.Logging
	args                *DistributedStatesMergerWorkerArgs
	ctx                 func() context.Context
	ctxcancel           func(error)
	mergers             util.LockedMap[string /* state key */, StateMerger]
	valuespool          util.LockedMap[uint64 /* opindex */, []StateMergeValue]
	phaseStatuses       util.LockedMap[int /* phase */, uint8 /* status; 1: no more merge/unmerge 2: local done 3: closed */]
	phaseDoneRecords    util.LockedMap[int /* phase */, map[string] /* worker */ bool /* has unmerged ops */]
	tempWorkerPhaseDone map[ /* phase */ int]map[ /* worker */ string] /* unmerged ops */ []uint64
	psms                util.LockedMap[int /* phase */, *phaseStatesMerger]
	phaseOps            util.LockedMap[int /* phase */, map[uint64] /* opindex */ struct{}]
	workerName          string // NOTE for testing
	clean               func()
}

func NewDistributedStatesMergerWorker(
	ctx context.Context,
	args *DistributedStatesMergerWorkerArgs,
) (*DistributedStatesMergerWorker, error) {
	gctx, gctxcancel := context.WithCancelCause(ctx)

	dw := &DistributedStatesMergerWorker{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "distributed-states-merger")
		}),
		args:                args,
		ctx:                 func() context.Context { return gctx },
		ctxcancel:           gctxcancel,
		tempWorkerPhaseDone: map[int]map[string][]uint64{},
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
		dw.phaseStatuses = i
	}

	switch i, err := util.NewShardedMap[int, map[string]bool](args.MapSize, nil); {
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

	switch i, err := util.NewShardedMap[int, map[uint64]struct{}](args.MapSize, nil); {
	case err != nil:
		return nil, err
	default:
		dw.phaseOps = i
	}

	var cleanOnce sync.Once

	dw.clean = func() {
		cleanOnce.Do(func() {
			dw.cleanf()
		})
	}

	return dw, nil
}

func (dw *DistributedStatesMergerWorker) InsertOp(opindex uint64, values []StateMergeValue) error {
	if _, created, _ := dw.valuespool.Set(opindex, func(_ []StateMergeValue, found bool) ([]StateMergeValue, error) {
		if found {
			return nil, util.ErrLockedSetIgnore
		}

		return values, nil
	}); !created {
		return util.ErrFound.Errorf("operation already inserted")
	}

	return dw.phaseStatuses.Get(-1, func(status uint8, found bool) error {
		if found && status > 0 {
			return errors.Errorf("already insert done")
		}

		switch psm, err := dw.psm(0); {
		case err != nil:
			return err
		default:
			return psm.Merge(opindex)
		}
	})
}

func (dw *DistributedStatesMergerWorker) InsertDone() error {
	return dw.workerInsertDone(localStatesMergerWorker)
}

func (dw *DistributedStatesMergerWorker) WorkerInsertDone(worker string) error {
	return dw.workerInsertDone(worker)
}

func (dw *DistributedStatesMergerWorker) WorkerPhaseDone(phase int, unmergedOps []uint64, worker string) error {
	if err := util.TraverseSliceV1(unmergedOps, func(_ int, opindex uint64) (bool, error) {
		if !dw.valuespool.Exists(opindex) {
			return false, util.ErrNotFound.Errorf("unknown operation")
		}

		return true, nil
	}); err != nil {
		return err
	}

	return dw.otherWorkerPhaseDone(phase, unmergedOps, worker)
}

func (dw *DistributedStatesMergerWorker) Cancel() {
	dw.ctxcancel(nil)
	// FIXME cancel psms
	dw.clean()
}

func (dw *DistributedStatesMergerWorker) Done() error {
	defer dw.clean()

	<-dw.ctx().Done()

	switch err := context.Cause(dw.ctx()); {
	case errors.Is(err, distributedStatesMergerWorkerDone):
	case err != nil:
		return err
	}

	return nil
}

func (dw *DistributedStatesMergerWorker) cleanf() {
	dw.valuespool.Close()
	dw.mergers.Close()
	dw.phaseStatuses.Close()
	dw.phaseDoneRecords.Close()
	dw.psms.Close()
	dw.phaseOps.Close()

	clear(dw.tempWorkerPhaseDone)
}

func (dw *DistributedStatesMergerWorker) workerInsertDone(worker string) error {
	l := dw.Log().With().Str("worker", worker).Logger()

	_, _, err := dw.phaseStatuses.Set(-1, func(status uint8, found bool) (uint8, error) {
		if found && status > 0 {
			return 0, util.ErrLockedSetIgnore
		}

		switch added, isMergeDoneEnough := dw.setInsertDoneRecord(-1, worker); {
		case !added:
			return 0, util.ErrFound.Errorf("worker, %q already insert done", worker)
		case !isMergeDoneEnough:
			return 0, nil
		}

		return 3, dw.mergeDone(0)
	})
	if err != nil {
		l.Error().Err(err).Msg("worker insert")

		return err
	}

	l.Debug().Msg("worker insert")

	return nil
}

func (dw *DistributedStatesMergerWorker) localPhaseDone(phase int, psm *phaseStatesMerger) error {
	var unmergedOps []uint64

	var updatef phaseStatesMergerDoneUpdateFunc

	if psm != nil {
		var err error

		if _, unmergedOps, updatef, err = psm.Done(); err != nil {
			return err
		}

		if updatef != nil {
			if err := updatef(
				dw.updatedValues(phase),
				dw.updatedReasons(phase),
			); err != nil {
				return err
			}
		}
	}

	var isEmptyEnough bool

	if _, _, err := dw.phaseStatuses.Set(phase, func(status uint8, _ bool) (uint8, error) {
		if status > 1 {
			return 0, util.ErrLockedSetIgnore
		}

		// NOTE dump unmerged ops from temp
		if _, err := dw.dumpTempWorkerPhaseDone(phase, unmergedOps); err != nil {
			return 0, err
		}

		switch isClosed, id, err := dw.workerPhaseDone(phase, localStatesMergerWorker, nil); {
		case err != nil:
			return 0, err
		case id:
			isEmptyEnough = true

			return 3, nil
		case isClosed:
			return 3, nil
		default:
			return 2, nil
		}
	}); err != nil {
		return err
	}

	if err := dw.args.BroadcastPhaseDoneFunc(phase, unmergedOps); err != nil {
		return err
	}

	if isEmptyEnough {
		dw.Log().Debug().Int("phase", phase).Msg("done")

		dw.ctxcancel(distributedStatesMergerWorkerDone)
	}

	return nil
}

func (dw *DistributedStatesMergerWorker) otherWorkerPhaseDone(phase int, unmergedOps []uint64, worker string) error {
	var isEmptyEnough bool

	if _, _, err := dw.phaseStatuses.Set(phase, func(status uint8, _ bool) (uint8, error) {
		switch {
		case status > 2:
			return 0, util.ErrLockedSetIgnore
		case status < 2:
			_ = dw.setTempWorkerPhaseDone(phase, worker, unmergedOps)
		case len(unmergedOps) > 0:
			if err := dw.unmerge(phase+1, unmergedOps); err != nil {
				return 0, err
			}
		}

		switch ic, id, err := dw.workerPhaseDone(phase, worker, unmergedOps); {
		case err != nil:
			return 0, err
		case id:
			isEmptyEnough = true

			return 3, nil
		case ic:
			return 3, nil
		default:
			return 0, util.ErrLockedSetIgnore
		}
	}); err != nil {
		return err
	}

	l := dw.Log().With().
		Int("phase", phase).
		Str("worker", worker).
		Bool("is_empty_enough", isEmptyEnough).
		Logger()

	l.Debug().Int("unmerged_ops", len(unmergedOps)).Msg("other worker phase done")

	if isEmptyEnough {
		l.Debug().Msg("done by worker")

		dw.ctxcancel(distributedStatesMergerWorkerDone)
	}

	return nil
}

func (dw *DistributedStatesMergerWorker) workerPhaseDone(phase int, worker string, unmergedOps []uint64) (isClosed, isEmptyEnough bool, _ error) {
	l := dw.Log().With().
		Int("phase", phase).
		Str("worker", worker).
		Int("unmerged_ops", len(unmergedOps)).
		Logger()

	var added, isMergeDoneEnough bool

	if added, isEmptyEnough, isMergeDoneEnough = dw.setPhaseDoneRecord(phase, worker, len(unmergedOps) > 0); !added {
		return false, false, errors.Errorf("worker, %q already phase done", worker)
	}

	switch {
	case isEmptyEnough:
		l.Debug().Msg("is empty enough")

		return true, true, nil
	case isMergeDoneEnough:
		l.Debug().Msg("is enough")

		return true, false, dw.mergeDone(phase + 1)
	}

	return isMergeDoneEnough || isEmptyEnough, isEmptyEnough, nil
}

func (dw *DistributedStatesMergerWorker) psm(phase int) (psm *phaseStatesMerger, _ error) {
	err := dw.psms.GetOrCreate(
		phase,
		func(i *phaseStatesMerger, _ bool) error {
			psm = i

			return nil
		},
		func() (*phaseStatesMerger, error) {
			i, err := newPhaseStatesMerger(
				dw.ctx(),
				dw.mergers.Value,
				dw.valuespool.Value,
				dw.args.GetStateValue,
				dw.args.NewDistributedWorker,
				dw.args.MapSize,
			)
			if err != nil {
				return nil, err
			}
			i.workerName = fmt.Sprintf("%s-%d", dw.workerName, phase)

			go func() {
				if err := dw.localPhaseDone(phase, i); err != nil {
					dw.ctxcancel(err)
				}
			}()

			return i, nil
		},
	)

	return psm, err
}

func (dw *DistributedStatesMergerWorker) setInsertDoneRecord(phase int, worker string) (added, isMergeDoneEnough bool) {
	isLocalFound := worker == localStatesMergerWorker

	var ne uint64

	_, _, _ = dw.phaseDoneRecords.Set(phase, func(prev map[string]bool, found bool) (map[string]bool, error) {
		if !found {
			added = true

			ne = 1

			return map[string]bool{worker: false}, nil
		}

		if _, found := prev[worker]; found {
			return nil, util.ErrLockedSetIgnore
		}

		added = true

		prev[worker] = false

		_, isLocalFound = prev[localStatesMergerWorker]

		ne = uint64(len(prev))

		return prev, nil
	})

	if !isLocalFound {
		return added, false
	}

	return added, dw.args.IsEnoughWorkers(ne)
}

func (dw *DistributedStatesMergerWorker) setPhaseDoneRecord(phase int, worker string, hasUnmergeOps bool) (added, isEmptyEnough, isMergeDoneEnough bool) {
	isLocalFound := worker == localStatesMergerWorker
	isLocalEmptyDone := worker == localStatesMergerWorker && !hasUnmergeOps

	var ne, nee uint64

	_, _, _ = dw.phaseDoneRecords.Set(phase, func(prev map[string]bool, found bool) (map[string]bool, error) {
		if !found {
			added = true
			ne = 1

			if !hasUnmergeOps {
				nee = 1
			}

			return map[string]bool{worker: hasUnmergeOps}, nil
		}

		if _, found := prev[worker]; found {
			return nil, util.ErrLockedSetIgnore
		}

		added = true

		prev[worker] = hasUnmergeOps

		if i, found := prev[localStatesMergerWorker]; found {
			isLocalFound = true
			isLocalEmptyDone = !i
		}

		for i := range prev {
			ne++

			if !prev[i] {
				nee++
			}
		}

		return prev, nil
	})

	if isLocalFound {
		isMergeDoneEnough = dw.args.IsEnoughWorkers(ne)
	}

	if !isLocalEmptyDone {
		return added, false, isMergeDoneEnough
	}

	return added, dw.args.IsEnoughWorkers(nee), isMergeDoneEnough
}

func (dw *DistributedStatesMergerWorker) setTempWorkerPhaseDone(phase int, worker string, unmergedOps []uint64) (added bool) {
	var m map[string][]uint64

	switch i, found := dw.tempWorkerPhaseDone[phase]; {
	case !found:
		m = map[string][]uint64{}
	case i == nil:
		return false
	default:
		m = i
	}

	var found bool

	if _, found = m[worker]; !found {
		m[worker] = unmergedOps
	}

	dw.tempWorkerPhaseDone[phase] = m

	return !found
}

func (dw *DistributedStatesMergerWorker) dumpTempWorkerPhaseDone(phase int, localUnmergedOps []uint64) (unmerged bool, _ error) {
	addOp, doneOp := util.UniqCompactAppendSlice[uint64](dw.valuespool.Len())

	_ = util.TraverseSliceV1(localUnmergedOps, func(_ int, opindex uint64) (bool, error) {
		addOp(opindex)

		return true, nil
	})

	switch m, found := dw.tempWorkerPhaseDone[phase]; {
	case !found:
	default:
		for worker := range m {
			_ = util.TraverseSliceV1(m[worker], func(_ int, opindex uint64) (bool, error) {
				addOp(opindex)

				return true, nil
			})
		}
	}

	dw.tempWorkerPhaseDone[phase] = nil

	unmergedOps := doneOp()
	if len(unmergedOps) < 1 {
		return false, nil
	}

	return true, dw.unmerge(phase+1, unmergedOps)
}

func (dw *DistributedStatesMergerWorker) mergeDone(phase int) error {
	if !dw.psms.Exists(phase) {
		go func() {
			if err := dw.localPhaseDone(phase, nil); err != nil {
				dw.ctxcancel(err)
			}
		}()

		return nil
	}

	switch psm, err := dw.psm(phase); {
	case err != nil:
		return err
	default:
		return psm.MergeDone()
	}
}

func (dw *DistributedStatesMergerWorker) unmerge(phase int, unmergedOps []uint64) error {
	var psm *phaseStatesMerger

	switch i, err := dw.psm(phase); {
	case err != nil:
		return err
	default:
		psm = i
	}

	addOps, doneOps := util.CompactAppendSlice[uint64](len(unmergedOps))

	if _, _, err := dw.phaseOps.Set(phase, func(m map[uint64]struct{}, found bool) (map[uint64]struct{}, error) {
		if !found {
			m = map[uint64]struct{}{}
		}

		if err := util.TraverseSliceV1(unmergedOps, func(_ int, opindex uint64) (bool, error) {
			if _, found := m[opindex]; found {
				return true, nil
			}

			if !dw.valuespool.Exists(opindex) {
				return false, util.ErrNotFound.Errorf("unknown operation")
			}

			m[opindex] = struct{}{}

			_ = addOps(opindex)

			return true, nil
		}); err != nil {
			return nil, err
		}

		return m, nil
	}); err != nil {
		return err
	}

	return util.TraverseSliceV1(doneOps(), func(_ int, opindex uint64) (bool, error) {
		return true, psm.Unmerge(opindex)
	})
}

func (dw *DistributedStatesMergerWorker) updatedValues(phase int) phaseStatesMergerDoneUpdatedValuesFunc {
	dw.Log().Debug().Int("phase", phase).Msg("updated values")

	return func(stateKey string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
		if _, _, err := dw.mergers.Set(stateKey, func(_ StateMerger, found bool) (StateMerger, error) {
			return merger, dw.args.UpdatedValuesFunc(phase, stateKey, merger, mergedOps, unmergedOps, maxUpdatedValues)
		}); err != nil {
			return err
		}

		return nil
	}
}

func (dw *DistributedStatesMergerWorker) updatedReasons(phase int) phaseStatesMergerDoneUpdatedReasonsFunc {
	dw.Log().Debug().Int("phase", phase).Msg("updated reasons")

	return func(opindex uint64, reason string) error {
		dw.Log().Debug().Int("phase", phase).Uint64("opindex", opindex).Msg("update reasons")

		return dw.args.UpdateReasonsFunc(phase, opindex, reason)
	}
}
