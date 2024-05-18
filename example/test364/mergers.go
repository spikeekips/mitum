package testopsworkers

import (
	"cmp"
	"context"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var (
	ErrStatesMergerDone = util.NewIDError("done")
	// NOTE no more new merge/unmerge ops
	ErrPhaseStatesMergerAlreadyClosed = util.NewIDError("phase states merger already closed")
)

var localStatesMergerWorker = "local"

type DistributedStatesMergerWorkerArgs struct {
	DoneEnough                func(n uint64) bool
	GetStateValue             func(key string) (base.StateValue, bool, error)
	BroadcastPhaseDoneFunc    func(phase int, unmergedOps []uint64) error
	SyncMergedStateValuesFunc func(_ context.Context, phase int, keys []string, values []base.StateValue) error
	NewDistributedWorker      func(context.Context) (distributedWorker, error)
	WhenDoneFunc              func(phase int) error
	SaveState                 func(_ context.Context, key string, value base.StateValue, ops []uint64) error
	SaveOperationState        func(_ context.Context, opindex uint64, inState bool, reason base.OperationProcessReasonError) error
	isLocalKey                func(key string) bool
	Mapsize                   uint64
}

func NewDistributedStatesMergerWorkerArgs() *DistributedStatesMergerWorkerArgs {
	return &DistributedStatesMergerWorkerArgs{
		DoneEnough:                func(uint64) bool { return false },
		GetStateValue:             func(string) (base.StateValue, bool, error) { return nil, false, nil },
		BroadcastPhaseDoneFunc:    func(int, []uint64) error { return nil },
		SyncMergedStateValuesFunc: func(context.Context, int, []string, []base.StateValue) error { return nil },
		NewDistributedWorker: func(ctx context.Context) (distributedWorker, error) {
			return util.NewBaseJobWorker(ctx, 1<<6)
		},
		WhenDoneFunc:       func(int) error { return nil },
		SaveState:          func(context.Context, string, base.StateValue, []uint64) error { return nil },
		SaveOperationState: func(context.Context, uint64, bool, base.OperationProcessReasonError) error { return nil },
		isLocalKey:         func(string) bool { return true },
		Mapsize:            1 << 9,
	}
}

type DistributedStatesMergerWorker struct {
	*logging.Logging
	args                        *DistributedStatesMergerWorkerArgs
	gctx                        func() context.Context
	gcancel                     func(error)
	dctx                        func() context.Context
	dcancel                     func(error)
	phases                      [] /* phase */ *phaseStatesMerger
	mergers                     util.LockedMap[string /* state key */, StateMerger]
	valuespool                  util.LockedMap[uint64 /* opindex */, []StateMergeValue]
	phasesState                 util.LockedMap[int /* phase */, uint8 /* 1: no more merge/unmerge 2: local done */]
	statesMergedOps             util.LockedMap[string /* state key */, []uint64 /* ops */]
	tempWorkerPhaseDone         map[int] /* phase */ map[string] /* worker */ [2][]uint64
	allMergedOps                map[uint64] /* opindex */ struct{}
	isDone                      bool
	lastLocalPhaseDone          int
	phaseDoneRecords            util.LockedMap[int /* phase */, map[string] /* worker */ bool /* has unmerged ops */]
	syncMergedStateValuesWorker distributedWorker
	saveWorker                  distributedWorker
}

func newDistributedStatesMergerWorker(
	ctx context.Context,
	local string,
	args *DistributedStatesMergerWorkerArgs,
) (*DistributedStatesMergerWorker, error) {
	gctx, gcancel := context.WithCancelCause(ctx)
	dctx, dcancel := context.WithCancelCause(gctx)

	dm := &DistributedStatesMergerWorker{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "distributed-states-merger").Str("local", local)
		}),
		args:                args,
		gctx:                func() context.Context { return gctx },
		gcancel:             gcancel,
		dctx:                func() context.Context { return dctx },
		dcancel:             dcancel,
		tempWorkerPhaseDone: map[int]map[string][2][]uint64{},
		allMergedOps:        map[uint64]struct{}{},
		phaseDoneRecords:    util.NewSingleLockedMap[int, map[string]bool](),
	}

	switch i, err := util.NewShardedMap[string, StateMerger](args.Mapsize, nil); {
	case err != nil:
		return nil, err
	default:
		dm.mergers = i
	}

	switch i, err := util.NewShardedMap[uint64, []StateMergeValue](args.Mapsize, nil); {
	case err != nil:
		return nil, err
	default:
		dm.valuespool = i
	}

	switch i, err := util.NewShardedMap[int, uint8](args.Mapsize, nil); {
	case err != nil:
		return nil, err
	default:
		dm.phasesState = i
	}

	switch i, err := util.NewShardedMap[string, []uint64](args.Mapsize, nil); {
	case err != nil:
		return nil, err
	default:
		dm.statesMergedOps = i
	}

	switch i, err := args.NewDistributedWorker(dctx); {
	case err != nil:
		return nil, err
	default:
		dm.syncMergedStateValuesWorker = i
	}

	switch i, err := args.NewDistributedWorker(gctx); {
	case err != nil:
		return nil, err
	default:
		dm.saveWorker = i
	}

	return dm, nil
}

func (dm *DistributedStatesMergerWorker) Done() error {
	defer dm.cleanDone()

	if err := dm.checkIsDone(); err != nil {
		return err
	}

	return dm.startSave()
}

func (dm *DistributedStatesMergerWorker) SaveDone() error {
	if err := dm.checkIsDone(); err != nil {
		return err
	}

	dm.saveWorker.Done()

	return dm.saveWorker.Wait()
}

func (dm *DistributedStatesMergerWorker) Cancel() {
	defer dm.clean()

	_ = dm.cancel(nil)
}

func (dm *DistributedStatesMergerWorker) LocalMerge(opindex uint64, values []StateMergeValue) error {
	return dm.workerMerge(localStatesMergerWorker, opindex, values)
}

func (dm *DistributedStatesMergerWorker) WorkerMerge(worker string, opindex uint64, values []StateMergeValue) error {
	return dm.workerMerge(worker, opindex, values)
}

func (dm *DistributedStatesMergerWorker) workerMerge(worker string, opindex uint64, values []StateMergeValue) error {
	filtered := values

	if values != nil {
		filtered = dm.filterLocalValues(values)
		if len(filtered) < 1 {
			dm.Log().Debug().Uint64("opindex", opindex).Msg("none-local values found")

			return nil
		}
	}

	return dm.underGetPhasesState(0, func(state uint8, _ bool) error {
		switch {
		case state > 0:
			return ErrPhaseStatesMergerAlreadyClosed.WithStack()
		case filtered == nil && !dm.setPhaseDoneRecords(0, worker, false):
			return nil
		}

		if values != nil {
			if _, created, _ := dm.valuespool.Set(opindex, func(_ []StateMergeValue, found bool) ([]StateMergeValue, error) {
				if found {
					return nil, util.ErrLockedSetIgnore
				}

				return filtered, nil
			}); !created {
				return util.ErrFound.Errorf("already merged initially, %d", opindex)
			}
		}

		if filtered == nil {
			switch isDone, err := dm.checkInitialPhaseDone(); {
			case err != nil:
				return err
			case !isDone:
				return nil
			}
		}

		dm.Log().Debug().
			Str("worker", worker).
			Uint64("opindex", opindex).
			Int("values", len(filtered)).
			Msg("merge")

		switch psm, err := dm.newPhaseMerger(1); {
		case err != nil:
			return err
		default:
			return psm.Merge(opindex, filtered)
		}
	})
}

func (dm *DistributedStatesMergerWorker) WorkerPhaseDone(worker string, phase int, unmergedOps []uint64) error {
	l := dm.Log().With().
		Str("worker", worker).
		Int("phase", phase).
		Uints64("unmerged_ops", unmergedOps).
		Logger()

	if err := dm.workerPhaseDone(phase, worker, unmergedOps); err != nil {
		l.Error().Err(err).Msg("worker done failed")

		return err
	}

	l.Debug().Msg("worker done")

	return nil
}

func (dm *DistributedStatesMergerWorker) workerPhaseDone(phase int, worker string, unmergedOps []uint64) error {
	if err := dm.validateOtherWorkerDone(worker, phase, unmergedOps); err != nil {
		return err
	}

	var isPhaseClosed bool

	if err := dm.underSetPhasesState(phase, func(state uint8, _ bool) (uint8, error) {
		switch {
		case state == 1: // NOTE if closed, ignore
			return 0, ErrPhaseStatesMergerAlreadyClosed.WithStack()
		case
			!dm.setTempWorkerPhaseDone(phase, worker, unmergedOps),
			!dm.setPhaseDoneRecords(phase, worker, len(unmergedOps) > 0):
			return 0, errors.Errorf("worker already done, phase %d", phase)
		}

		if state == 0 {
			switch isClosed, err := dm.workerPhaseDoneBeforeLocalIsDone(phase); {
			case err != nil:
				return 0, err
			case isClosed:
				isPhaseClosed = true

				return 1, nil
			}
		}

		if state == 2 {
			if _, err := dm.checkDone(phase); err != nil {
				return 0, err
			}
		}

		return 0, util.ErrLockedSetIgnore
	}); err != nil {
		return err
	}

	if isPhaseClosed {
		var emptyPSM bool

		if err := dm.underGetPhasesState(phase, func(state uint8, _ bool) error {
			if state != 1 {
				return nil
			}

			switch psm, err := dm.existingPhaseMerger(phase, false); {
			case err != nil:
				return err
			case psm == nil: // NOTE psm is not created, means local phase is done
				emptyPSM = true
			}

			return nil
		}); err != nil {
			return err
		}

		if emptyPSM {
			if err := dm.localPhaseDone(phase, nil); err != nil {
				return err
			}
		}
	}

	return nil
}

func (dm *DistributedStatesMergerWorker) cancel(err error) error {
	dm.gcancel(err)

	return err
}

func (dm *DistributedStatesMergerWorker) clean() {
	clear(dm.allMergedOps)

	dm.syncMergedStateValuesWorker.Close()
	dm.saveWorker.Close()
	dm.mergers.Close()
	dm.valuespool.Close()
	dm.phasesState.Close()
	dm.statesMergedOps.Close()
	dm.phaseDoneRecords.Close()
}

func (dm *DistributedStatesMergerWorker) cleanDone() {
	dm.phasesState.Close()
	dm.syncMergedStateValuesWorker.Close()
	dm.phaseDoneRecords.Close()
}

func (dm *DistributedStatesMergerWorker) merge(phase int, opindex uint64, values []StateMergeValue) error {
	l := dm.Log().With().
		Int("phase", phase).
		Uint64("opindex", opindex).
		Int("values", len(values)).
		Logger()

	var psm *phaseStatesMerger

	switch i, err := func() (*phaseStatesMerger, error) {
		switch {
		case values == nil:
			return dm.existingPhaseMerger(phase, true)
		default:
			return dm.newPhaseMerger(phase)
		}
	}(); {
	case err != nil:
		return err
	default:
		psm = i
	}

	if err := psm.Merge(opindex, values); err != nil {
		l.Error().Err(err).Msg("merge failed")

		return err
	}

	l.Debug().Msg("merged")

	return nil
}

func (dm *DistributedStatesMergerWorker) validateOtherWorkerDone(worker string, phase int, unmergedOps []uint64) error {
	switch {
	case worker == localStatesMergerWorker:
		return util.ErrInvalid.Errorf("worker is local")
	case phase < 0:
		return util.ErrInvalid.Errorf("under zero phase")
	case isDuplicatedSlice(unmergedOps, func(i uint64) (bool, uint64) { return true, i }):
		return errors.Errorf("duplicated item found in unmerged ops")
	}

	return nil
}

func (dm *DistributedStatesMergerWorker) workerPhaseDoneBeforeLocalIsDone(phase int) (isClosed bool, _ error) {
	others := dm.tempWorkerPhaseDone[phase]
	nOthers := uint64(len(others))

	if nOthers > 0 {
		addUnmergedOps, doneUnmergedOps := util.UniqCompactAppendSlice[uint64](dm.valuespool.Len())

		for o := range others {
			if err := util.TraverseSlice(
				others[o][1],
				func(_ int, opindex uint64) error {
					addUnmergedOps(opindex)

					return nil
				},
			); err != nil {
				return false, err
			}
		}

		dm.tempWorkerPhaseDone[phase] = nil // NOTE no more used

		if err := util.TraverseSlice(doneUnmergedOps(), func(_ int, opindex uint64) error {
			if values, found := dm.valuespool.Value(opindex); found {
				return dm.merge(phase, opindex, values)
			}

			return nil
		}); err != nil {
			return false, err
		}
	}

	if dm.args.DoneEnough(nOthers + 1) {
		if phase > 1 {
			// NOTE enough from others, make phase states merger 'done'
			if err := dm.merge(phase, 0, nil); err != nil {
				return false, err
			}
		}

		isClosed = true
	}

	return isClosed, nil
}

func (dm *DistributedStatesMergerWorker) cancelPhases() {
	for i := range dm.phases {
		dm.phases[i].Cancel()
	}
}

func (dm *DistributedStatesMergerWorker) existingPhaseMerger(phase int, fill bool) (psm *phaseStatesMerger, _ error) {
	// NOTE first phase starts from 1
	l := len(dm.phases)

	switch {
	case phase == l:
		return dm.phases[phase-1], nil
	case phase-1 == l: // NOTE expected next phase
	case phase-1 > l:
		return nil, errors.Errorf("too higher phase, %d > %d", phase, l)
	default:
		return nil, ErrPhaseStatesMergerAlreadyClosed.WithStack()
	}

	if fill {
		phases := make([]*phaseStatesMerger, phase)
		if l > 0 {
			copy(phases, dm.phases)
		}

		phases[phase-1] = nil
		dm.phases = phases
	}

	return nil, nil
}

func (dm *DistributedStatesMergerWorker) newPhaseMerger(phase int) (psm *phaseStatesMerger, _ error) {
	switch i, err := dm.existingPhaseMerger(phase, false); {
	case err != nil:
		return nil, err
	case i != nil:
		return i, nil
	}

	l := len(dm.phases)

	switch i, phaseDone, err := dm.newPhaseStatesMerger(phase); {
	case err != nil:
		return nil, err
	default:
		psm = i

		phases := make([]*phaseStatesMerger, phase)
		if l > 0 {
			copy(phases, dm.phases)
		}

		phases[phase-1] = psm
		dm.phases = phases

		go func() {
			if err := phaseDone(); err != nil {
				_ = dm.cancel(err)
			}
		}()

		return psm, nil
	}
}

func (dm *DistributedStatesMergerWorker) newPhaseStatesMerger(phase int) (
	psm *phaseStatesMerger,
	phaseDone func() error,
	_ error,
) {
	l := dm.Log().With().Int("phase", phase).Logger()

	switch i, err := newPhaseStatesMerger(
		dm.gctx(),
		dm.mergers.Value,
		dm.valuespool.Value,
		dm.args.NewDistributedWorker,
		dm.args.GetStateValue,
		dm.statesMergedOps.Value,
		dm.args.Mapsize,
	); {
	case err != nil:
		l.Error().Err(err).Msg("new phase states merger failed")

		return nil, nil, err
	default:
		l.Debug().Msg("new phase states merger created")

		psm = i

		_ = psm.SetLogging(dm.Logging)
	}

	return psm, func() error {
		if err := dm.localPhaseDone(phase, psm); err != nil {
			l.Error().Err(err).Msg("phase states merger done failed")

			return err
		}

		l.Debug().Msg("phase states merger done")

		return nil
	}, nil
}

func (dm *DistributedStatesMergerWorker) localPhaseDone(phase int, psm *phaseStatesMerger) error {
	var unmergedOps []uint64

	updated := func(phaseStatesMergerUpdatedFunc) error {
		return nil
	}

	if psm != nil {
		switch _, i, j, err := psm.Done(); { // FIXME unmergedOps []uint64 -> map[opindex]base.OperationProcessReasonError
		case err != nil:
			return err
		default:
			unmergedOps = i
			updated = j
		}
	}

	if err := dm.underSetPhasesState(phase, func(state uint8, found bool) (uint8, error) {
		if state == 2 {
			return 0, ErrPhaseStatesMergerAlreadyClosed.WithStack()
		}

		updatedf, donef := dm.phaseDoneUpdateStates(phase)
		if err := updated(updatedf); err != nil {
			return 0, err
		}

		if err := donef(); err != nil {
			return 0, err
		}

		dm.lastLocalPhaseDone = phase

		_ = dm.setPhaseDoneRecords(phase, localStatesMergerWorker, len(unmergedOps) > 0)

		go dm.broadcastPhaseDone(phase, unmergedOps)

		// FIXME upload merged states info to manager

		if _, err := dm.checkDone(phase); err != nil {
			return 0, err
		}

		return 2, nil
	}); err != nil {
		return err
	}

	return nil
}

func (dm *DistributedStatesMergerWorker) phaseDoneUpdateStates(phase int) (
	_ phaseStatesMergerUpdatedFunc,
	done func() error,
) {
	var addKeys func(string) bool
	var addValues func(base.StateValue) bool
	doneKeys := func() []string { return nil }
	doneValues := func() []base.StateValue { return nil }

	var once sync.Once

	return func(key string, merger StateMerger, mergedOps []uint64, maxMergersLength int) error {
			once.Do(func() {
				addKeys, doneKeys = util.CompactAppendSlice[string](maxMergersLength)
				addValues, doneValues = util.CompactAppendSlice[base.StateValue](maxMergersLength)
			})

			_ = addKeys(key)

			if len(mergedOps) < 1 {
				_ = addValues(nil)
			} else {
				switch stv, err := merger.Close(); {
				case err != nil:
					return err
				default:
					_ = addValues(stv)
				}
			}

			_ = dm.mergers.SetValue(key, merger)

			if _, _, _, err := dm.statesMergedOps.SetOrRemove(key, func(ops []uint64, _ bool) ([]uint64, bool, error) {
				if len(ops) > 0 {
					for i := range ops {
						if opindex := ops[i]; slices.Index(mergedOps, opindex) < 0 {
							delete(dm.allMergedOps, opindex)
						}
					}
				}

				if len(mergedOps) < 1 {
					return nil, true, nil // NOTE remove existing state key
				}

				// NOTE store all the merged ops
				for i := range mergedOps {
					opindex := mergedOps[i]

					if _, found := dm.allMergedOps[opindex]; !found {
						dm.allMergedOps[opindex] = struct{}{}
					}
				}

				return mergedOps, false, nil
			}); err != nil {
				return err
			}

			return nil
		}, func() error {
			return dm.syncMergedStateValues(phase, doneKeys(), doneValues())
		}
}

func (dm *DistributedStatesMergerWorker) broadcastPhaseDone(phase int, unmergedOps []uint64) {
	if err := dm.args.BroadcastPhaseDoneFunc(phase, unmergedOps); err != nil {
		_ = dm.cancel(err)
	}
}

func (dm *DistributedStatesMergerWorker) setPhaseDoneRecords(phase int, worker string, hasUnmergedOps bool) bool {
	var added bool

	_, _, _ = dm.phaseDoneRecords.Set(phase, func(m map[string]bool, found bool) (map[string]bool, error) {
		switch {
		case found:
			if _, workerfound := m[worker]; workerfound {
				return nil, util.ErrLockedSetIgnore
			}
		default:
			m = map[string]bool{}
		}

		added = true
		m[worker] = hasUnmergedOps

		return m, nil
	})

	return added
}

func (dm *DistributedStatesMergerWorker) setTempWorkerPhaseDone(phase int, worker string, unmergedOps []uint64) bool {
	var m map[string][2][]uint64

	switch i, found := dm.tempWorkerPhaseDone[phase]; {
	case !found:
		m = map[string][2][]uint64{}
	default:
		if _, found := m[worker]; found {
			return false
		}

		m = i
	}

	m[worker] = [2][]uint64{unmergedOps, dm.filterUnmerges(unmergedOps)}

	dm.tempWorkerPhaseDone[phase] = m

	return true
}

func (dm *DistributedStatesMergerWorker) filterUnmerges(unmergedOps []uint64) []uint64 {
	if len(unmergedOps) < 1 {
		return nil
	}

	ok := func(opindex uint64) bool {
		_, found := dm.allMergedOps[opindex]

		return found
	}

	addUnmerge, doneUnmerge := util.UniqCompactAppendSlice[uint64](len(unmergedOps))

	for i := range unmergedOps {
		if opindex := unmergedOps[i]; ok(opindex) {
			addUnmerge(opindex)
		}
	}

	return doneUnmerge()
}

func (dm *DistributedStatesMergerWorker) checkInitialPhaseDone() (bool, error) {
	var m map[string]bool

	switch i, found := dm.phaseDoneRecords.Value(0); {
	case !found:
		return false, util.ErrNotFound.Errorf("initial phase")
	default:
		m = i
	}

	return dm.args.DoneEnough(uint64(len(m))), nil
}

func (dm *DistributedStatesMergerWorker) checkDone(phase int) (bool, error) {
	l := dm.Log().With().Int("phase", phase).Logger()

	if phase > dm.lastLocalPhaseDone {
		l.Debug().Int("local_phase_done", dm.lastLocalPhaseDone).Msg("higher phase; ignored")

		return false, nil
	}

	var m map[string]bool

	switch i, found := dm.phaseDoneRecords.Value(phase); {
	case !found:
		return false, nil
	default:
		m = i
	}

	var noMoreNewUnmergedOps uint64

	for i := range m {
		if !m[i] {
			noMoreNewUnmergedOps++
		}
	}

	if !dm.args.DoneEnough(noMoreNewUnmergedOps) {
		l.Debug().Msg("not yet done")

		return false, nil
	}

	dm.isDone = true

	dm.cancelPhases()

	// NOTE wait until all states to upload to manager
	dm.syncMergedStateValuesWorker.Done()

	if err := dm.syncMergedStateValuesWorker.Wait(); err != nil {
		return false, err
	}

	dm.dcancel(ErrStatesMergerDone)

	if err := dm.args.WhenDoneFunc(phase); err != nil {
		return false, err
	}

	l.Debug().Msg("done")

	return true, nil
}

func (dm *DistributedStatesMergerWorker) underGetPhasesState(
	phase int,
	f func(state uint8, found bool) error,
) error {
	return dm.phasesState.Get(phase, func(state uint8, found bool) error {
		switch {
		case dm.isDone:
			return ErrPhaseStatesMergerAlreadyClosed.WithStack()
		case phase < dm.lastLocalPhaseDone:
			return errors.Errorf("worker done, but old phase, %d", phase)
		default:
			return f(state, found)
		}
	})
}

func (dm *DistributedStatesMergerWorker) underSetPhasesState(
	phase int,
	f func(state uint8, found bool) (uint8, error),
) error {
	_, _, err := dm.phasesState.Set(phase, func(state uint8, found bool) (uint8, error) {
		switch {
		case dm.isDone:
			return 0, ErrPhaseStatesMergerAlreadyClosed.WithStack()
		case phase < dm.lastLocalPhaseDone:
			return 0, errors.Errorf("worker done, but old phase, %d", phase)
		default:
			return f(state, found)
		}
	})

	return err
}

func (dm *DistributedStatesMergerWorker) syncMergedStateValues(phase int, keys []string, values []base.StateValue) error {
	return dm.syncMergedStateValuesWorker.NewJob(func(ctx context.Context, _ uint64) error {
		if len(keys) != len(values) {
			return errors.Errorf("len(keys) != len(values)")
		}

		return dm.args.SyncMergedStateValuesFunc(ctx, phase, keys, values)
	})
}

func (dm *DistributedStatesMergerWorker) checkIsDone() error {
	defer dm.cleanDone()

	select {
	case <-dm.gctx().Done():
		return dm.gctx().Err()
	case <-dm.dctx().Done():
	}

	if !dm.isDone {
		return errors.WithStack(context.Canceled)
	}

	if err := context.Cause(dm.dctx()); !errors.Is(err, ErrStatesMergerDone) {
		return errors.WithStack(err)
	}

	return nil
}

func (dm *DistributedStatesMergerWorker) startSave() error {
	var gerr error

	_ = dm.mergers.Traverse(func(key string, merger StateMerger) bool {
		if err := dm.newJobSaveState(key, merger); err != nil {
			gerr = err
		}

		return gerr == nil
	})

	return gerr
}

func (dm *DistributedStatesMergerWorker) newJobSaveState(key string, merger StateMerger) error {
	return dm.saveWorker.NewJob(func(ctx context.Context, _ uint64) error {
		var ops []uint64

		switch i, found := dm.statesMergedOps.Value(key); {
		case !found:
			return nil
		default:
			ops = i
		}

		switch value, err := merger.Close(); {
		case err != nil:
			return err
		default:
			return dm.args.SaveState(ctx, key, value, ops)
		}
	})
}

func (dm *DistributedStatesMergerWorker) filterLocalValues(values []StateMergeValue) []StateMergeValue {
	return util.FilterSlice(values, func(value StateMergeValue) bool {
		return dm.args.isLocalKey(value.Key())
	})
}

func isDuplicatedSlice[T any, Y cmp.Ordered](s []T, f func(T) (bool, Y)) bool {
	if len(s) < 1 {
		return false
	}

	m := map[Y]T{}

	for i := range s {
		keep, k := f(s[i])
		if _, found := m[k]; found {
			return true
		}

		if !keep {
			return false
		}

		m[k] = s[i]
	}

	return false
}
