package testopsworkers

import (
	"cmp"
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	phaseStatesMergerInitialXMergeFunc      func(opindex uint64) error
	phaseStatesMergerDoneUpdatedValuesFunc  func(stateKey string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error
	phaseStatesMergerDoneUpdatedReasonsFunc func(opindex uint64, reason string) error
	phaseStatesMergerDoneUpdateFunc         func(
		phaseStatesMergerDoneUpdatedValuesFunc,
		phaseStatesMergerDoneUpdatedReasonsFunc,
	) error
	phaseStatesMergerDoneFunc func() (
		newlyMerged, newlyUnmerged []uint64,
		update phaseStatesMergerDoneUpdateFunc,
		_ error,
	)
)

type phaseStatesMerger struct {
	*logging.Logging
	prevMerger           func(stateKey string) (StateMerger, bool)
	opValues             func(opindex uint64) ([]StateMergeValue, bool)
	stateValue           func(context.Context, string) (base.StateValue, bool, error)
	newJobWorker         func(context.Context) (util.JobWorker, error)
	mergers              *util.MultiKeysLockedMap[string /* state key */, StateMerger]
	opStatus             util.LockedMap[uint64 /* opindex */, [3]interface{} /* {initial xMergeFlag, swphase, xMergeFlag} */]
	mergedOps            util.LockedMap[string /* state key */, map[ /* opindex */ uint64] /* {swphase, xMergeFlag} */ [2]interface{}]
	reasons              util.LockedMap[uint64 /* opindex */, string /* reason */]
	jobm                 util.LockedMap[string, struct{}]
	ctx                  func() context.Context
	ctxCancel            func()
	initialMergeFunc     phaseStatesMergerInitialXMergeFunc
	initialUnmergeFunc   phaseStatesMergerInitialXMergeFunc
	initialMergeDoneFunc func() error
	doneFunc             phaseStatesMergerDoneFunc
	workerName           string // NOTE for testing
}

func newPhaseStatesMerger(
	ctx context.Context,
	prevMerger func(stateKey string) (StateMerger, bool),
	opValues func(opindex uint64) ([]StateMergeValue, bool),
	stateValue func(context.Context, string) (base.StateValue, bool, error),
	newJobWorker func(context.Context) (util.JobWorker, error),
	mapsize uint64,
) (*phaseStatesMerger, error) {
	pctx, pctxCancel := context.WithCancel(ctx)

	sm := &phaseStatesMerger{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "phase-states-merger")
		}),
		prevMerger:   prevMerger,
		opValues:     opValues,
		stateValue:   stateValue,
		newJobWorker: newJobWorker,
		ctx:          func() context.Context { return pctx },
	}

	switch i, err := util.NewShardedMap[string, StateMerger](mapsize, nil); {
	case err != nil:
		return nil, err
	default:
		sm.mergers = util.NewMultiKeysLockedMap[string, StateMerger](i)
	}

	switch i, err := util.NewShardedMap[string, map[uint64][2]interface{}](mapsize, nil); {
	case err != nil:
		return nil, err
	default:
		sm.mergedOps = i
	}

	switch i, err := util.NewShardedMap[uint64, [3]interface{}](mapsize, nil); {
	case err != nil:
		return nil, err
	default:
		sm.opStatus = i
	}

	switch i, err := util.NewShardedMap[uint64, string](mapsize, nil); {
	case err != nil:
		return nil, err
	default:
		sm.reasons = i
	}

	switch i, err := util.NewShardedMap[string, struct{}](mapsize, nil); {
	case err != nil:
		return nil, err
	default:
		sm.jobm = i
	}

	var swNewJob func(serialWorkerNewNextJobFunc) error
	var swDone func() error
	var swCancel func()

	switch i, err := NewSerialWorkers(newJobWorker); {
	case err != nil:
		return nil, err
	default:
		swNewJob, swDone, swCancel = i.New(pctx)
	}

	sm.ctxCancel = func() {
		swCancel()
		pctxCancel()
	}

	sm.initialMergeFunc = sm.newInitialMergeFunc(swNewJob)
	sm.initialUnmergeFunc = sm.newInitialUnmergeFunc(swNewJob)
	sm.initialMergeDoneFunc = sm.newInitialMergeDoneFunc(swNewJob)
	sm.doneFunc = sm.newDoneFunc(swDone)

	return sm, nil
}

func (sm *phaseStatesMerger) SetWorkerName(s string) {
	sm.workerName = s

	_ = sm.SetLogger(sm.Log().With().Str("worker_name", s).Logger())
}

func (sm *phaseStatesMerger) Cancel() {
	sm.ctxCancel()

	sm.clean()
}

func (sm *phaseStatesMerger) Merge(opindex uint64) error {
	switch err := sm.initialMergeFunc(opindex); {
	case errors.Is(err, util.ErrLockedMapClosed):
		return context.Canceled
	default:
		return err
	}
}

func (sm *phaseStatesMerger) Unmerge(opindex uint64) error {
	switch err := sm.initialUnmergeFunc(opindex); {
	case errors.Is(err, util.ErrLockedMapClosed):
		return context.Canceled
	default:
		return err
	}
}

func (sm *phaseStatesMerger) MergeDone() error {
	return sm.initialMergeDoneFunc()
}

func (sm *phaseStatesMerger) Done() (
	newlyMerged, newlyUnmerged []uint64,
	_ phaseStatesMergerDoneUpdateFunc,
	_ error,
) {
	return sm.doneFunc()
}

func (sm *phaseStatesMerger) newInitialMergeFunc(
	swNewJob serialWorkerNewJobFunc,
) phaseStatesMergerInitialXMergeFunc {
	return func(opindex uint64) error {
		var values []StateMergeValue

		switch i, found := sm.opValues(opindex); {
		case !found:
			return util.ErrNotFound.Errorf("operation values")
		case len(i) < 1:
			return errors.Errorf("empty values")
		default:
			values = i
		}

		var created bool

		switch _, _, err := sm.opStatus.Set(opindex, func(i [3]interface{}, found bool) (v [3]interface{}, _ error) {
			if found {
				switch _, ok := i[0].(xMergeFlag); {
				case !ok:
					created = true

					return [3]interface{}{mergeFlag, i[1], i[2]}, nil
				default:
					return v, util.ErrLockedSetIgnore
				}
			}

			created = true

			return [3]interface{}{mergeFlag, nil, nil}, nil
		}); {
		case err != nil:
			return err
		case !created:
			return util.ErrFound.Errorf("opindex already merged initially")
		}

		return swNewJob(sm.newJob(
			opindex,
			func(ctx context.Context, _ uint64, swphase int, smNext serialWorkerNewJobFunc) error {
				return sm.smMerge(ctx, swphase, opindex, values, smNext)
			},
		))
	}
}

func (sm *phaseStatesMerger) newInitialUnmergeFunc(
	swNewJob serialWorkerNewJobFunc,
) phaseStatesMergerInitialXMergeFunc {
	return func(opindex uint64) error {
		var values []StateMergeValue

		switch i, found := sm.opValues(opindex); {
		case !found:
			return util.ErrNotFound.Errorf("operation values")
		case len(i) < 1:
			return errors.Errorf("empty values")
		default:
			values = i
		}

		var created bool

		switch _, _, err := sm.opStatus.Set(opindex, func(i [3]interface{}, found bool) (v [3]interface{}, _ error) {
			if found {
				switch _, ok := i[0].(xMergeFlag); {
				case !ok:
					created = true

					return [3]interface{}{unmergeFlag, i[1], i[2]}, nil
				default:
					return v, util.ErrLockedSetIgnore
				}
			}

			created = true

			return [3]interface{}{unmergeFlag, nil, nil}, nil
		}); {
		case err != nil:
			return err
		case !created:
			return util.ErrFound.Errorf("opindex already unmerged initially")
		}

		return swNewJob(sm.newJob(
			opindex,
			func(ctx context.Context, _ uint64, swphase int, smNext serialWorkerNewJobFunc) error {
				return sm.smUnmerge(ctx, swphase, opindex, values, smNext)
			},
		))
	}
}

func (sm *phaseStatesMerger) newInitialMergeDoneFunc(swNewJob serialWorkerNewJobFunc) func() error {
	return func() error {
		return swNewJob(nil)
	}
}

func (sm *phaseStatesMerger) newDoneFunc(swDone func() error) phaseStatesMergerDoneFunc {
	return func() (
		newlyMerged, newlyUnmerged []uint64,
		_ phaseStatesMergerDoneUpdateFunc,
		_ error,
	) {
		if err := swDone(); err != nil {
			return nil, nil, nil, err
		}

		doneMergedOps, doneUnmergedOps := sm.extractUpdatedOpsFromOpStatus()

		unmergedOps := doneUnmergedOps()

		return doneMergedOps(), unmergedOps,
			func(
				updatedValuesf phaseStatesMergerDoneUpdatedValuesFunc,
				reasonf phaseStatesMergerDoneUpdatedReasonsFunc,
			) error {
				defer sm.clean()

				if updatedValuesf != nil {
					if err := sm.doneUpdatedValues(uint64(sm.mergedOps.Len()), updatedValuesf); err != nil {
						return err
					}
				}

				if reasonf != nil {
					if err := sm.doneUpdateReasons(reasonf, unmergedOps); err != nil {
						return err
					}
				}

				return nil
			},
			nil
	}
}

func (sm *phaseStatesMerger) clean() {
	sm.mergers.Close()
	sm.opStatus.Close()
	sm.mergedOps.Close()
	sm.reasons.Close()
	sm.jobm.Close()
}

func (sm *phaseStatesMerger) smMerge(ctx context.Context, swphase int, opindex uint64, values []StateMergeValue, smNext serialWorkerNewJobFunc) error {
	l := sm.Log().With().Int("phase", swphase).Uint64("opindex", opindex).Logger()

	keys := sm.extractKeys(values)

	var nextOps []uint64

	if err := sm.mergers.LockFunc(keys, func(m util.LockedMap[string, StateMerger]) error {
		l.Debug().Strs("state_keys", keys).Msg("mergers.merge")

		flag := mergeFlag

		switch merged, i, err := sm.merge(ctx, swphase, opindex, values, m); {
		case err != nil:
			return err
		case !merged:
			flag = unmergeFlag
		default:
			nextOps = i
		}

		_ = sm.updateMergeOpStatus(swphase, opindex, flag)

		return nil
	}); err != nil {
		return err
	}

	for i := range nextOps {
		nop := nextOps[i]

		if nvalues, found := sm.opValues(nop); found {
			if err := smNext(sm.newJob(
				nop,
				func(ctx context.Context, _ uint64, swphase int, smNext serialWorkerNewJobFunc) error {
					return sm.smUnmerge(ctx, swphase, nop, nvalues, smNext)
				},
			)); err != nil {
				return err
			}
		}
	}

	l.Debug().Interface("values", values).Msg("merge")

	return nil
}

func (sm *phaseStatesMerger) smUnmerge(ctx context.Context, swphase int, opindex uint64, values []StateMergeValue, smNext serialWorkerNewJobFunc) error {
	l := sm.Log().With().Int("phase", swphase).Uint64("opindex", opindex).Logger()

	keys := sm.extractKeys(values)

	var nextOps []uint64

	if err := sm.mergers.LockFunc(keys, func(m util.LockedMap[string, StateMerger]) error {
		l.Debug().Strs("state_keys", keys).Msg("mergers.unmerge")

		switch i, err := sm.unmerge(ctx, swphase, opindex, values, m); {
		case err != nil:
			return err
		default:
			nextOps = i

			_ = sm.updateUnmergeOpStatus(swphase, opindex)

			return nil
		}
	}); err != nil {
		return err
	}

	for i := range nextOps {
		nop := nextOps[i]

		if nvalues, found := sm.opValues(nop); found {
			if err := smNext(sm.newJob(
				nop,
				func(ctx context.Context, _ uint64, swphase int, smNext serialWorkerNewJobFunc) error {
					return sm.smMerge(ctx, swphase, nop, nvalues, smNext)
				},
			)); err != nil {
				return err
			}
		}
	}

	l.Debug().Interface("values", values).Msg("unmerge")

	return nil
}

func (sm *phaseStatesMerger) merge(
	ctx context.Context,
	swphase int,
	opindex uint64,
	values []StateMergeValue,
	m util.LockedMap[string, StateMerger],
) (merged bool, nextOps []uint64, _ error) {
	l := sm.Log().With().Int("phase", swphase).Uint64("opindex", opindex).Logger()

	addKey, doneKey := util.CompactAppendSlice[string](len(values))
	addCommit, doneCommit := util.CompactAppendSlice[func() error](len(values))
	addNextOps, doneNextOps := util.UniqCompactAppendSlice[uint64](len(values))

	flag := mergeFlag

	if err := util.TraverseSliceV1(values, func(i int, value StateMergeValue) (bool, error) {
		addKey(value.Key())

		switch commit, nextop, reason, err := sm.mergeValue(swphase, opindex, value, m); {
		case err != nil:
			return false, err
		case commit == nil:
			l.Debug().Msg("merge op ignored")

			flag = unmergeFlag
			sm.setReason(opindex, reason)

			_ = util.TraverseSliceV1(values[i+1:], func(_ int, value StateMergeValue) (bool, error) {
				addKey(value.Key())

				return true, nil
			})

			return false, nil
		default:
			if nextop != opindex {
				addNextOps(nextop)
				sm.setReason(nextop, reason)
			}

			addCommit(commit)

			return true, nil
		}
	}); err != nil {
		return false, nil, err
	}

	nextOps = doneNextOps()

	l.Debug().Stringer("flag", flag).Uints64("next_ops", nextOps).Msg("merge op")

	_ = util.TraverseSliceV1(doneKey(), func(_ int, stateKey string) (bool, error) {
		_ = sm.updateMergedOps(swphase, opindex, stateKey, flag)

		return true, nil
	})

	if flag == unmergeFlag {
		return false, nil, nil
	}

	if err := sm.runCommits(ctx, doneCommit()); err != nil {
		return false, nil, err
	}

	return true, nextOps, nil
}

func (sm *phaseStatesMerger) unmerge(
	ctx context.Context,
	swphase int,
	opindex uint64,
	values []StateMergeValue,
	m util.LockedMap[string, StateMerger],
) (nextOps []uint64, _ error) {
	l := sm.Log().With().Int("phase", swphase).Uint64("opindex", opindex).Logger()

	addKey, doneKey := util.CompactAppendSlice[string](len(values))
	addCommit, doneCommit := util.CompactAppendSlice[func() error](len(values))
	addNextOps, doneNextOps := util.UniqCompactAppendSlice[uint64](len(values))

	var notUnmerged bool

end:
	for i := range values {
		value := values[i]

		switch commit, nextop, err := sm.unmergeValue(swphase, opindex, value, m); {
		case err != nil:
			return nil, err
		case commit == nil:
			l.Debug().Msg("unmerge op ignored")

			notUnmerged = true

			break end
		default:
			if nextop != opindex {
				addNextOps(nextop)
			}

			addKey(value.Key())
			addCommit(commit)
		}
	}

	if notUnmerged {
		return nil, nil
	}

	l.Debug().Uints64("next_ops", nextOps).Msg("unmerge op")

	nextOps = doneNextOps()

	_ = util.TraverseSliceV1(doneKey(), func(_ int, stateKey string) (bool, error) {
		_ = sm.updateMergedOps(swphase, opindex, stateKey, unmergeFlag)

		return true, nil
	})

	if err := sm.runCommits(ctx, doneCommit()); err != nil {
		return nil, err
	}

	return nextOps, nil
}

func (sm *phaseStatesMerger) mergeValue(
	swphase int,
	opindex uint64,
	value StateMergeValue,
	m util.LockedMap[string, StateMerger],
) (func() error, uint64, string, error) {
	var merger StateMerger

	if err := m.GetOrCreate(
		value.Key(),
		func(v StateMerger, created bool) error {
			merger = v

			return nil
		},
		func() (StateMerger, error) {
			if v, found := sm.prevMerger(value.Key()); found {
				return v.Clone(), nil
			}

			switch v, found, err := sm.stateValue(sm.ctx(), value.Key()); {
			case err != nil:
				return nil, err
			case !found:
				return value.Merger(nil)
			default:
				return value.Merger(v)
			}
		},
	); err != nil {
		return nil, opindex, "", err
	}

	return merger.Merge(opindex, value.Value())
}

func (sm *phaseStatesMerger) unmergeValue(
	swphase int,
	opindex uint64,
	value StateMergeValue,
	m util.LockedMap[string, StateMerger],
) (func() error, uint64, error) {
	var merger StateMerger

	if err := m.GetOrCreate(
		value.Key(),
		func(v StateMerger, _ bool) error {
			merger = v

			return nil
		},
		func() (StateMerger, error) {
			switch i, found := sm.prevMerger(value.Key()); {
			case found:
				return i.Clone(), nil
			default:
				return nil, util.ErrLockedSetIgnore // NOTE if empty merger, ignore operation
			}
		},
	); err != nil {
		return nil, opindex, err
	}

	if merger == nil {
		return nil, opindex, nil
	}

	return merger.Unmerge(opindex, value.Value())
}

func (sm *phaseStatesMerger) newJob(opindex uint64, f serialWorkerNextJobFunc) serialWorkerNewNextJobFunc {
	return func(swphase int) (serialWorkerNextJobFunc, bool, error) {
		switch _, created, err := sm.jobm.Set(fmt.Sprintf("%d-%d", swphase, opindex), func(_ struct{}, found bool) (struct{}, error) {
			if found {
				return struct{}{}, util.ErrLockedSetIgnore
			}

			return struct{}{}, nil
		}); {
		case err != nil:
			return nil, false, err
		case !created:
			return nil, false, nil
		default:
			return f, true, nil
		}
	}
}

func (sm *phaseStatesMerger) extractKeys(values []StateMergeValue) []string {
	addKeys, doneKeys := util.CompactAppendSlice[string](len(values))

	_ = util.TraverseSliceV1(values, func(_ int, v StateMergeValue) (bool, error) {
		addKeys(v.Key())

		return true, nil
	})

	return doneKeys()
}

func (sm *phaseStatesMerger) setReason(opindex uint64, reason string) {
	if len(reason) < 1 {
		return
	}

	_ = sm.reasons.SetValue(opindex, reason)
}

func (sm *phaseStatesMerger) runCommits(ctx context.Context, fs []func() error) error {
	switch i := len(fs); {
	case i < 1:
		return nil
	case i == 1:
		return fs[0]()
	}

	var ew util.JobWorker

	switch i, err := sm.newJobWorker(ctx); {
	case err != nil:
		return err
	default:
		defer i.Close()

		ew = i
	}

	if err := util.TraverseSliceV1(fs, func(_ int, f func() error) (bool, error) {
		return true, ew.NewJob(func(context.Context, uint64) error {
			return f()
		})
	}); err != nil {
		return err
	}

	ew.Done()

	return ew.Wait()
}

func (sm *phaseStatesMerger) updateMergedOps(swphase int, opindex uint64, stateKey string, flag xMergeFlag) bool {
	var updated bool

	_, _, _ = sm.mergedOps.Set(stateKey, func(prev map[uint64][2]interface{}, found bool) (map[uint64][2]interface{}, error) {
		if !found {
			updated = true

			return map[uint64][2]interface{}{opindex: {swphase, flag}}, nil
		}

		switch i, found := prev[opindex]; {
		case found && swphase <= i[0].(int):
			return nil, util.ErrLockedSetIgnore
		default:
			prev[opindex] = [2]interface{}{swphase, flag}

			updated = true

			return prev, nil
		}
	})

	return updated
}

func (sm *phaseStatesMerger) updateMergeOpStatus(swphase int, opindex uint64, flag xMergeFlag) (updated bool) {
	_, _, _ = sm.opStatus.Set(opindex, func(i [3]interface{}, found bool) (v [3]interface{}, _ error) {
		if !found {
			updated = true

			return [3]interface{}{nil, swphase, flag}, nil
		}

		switch prevswphase, ok := i[1].(int); {
		case ok && swphase <= prevswphase:
			return v, util.ErrLockedSetIgnore
		default:
			updated = true

			return [3]interface{}{i[0], swphase, flag}, nil
		}
	})

	return updated
}

func (sm *phaseStatesMerger) updateUnmergeOpStatus(swphase int, opindex uint64) (updated bool) {
	_, _, _ = sm.opStatus.Set(opindex, func(i [3]interface{}, found bool) (v [3]interface{}, _ error) {
		if !found {
			updated = true

			return [3]interface{}{nil, swphase, unmergeFlag}, nil
		}

		switch prevswphase, ok := i[1].(int); {
		case ok && swphase <= prevswphase:
			return v, util.ErrLockedSetIgnore
		default:
			updated = true

			return [3]interface{}{i[0], swphase, unmergeFlag}, nil
		}
	})

	return updated
}

func (sm *phaseStatesMerger) extractUpdatedOpsFromOpStatus() (func() []uint64, func() []uint64) {
	if sm.opStatus.Len() < 1 {
		return func() []uint64 { return nil }, func() []uint64 { return nil }
	}

	addMergedOps, doneMergedOps := util.CompactAppendSlice[uint64](sm.opStatus.Len())
	addUnmergedOps, doneUnmergedOps := util.CompactAppendSlice[uint64](sm.opStatus.Len())

	_ = sm.opStatus.Traverse(func(opindex uint64, v [3]interface{}) bool {
		var initial xMergeFlag

		if i, ok := v[0].(xMergeFlag); ok {
			initial = i
		}

		switch i, ok := v[2].(xMergeFlag); {
		case !ok:
		case initial == i:
		case i == mergeFlag:
			addMergedOps(opindex)
		case i == unmergeFlag:
			addUnmergedOps(opindex)
		}

		return true
	})

	return doneMergedOps, doneUnmergedOps
}

func (sm *phaseStatesMerger) doneUpdatedValues(maxUpdatedValues uint64, f phaseStatesMergerDoneUpdatedValuesFunc) error {
	var gerr error

	_ = sm.mergedOps.Traverse(func(stateKey string, m map[uint64][2]interface{}) bool {
		addMergedOps, doneMergedOps := util.SortCompactAppendSlice[uint64](len(m), cmp.Less[uint64])
		addUnmergedOps, doneUnmergedOps := util.SortCompactAppendSlice[uint64](len(m), cmp.Less[uint64])

		for opindex := range m {
			if m[opindex][0] == nil {
				continue
			}

			switch j, ok := m[opindex][1].(xMergeFlag); {
			case !ok:
				continue
			case j == mergeFlag:
				addMergedOps(opindex)
			case j == unmergeFlag:
				addUnmergedOps(opindex)
			}
		}

		mops, unmops := doneMergedOps(), doneUnmergedOps()
		if len(mops) < 1 && len(unmops) < 1 {
			return true
		}

		var merger StateMerger

		switch i, found := sm.mergers.Map().Value(stateKey); {
		case !found:
		default:
			merger = i
		}

		gerr = f(stateKey, merger, mops, unmops, maxUpdatedValues)

		return gerr == nil
	})

	return gerr
}

func (sm *phaseStatesMerger) doneUpdateReasons(f phaseStatesMergerDoneUpdatedReasonsFunc, unmergedOps []uint64) error {
	return util.TraverseSliceV1(unmergedOps, func(_ int, opindex uint64) (bool, error) {
		reason, _ := sm.reasons.Value(opindex)

		if len(reason) < 1 { // NOTE empty reason ignored
			return true, nil
		}

		return true, f(opindex, reason)
	})
}
