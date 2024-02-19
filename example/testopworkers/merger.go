package testopsworkers

import (
	"context"
	"fmt"
	"slices"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type StateMergeValue interface {
	Key() string
	Value() base.StateValue
	Merger(prev base.StateValue) (StateMerger, error)
}

type StateMerger interface {
	// Key is state key.
	Key() string
	// Close sets StateMerger not to get no more Merge or Unmerge.
	Close() (mergedValue base.StateValue, _ error)
	// Clone clones StateMerger itself.
	Clone() StateMerger
	// Merge merges new (partial) state value to state. unmergeOp is the
	// operation index in proposal to be unmerge after merging the new state
	// value.
	//
	// - If commit is nil, the operation should be ignored.
	// - If unmergeOp is same with the new operation index, it means there is
	// nothing to unmerge.
	Merge(opindex uint64, value base.StateValue) (
		commit func() error,
		unmergeOp uint64,
		_ error,
	)
	// Unmerge cancels merged state value. mergeOp is the operation index
	// in proposal to be merge after canceling the state value.
	//
	// - If commit is nil, the operation should be ignored.
	// - If mergeOp is same with the operation index, it means there is nothing to
	// cancel to merge.
	Unmerge(opindex uint64, value base.StateValue) (
		commit func() error,
		mergeOp uint64,
		_ error,
	)
}

type (
	StateMergeXMergeFunc func(opindex uint64, value base.StateValue) (
		commit func() error,
		nextop uint64,
		_ error,
	)
	statesMergerXMergeFunc            func(opindex uint64, values []StateMergeValue) error
	phaseStatesMergerXMergeStatesFunc func(
		ctx context.Context,
		swphase, opindex uint64,
		values []StateMergeValue,
		smNext serialWorkerAddNextJobFunc,
	) error
	phaseStatesMergerUpdatedFunc func(key string, _ StateMerger, operations []uint64, maxMergersLength int) error
	phaseStatesMergerDoneFunc    func() (
		newlymerged, newlyunmerged []uint64,
		newlyUpdated func(phaseStatesMergerUpdatedFunc) error,
		_ error,
	)
)

type xMergeFlag uint8

const (
	mergeFlag   xMergeFlag = 1
	unmergeFlag xMergeFlag = 2
)

type phaseStatesMerger struct {
	*logging.Logging
	sourceMerger                          func(key string) (StateMerger, bool)
	opValues                              func(opindex uint64) ([]StateMergeValue, bool)
	mergedOps                             util.LockedMap[string /* state key */, map[uint64] /* opindex */ uint64 /*swphase */]
	newWorker                             func(context.Context) (distributedWorker, error)
	getStateValue                         func(string) (base.StateValue, bool, error)
	prevMergedOps                         func(key string) ([]uint64, bool)
	mapsize                               uint64
	eventf                                func(name string, swphase, opindex uint64, format string, a ...any)
	mergers                               *util.MultiKeysLockedMap[string /* state key */, StateMerger]
	mergedStatus                          util.LockedMap[uint64 /* opindex */, [3]uint64 /* initial, swphase, next; 1: merged 2: unmerged*/]
	jobm                                  util.LockedMap[string, struct{}]
	Merge                                 statesMergerXMergeFunc
	Unmerge                               statesMergerXMergeFunc
	Done                                  phaseStatesMergerDoneFunc
	Cancel                                func()
	isPreviouslyMergedAtZeroSWPhaseFunc   func(uint64, uint64, []string) bool
	isPreviouslyUnmergedAtZeroSWPhaseFunc func(uint64, uint64, []string) bool
}

func newPhaseStatesMerger(
	ctx context.Context,
	sourceMerger func(key string) (StateMerger, bool),
	opValues func(opindex uint64) ([]StateMergeValue, bool),
	newWorker func(context.Context) (distributedWorker, error),
	getStateValue func(key string) (base.StateValue, bool, error),
	prevMergedOps func(key string) ([]uint64, bool),
	mapsize uint64,
) (*phaseStatesMerger, error) {
	if prevMergedOps == nil {
		prevMergedOps = func(key string) ([]uint64, bool) { return nil, false }
	}

	sm := &phaseStatesMerger{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "phase-states-merger")
		}),
		sourceMerger:  sourceMerger,
		opValues:      opValues,
		newWorker:     newWorker,
		getStateValue: getStateValue,
		prevMergedOps: prevMergedOps,
		mapsize:       mapsize,
		eventf:        func(string, uint64, uint64, string, ...any) {},
	}

	switch i, err := util.NewShardedMap[uint64, [3]uint64](mapsize, nil); {
	case err != nil:
		return nil, err
	default:
		sm.mergedStatus = i
	}

	switch i, err := util.NewShardedMap[string, map[uint64]uint64](mapsize, nil); {
	case err != nil:
		return nil, err
	default:
		sm.mergedOps = i
	}

	switch i, err := util.NewShardedMap[string, StateMerger](mapsize, nil); {
	case err != nil:
		return nil, err
	default:
		sm.mergers = util.NewMultiKeysLockedMap[string, StateMerger](i)
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

	switch i, err := NewSerialWorkers(sm.newWorker); {
	case err != nil:
		return nil, err
	default:
		swNewJob, swDone, swCancel = i.New(ctx)
	}

	sm.isPreviouslyMergedAtZeroSWPhaseFunc = sm.isPreviouslyXMergedAtZeroSWPhaseFunc(mergeFlag)
	sm.isPreviouslyUnmergedAtZeroSWPhaseFunc = sm.isPreviouslyXMergedAtZeroSWPhaseFunc(unmergeFlag)

	sm.Merge = sm.xMergeFunc(mergeFlag, swNewJob)
	sm.Unmerge = sm.xMergeFunc(unmergeFlag, swNewJob)
	sm.Done = func() ([]uint64, []uint64, func(phaseStatesMergerUpdatedFunc) error, error) {
		if err := swDone(); err != nil {
			return nil, nil, nil, err
		}

		return sm.doneMergers()
	}
	sm.Cancel = func() {
		swCancel()

		sm.clean()
	}

	return sm, nil
}

func (sm *phaseStatesMerger) clean() {
	sm.mergedStatus.Close()
	sm.mergers.Close()
}

func (sm *phaseStatesMerger) xMergeFunc(xmf xMergeFlag, swNewJob serialWorkerAddNextJobFunc) statesMergerXMergeFunc {
	var name string
	var xsmMergef phaseStatesMergerXMergeStatesFunc
	var ignoref func(known bool) bool

	switch xmf {
	case mergeFlag:
		name = "> merge"
		xsmMergef = sm.smXMergeValuesFunc(mergeFlag)
		ignoref = func(known bool) bool { return known }
	case unmergeFlag:
		name = "< unmerge"
		xsmMergef = sm.smXMergeValuesFunc(unmergeFlag)
		ignoref = func(known bool) bool { return !known }
	}

	return func(opindex uint64, values []StateMergeValue) error {
		if len(values) < 1 {
			return swNewJob(nil)
		}

		if ignoref(sm.mergedStatus.Exists(opindex)) {
			return nil
		}

		return swNewJob(sm.newJob(
			opindex,
			func(ctx context.Context, _, swphase uint64, smNext serialWorkerAddNextJobFunc) error {
				sm.Log().Debug().Uint64("op", opindex).Interface("values", values).Msg(name)

				return xsmMergef(ctx, swphase, opindex, values, smNext)
			},
		))
	}
}

func (sm *phaseStatesMerger) smXMergeValuesFunc(xmf xMergeFlag) phaseStatesMergerXMergeStatesFunc {
	var name string
	var mergef func(_ context.Context, swphase, opindex uint64, values []StateMergeValue,
		_ util.LockedMap[string, StateMerger]) (nextops []uint64, _ error)
	var isAlreadyMerged func(uint64, uint64, []string) bool

	switch xmf {
	case mergeFlag:
		name = "merge"
		mergef = sm.mergeValues
		isAlreadyMerged = sm.isPreviouslyMergedAtZeroSWPhaseFunc
	case unmergeFlag:
		name = "unmerge"
		mergef = sm.unmergeValues
		isAlreadyMerged = sm.isPreviouslyUnmergedAtZeroSWPhaseFunc
	}

	return func(
		ctx context.Context,
		swphase, opindex uint64,
		values []StateMergeValue,
		smNext serialWorkerAddNextJobFunc,
	) error {
		var smNextMergef phaseStatesMergerXMergeStatesFunc

		switch xmf {
		case mergeFlag:
			smNextMergef = sm.smXMergeValuesFunc(unmergeFlag)
		case unmergeFlag:
			smNextMergef = sm.smXMergeValuesFunc(mergeFlag)
		}

		keys, filtered := filterValues(values)
		if len(keys) < 1 {
			return nil
		}

		if isAlreadyMerged(swphase, opindex, keys) {
			return nil
		}

		var nextops []uint64

		if err := sm.mergers.LockFunc(keys, func(m util.LockedMap[string, StateMerger]) error {
			sm.eventf(name, swphase, opindex, "keys=%v", keys)

			switch i, err := mergef(ctx, swphase, opindex, filtered, m); {
			case err != nil:
				return err
			default:
				nextops = i

				return nil
			}
		}); err != nil {
			return err
		}

		for i := range nextops {
			uop := nextops[i]

			if uvalues, found := sm.opValues(uop); found {
				if err := smNext(sm.newJob(
					uop,
					func(ctx context.Context, _, swphase uint64, smNext serialWorkerAddNextJobFunc) error {
						return smNextMergef(ctx, swphase, uop, uvalues, smNext)
					},
				)); err != nil {
					return err
				}
			}
		}

		return nil
	}
}

func (sm *phaseStatesMerger) mergeValues(
	ctx context.Context,
	swphase, opindex uint64,
	values []StateMergeValue,
	m util.LockedMap[string, StateMerger],
) ([]uint64, error) {
	ignored, keys, nextops, err := sm.xMergeValuesFunc(mergeFlag, m)(ctx, swphase, opindex, values)
	if err != nil {
		return nil, err
	}

	var xmf xMergeFlag

	switch ignored {
	case true:
		xmf = unmergeFlag
	default:
		xmf = mergeFlag
	}

	sm.updateMergedOps(swphase, opindex, keys, xmf)

	if swphase == 0 {
		sm.setInitialMerged(opindex, 1)
	}

	switch {
	case ignored:
		sm.setXMerged(swphase, opindex, unmergeFlag)
	case swphase > 0:
		sm.setXMerged(swphase, opindex, mergeFlag)
	}

	return nextops, nil
}

func (sm *phaseStatesMerger) unmergeValues(
	ctx context.Context,
	swphase, opindex uint64,
	values []StateMergeValue,
	m util.LockedMap[string, StateMerger],
) ([]uint64, error) {
	ignored, keys, nextops, err := sm.xMergeValuesFunc(unmergeFlag, m)(ctx, swphase, opindex, values)
	if err != nil {
		return nil, err
	}

	if !ignored {
		sm.updateMergedOps(swphase, opindex, keys, unmergeFlag)
	}

	if swphase == 0 {
		sm.setInitialMerged(opindex, 2)
	}

	if swphase > 0 && !ignored {
		sm.setXMerged(swphase, opindex, unmergeFlag)
	}

	return nextops, nil
}

func (sm *phaseStatesMerger) xMergeValuesFunc(xmf xMergeFlag, m util.LockedMap[string, StateMerger]) func(
	context.Context, uint64, uint64, []StateMergeValue) (bool, []string, []uint64, error) {
	var newMergerf func(StateMergeValue) (StateMerger, error)

	switch xmf {
	case mergeFlag:
		newMergerf = sm.findMergerFunc(m, func(value StateMergeValue) (StateMerger, error) {
			switch i, found, err := sm.getStateValue(value.Key()); {
			case err != nil:
				return nil, err
			case !found:
				return value.Merger(nil)
			default:
				return value.Merger(i)
			}
		})
	case unmergeFlag:
		newMergerf = sm.findMergerFunc(m, func(StateMergeValue) (StateMerger, error) {
			return nil, nil
		})
	}

	xMergeValue := sm.xMergeValueFunc(xmf)

	return func(
		ctx context.Context,
		swphase, opindex uint64,
		values []StateMergeValue,
	) (ignored bool, keys []string, nextops []uint64, _ error) {
		addCommit, doneCommit := util.CompactAppendSlice[func() error](len(values))
		addKeys, doneKeys := util.UniqCompactAppendSlice[string](len(values))
		addNextOps, doneNextOps := util.UniqCompactAppendSlice[uint64](len(values))

		for i := range values {
			value := values[i]

			switch merged, commit, nextop, err := xMergeValue(
				swphase, opindex,
				value,
				newMergerf,
			); {
			case err != nil:
				return false, nil, nil, err
			case !merged:
				return true, nil, nil, nil
			default:
				addCommit(commit)
				addKeys(value.Key())

				if nextop != opindex {
					addNextOps(nextop)
				}
			}
		}

		if err := sm.runCommits(ctx, doneCommit()); err != nil {
			return false, nil, nil, err
		}

		nextops = doneNextOps()

		if len(nextops) > 0 {
			sm.eventf("nextops", swphase, opindex, "ops=%v", nextops)
		}

		return false, doneKeys(), nextops, nil
	}
}

func (sm *phaseStatesMerger) xMergeValueFunc(xmf xMergeFlag) func(
	uint64,
	uint64,
	StateMergeValue,
	func(value StateMergeValue) (StateMerger, error),
) (
	bool, func() error, uint64, error,
) {
	var mergef func(StateMerger) StateMergeXMergeFunc

	switch xmf {
	case mergeFlag:
		mergef = func(merger StateMerger) StateMergeXMergeFunc { return merger.Merge }
	case unmergeFlag:
		mergef = func(merger StateMerger) StateMergeXMergeFunc { return merger.Unmerge }
	}

	return func(
		swphase, opindex uint64,
		value StateMergeValue,
		newMergerf func(value StateMergeValue) (StateMerger, error),
	) (
		merged bool,
		commit func() error,
		nextop uint64,
		_ error,
	) {
		var merger StateMerger

		switch i, err := newMergerf(value); {
		case err != nil:
			return false, commit, nextop, err
		case i == nil:
			return false, commit, nextop, nil
		default:
			merger = i
		}

		switch f, nextop, err := mergef(merger)(opindex, value.Value()); {
		case err != nil:
			return false, commit, nextop, err
		case f == nil:
			sm.eventf("ignored", swphase, opindex, "key=%s", value.Key())

			return false, commit, nextop, nil
		default:
			return true, f, nextop, nil
		}
	}
}

func (sm *phaseStatesMerger) runCommits(ctx context.Context, fs []func() error) error {
	switch i := len(fs); {
	case i < 1:
		return nil
	case i == 1:
		return fs[0]()
	}

	var ew distributedWorker

	switch i, err := sm.newWorker(ctx); {
	case err != nil:
		return err
	default:
		defer i.Close()

		ew = i
	}

	if err := util.TraverseSlice(fs, func(_ int, f func() error) error {
		return ew.NewJob(func(context.Context, uint64) error {
			return f()
		})
	}); err != nil {
		return err
	}

	ew.Done()

	return ew.Wait()
}

func (sm *phaseStatesMerger) updateMergedOps(
	swphase, opindex uint64,
	keys []string,
	xmf xMergeFlag,
) {
	_ = util.TraverseSlice(keys, func(_ int, k string) error {
		_, _, _, _ = sm.mergedOps.SetOrRemove(k, func(m map[uint64]uint64, found bool) (map[uint64]uint64, bool, error) {
			if !found {
				nm := map[uint64]uint64{}

				if xmf == mergeFlag {
					nm[opindex] = swphase
				}

				switch preops, found := sm.prevMergedOps(k); {
				case !found:
				default:
					_ = util.TraverseSlice(preops, func(_ int, op uint64) error {
						if op != opindex {
							nm[op] = 0 // NOTE zero phase
						}

						return nil
					})
				}

				return nm, false, nil
			}

			if xmf == mergeFlag {
				switch prevswphase, itemfound := m[opindex]; {
				case itemfound && swphase <= prevswphase:
					return nil, false, util.ErrLockedSetIgnore
				case itemfound:
					m[opindex] = swphase

					return m, false, nil
				default:
					m[opindex] = swphase

					return m, false, nil
				}
			}

			switch prevswphase, itemfound := m[opindex]; {
			case itemfound && swphase < prevswphase:
				return nil, false, util.ErrLockedSetIgnore
			case itemfound:
				delete(m, opindex)

				if len(m) < 1 {
					return nil, true, nil
				}

				return m, false, nil
			default:
				return nil, false, util.ErrLockedSetIgnore
			}
		})

		return nil
	})
}

func (sm *phaseStatesMerger) setInitialMerged(opindex uint64, v uint64) {
	_, _, _ = sm.mergedStatus.Set(opindex, func(i [3]uint64, found bool) ([3]uint64, error) {
		if !found {
			return [3]uint64{v, 0, 0}, nil
		}

		return [3]uint64{v, i[1], i[2]}, nil
	})
}

func (sm *phaseStatesMerger) setXMerged(swphase, opindex uint64, xmf xMergeFlag) {
	v := uint64(xmf)

	_, _, _ = sm.mergedStatus.Set(opindex, func(i [3]uint64, _ bool) ([3]uint64, error) {
		prevswphase, prevstatus := i[1], i[2]

		switch {
		case prevstatus == 0:
			return [3]uint64{i[0], swphase, v}, nil
		case swphase < prevswphase:
			return [3]uint64{}, util.ErrLockedSetIgnore
		case prevstatus == v:
		case swphase == prevswphase:
			return [3]uint64{}, util.ErrLockedSetIgnore
		}

		return [3]uint64{i[0], swphase, v}, nil
	})
}

func (sm *phaseStatesMerger) doneMergers() (
	newlymerged, newlyunmerged []uint64,
	newlyupdated func(phaseStatesMergerUpdatedFunc) error,
	_ error,
) {
	doneMerged := func() []uint64 { return nil }
	doneUnmerged := func() []uint64 { return nil }

	if sm.mergedStatus.Len() > 0 {
		var addMerged, addUnmerged func(uint64) bool

		addMerged, doneMerged = util.CompactAppendSlice[uint64](sm.mergedStatus.Len())
		addUnmerged, doneUnmerged = util.CompactAppendSlice[uint64](sm.mergedStatus.Len())

		_ = sm.mergedStatus.Traverse(func(opindex uint64, v [3]uint64) bool {
			initial, status := v[0], v[2]
			if initial != 0 && initial == status {
				return true
			}

			switch status {
			case 1:
				addMerged(opindex)
			case 2:
				addUnmerged(opindex)
			}

			return true
		})
	}

	return doneMerged(), doneUnmerged(), func(f phaseStatesMergerUpdatedFunc) error {
		defer sm.clean()

		if f == nil {
			return nil
		}

		var gerr error

		maxMergersLength := sm.mergers.Len()

		_ = sm.mergers.Traverse(func(key string, v StateMerger) bool {
			gerr = f(key, v, countMergedOps(key, sm.mergedOps), maxMergersLength) // NOTE if empty merged ops, means no changed state value

			return gerr == nil
		})

		return gerr
	}, nil
}

func (sm *phaseStatesMerger) findMergerFunc(
	m util.LockedMap[string, StateMerger],
	newMergerf func(StateMergeValue) (StateMerger, error),
) func(StateMergeValue) (StateMerger, error) {
	return func(value StateMergeValue) (StateMerger, error) {
		if i, found := m.Value(value.Key()); found {
			return i, nil
		}

		if i, found := sm.sourceMerger(value.Key()); found {
			return i.Clone(), nil
		}

		switch i, err := newMergerf(value); {
		case err != nil:
			return nil, err
		case i == nil:
			return nil, nil
		default:
			_ = m.SetValue(value.Key(), i)

			return i, nil
		}
	}
}

func (sm *phaseStatesMerger) newJob(opindex uint64, f serialWorkerNextJobFunc) serialWorkerNewNextJobFunc {
	return func(swphase uint64) (serialWorkerNextJobFunc, bool) {
		var jobfound bool

		_, _, _ = sm.jobm.Set(fmt.Sprintf("%d-%d", swphase, opindex), func(_ struct{}, found bool) (struct{}, error) {
			if found {
				jobfound = true

				return struct{}{}, util.ErrLockedSetIgnore
			}

			return struct{}{}, nil
		})

		if jobfound {
			return nil, false
		}

		return f, true
	}
}

func (sm *phaseStatesMerger) isPreviouslyXMergedAtZeroSWPhaseFunc(xmf xMergeFlag) func(uint64, uint64, []string) bool {
	return func(swphase, opindex uint64, keys []string) bool {
		if swphase > 0 {
			return false
		}

		for i := range keys {
			prev, found := sm.prevMergedOps(keys[i])
			if !found {
				continue
			}

			if xmf == mergeFlag && slices.Index(prev, opindex) >= 0 { // NOTE zero swphase and previously merged
				return true
			}

			if xmf == unmergeFlag && slices.Index(prev, opindex) < 0 { // NOTE zero swphase and previously unmerged
				return true
			}
		}

		return false
	}
}

func filterValues(values []StateMergeValue) ([]string, []StateMergeValue) {
	addKeys, doneKeys := util.UniqCompactAppendSlice[string](len(values))

	_ = util.TraverseSlice(values, func(_ int, v StateMergeValue) error {
		addKeys(v.Key())

		return nil
	})

	return doneKeys(), values
}

func countMergedOps(key string, m util.LockedMap[string, map[uint64]uint64]) (ops []uint64) {
	om, found := m.Value(key)

	if !found || len(om) < 1 {
		return nil
	}

	addOps, doneOps := util.SortCompactAppendSlice[uint64](
		len(om),
		func(i uint64, j uint64) bool {
			return i < j
		},
	)

	for i := range om {
		addOps(i)
	}

	return doneOps()
}
