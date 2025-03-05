package dop5

import (
	"context"
	"maps"
	"slices"

	"github.com/rs/zerolog"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	addOwnersFunc              = func(owners []string)
	workerSMVsFunc             = func(worker string, _ []SMV) error
	extractStateOwnersSMVsFunc = func(opindex uint64, subv SMV, addvs []SMV) func(workerSMVsFunc) error

	insertStateOwnerSetFunc        = func(subw string, affected []string) error
	insertStateOwnersSetFunc       = func(subws, affected []string) error
	workerStatesOwnersSetFunc      = func(worker string, subws, affected []string) error
	localExtractedStatesOwnersFunc = func(getStateOwnersFunc) (
		extractStateOwnersSMVsFunc,
		dutil.InErrorFunc[workerStatesOwnersSetFunc],
		dutil.OutFunc[[]string],
		error,
	)
	workerInsertStatesOwnersSetFunc = func(worker string, _ insertStateOwnersSetFunc) error
	workerStatesOwnersDoneFunc      = func() (subws, affected []string, _ error)
)

func newFuncWorkerStatesOwners(
	ctx context.Context,
	local string,
	workersSize uint64,
) (
	localExtractedStatesOwnersFunc,
	workerStatesOwnersSetFunc,
	workerStatesOwnersDoneFunc,
	error,
) {
	log := dutil.NewModuleLoggerFromContext(ctx, "worker-state-owners", nil)

	var addSubStateOwners, addAffectedStateOwners addOwnersFunc
	var doneSubStateOwners, doneAffectedStateOwners dutil.OutFunc[[]string]

	switch a, d, err := newFuncAddStateOwners(workersSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addSubStateOwners, doneSubStateOwners = a, d
	}

	switch a, d, err := newFuncAddStateOwners(workersSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addAffectedStateOwners, doneAffectedStateOwners = a, d
	}

	return func(getStateOwners getStateOwnersFunc) (
			extractStateOwnersSMVsFunc,
			dutil.InErrorFunc[workerStatesOwnersSetFunc],
			dutil.OutFunc[[]string],
			error,
		) {
			var localInsertValues extractStateOwnersSMVsFunc
			var traverseOwnersSet dutil.InErrorFunc[workerStatesOwnersSetFunc]
			var localOwners dutil.OutFunc[[]string]

			switch i, t, l, err := newFuncLocalExtractedStateOwners(workersSize, getStateOwners); {
			case err != nil:
				return nil, nil, nil, err
			default:
				localInsertValues, traverseOwnersSet, localOwners = i, t, l
			}

			return localInsertValues,
				func(f workerStatesOwnersSetFunc) error {
					return traverseOwnersSet(func(worker string, subws, affected []string) error {
						if worker == local {
							addSubStateOwners(subws)
							addAffectedStateOwners(affected)
						}

						if f != nil {
							if err := f(worker, subws, affected); err != nil {
								return err
							}
						}

						return nil
					})
				},
				localOwners,
				nil
		},
		func(worker string, subws, affected []string) error {
			if worker == local {
				return util.ErrFound.Errorf("unexpected local")
			}

			log.Debug().
				Str("worker", worker).
				Dict("state_owners", zerolog.Dict().
					Strs("sub", subws).
					Strs("affected", affected),
				).
				Msg("worker owners inserted")

			addSubStateOwners(subws)
			addAffectedStateOwners(affected)

			return nil
		},
		func() ([]string, []string, error) {
			return doneSubStateOwners(), doneAffectedStateOwners(), nil
		},
		nil
}

func newFuncLocalExtractedStateOwners(
	workersSize uint64,
	getStateOwners getStateOwnersFunc,
) (
	extractStateOwnersSMVsFunc,
	dutil.InErrorFunc[workerStatesOwnersSetFunc],
	dutil.OutFunc[[]string],
	error,
) {
	var addOwnersSet insertStateOwnerSetFunc
	var traverseWorkerOwnersSet dutil.InErrorFunc[workerStatesOwnersSetFunc]

	switch a, w, err := newFuncWorkerCollectStateOwnersSet(workersSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addOwnersSet, traverseWorkerOwnersSet = a, w
	}

	var addOwners dutil.AppendSliceAddFunc[string]
	var doneOwners dutil.OutFunc[[]string]

	switch a, d, err := dutil.NewLockComparableUniqueAppendSlice[string](workersSize, 0); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addOwners, doneOwners = a, d
	}

	return func(opindex uint64, subv SMV, addvs []SMV) func(workerSMVsFunc) error {
			addValue, traverseValues := newFuncCollectWorkerSMVs()

			var subws []string

			if subv != nil {
				subws = getStateOwners(subv.Key())
				addOwners(subws...)

				for w := range slices.Values(subws) {
					addValue(w, subv)
				}
			}

			for value := range slices.Values(addvs) {
				adds := getStateOwners(value.Key())
				addOwners(adds...)

				for subw := range slices.Values(subws) {
					addOwnersSet(subw, adds)
				}

				for w := range slices.Values(adds) {
					addValue(w, value)
				}
			}

			return traverseValues
		},
		traverseWorkerOwnersSet,
		doneOwners,
		nil
}

func newFuncWorkerCollectStateOwnersSet(workersSize uint64) (
	insertStateOwnerSetFunc,
	dutil.InErrorFunc[workerStatesOwnersSetFunc],
	error,
) {
	var addOwners dutil.AppendSliceAddFunc[string]
	var doneOwners dutil.OutFunc[[]string]

	switch a, d, err := dutil.NewLockComparableUniqueAppendSlice[string](workersSize, 0); {
	case err != nil:
		return nil, nil, err
	default:
		addOwners, doneOwners = a, d
	}

	var msubws dutil.LockedMap[string /* sub worker */, map[string]struct{} /* affected */]
	var maffected dutil.LockedMap[string /* affected */, map[string]struct{} /* sub workers */]

	switch i, err := dutil.NewLockedMap[string, map[string]struct{}](nil, workersSize, workersSize); {
	case err != nil:
		return nil, nil, err
	default:
		msubws = i
	}

	switch i, err := dutil.NewLockedMap[string, map[string]struct{}](nil, workersSize, workersSize); {
	case err != nil:
		return nil, nil, err
	default:
		maffected = i
	}

	add := func(
		m dutil.LockedMap[string, map[string]struct{}],
		a string, b []string,
	) {
		_, _ = m.Set(a, func(prev map[string]struct{}, _ bool) (map[string]struct{}, error) {
			addOwners(a)
			addOwners(b...)

			for worker := range slices.Values(b) {
				switch {
				case len(prev) < 1:
					prev = map[string]struct{}{worker: {}}
				default:
					if _, found := prev[worker]; !found {
						prev[worker] = struct{}{}
					}
				}
			}

			return prev, nil
		})
	}

	return func(subw string, affected []string) error {
			if subw != "" {
				if len(affected) > 0 {
					add(msubws, subw, affected)
				}

				for worker := range slices.Values(affected) {
					add(maffected, worker, []string{subw})
				}
			}

			return nil
		},
		func(f workerStatesOwnersSetFunc) error {
			defer func() {
				msubws.Close()
				maffected.Close()
			}()

			if f != nil {
				owners := doneOwners()

				defer dutil.ClearSlice(&owners)

				for worker := range slices.Values(owners) {
					var subws, affected []string

					if i, found := msubws.Value(worker); found {
						affected = append(affected, slices.Collect(maps.Keys(i))...)
					}

					if i, found := maffected.Value(worker); found {
						subws = append(subws, slices.Collect(maps.Keys(i))...)
					}

					slices.Sort(subws)
					slices.Sort(affected)

					if err := f(worker, slices.Compact(subws), slices.Compact(affected)); err != nil {
						return err
					}
				}
			}

			return nil
		},
		nil
}

func newFuncCollectWorkerSMVs() (
	func(worker string, _ SMV),
	func(workerSMVsFunc) error,
) {
	m := map[string][]SMV{}

	return func(worker string, value SMV) {
			m[worker] = append(m[worker], value)
		},
		func(f workerSMVsFunc) error {
			defer clear(m)

			for worker, values := range maps.All(m) {
				if err := f(worker, values); err != nil {
					return err
				}
			}

			return nil
		}
}

func newFuncAddStateOwners(
	maxSize uint64,
) (
	addOwnersFunc,
	dutil.OutFunc[[]string],
	error,
) {
	switch add, done, err := dutil.NewLockComparableUniqueAppendSlice[string](maxSize, 1<<5); {
	case err != nil:
		return nil, nil, err
	default:
		return addOwnersFunc(func(owners []string) {
				if len(owners) < 1 {
					return
				}

				add(owners...)
			}),
			done,
			nil
	}
}
