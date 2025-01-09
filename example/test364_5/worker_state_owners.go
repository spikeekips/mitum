package main

import (
	"context"
	"maps"
	"slices"

	"github.com/rs/zerolog"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	addOwnersFunc                          = func(owners []string)
	workerStateMergeValuesFunc             = func(worker string, _ []StateMergeValue) error
	extractStateOwnersStateMergeValuesFunc = func(opindex uint64, subv StateMergeValue, addvs []StateMergeValue) func(workerStateMergeValuesFunc) error

	insertStateOwnerSetFunc       = func(subw string, affected []string) error
	insertStateOwnersSetFunc      = func(subws, affected []string) error
	workerStateOwnersSetFunc      = func(worker string, subws, affected []string) error
	localExtractedStateOwnersFunc = func(getStateOwnersFunc) (
		extractStateOwnersStateMergeValuesFunc,
		dutil.InErrorFunc[workerStateOwnersSetFunc],
		dutil.OutFunc[[]string],
		error,
	)
	workerInsertStateOwnersSetFunc = func(worker string, _ insertStateOwnersSetFunc) error
	workerStateOwnersDoneFunc      = func() (subws, affected []string, _ error)
)

func newFuncWorkerStateOwners(
	ctx context.Context,
	local string,
	maxWorkersSize uint64,
) (
	localExtractedStateOwnersFunc,
	workerStateOwnersSetFunc,
	workerStateOwnersDoneFunc,
	error,
) {
	log := dutil.NewModuleLoggerFromContext(ctx, "worker-state-owners", nil)

	var addSubStateOwners, addAffectedStateOwners addOwnersFunc
	var doneSubStateOwners, doneAffectedStateOwners dutil.OutFunc[[]string]

	switch a, d, err := newFuncAddStateOwners(maxWorkersSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addSubStateOwners, doneSubStateOwners = a, d
	}

	switch a, d, err := newFuncAddStateOwners(maxWorkersSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addAffectedStateOwners, doneAffectedStateOwners = a, d
	}

	return func(getStateOwners getStateOwnersFunc) (
			extractStateOwnersStateMergeValuesFunc,
			dutil.InErrorFunc[workerStateOwnersSetFunc],
			dutil.OutFunc[[]string],
			error,
		) {
			var localInsertValues extractStateOwnersStateMergeValuesFunc
			var traverseOwnersSet dutil.InErrorFunc[workerStateOwnersSetFunc]
			var localOwners dutil.OutFunc[[]string]

			switch i, t, l, err := newFuncLocalExtractedStateOwners(maxWorkersSize, getStateOwners); {
			case err != nil:
				return nil, nil, nil, err
			default:
				localInsertValues, traverseOwnersSet, localOwners = i, t, l
			}

			return localInsertValues,
				func(f workerStateOwnersSetFunc) error {
					return traverseOwnersSet(func(worker string, subws, affected []string) error {
						if worker == local {
							addSubStateOwners(subws)
							addAffectedStateOwners(affected)
						}

						if f != nil {
							return f(worker, subws, affected)
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
	maxWorkersSize uint64,
	getStateOwners getStateOwnersFunc,
) (
	extractStateOwnersStateMergeValuesFunc,
	dutil.InErrorFunc[workerStateOwnersSetFunc],
	dutil.OutFunc[[]string],
	error,
) {
	var addOwnersSet insertStateOwnerSetFunc
	var traverseWorkerOwnersSet dutil.InErrorFunc[workerStateOwnersSetFunc]

	switch a, w, err := newFuncWorkerCollectStateOwnersSet(maxWorkersSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addOwnersSet, traverseWorkerOwnersSet = a, w
	}

	var addOwners dutil.AppendSliceAddFunc[string]
	var doneOwners dutil.OutFunc[[]string]

	switch a, d, err := dutil.NewLockComparableUniqueAppendSlice[string](maxWorkersSize, 0); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addOwners, doneOwners = a, d
	}

	return func(opindex uint64, subv StateMergeValue, addvs []StateMergeValue) func(workerStateMergeValuesFunc) error {
			addValue, traverseValues := newFuncCollectWorkerStateMergeValues()

			var subws []string

			if subv != nil {
				subws = getStateOwners(subv.Key())
				addOwners(subws...)

				for w := range slices.Values(subws) {
					addValue(w, subv)
				}
			}

			_ = dutil.TraverseSlice(addvs, func(_ int, value StateMergeValue) (bool, error) {
				adds := getStateOwners(value.Key())
				addOwners(adds...)

				dutil.TraverseSlice(subws, func(_ int, subw string) (bool, error) {
					addOwnersSet(subw, adds)

					return true, nil
				})

				_ = dutil.TraverseSlice(adds, func(_ int, addw string) (bool, error) {
					addValue(addw, value)

					return true, nil
				})

				return true, nil
			})

			return traverseValues
		},
		traverseWorkerOwnersSet,
		doneOwners,
		nil
}

func newFuncWorkerCollectStateOwnersSet(maxWorkersSize uint64) (
	insertStateOwnerSetFunc,
	dutil.InErrorFunc[workerStateOwnersSetFunc],
	error,
) {
	var msubs util.LockedMap[string /* sub worker */, map[string]struct{} /* affected */]
	var maffected util.LockedMap[string /* affected */, map[string]struct{} /* sub workers */]

	switch i, err := util.NewLockedMap[string, map[string]struct{}](maxWorkersSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		msubs = i
	}

	switch i, err := util.NewLockedMap[string, map[string]struct{}](maxWorkersSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		maffected = i
	}

	add := func(
		m util.LockedMap[string, map[string]struct{}],
		a string, b []string,
	) {
		m.Set(a, func(prev map[string]struct{}, _ bool) (map[string]struct{}, error) {
			_ = dutil.TraverseSlice(b, func(_ int, worker string) (bool, error) {
				switch {
				case len(prev) < 1:
					prev = map[string]struct{}{worker: {}}
				default:
					if _, found := prev[worker]; !found {
						prev[worker] = struct{}{}
					}
				}

				return true, nil
			})

			return prev, nil
		})
	}

	return func(subw string, affected []string) error {
			if len(affected) < 1 {
				return nil
			}

			add(msubs, subw, affected)

			_ = dutil.TraverseSlice(affected, func(_ int, worker string) (bool, error) {
				add(maffected, worker, []string{subw})

				return true, nil
			})

			return nil
		},
		func(f workerStateOwnersSetFunc) error {
			defer func() {
				msubs.Close()
				maffected.Close()
			}()

			if f != nil {
				var gerr error

				_ = msubs.Traverse(func(worker string, i map[string]struct{}) bool {
					var affected, subws []string

					if len(i) > 0 {
						affected = slices.Collect(maps.Keys(i))
					}

					if i, found := maffected.Value(worker); found && len(i) > 0 {
						subws = slices.Collect(maps.Keys(i))
					}

					gerr = f(worker, subws, affected)

					return gerr == nil
				})
			}

			return nil
		},
		nil
}

func newFuncCollectWorkerStateMergeValues() (
	func(worker string, _ StateMergeValue),
	func(workerStateMergeValuesFunc) error,
) {
	m := map[string][]StateMergeValue{}

	return func(worker string, value StateMergeValue) {
			m[worker] = append(m[worker], value)
		},
		func(f workerStateMergeValuesFunc) error {
			defer dutil.ClearMap(m)

			return dutil.TraverseMap(m, func(worker string, values []StateMergeValue) (bool, error) {
				err := f(worker, values)

				return err == nil, err
			})
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
