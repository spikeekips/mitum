package main

import (
	"context"
	"maps"
	"slices"

	"github.com/rs/zerolog"
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
		inErrorFunc[workerStateOwnersSetFunc],
		error,
	)
	workerInsertStateOwnersSetFunc = func(worker string, _ insertStateOwnersSetFunc) error
)

func newFuncWorkerStateOwners(
	ctx context.Context,
	local string,
	maxWorkerSize uint64,
) (
	localExtractedStateOwnersFunc,
	workerStateOwnersSetFunc,
	inErrorFunc[insertStateOwnersSetFunc],
	error,
) {
	log := newModuleLoggerFromContext(ctx, "worker-state-owners", nil)

	var addSubStateOwners, addAffectedStateOwners addOwnersFunc
	var doneSubStateOwners, doneAffectedStateOwners compactAppendSliceDoneFunc[string]

	if err := newFuncAddStateOwners(
		maxWorkerSize, &addSubStateOwners, &doneSubStateOwners); err != nil {
		return nil, nil, nil, err
	}

	if err := newFuncAddStateOwners(
		maxWorkerSize, &addAffectedStateOwners, &doneAffectedStateOwners); err != nil {
		return nil, nil, nil, err
	}

	return func(getStateOwners getStateOwnersFunc) (
			extractStateOwnersStateMergeValuesFunc,
			inErrorFunc[workerStateOwnersSetFunc],
			error,
		) {
			var localInsertValues extractStateOwnersStateMergeValuesFunc
			var traverseOwnersSet inErrorFunc[workerStateOwnersSetFunc]

			switch i, t, err := newFuncLocalExtractedStateOwners(maxWorkerSize, getStateOwners); {
			case err != nil:
				return nil, nil, err
			default:
				localInsertValues = i
				traverseOwnersSet = t
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
		func(f insertStateOwnersSetFunc) error {
			defer func() {
				doneSubStateOwners(nil)
				doneAffectedStateOwners(nil)
			}()

			if f == nil {
				return nil
			}

			return doneSubStateOwners(func(subws []string) error {
				return doneAffectedStateOwners(func(affected []string) error {
					log.Debug().Dict("state_owners", zerolog.Dict().
						Strs("sub", subws).
						Strs("affected", affected),
					).Msg("state owners")

					return f(subws, affected)
				})
			})
		},
		nil
}

func newFuncLocalExtractedStateOwners(
	maxWorkerSize uint64,
	getStateOwners getStateOwnersFunc,
) (
	extractStateOwnersStateMergeValuesFunc,
	inErrorFunc[workerStateOwnersSetFunc],
	error,
) {
	var addOwnersSet insertStateOwnerSetFunc
	var traverseWorkerOwnersSet inErrorFunc[workerStateOwnersSetFunc]

	switch a, w, err := newFuncWorkerCollectStateOwnersSet(maxWorkerSize); {
	case err != nil:
		return nil, nil, err
	default:
		addOwnersSet = a
		traverseWorkerOwnersSet = w
	}

	return func(opindex uint64, subv StateMergeValue, addvs []StateMergeValue) func(workerStateMergeValuesFunc) error {
			addValue, traverseValues := newFuncCollectWorkerStateMergeValues()

			var subws []string

			if subv != nil {
				ws := getStateOwners(subv.Key())
				subws = make([]string, len(ws))

				_ = traverseSlice(ws, func(i int, w string) (bool, error) {
					addValue(w, subv)

					subws[i] = w

					return true, nil
				})
			}

			_ = traverseSlice(addvs, func(_ int, value StateMergeValue) (bool, error) {
				adds := getStateOwners(value.Key())

				traverseSlice(subws, func(_ int, subw string) (bool, error) {
					addOwnersSet(subw, adds)

					return true, nil
				})

				_ = traverseSlice(adds, func(_ int, addw string) (bool, error) {
					addValue(addw, value)

					return true, nil
				})

				return true, nil
			})

			return traverseValues
		},
		traverseWorkerOwnersSet,
		nil
}

func newFuncWorkerCollectStateOwnersSet(maxWorkerSize uint64) (
	insertStateOwnerSetFunc,
	inErrorFunc[workerStateOwnersSetFunc],
	error,
) {
	var msubs util.LockedMap[string /* sub worker */, map[string]struct{} /* affected */]
	var maffected util.LockedMap[string /* affected */, map[string]struct{} /* sub workers */]

	switch i, err := util.NewLockedMap[string, map[string]struct{}](maxWorkerSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		msubs = i
	}

	switch i, err := util.NewLockedMap[string, map[string]struct{}](maxWorkerSize, nil); {
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
			_ = traverseSlice(b, func(_ int, worker string) (bool, error) {
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

			_ = traverseSlice(affected, func(_ int, worker string) (bool, error) {
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
						defer clear(affected)
					}

					if i, found := maffected.Value(worker); found && len(i) > 0 {
						subws = slices.Collect(maps.Keys(i))
						defer clear(subws)
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
			defer clearMap(m)

			return traverseMap(m, func(worker string, values []StateMergeValue) (bool, error) {
				err := f(worker, values)

				return err == nil, err
			})
		}
}

func newFuncAddStateOwners(
	maxSize uint64,
	add *addOwnersFunc,
	done *compactAppendSliceDoneFunc[string],
) error {
	var (
		addf  compactAppendSliceAddFunc[string]
		donef compactAppendSliceDoneFunc[string]
	)

	if err := rUniqueCompactAppendSlice[string](
		maxSize, 1<<5, &addf, &donef); err != nil {
		return err
	}

	a := addOwnersFunc(func(owners []string) {
		if len(owners) < 1 {
			return
		}

		addf(owners[0], owners[1:]...)
	})

	if err := util.ReflectSetInterfaceValue(a, add); err != nil {
		return err
	}

	return util.ReflectSetInterfaceValue(donef, done)
}
