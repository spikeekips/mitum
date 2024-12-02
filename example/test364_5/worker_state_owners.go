package main

import (
	"context"
	"maps"
	"slices"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
)

type (
	addOwnersFunc                                func(owners []string)
	extractStateOwnersWorkerStateMergeValuesFunc func(worker string, _ []StateMergeValue) error
	extractStateOwnersStateMergeValuesFunc       func(opindex uint64, subv StateMergeValue, addvs []StateMergeValue) (
		func(extractStateOwnersWorkerStateMergeValuesFunc) error,
		error,
	)
	insertStateOwnerSetFunc       func(subw string, affected []string) error
	insertStateOwnersSetFunc      func(subws, affected []string) error
	localExtractedStateOwnersFunc func(getStateOwnersFunc) (
		extractStateOwnersStateMergeValuesFunc,
		inErrorFunc[workerStateOwnersSetFunc],
		error,
	)
	workerInsertStateOwnersSetFunc       func(worker string, _ insertStateOwnersSetFunc) error
	workerStateOwnersSetFunc             func(worker string, subws, affected []string) error
	workerDistributeStateMergeValuesFunc func(_ context.Context, worker string) (
		_ insertStateMergeValuesFunc,
		owners insertStateOwnersSetFunc,
		_ error,
	)
)

func newFuncWorkerStateOwners(
	ctx context.Context,
	local string,
	maxWorkerSize uint64,
) (
	localExtractedStateOwnersFunc,
	workerStateOwnersSetFunc,
	func(insertStateOwnersSetFunc) error,
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

			switch i, t, err := newFuncLocalExtractedStateOwners(ctx, maxWorkerSize, getStateOwners); {
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

			return contextCauseError(ctx, dummyErrorInFunc, func(context.Context) error {
				addSubStateOwners(subws)
				addAffectedStateOwners(affected)

				return nil
			})
		},
		func(f insertStateOwnersSetFunc) error {
			defer func() {
				doneSubStateOwners(nil)
				doneAffectedStateOwners(nil)
			}()

			return contextCauseError(ctx, dummyErrorInFunc, func(context.Context) error {
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
			})
		},
		nil
}

func newFuncLocalExtractedStateOwners(
	ctx context.Context,
	maxWorkerSize uint64,
	getStateOwners getStateOwnersFunc,
) (
	extractStateOwnersStateMergeValuesFunc,
	inErrorFunc[workerStateOwnersSetFunc],
	error,
) {
	var addOwnersSet insertStateOwnerSetFunc
	var traverseWorkerOwnersSet inErrorFunc[workerStateOwnersSetFunc]

	switch a, w, err := newFuncWorkerCollectStateOwnersSet(ctx, maxWorkerSize); {
	case err != nil:
		return nil, nil, err
	default:
		addOwnersSet = a
		traverseWorkerOwnersSet = w
	}

	return func(opindex uint64, subv StateMergeValue, addvs []StateMergeValue) (
			f func(extractStateOwnersWorkerStateMergeValuesFunc) error,
			_ error,
		) {
			return f, contextCauseError(ctx, dummyErrorInFunc, func(context.Context) error {
				addValue, traverseValues := newFuncCollectWorkerStateMergeValues()

				_ = traverseSlice(getStateOwners(subv.Key()), func(_ int, subw string) (bool, error) {
					addValue(subw, subv)

					_ = traverseSlice(addvs, func(_ int, value StateMergeValue) (bool, error) {
						adds := getStateOwners(value.Key())

						addOwnersSet(subw, adds)

						_ = traverseSlice(adds, func(_ int, addw string) (bool, error) {
							addValue(addw, value)

							return true, nil
						})

						return true, nil
					})

					return true, nil
				})

				f = traverseValues

				return nil
			})
		},
		traverseWorkerOwnersSet,
		nil
}

func newFuncWorkerCollectStateOwnersSet(ctx context.Context, maxWorkerSize uint64) (
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
				case worker == a:
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
			return contextCauseError(ctx, dummyErrorInFunc, func(context.Context) error {
				if len(affected) < 1 {
					return nil
				}

				add(msubs, subw, affected)

				_ = traverseSlice(affected, func(_ int, worker string) (bool, error) {
					add(maffected, worker, []string{subw})

					return true, nil
				})

				return nil
			})
		},
		func(f workerStateOwnersSetFunc) (gerr error) {
			defer func() {
				msubs.Close()
				maffected.Close()
			}()

			return contextCauseError(ctx, dummyErrorInFunc, func(context.Context) error {
				if f != nil {
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

				return gerr
			})
		},
		nil
}

func newFuncCollectWorkerStateMergeValues() (
	func(worker string, _ StateMergeValue),
	func(extractStateOwnersWorkerStateMergeValuesFunc) error,
) {
	m := map[string][]StateMergeValue{}

	return func(worker string, value StateMergeValue) {
			m[worker] = append(m[worker], value)
		},
		func(f extractStateOwnersWorkerStateMergeValuesFunc) error {
			defer clearMap(m)

			return traverseMap(m, func(worker string, values []StateMergeValue) (bool, error) {
				err := f(worker, values)

				return err == nil, err
			})
		}
}

func newFuncInsertStateMergeValuesToWorker( // FIXME remove
	write func(interface{}) error,
) (
	workerDistributeStateMergeValuesFunc,
	func() error,
) {
	doneLocked := util.NewLocked[errorFunc](func() error { return errors.Errorf("not yet started") })

	return func(ctx context.Context, worker string) (
			_ insertStateMergeValuesFunc,
			owners insertStateOwnersSetFunc,
			_ error,
		) {
			wctx, wctxCancel, done := newDoneContext(ctx)

			_ = doneLocked.SetValue(done)

			return func(opindex uint64, values []StateMergeValue) error {
					return contextCauseError(wctx, ifErrErrorInFunc(wctxCancel), func(context.Context) error {
						return write([2]interface{}{opindex, values})
					})
				},
				func(subws, affected []string) error {
					return contextCauseError(wctx, wctxCancel, func(context.Context) error {
						return write([2]interface{}{subws, affected})
					})
				},
				nil
		},
		func() error {
			defer doneLocked.EmptyValue()

			i, _ := doneLocked.Value()

			return i()
		}
}

func newFuncWorkerDistributeStateMergeValue( // FIXME remove
	ctx context.Context,
	maxWorkerSize uint64,
	newDistributef workerDistributeStateMergeValuesFunc,
) (
	insert func(worker string, opindex uint64, _ []StateMergeValue) error,
	traverse func(func(worker string) (subws, affected []string)) error,
	closef func(),
	_ error,
) {
	var m util.LockedMap[string /* worker */, [2]interface{} /* funcs */]

	switch i, err := util.NewLockedMap[string, [2]interface{}](maxWorkerSize, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		m = i
	}

	return func(worker string, opindex uint64, values []StateMergeValue) error {
			var f insertStateMergeValuesFunc

			_, _, _ = m.Set(worker, func(prev [2]interface{}, found bool) (v [2]interface{}, _ error) {
				if found {
					f = prev[0].(insertStateMergeValuesFunc)

					return v, util.ErrLockedSetIgnore
				}

				i, o, err := newDistributef(ctx, worker)
				if err != nil {
					return v, err
				}

				f = i

				return [2]interface{}{i, o}, nil
			})

			return f(opindex, values)
		},
		func(f func(worker string) (subws, affected []string)) (gerr error) {
			_ = m.Traverse(func(worker string, i [2]interface{}) bool {
				subws, affected := f(worker)

				gerr = i[1].(func(_, _ []string) error)(subws, affected)

				return gerr == nil
			})

			return gerr
		},
		m.Close,
		nil
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
