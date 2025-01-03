package main

import (
	"math"
	"slices"
	"strings"
	"sync"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	workerOperationsFunc            = dutil.In2ErrorFunc[string /* worker */, opindexOpHashesFunc]
	minEnoughMaxFailedFunc          = func(workers, share uint64) (minEnough, maxFailed uint64, _ error)
	checkFailedWorkerFunc           = func(worker string, _ func() error) (ok, failed bool, _ error)
	checkEnoughFailedWorkerFunc     = func(worker string, _ func() error) (ok, enough, failed bool, _ error)
	newCheckEnoughFailedWorkersFunc = func(minEnough, maxFailed uint64) (checkEnoughFailedWorkerFunc, func(), error)
	getStateOwnersFunc              = func(stateKey string) []string
	getOperationOwnersFunc          = func(opindex uint64) []string
	mergeStateOpFunc                = func(opindex uint64, fact util.Hash, stateKeys []string) bool
	unmergeStateOpFunc              = func(ops []uint64) bool
	getStateOpsFunc                 = func(stateKey string) ([]util.Hash, error)
	traverseStateOpsFunc            = func(dutil.In2ErrorFunc[int, getStateOpsFunc]) error
)

func minEnoughMaxFailed(workers, share uint64) (minEnough, maxFailed uint64, _ error) {
	switch {
	case workers < 1:
		return 0, 0, util.ErrInvalid.Errorf("zero workers")
	case share < 1:
		return 0, 0, util.ErrInvalid.Errorf("zero share")
	case workers < share:
		share = workers
	}

	return workers - share + 1, share, nil
}

var errWorkersFailed = util.NewIDError("workers failed")

func newFuncCheckFailedWorkers(
	maxWorkersSize,
	maxFailed uint64,
) (
	checkFailedWorkerFunc,
	func(),
	error,
) {
	if maxWorkersSize < 1 {
		return nil, nil, errors.Errorf("zero max workers size")
	}

	var m util.LockedMap[string, struct{}]

	switch i, err := util.NewLockedMap[string, struct{}](maxWorkersSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i
	}

	count := func() func(string, error) (_, _ bool) {
		var gIsClosed bool
		var failedCount *uint64
		var l sync.RWMutex

		f := func() bool {
			return failedCount != nil && *failedCount >= maxFailed
		}

		c := func() (_, _ bool) {
			l.RLock()
			defer l.RUnlock()

			return gIsClosed, f()
		}

		return func(worker string, err error) (isClosed, failed bool) {
			if isClosed, failed = c(); isClosed {
				return isClosed, failed
			}

			l.Lock()
			defer l.Unlock()

			if err != nil {
				if failedCount == nil {
					failedCount = new(uint64)
				}

				*failedCount++
			}

			failed = f()

			if failed {
				gIsClosed = true
			}

			return false, failed
		}
	}()

	return func(worker string, f func() error) (ok, failed bool, gerr error) {
			var isClosed bool

			switch _, _, err := m.Set(worker, func(_ struct{}, found bool) (struct{}, error) {
				if found {
					return struct{}{}, util.ErrLockedSetIgnore
				}

				if f != nil {
					gerr = f()
				}

				switch isClosed, failed = count(worker, gerr); {
				case isClosed:
					return struct{}{}, util.ErrLockedSetIgnore
				default:
					ok = true

					return struct{}{}, nil
				}
			}); {
			case errors.Is(err, util.ErrLockedMapClosed):
				ok = false
			default:
				if isClosed {
					m.Close()
				}
			}

			if failed {
				switch {
				case gerr != nil:
					gerr = errWorkersFailed.Wrap(gerr)
				default:
					gerr = errWorkersFailed
				}
			}

			return ok, failed, gerr
		},
		m.Close,
		nil
}

func newFuncBaseCheckEnoughFailedWorkers(
	minEnough, maxFailed uint64,
	// notEnoughUntil suspends enough until true, enough will not be reached
	// even though over minEnough.
	notEnoughUntil func(worker string) bool,
) (
	checkEnoughFailedWorkerFunc,
	func(),
	error,
) {
	if minEnough < 1 {
		return nil, nil, errors.Errorf("zero min enough")
	}

	var m util.LockedMap[string, struct{}]

	switch i, err := util.NewLockedMap[string, struct{}](minEnough+maxFailed, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i
	}

	count := func() func(string, error) (_, _, _ bool) {
		var readyEnough, gIsClosed bool
		var enoughCount *uint64
		var failedCount *uint64
		var l sync.RWMutex

		f := func() (_, _ bool) {
			var enough, failed bool

			if readyEnough && enoughCount != nil {
				enough = *enoughCount >= minEnough
			}

			if failedCount != nil {
				failed = *failedCount >= maxFailed
			}

			return !failed && enough, failed
		}

		c := func() (_, _, _ bool) {
			l.RLock()
			defer l.RUnlock()

			enough, failed := f()

			return gIsClosed, enough, failed
		}

		return func(worker string, err error) (isClosed, enough, failed bool) {
			if isClosed, enough, failed = c(); isClosed {
				return isClosed, enough, failed
			}

			l.Lock()
			defer l.Unlock()

			if !readyEnough {
				readyEnough = notEnoughUntil(worker)
			}

			switch {
			case err != nil:
				if failedCount == nil {
					failedCount = new(uint64)
				}

				*failedCount++
			default:
				if enoughCount == nil {
					enoughCount = new(uint64)
				}

				*enoughCount++
			}

			enough, failed = f()

			if enough || failed {
				gIsClosed = true
			}

			return false, enough, failed
		}
	}()

	return func(worker string, f func() error) (ok, enough, failed bool, gerr error) {
			var isClosed bool

			switch _, _, err := m.Set(worker, func(_ struct{}, found bool) (struct{}, error) {
				if found {
					return struct{}{}, util.ErrLockedSetIgnore
				}

				if f != nil {
					gerr = f()
				}

				switch isClosed, enough, failed = count(worker, gerr); {
				case isClosed:
					return struct{}{}, util.ErrLockedSetIgnore
				default:
					ok = true

					return struct{}{}, nil
				}
			}); {
			case errors.Is(err, util.ErrLockedMapClosed):
				ok = false
			default:
				if isClosed {
					m.Close()
				}
			}

			if failed {
				switch {
				case gerr != nil:
					gerr = errWorkersFailed.Wrap(gerr)
				default:
					gerr = errWorkersFailed
				}
			}

			return ok, enough, failed, gerr
		},
		m.Close,
		nil
}

func newFuncCheckEnoughFailedWorkers(minEnough, maxFailed uint64) (
	checkEnoughFailedWorkerFunc,
	func(),
	error,
) {
	return newFuncBaseCheckEnoughFailedWorkers(
		minEnough,
		maxFailed,
		func(string) bool { return true },
	)
}

func newFuncLocalCheckEnoughFailedWorkers(local string) newCheckEnoughFailedWorkersFunc {
	return func(minEnough, maxFailed uint64) (checkEnoughFailedWorkerFunc, func(), error) {
		return newFuncBaseCheckEnoughFailedWorkers(
			minEnough, maxFailed,
			func(worker string) bool { return worker == local },
		)
	}
}

type traverseStateMergersFunc = func(func(stateKey string, merger StateMerger) (bool, error)) error

func newFuncStateMergers(
	poolSize uint64,
	getState getStateFunc,
	extractStateMergeValueKeys extractStateMergeValueKeysFunc,
) (
	mergerGetStateMergersFunc,
	func() (size uint64, _ traverseStateMergersFunc),
	error,
) {
	var pool util.LockedMap[string, StateMerger]

	switch i, err := util.NewLockedMap[string, StateMerger](poolSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		pool = i
	}

	g, c := newFuncGetStateMergers(
		pool,
		getState,
		extractStateMergeValueKeys,
	)
	return g,
		func() (uint64, traverseStateMergersFunc) {
			return uint64(pool.Len()),
				func(f func(string, StateMerger) (bool, error)) (gerr error) {
					defer func() {
						c()
						pool.Close()
					}()

					_ = pool.Traverse(func(stateKey string, merger StateMerger) (ok bool) {
						ok, gerr = f(stateKey, merger)

						return ok && gerr == nil
					})

					return gerr
				}
		},
		nil
}

func newFuncSetStateOps(mapSize uint64) (
	mergeStateOpFunc,
	unmergeStateOpFunc,
	traverseStateOpsFunc,
	error,
) {
	var factPool util.LockedMap[uint64, util.Hash]

	switch i, err := util.NewLockedMap[uint64, util.Hash](mapSize, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		factPool = i
	}

	var m util.LockedMap[string, map[uint64]struct{}]

	switch i, err := util.NewLockedMap[string, map[uint64]struct{}](mapSize, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		m = i
	}

	var keyPool util.LockedMap[uint64, []string]

	switch i, err := util.NewLockedMap[uint64, []string](mapSize, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		keyPool = i
	}

	set := func(opindex uint64, fact util.Hash, stateKeys []string, x xMergeFlag) (ok bool) {
		_ = dutil.TraverseSlice(stateKeys, func(_ int, stateKey string) (bool, error) {
			_, _, _, _ = m.SetOrRemove(stateKey,
				func(prev map[uint64]struct{}, _ bool) (map[uint64]struct{}, bool, error) {
					switch x {
					case mergeFlag:
						if fact == nil {
							return nil, false, errors.Errorf("nil fact hash")
						}

						if _, found := prev[opindex]; found {
							return nil, false, util.ErrLockedSetIgnore
						}

						if prev == nil {
							prev = map[uint64]struct{}{}
						}

						prev[opindex] = struct{}{}
					case unmergeFlag:
						if fact != nil {
							return nil, false, errors.Errorf("not nil fact hash")
						}

						if _, found := prev[opindex]; !found {
							return nil, false, util.ErrLockedSetIgnore
						}

						if len(prev) < 2 {
							ok = true

							return nil, true, nil
						}

						delete(prev, opindex)
					}

					ok = true

					return prev, false, nil
				},
			)

			return true, nil
		})

		return ok
	}

	unmerge := func(opindex uint64) bool {
		var stateKeys []string

		_, _ = keyPool.Remove(opindex, func(s []string, found bool) error {
			if !found {
				return nil
			}

			stateKeys = s

			return nil
		})

		if len(stateKeys) < 1 {
			return false
		}

		return set(opindex, nil, stateKeys, unmergeFlag)
	}

	return func(opindex uint64, fact util.Hash, stateKeys []string) bool {
			if _, created, _ := keyPool.Set(opindex, func(_ []string, found bool) ([]string, error) {
				if found {
					return nil, util.ErrLockedSetIgnore
				}

				return stateKeys, nil
			}); !created {
				return false
			}

			ok := set(opindex, fact, stateKeys, mergeFlag)
			if ok {
				_ = factPool.SetValue(opindex, fact)
			}

			return ok
		},
		func(ops []uint64) bool {
			var notOk bool

			dutil.TraverseSlice(ops, func(_ int, opindex uint64) (bool, error) {
				if !unmerge(opindex) {
					notOk = true
				}

				return true, nil
			})

			return !notOk
		},
		func(f dutil.In2ErrorFunc[int, getStateOpsFunc]) error {
			defer func() {
				m.Close()
				keyPool.Close()
				factPool.Close()
			}()

			return f(m.Len(), func(stateKey string) ([]util.Hash, error) {
				switch i, found := m.Value(stateKey); {
				case !found, len(i) < 1:
					return nil, nil
				default:
					defer dutil.ClearMap(i)

					facts := make([]util.Hash, len(i))

					var n int

					if err := dutil.TraverseMap(i, func(opindex uint64, _ struct{}) (bool, error) {
						switch h, found := factPool.Value(opindex); {
						case !found:
							return false, util.ErrNotFound.Errorf("fact not found, %d", opindex)
						default:
							facts[n] = h
							n++

							return true, nil
						}
					}); err != nil {
						return nil, err
					}

					slices.SortFunc(facts, func(a, b util.Hash) int {
						return strings.Compare(a.String(), b.String())
					})

					return facts, nil
				}
			})
		},
		nil
}

func newStateMergeValuesPool(poolSize uint64) (
	_ opindexGetStateMergeValuesFunc,
	_ opindexSetStateMergeValuesFunc,
	closef func(),
	_ error,
) {
	var pool util.LockedMap[uint64, []StateMergeValue]

	switch i, err := util.NewLockedMap[uint64, []StateMergeValue](poolSize, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pool = i
	}

	return newFuncGetStateMergeValuesFromPool(pool),
		func(opindex uint64, values []StateMergeValue) (bool, error) {
			_, created, _ := pool.Set(opindex, func(_ []StateMergeValue, found bool) ([]StateMergeValue, error) {
				if found {
					return nil, util.ErrLockedSetIgnore
				}

				return values, nil
			})

			return created, nil
		},
		pool.Close,
		nil
}

func stateOwnersFuncByCount(workers []string, share uint64) (getStateOwnersFunc, func(), error) {
	var f func(string) []uint64
	var closef func()

	switch i, c, err := dutil.DistributeByCount[string](uint64(len(workers)), share); {
	case err != nil:
		return nil, nil, err
	default:
		f, closef = i, c
	}

	return func(stateKey string) (owners []string) {
			p := f(stateKey)

			owners = make([]string, len(p))
			for i := range p {
				owners[i] = workers[p[i]]
			}

			return owners
		},
		closef,
		nil
}

func opsNShareWorkers(workers []string, share uint64, ophs [][2]util.Hash) (
	func() (opsWorkers []string),
	workerOperationsFunc,
	error,
) {
	var workerMaxItems dutil.InFunc[uint64, uint64]
	var distribute dutil.In2ErrorFunc[uint64, dutil.InErrorFunc[uint64]]

	switch w, i, err := newDistributeSerialNShare(uint64(len(workers)), uint64(len(ophs)), share); {
	case err != nil:
		return nil, nil, err
	default:
		workerMaxItems, distribute = w, i
	}

	var add dutil.AppendSliceAddFunc[string]
	var done dutil.OutFunc[[]string]

	if err := dutil.RAppendSlice[string](uint64(len(workers)), 0, &add, &done); err != nil {
		return nil, nil, err
	}

	return func() (opsWorkers []string) {
			_ = dutil.TraverseSlice(workers, func(i int, worker string) (bool, error) {
				if workerMaxItems(uint64(i)) > 0 {
					add(worker)
				}

				return true, nil
			})

			return done()
		},
		func(worker string, f opindexOpHashesFunc) error {
			workerIndex := slices.Index(workers, worker)
			if workerIndex < 0 {
				return util.ErrNotFound.Errorf("unknown worker=%q", worker)
			}

			if f != nil {
				return distribute(uint64(workerIndex), func(opindex uint64) error {
					return f(opindex, ophs[opindex][0], ophs[opindex][1])
				})
			}

			return nil
		},
		nil
}

func newDistributeSerialNShare(workers, items, share uint64) (
	dutil.InFunc[uint64, uint64],
	dutil.In2ErrorFunc[uint64 /* worker */, dutil.InErrorFunc[uint64]],
	error,
) {
	switch {
	case workers < 1:
		return nil, nil, errors.Errorf("zero workers")
	case items < 1:
		return func(uint64) uint64 { return 0 },
			func(uint64, dutil.InErrorFunc[uint64]) error {
				return nil
			},
			nil
	case share < 1:
		return nil, nil, errors.Errorf("zero share")
	case share > workers:
		share = workers
	}

	pos := func(worker, item uint64) uint64 {
		return uint64(math.Floor(
			float64((workers*item)+worker) / float64(share),
		))
	}

	workerMaxItems := newFuncDistributeWorkerMaxItems(workers, items, share)

	return workerMaxItems, func(worker uint64, f dutil.InErrorFunc[uint64]) error {
			switch {
			case worker >= workers:
				return util.ErrNotFound.Errorf("unknown worker=%d", worker)
			case f == nil:
				return nil
			}

			maxItems := workerMaxItems(worker)
			if maxItems < 1 {
				return nil
			}

			for item := uint64(0); item < maxItems; item++ {
				if err := f(pos(worker, item)); err != nil {
					return err
				}
			}

			return nil
		},
		nil
}

func newFuncDistributeWorkerMaxItems(workers, items, share uint64) dutil.InFunc[uint64, uint64] {
	total := items * share

	i := uint64(math.Floor(float64(total) / float64(workers)))
	j := total - (i * workers)

	return func(worker uint64) uint64 {
		if worker >= j {
			return i
		}

		return i + 1
	}
}
