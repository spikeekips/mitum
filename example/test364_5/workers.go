package main

import (
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type (
	checkEnoughFailedWorkerFunc  func(worker string, _ func() error) (ok, enough, failed bool, _ error)
	checkEnoughFailedWorkersFunc func(minEnough, maxFailed uint64) (checkEnoughFailedWorkerFunc, func(), error)
)

var errWorkersFailed = util.NewIDError("workers failed")

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

			if failed && gerr == nil {
				gerr = errWorkersFailed
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

func newFuncLocalCheckEnoughFailedWorkers(local string) checkEnoughFailedWorkersFunc {
	return func(minEnough, maxFailed uint64) (checkEnoughFailedWorkerFunc, func(), error) {
		return newFuncBaseCheckEnoughFailedWorkers(
			minEnough, maxFailed,
			func(worker string) bool { return worker == local },
		)
	}
}

type traverseStateMergersFunc func(func(stateKey string, merger StateMerger) (bool, error)) error

func newFuncStateMergers(
	poolSize uint64,
	getState getStateFunc,
	extractStateMergeValueKeys extractStateMergeValueKeysFunc,
) (
	getStateMergers mergerGetStateMergersFunc,
	traverse traverseStateMergersFunc,
	closef func(),
	_ error,
) {
	var pool util.LockedMap[string, StateMerger]

	switch i, err := util.NewShardedMap[string, StateMerger](poolSize, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pool = i
	}

	g, c := newFuncGetStateMergers(
		pool,
		getState,
		extractStateMergeValueKeys,
	)
	return g,
		func(f func(string, StateMerger) (bool, error)) (gerr error) {
			_ = pool.Traverse(func(stateKey string, merger StateMerger) (ok bool) {
				ok, gerr = f(stateKey, merger)

				return ok && gerr == nil
			})

			return gerr
		},
		func() {
			c()
			pool.Close()
		},
		nil
}

func newFuncSetStateOps(mapSize uint64) (
	setStateOpFunc,
	getStateOpsFunc,
	error,
) {
	var m util.LockedMap[string, map[uint64] /* {phase, count} */ [2]int]

	switch i, err := util.NewShardedMap[string, map[uint64][2]int](mapSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i
	}

	return func(phase int, opindex uint64, stateKeys []string, x xMergeFlag) {
			_ = util.TraverseSliceV1(stateKeys, func(_ int, stateKey string) (bool, error) {
				_, _, _ = m.Set(stateKey, func(prev map[uint64][2]int, found bool) (map[uint64][2]int, error) {
					c := 1
					if x == unmergeFlag {
						c = -1
					}

					switch {
					case !found:
						prev = map[uint64][2]int{opindex: {phase, c}}
					default:
						switch v, opfound := prev[opindex]; {
						case !opfound:
							prev[opindex] = [2]int{phase, c}
						case phase <= v[0]:
							return nil, util.ErrLockedSetIgnore
						default:
							prev[opindex] = [2]int{phase, v[1] + c}
						}
					}

					return prev, nil
				})

				return true, nil
			})
		},
		func(f func(getStateOpsCallbackFunc) error) error {
			defer m.Close()

			return f(func(stateKey string) (ops []uint64) {
				_ = m.Get(stateKey, func(mops map[uint64][2]int, found bool) error {
					if !found || len(mops) < 1 {
						return nil
					}

					var add util.CompactAppendSliceAddFunc[uint64]
					var done util.CompactAppendSliceDoneFunc[uint64]

					switch a, d, err := util.CompactAppendSliceV1[uint64](uint64(len(mops))); {
					case err != nil:
						return nil
					default:
						defer d(nil)

						add = a
						done = d
					}

					_ = traverseMap(mops, func(opindex uint64, v [2]int) (bool, error) {
						if v[1] > 0 {
							add(opindex)
						}

						return true, nil
					})

					_ = done(func(s []uint64) error {
						ops = slices.Clone(s)

						return nil
					})

					return nil
				})

				return ops
			})
		},
		nil
}

func newStateMergeValuesPool(maxOps, poolSize uint64) (
	_ mergerGetStateMergeValuesFunc,
	closef func(),
	_ error,
) {
	if maxOps < 1 {
		maxOps = 1
	}

	if poolSize < maxOps {
		poolSize = maxOps
	}

	var pool util.LockedMap[uint64, []StateMergeValue]

	switch i, err := util.NewLockedMap[uint64, []StateMergeValue](poolSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		pool = i
	}

	return getStateMergeValuesFuncFromPool(pool),
		pool.Close,
		nil
}
