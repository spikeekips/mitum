package main

import (
	"fmt"
	"iter"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	workerOperationsFunc         = dutil.In2ErrorFunc[string /* worker */, opindexOpHashesFunc]
	checkFailedWorkersFunc       = func(worker string, err error) (ok, enough bool, failed error)
	checkWeightFailedWorkersFunc = func(worker string, weight uint64, err error) (ok, enough bool, failed error)
	newCheckFailedWorkersFunc    = func(worker, share uint64, blockUntil func(string) bool) (
		checkFailedWorkersFunc,
		error,
	)
	getStateOwnersFunc     = func(stateKey string) []string
	getOperationOwnersFunc = func(opindex uint64) []string
	mergeStateOpFunc       = func(opindex uint64, fact util.Hash, stateKeys []string) bool
	unmergeStateOpFunc     = func(ops []uint64) bool
	getStateOpsFunc        = func(stateKey string) ([]util.Hash, error)
	traverseStateOpsFunc   = func(dutil.In2ErrorFunc[int, getStateOpsFunc]) error
)

var errWorkersFailed = util.NewIDError("workers failed")

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

func newFuncCheckWeightFailedWorkers(
	workers, share uint64,
	blockUntil func(worker string) bool,
) (
	checkWeightFailedWorkersFunc,
	error,
) {
	var minEnough, maxFailed uint64

	switch e, f, err := minEnoughMaxFailed(workers, share); {
	case err != nil:
		return nil, err
	default:
		minEnough, maxFailed = e, f
	}

	if blockUntil == nil {
		blockUntil = func(string) bool { return true }
	}

	var l sync.Mutex

	var ready bool
	var enoughCount, failedCount uint64
	var doneEnough bool
	var doneFailed error
	var lastError error

	pool := map[string]struct{}{}

	return func(worker string, weight uint64, err error) (ok, enough bool, failed error) {
			l.Lock()
			defer l.Unlock()

			if doneEnough || doneFailed != nil {
				return false, false, doneFailed
			}

			if _, found := pool[worker]; found {
				return false, false, nil
			}

			if !ready && blockUntil(worker) {
				ready = true
			}

			pool[worker] = struct{}{}

			switch {
			case err != nil:
				lastError = err
				failedCount += weight
			default:
				enoughCount += weight
			}

			if ready && failedCount >= maxFailed {
				var gerr error = errWorkersFailed
				if lastError != nil {
					gerr = errWorkersFailed.Wrap(lastError)
				}

				doneFailed = gerr
			}

			if ready && doneFailed == nil && enoughCount >= minEnough {
				doneEnough = true
			}

			if doneEnough || doneFailed != nil {
				dutil.ClearMap(pool)
			}

			if !ready {
				return true, false, nil
			}

			return true, doneEnough, doneFailed
		},
		nil
}

func newFuncCheckFailedWorkers(
	workers, share uint64,
	blockUntil func(worker string) bool,
) (
	checkFailedWorkersFunc,
	error,
) {
	var check checkWeightFailedWorkersFunc

	switch c, err := newFuncCheckWeightFailedWorkers(workers, share, blockUntil); {
	case err != nil:
		return nil, err
	default:
		check = c
	}

	return func(worker string, err error) (bool, bool, error) {
			return check(worker, 1, err)
		},
		nil
}

func newFuncStateValueMergers(
	poolSize uint64,
	getState getStateFunc,
	extractStateMergeValueKeys extractStateMergeValueKeysFunc,
) (
	mergerGetStateValueMergersFunc,
	func() (size uint64 /* approximate size */, _ iter.Seq2[string /* state key */, stateValueMerger]),
	error,
) {
	var pool util.LockedMap[string, stateValueMerger]

	switch i, err := util.NewLockedMap[string, stateValueMerger](poolSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		pool = i
	}

	g, c := newFuncGetStateValueMergers(
		pool,
		getState,
		extractStateMergeValueKeys,
	)
	return g,
		func() (uint64, iter.Seq2[string, stateValueMerger]) {
			return uint64(pool.Len()),
				func(yield func(string, stateValueMerger) bool) {
					defer func() {
						c()
						pool.Close()
					}()

					if yield != nil {
						_ = pool.Traverse(func(stateKey string, merger stateValueMerger) bool {
							return yield(stateKey, merger)
						})
					}
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

	var m util.LockedMap[string, map[uint64]int]

	switch i, err := util.NewLockedMap[string, map[uint64]int](mapSize, nil); {
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

	set := func(opindex uint64, stateKeys []string, x xMergeFlag) (ok bool) {
		for stateKey := range slices.Values(stateKeys) {
			_, _, _ = m.Set(stateKey, func(prev map[uint64]int, _ bool) (map[uint64]int, error) {
				switch x {
				case mergeFlag:
					if i, found := prev[opindex]; found && i < 1 {
						return nil, util.ErrLockedSetIgnore
					}

					if prev == nil {
						prev = map[uint64]int{}
					}

					prev[opindex] = 1
				case unmergeFlag:
					if i, found := prev[opindex]; found && i < 1 {
						return nil, util.ErrLockedSetIgnore
					}

					if prev == nil {
						prev = map[uint64]int{}
					}

					prev[opindex] = 0
				}

				ok = true

				return prev, nil
			})
		}

		return ok
	}

	unmerge := func(opindex uint64) bool {
		switch s, found := keyPool.Value(opindex); {
		case !found:
			return false
		default:
			_ = set(opindex, s, unmergeFlag)

			return true
		}
	}

	return func(opindex uint64, fact util.Hash, stateKeys []string) bool {
			_, created, _ := keyPool.Set(opindex, func(_ []string, found bool) ([]string, error) {
				if found {
					return nil, util.ErrLockedSetIgnore
				}

				_ = set(opindex, stateKeys, mergeFlag)
				_ = factPool.SetValue(opindex, fact)

				return stateKeys, nil
			})

			return created
		},
		func(ops []uint64) bool {
			var notOk bool

			for opindex := range slices.Values(ops) {
				if !unmerge(opindex) {
					notOk = true
				}
			}

			return !notOk
		},
		func(f dutil.In2ErrorFunc[int, getStateOpsFunc]) error {
			defer func() {
				m.Close()
				keyPool.Close()
				factPool.Close()
			}()

			return f(m.Len(), func(stateKey string) ([]util.Hash, error) {
				var facts []util.Hash

				_ = m.Get(stateKey, func(i map[uint64]int, found bool) error {
					if !found || len(i) < 1 {
						return nil
					}

					facts = make([]util.Hash, len(i))

					var n int

					for opindex, x := range maps.All(i) {
						if x < 1 {
							continue
						}

						switch h, found := factPool.Value(opindex); {
						case !found:
							return util.ErrNotFound.Errorf("fact not found, %d", opindex)
						default:
							facts[n] = h
							n++
						}
					}

					facts = facts[:n]

					slices.SortFunc(facts, func(a, b util.Hash) int {
						return strings.Compare(a.String(), b.String())
					})

					return nil
				})

				return facts, nil
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
	var pool util.LockedMap[uint64, []stateMergeValue]

	switch i, err := util.NewLockedMap[uint64, []stateMergeValue](poolSize, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pool = i
	}

	return newFuncGetStateMergeValuesFromPool(pool),
		func(opindex uint64, values []stateMergeValue) (bool, error) {
			_, created, _ := pool.Set(opindex, func(_ []stateMergeValue, found bool) ([]stateMergeValue, error) {
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

	switch a, d, err := dutil.NewLockAppendSlice[string](uint64(len(workers)), 0); {
	case err != nil:
		return nil, nil, err
	default:
		add, done = a, d
	}

	var once sync.Once

	return func() (opsWorkers []string) {
			once.Do(func() {
				for i, worker := range slices.All(workers) {
					if workerMaxItems(uint64(i)) > 0 {
						add(worker)
					}
				}
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

type failedWorkerPool struct {
	pool             map[string]error
	opsShare         uint64
	statesShare      uint64
	ws               []string
	opsws            []string
	sos              []string
	failedOpsWorkers uint64
	failedSos        uint64
	isSosReady       bool
	failed           bool
	l                sync.RWMutex
}

func newFailedWorkerPool(workers, opsWorkers []string, opsShare, statesShare uint64) *failedWorkerPool {
	pool := map[string]error{}

	for worker := range slices.Values(workers) {
		pool[worker] = nil
	}

	return &failedWorkerPool{
		pool:        pool,
		ws:          workers,
		opsws:       opsWorkers,
		opsShare:    opsShare,
		statesShare: statesShare,
	}
}

func (p *failedWorkerPool) Close() {
	p.l.Lock()
	defer p.l.Unlock()

	p.failed = true

	dutil.ClearMap(p.pool)
}

func (p *failedWorkerPool) workerError(worker string, err error) (added, failed bool, _ error) {
	if err == nil {
		return false, false, errors.Errorf("nil error")
	}

	fmt.Printf("worker error: worker=%q err=%+v\n", worker, err)

	p.l.Lock()
	defer p.l.Unlock()

	if p.failed {
		return false, true, nil
	}

	switch prev, found := p.pool[worker]; {
	case !found:
		return false, false, util.ErrNotFound.Errorf("unknown worker, %q", worker)
	case prev != nil:
		return false, false, nil
	}

	p.pool[worker] = err

	if slices.Index(p.opsws, worker) >= 0 {
		p.failedOpsWorkers++
		if p.failedOpsWorkers >= p.opsShare {
			p.failed = true
			dutil.ClearMap(p.pool)

			return true, true, nil
		}
	}

	if p.isSosReady {
		if p.subOwnerError(worker, err) {
			return true, true, nil
		}
	}

	return true, false, nil
}

func (p *failedWorkerPool) subOwnerError(worker string, err error) (failed bool) {
	if slices.Index(p.sos, worker) < 0 {
		return false
	}

	p.failedSos++

	if p.failedSos >= p.statesShare {
		p.failed = true
		dutil.ClearMap(p.pool)

		return true
	}

	return false
}

func (p *failedWorkerPool) statesOwnersReady(sos []string) (failed bool, _ error) {
	p.l.Lock()
	defer p.l.Unlock()

	if p.failed {
		return true, nil
	}

	for worker := range slices.Values(sos) {
		if _, found := p.pool[worker]; !found {
			return false, util.ErrNotFound.Errorf("unknown sub owners, %q", worker)
		}
	}

	p.sos = sos
	p.isSosReady = true

	for worker := range slices.Values(sos) {
		if err := p.pool[worker]; err != nil && p.subOwnerError(worker, err) {
			return true, nil
		}
	}

	return false, nil
}

func (p *failedWorkerPool) workers() []string {
	return p.ws
}

func (p *failedWorkerPool) opsWorkers() []string {
	return p.opsws
}

func (p *failedWorkerPool) statesOwners() ([]string, error) {
	p.l.RLock()
	defer p.l.RUnlock()

	if !p.isSosReady {
		return nil, errors.Errorf("not ready")
	}

	return p.sos, nil
}

func (p *failedWorkerPool) activeWorkers() (failed bool, _ iter.Seq[string]) {
	p.l.RLock()
	defer p.l.RUnlock()

	return p.failed,
		func(yield func(string) bool) {
			p.l.RLock()
			defer p.l.RUnlock()

			slices.Values(p.ws)(func(worker string) bool {
				if p.pool[worker] != nil {
					return true
				}

				return yield(worker)
			})
		}
}

func (p *failedWorkerPool) activeOpsWorkers(f func(iter.Seq[string])) (failed bool) {
	p.l.RLock()
	defer p.l.RUnlock()

	if p.failed {
		return true
	}

	f(func(yield func(string) bool) {
		slices.Values(p.opsws)(func(worker string) bool {
			if p.pool[worker] != nil {
				return true
			}

			return yield(worker)
		})
	})

	return false
}

func (p *failedWorkerPool) activeStatesOwners(f func(iter.Seq[string])) (failed bool, _ error) {
	p.l.RLock()
	defer p.l.RUnlock()

	if p.failed {
		return true, nil
	}

	if !p.isSosReady {
		return false, errors.Errorf("not ready")
	}

	f(func(yield func(string) bool) {
		slices.Values(p.sos)(func(worker string) bool {
			if p.pool[worker] != nil {
				return true
			}

			return yield(worker)
		})
	})

	return false, nil
}

func (p *failedWorkerPool) activeNonStatesOwners(f func(iter.Seq[string])) (failed bool, _ error) {
	p.l.RLock()
	defer p.l.RUnlock()

	if p.failed {
		return true, nil
	}

	if !p.isSosReady {
		return false, errors.Errorf("not ready")
	}

	f(func(yield func(string) bool) {
		slices.Values(p.ws)(func(worker string) bool {
			switch {
			case p.pool[worker] != nil,
				slices.Index(p.sos, worker) >= 0:
				return true
			default:
				return yield(worker)
			}
		})
	})

	return false, nil
}
