package dop5

import (
	"context"
	stderrors "errors"
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
	workerOperationsFunc         = dutil.In2ErrorFunc[string /* worker */, OpindexOpHashesFunc]
	CheckFailedWorkersFunc       = func(worker string, err error) (ok, enough bool, failed error)
	checkWeightFailedWorkersFunc = func(worker string, weight uint64, err error) (ok, enough bool, failed error)
	getStateOwnersFunc           = func(stateKey string) []string
	mergeStateOpFunc             = func(opindex uint64, facth util.Hash, stateKeys []string) bool
	unmergeStateOpFunc           = func(ops []uint64) bool
	getStateOpsFunc              = func(stateKey string) ([]util.Hash, error)
	traverseStateOpsFunc         = func(dutil.In2ErrorFunc[int, getStateOpsFunc]) error
)

var ErrWorkersFailed = dutil.NewSError("workers failed")

func minEnoughMaxFailed(workers, share uint64) (minEnough, maxFailed uint64, _ error) { //nolint:nonamedreturns
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

	return func(worker string, weight uint64, err error) (ok, enough bool, failed error) { //nolint:nonamedreturns
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
				var gerr error = ErrWorkersFailed
				if lastError != nil {
					gerr = stderrors.Join(ErrWorkersFailed, lastError)
				}

				doneFailed = gerr
			}

			if ready && doneFailed == nil && enoughCount >= minEnough {
				doneEnough = true
			}

			if doneEnough || doneFailed != nil {
				clear(pool)
			}

			if !ready {
				return true, false, nil
			}

			return true, doneEnough, doneFailed
		},
		nil
}

func NewFuncCheckFailedWorkers(
	workers, share uint64,
	blockUntil func(worker string) bool,
) (
	CheckFailedWorkersFunc,
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

func newFuncSVMs(
	poolSize uint64,
	getState GetStateFunc,
	extractSMVKeys extractSMVKeysFunc,
) (
	mergerGetSVMsFunc,
	func() (size uint64 /* approximate size */, _ iter.Seq2[string /* state key */, SVM]),
	error,
) {
	var pool dutil.LockedMap[string, SVM]

	switch i, err := dutil.NewLockedMap[string, SVM](nil, poolSize); {
	case err != nil:
		return nil, nil, err
	default:
		pool = i
	}

	g, c := newFuncGetSVMs(
		pool,
		getState,
		extractSMVKeys,
	)

	return g,
		func() (uint64, iter.Seq2[string, SVM]) {
			return dutil.SafeIntToUint64(pool.Len()),
				func(yield func(string, SVM) bool) {
					defer func() {
						c()
						pool.Close()
					}()

					if yield != nil {
						for stateKey, merger := range pool.Seq2 {
							if !yield(stateKey, merger) {
								return
							}
						}
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
	var factPool dutil.LockedMap[uint64, util.Hash]

	switch i, err := dutil.NewLockedMap[uint64, util.Hash](nil, mapSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		factPool = i
	}

	var m dutil.LockedMap[string, map[uint64]int]

	switch i, err := dutil.NewLockedMap[string, map[uint64]int](nil, mapSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		m = i
	}

	var keyPool dutil.LockedMap[uint64, []string]

	switch i, err := dutil.NewLockedMap[uint64, []string](nil, mapSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		keyPool = i
	}

	set := func(opindex uint64, stateKeys []string, x xMergeFlag) bool {
		var ok bool

		for stateKey := range slices.Values(stateKeys) {
			_, _ = m.Set(stateKey, func(prev map[uint64]int, _ bool) (map[uint64]int, error) {
				switch x {
				case mergeFlag:
					if i, found := prev[opindex]; found && i < 1 {
						return nil, dutil.ErrLockedSetIgnore
					}

					if prev == nil {
						prev = map[uint64]int{}
					}

					prev[opindex] = 1
				case unmergeFlag:
					if i, found := prev[opindex]; found && i < 1 {
						return nil, dutil.ErrLockedSetIgnore
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

	return func(opindex uint64, facth util.Hash, stateKeys []string) bool {
			created, _ := keyPool.Set(opindex, func(_ []string, found bool) ([]string, error) {
				if found {
					return nil, dutil.ErrLockedSetIgnore
				}

				_ = set(opindex, stateKeys, mergeFlag)
				_ = factPool.SetValue(opindex, facth)

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

func newSMVsPool(poolSize uint64) (
	opindexGetSMVsFunc,
	opindexSetSMVsFunc,
	func(), /* close */
	error,
) {
	var pool dutil.LockedMap[uint64, []SMV]

	switch i, err := dutil.NewLockedMap[uint64, []SMV](nil, poolSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pool = i
	}

	return newFuncGetSMVsFromPool(pool),
		func(opindex uint64, values []SMV) (bool, error) {
			created, _ := pool.Set(opindex, func(_ []SMV, found bool) ([]SMV, error) {
				if found {
					return nil, dutil.ErrLockedSetIgnore
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

	return func(stateKey string) []string {
			p := f(stateKey)

			owners := make([]string, len(p))
			for i := range p {
				owners[i] = workers[p[i]]
			}

			return owners
		},
		closef,
		nil
}

func opsNShareWorkers(workers []string, share uint64, ophs [][2]util.Hash) (
	iter.Seq2[string /* worker */, uint64 /* max items */],
	workerOperationsFunc,
	error,
) {
	var workerMaxItems dutil.InFunc[uint64, uint64]
	var distribute dutil.In2ErrorFunc[uint64, dutil.InErrorFunc[uint64]]

	switch w, i, err := NewDistributeSerialNShare(uint64(len(workers)), uint64(len(ophs)), share); {
	case err != nil:
		return nil, nil, err
	default:
		workerMaxItems, distribute = w, i
	}

	return func(yield func(string, uint64) bool) {
			for i, worker := range slices.All(workers) {
				if n := workerMaxItems(dutil.SafeIntToUint64(i)); n > 0 {
					if !yield(worker, n) {
						return
					}
				}
			}
		},
		func(worker string, f OpindexOpHashesFunc) error {
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

func NewDistributeSerialNShare(workers, items, share uint64) (
	dutil.InFunc[uint64, uint64],
	dutil.In2ErrorFunc[uint64 /* worker */, dutil.InErrorFunc[uint64]],
	error,
) {
	switch {
	case workers < 1:
		return nil, nil, errors.New("zero workers")
	case items < 1:
		return func(uint64) uint64 { return 0 },
			func(uint64, dutil.InErrorFunc[uint64]) error {
				return nil
			},
			nil
	case share < 1:
		return nil, nil, errors.New("zero share")
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

			for item := range maxItems {
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
	ws               []string
	opsws            []string
	sos              []string
	opsShare         uint64
	statesShare      uint64
	failedOpsWorkers uint64
	failedSos        uint64
	l                sync.RWMutex
	isSosReady       bool
	failed           bool
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

	clear(p.pool)
}

func (p *failedWorkerPool) workerError(worker string, err error) (added, failed bool, _ error) { //nolint:nonamedreturns
	if err == nil {
		return false, false, errors.New("nil error")
	}

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
			clear(p.pool)

			return true, true, nil
		}
	}

	if p.isSosReady {
		if p.subOwnerError(worker) {
			return true, true, nil
		}
	}

	return true, false, nil
}

func (p *failedWorkerPool) subOwnerError(worker string) bool {
	if slices.Index(p.sos, worker) < 0 {
		return false
	}

	p.failedSos++

	if p.failedSos >= p.statesShare {
		p.failed = true
		clear(p.pool)

		return true
	}

	return false
}

func (p *failedWorkerPool) statesOwnersReady(sos []string) (bool, error) {
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
		if err := p.pool[worker]; err != nil && p.subOwnerError(worker) {
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
		return nil, errors.New("not ready")
	}

	return p.sos, nil
}

func (p *failedWorkerPool) activeWorkers() (bool, iter.Seq[string]) {
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

func (p *failedWorkerPool) activeOpsWorkers(f func(iter.Seq[string])) bool {
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

func (p *failedWorkerPool) activeStatesOwners(f func(iter.Seq[string])) (bool, error) {
	p.l.RLock()
	defer p.l.RUnlock()

	if p.failed {
		return true, nil
	}

	if !p.isSosReady {
		return false, errors.New("not ready")
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

func (p *failedWorkerPool) activeNonStatesOwners(f func(iter.Seq[string])) (bool, error) {
	p.l.RLock()
	defer p.l.RUnlock()

	if p.failed {
		return true, nil
	}

	if !p.isSosReady {
		return false, errors.New("not ready")
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

type newWorkerDistributeSMVsContextChansFunc = func(context.Context) (
	dutil.InErrorFunc[interface{}],
	func(),
	iter.Seq[interface{}],
)

func newFuncDelayedWorkerDistributeSMVs(
	newf NewWorkerDistributeSMVsFunc,
	newChans newWorkerDistributeSMVsContextChansFunc,
) func(context.Context, string) (
	WorkerProcessInsertSMVsFunc,
	dutil.In2ErrorFunc[[]string, []string],
	dutil.ErrorFunc,
	error,
) {
	return func(ctx context.Context, worker string) (
		WorkerProcessInsertSMVsFunc,
		dutil.In2ErrorFunc[[]string, []string],
		dutil.ErrorFunc,
		error,
	) {
		wctx, wctxCancel, _, wait := dutil.NewContextWait(ctx)

		insert, insertDone, seq := newChans(wctx)

		runseq := func() error {
			var sendOp WorkerProcessInsertSMVsFunc
			var sendOwners dutil.In2ErrorFunc[[]string, []string]

			oncenew := sync.OnceValue(func() error {
				switch p, o, err := newf(wctx, worker); {
				case err != nil:
					return err
				default:
					sendOp, sendOwners = p, o

					return nil
				}
			})

			next, stop := iter.Pull(seq)
			defer stop()

		end:
			for {
				i, ok := next()
				if !ok {
					break end
				}

				switch err := oncenew(); {
				case err != nil:
					return err
				case sendOp == nil:
					break end
				}

				switch t := i.(type) {
				case [3]interface{}:
					if err := sendOp(t[0].(uint64), t[1].(util.Hash), t[2].([]SMV)); err != nil { //nolint:forcetypeassert
						return err
					}
				case [2][]string:
					if err := sendOwners(t[0], t[1]); err != nil {
						return err
					}
				}
			}

			return nil
		}

		go func() { _ = wctxCancel(runseq()) }()

		ifErrWctxCancel := dutil.NewIfErrErrorInFunc(wctxCancel, dutil.ErrContextUnlimitedChanInsertDone)

		return func(opindex uint64, facth util.Hash, values []SMV) error {
				return ifErrWctxCancel(insert([3]interface{}{opindex, facth, values}))
			},
			func(subws, affected []string) error {
				defer insertDone()

				return ifErrWctxCancel(insert([2][]string{subws, affected}))
			},
			wait,
			nil
	}
}

type newWorkersDistributeSMVsContextChansFunc = func(context.Context) (
	dutil.InErrorFunc[interface{}],
	func(),
	iter.Seq[interface{}],
)

func newFuncWorkerProcessWorkersDistributeSMVs( //nolint:gocyclo,funlen
	ctx context.Context,
	local string,
	workers []string,
	newf newWorkerProcessDistributeSMVsFunc,
	newChans newWorkersDistributeSMVsContextChansFunc,
) (
	workerDistributeWorkerSMVsFunc,
	workerStatesOwnersSetFunc,
	dutil.ErrorFunc,
	error,
) {
	log := dutil.LoadContextValueLogger(ctx)

	var pool dutil.LockedMap[string, [2]interface{}]

	nworkers := uint64(len(workers))

	switch i, err := dutil.NewLockedMap[string, [2]interface{}](nil, nworkers); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pool = i
	}

	var created dutil.LockedMap[string, struct{}]

	switch i, err := dutil.NewLockedMap[string, struct{}](nil, nworkers); {
	case err != nil:
		return nil, nil, nil, err
	default:
		created = i
	}

	wctx, wctxCancel, _, wait := dutil.NewContextWait(ctx)

	insertf, insertDone, seq := newChans(wctx)
	insert := func(v interface{}) error {
		if err := insertf(v); err != nil {
			return wctxCancel(err)
		}

		return nil
	}

	go func() {
		defer func() {
			_ = wctxCancel(nil)

			pool.Close()
			created.Close()
		}()

		for i := range seq {
			switch t := i.(type) {
			case [4]interface{}:
				worker := t[0].(string) //nolint:forcetypeassert

				var sendOp WorkerProcessInsertSMVsFunc
				var gerr error

				_, _ = pool.Set(worker, func(f [2]interface{}, found bool) ([2]interface{}, error) {
					var v [2]interface{}

					if !found {
						switch p, o, err := newf(ctx, worker); {
						case err != nil:
							gerr = err

							return [2]interface{}{err, nil}, nil //nolint:nilerr
						case p == nil:
							return v, nil
						default:
							sendOp = p

							return [2]interface{}{sendOp, o}, nil
						}
					} else if f[0] == nil {
						return v, dutil.ErrLockedSetIgnore
					}

					if err, ok := f[0].(error); ok {
						gerr = err

						return v, dutil.ErrLockedSetIgnore
					}

					sendOp = f[0].(WorkerProcessInsertSMVsFunc) //nolint:forcetypeassert

					return v, dutil.ErrLockedSetIgnore
				})

				if gerr == nil && sendOp != nil {
					gerr = sendOp(t[1].(uint64), t[2].(util.Hash), t[3].([]SMV)) //nolint:forcetypeassert
				}

				if gerr != nil {
					log.Error().Err(gerr).
						Str("worker", worker).
						Uint64("opindex", t[1].(uint64)). //nolint:forcetypeassert
						Msg("failed to send op")
				}
			case [3]interface{}: // owners
				worker := t[0].(string) //nolint:forcetypeassert

				var sendOwners dutil.In2ErrorFunc[[]string, []string]
				var gerr error

				_, _ = pool.Set(worker, func(f [2]interface{}, found bool) ([2]interface{}, error) {
					var v [2]interface{}

					if !found {
						switch _, o, err := newf(ctx, worker); {
						case err != nil:
							gerr = err

							return [2]interface{}{err, nil}, nil //nolint:nilerr
						default:
							sendOwners = o

							return v, nil
						}
					} else if f[1] == nil {
						return v, dutil.ErrLockedSetIgnore
					}

					if err, ok := f[0].(error); ok {
						gerr = err

						return v, dutil.ErrLockedSetIgnore
					}

					sendOwners = f[1].(dutil.In2ErrorFunc[[]string, []string]) //nolint:forcetypeassert

					return v, nil
				})

				if gerr == nil && sendOwners != nil {
					var subws, affected []string
					if t[1] != nil {
						subws = t[1].([]string) //nolint:forcetypeassert
					}

					if t[2] != nil {
						affected = t[2].([]string) //nolint:forcetypeassert
					}

					gerr = sendOwners(subws, affected)
				}

				if gerr != nil {
					log.Error().Err(gerr).
						Str("worker", worker).
						Msg("failed to send owners")
				}
			default:
				_ = wctxCancel(errors.Errorf("unexpected input, %T", t))
			}
		}
	}()

	return func(worker string, opindex uint64, facth util.Hash, values []SMV) error {
			if worker == local {
				return nil
			}

			return insert([4]interface{}{worker, opindex, facth, values})
		},
		func(worker string, subws, affected []string) error {
			if worker == local {
				return nil
			}

			_, _ = created.Set(worker, func(_ struct{}, found bool) (struct{}, error) {
				if found {
					return struct{}{}, dutil.ErrLockedSetIgnore
				}

				return struct{}{}, nil
			})

			return insert([3]interface{}{worker, subws, affected})
		},
		func() error {
			for worker := range slices.Values(workers) {
				if worker == local || created.Exists(worker) {
					continue
				}

				if err := insert([3]interface{}{worker, nil, nil}); err != nil {
					return err
				}
			}

			insertDone()

			return wait()
		},
		nil
}
