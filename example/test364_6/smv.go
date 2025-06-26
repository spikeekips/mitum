package dop6

import (
	"context"
	"iter"
	"maps"
	"slices"

	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	workerDistributeSMVsFunc    = func(uint64, SMV, []SMV) (localSubv SMV, localAddvs []SMV, subs []string, affected []string, _ error)
	NewWorkerDistributeSMVsFunc = func(_ context.Context, worker string) (
		mergerMergeSMVsFunc,
		workerWorkerStateOwnersFunc,
		error,
	)
)

func isEqualSMV(a, b SMV) bool {
	switch {
	case a == nil && b == nil,
		a == nil, b == nil:
		return false
	case a.Key() != b.Key(),
		!base.IsEqualStateValue(a.Value(), b.Value()):
		return false
	default:
		return true
	}
}

func newCollectStateOwners(workersSize uint64) (
	addStateOwnerSetFunc,
	func(string) ([]string, []string),
	func() ([]string, []string),
	func(),
	error,
) {
	var msubws dutil.LockedMap[string /* sub worker */, map[string]struct{} /* affected */]
	var maffected dutil.LockedMap[string /* affected */, map[string]struct{} /* sub workers */]

	switch i, err := dutil.NewLockedMap[string, map[string]struct{}](nil, workersSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		msubws = i
	}

	switch i, err := dutil.NewLockedMap[string, map[string]struct{}](nil, workersSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		maffected = i
	}

	add := func(
		m dutil.LockedMap[string, map[string]struct{}],
		a string, b []string,
	) {
		_, _ = m.Set(a, func(prev map[string]struct{}, _ bool) (map[string]struct{}, error) {
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

	return func(subw string, affected []string) {
			add(msubws, subw, affected)

			for worker := range slices.Values(affected) {
				add(maffected, worker, []string{subw})
			}
		},
		func(worker string) ([]string, []string) {
			seq := func(m iter.Seq[string]) iter.Seq[string] {
				return func(yield func(string) bool) {
					for o := range m {
						if o == "" {
							continue
						}

						if !yield(o) {
							return
						}
					}
				}
			}

			var subs, affected []string

			if i, found := maffected.Value(worker); found {
				subs = slices.Collect(seq(maps.Keys(i)))
				slices.Sort(subs)
			}

			if i, found := msubws.Value(worker); found {
				affected = slices.Collect(seq(maps.Keys(i)))
				slices.Sort(affected)
			}

			return subs, affected
		},
		func() ([]string, []string) {
			seq := func(m dutil.LockedMap[string, map[string]struct{}]) iter.Seq[string] {
				return func(yield func(string) bool) {
					for o := range m.Seq2 {
						if o == "" {
							continue
						}

						if !yield(o) {
							return
						}
					}
				}
			}

			return slices.Collect(seq(msubws)), slices.Collect(seq(maffected))
		},
		func() {
			msubws.Close()
			maffected.Close()
		},
		nil
}

func newWorkerDistributeSMVs(
	ctx context.Context,
	local string,
	workers []string,
	getFactStateOwners getFactStateOwnersFunc,
	newf NewWorkerDistributeSMVsFunc,
) (
	workerDistributeSMVsFunc,
	func(),
	func(error) (func() ([]string, []string), dutil.ErrorFunc, error),
	error,
) {
	workersSize := uint64(len(workers))

	var add func(uint64, SMV, []SMV) ([]string, []string, iter.Seq2[string, [2][]SMV], error)
	var getOwners func(string) ([]string, []string)
	var allOwners func() ([]string, []string)
	var closeCollectOwners func()

	switch a, g, o, c, err := newWorkerAssignSMVs(workersSize, getFactStateOwners); {
	case err != nil:
		return nil, nil, nil, err
	default:
		add, getOwners, allOwners, closeCollectOwners = a, g, o, c
	}

	newJob, jobDone, jobWait := NewFuncJobWorker(ctx, dutil.SafeUint64ToInt64(workersSize))

	var sendPool dutil.LockedMap[string, [3]any]

	switch m, err := dutil.NewLockedMap[string, [3]any](nil, workersSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		sendPool = m
	}

	sendSMVs := func(worker string, opindex uint64, subv SMV, addvs []SMV) error {
		var send mergerMergeSMVsFunc

		var gerr error

		_, _ = sendPool.Set(worker, func(i [3]any, found bool) ([3]any, error) {
			if !found {
				switch s, o, err := newf(ctx, worker); {
				case err != nil:
					gerr = err

					i[2] = err
				default:
					i = [3]any{s, o, nil}
				}
			}

			if i[2] == nil {
				send = i[0].(mergerMergeSMVsFunc) //nolint:forcetypeassert
			}

			return [3]any{}, dutil.ErrLockedSetIgnore
		})

		if gerr != nil {
			return gerr
		}

		return send(opindex, subv, addvs)
	}

	sendOwners := func(worker string) error {
		var send workerWorkerStateOwnersFunc

		if _, err := sendPool.Set(worker, func(i [3]any, found bool) ([3]any, error) {
			if !found {
				switch _, o, err := newf(ctx, worker); {
				case err != nil:
					return [3]any{}, err
				default:
					send = o

					return [3]any{}, dutil.ErrLockedSetIgnore
				}
			}

			if i[2] != nil {
				return [3]any{}, i[2].(error) //nolint:forcetypeassert
			}

			send = i[1].(workerWorkerStateOwnersFunc) //nolint:forcetypeassert

			return [3]any{}, dutil.ErrLockedSetIgnore
		}); err != nil {
			return err
		}

		subs, affected := getOwners(worker)

		return send(subs, affected)
	}

	return func(opindex uint64, subv SMV, addvs []SMV) (SMV, []SMV, []string, []string, error) {
			var subs, affected []string
			var seq iter.Seq2[string, [2][]SMV]

			switch b, a, s, err := add(opindex, subv, addvs); {
			case err != nil:
				return nil, nil, nil, nil, err
			default:
				subs, affected, seq = b, a, s
			}

			var localSubv SMV
			var localAddvs []SMV

			for worker, vs := range seq {
				if worker == local {
					if len(vs[0]) > 0 {
						localSubv = vs[0][0]
					}

					localAddvs = vs[1]

					continue
				}

				if err := newJob(func(context.Context) error {
					var s SMV
					if len(vs[0]) > 0 {
						s = vs[0][0]
					}

					_ = sendSMVs(worker, opindex, s, vs[1]) // NOTE ignore error

					return nil
				}); err != nil {
					return nil, nil, nil, nil, err
				}
			}

			return localSubv, localAddvs, subs, affected, nil
		},
		jobDone,
		func(err error) (func() ([]string, []string), dutil.ErrorFunc, error) {
			if err := jobWait(err); err != nil {
				defer func() {
					closeCollectOwners()
				}()

				return nil, nil, err
			}

			return allOwners,
				func() error {
					defer func() {
						closeCollectOwners()
					}()

					cnewJob, cjobDone, cjobWait := NewFuncJobWorker(ctx, dutil.SafeUint64ToInt64(workersSize))

					for worker := range slices.Values(workers) {
						if err := cnewJob(func(context.Context) error {
							_ = sendOwners(worker) // NOTE ignore error

							return nil
						}); err != nil {
							return err
						}
					}

					cjobDone()

					return cjobWait(nil)
				},
				nil
		},
		nil
}

func NewFuncGetStateOwnersMurmur3(workers []string, share uint64) GetStateOwnersFunc {
	o := uint64(len(workers))
	if share > o {
		share = o
	}

	return func(stateKey string) []string {
		p := dutil.Murmur3(0, stateKey)

		owners := make([]string, 0, share)
		for i := range share {
			owners = append(owners, workers[(p+i)%o])
		}

		return owners
	}
}

func newWorkerAssignSMVs(
	workersSize uint64,
	getFactStateOwners getFactStateOwnersFunc,
) (
	func(uint64, SMV, []SMV) (
		[]string, []string,
		iter.Seq2[string /* worker */, [2][]SMV],
		error,
	),
	func(string) ([]string, []string),
	func() ([]string, []string),
	func(),
	error,
) {
	var addOwner addStateOwnerSetFunc
	var getOwners func(string) ([]string, []string)
	var allOwners func() ([]string, []string)
	var closeCollectOwners func()

	switch a, g, o, c, err := newCollectStateOwners(workersSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		addOwner, getOwners, allOwners, closeCollectOwners = a, g, o, c
	}

	return func(opindex uint64, subv SMV, addvs []SMV) (
			[]string, []string,
			iter.Seq2[string, [2][]SMV],
			error,
		) {
			var addWorker dutil.AppendSliceAddFunc[string]
			var doneWorkers dutil.OutFunc[[]string]

			switch a, d, err := dutil.NewComparableUniqueAppendSlice[string](workersSize, 0); {
			case err != nil:
				return nil, nil, nil, err
			default:
				addWorker, doneWorkers = a, d
			}

			subs, affected := getFactStateOwners(opindex, subv, addvs)

			var msub map[string]SMV
			var maffected map[string][]SMV

			if len(subs) > 0 {
				msub = map[string]SMV{}

				for worker := range slices.Values(subs) {
					msub[worker] = subv
					_, _ = addWorker(worker)
				}
			}

			for i, a := range slices.All(affected) {
				for worker := range slices.Values(a) {
					if maffected == nil {
						maffected = map[string][]SMV{}
					}

					o := maffected[worker]
					if o == nil {
						o = make([]SMV, 0, len(addvs))
					}

					o = append(o, addvs[i])

					maffected[worker] = o
					_, _ = addWorker(worker)
				}
			}

			switch affected := slices.Collect(maps.Keys(maffected)); {
			case len(msub) > 0:
				for subw := range msub {
					addOwner(subw, affected)
				}
			default:
				addOwner("", affected)
			}

			caffected := slices.Concat(affected...)
			slices.Sort(caffected)

			return subs, slices.Compact(caffected), func(yield func(string, [2][]SMV) bool) {
					defer func() {
						clear(msub)
						clear(maffected)
					}()

					for worker := range slices.Values(doneWorkers()) {
						var subvs, affected []SMV

						if i, found := msub[worker]; found {
							subvs = []SMV{i}
						}

						if i, found := maffected[worker]; found {
							affected = i
						}

						if !yield(worker, [2][]SMV{subvs, affected}) {
							return
						}
					}
				},
				nil
		},
		getOwners,
		allOwners,
		closeCollectOwners,
		nil
}

func newFuncGetFactStateOwners(
	f GetStateOwnersFunc,
	cacheSize int,
) (getFactStateOwnersFunc, func()) {
	cache := util.NewLRUGCache[uint64, [2]any](cacheSize)

	return func(opindex uint64, subv SMV, addvs []SMV) ([]string, [][]string) {
			var s []string
			var a [][]string

			if i, found := cache.Get(opindex); found {
				if i[0] != nil {
					s = i[0].([]string)
				}

				if i[1] != nil {
					a = i[1].([][]string)
				}

				return s, a
			}

			if subv != nil {
				s = f(subv.Key())
			}

			if len(addvs) > 0 {
				a = make([][]string, len(addvs))

				for i, v := range slices.All(addvs) {
					a[i] = f(v.Key())
				}
			}

			cache.Set(opindex, [2]any{s, a}, 0)

			return s, a
		},
		cache.Close
}

func newDefaultGetFactStateOwners(
	workers []string,
	statesShare uint64,
	cacheSize int,
) (getFactStateOwnersFunc, func()) {
	return newFuncGetFactStateOwners(NewFuncGetStateOwnersMurmur3(workers, statesShare), cacheSize)
}
