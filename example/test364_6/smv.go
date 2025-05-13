package dop6

import (
	"context"
	"iter"
	"maps"
	"slices"

	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

type NewWorkerDistributeSMVsFunc = func(_ context.Context, worker string) (
	mergerMergeSMVsFunc,
	workerWorkerStateOwnersFunc,
	error,
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
			if subw != "" {
				if len(affected) > 0 {
					add(msubws, subw, affected)
				}

				for worker := range slices.Values(affected) {
					add(maffected, worker, []string{subw})
				}
			}
		},
		func(worker string) ([]string, []string) {
			var subs, affected []string

			if i, found := maffected.Value(worker); found {
				subs = slices.Collect(maps.Keys(i))
				slices.Sort(subs)
			}

			if i, found := msubws.Value(worker); found {
				affected = slices.Collect(maps.Keys(i))
				slices.Sort(affected)
			}

			return subs, affected
		},
		func() ([]string, []string) {
			return slices.Collect(maps.Keys(msubws.Map())),
				slices.Collect(maps.Keys(maffected.Map()))
		},
		func() {
			msubws.Close()
			maffected.Close()
		},
		nil
}

func newWorkerDistributeSMVs(
	ctx context.Context,
	workers []string,
	getStateOwners GetStateOwnersFunc,
	newf NewWorkerDistributeSMVsFunc,
) (
	mergerMergeSMVsFunc,
	func(),
	func(error) (func() ([]string, []string), dutil.ErrorFunc, error),
	error,
) {
	workersSize := uint64(len(workers))

	var add func(uint64, SMV, []SMV) (iter.Seq2[string, [2][]SMV], error)
	var getOwners func(string) ([]string, []string)
	var allOwners func() ([]string, []string)
	var closeCollectOwners func()

	switch a, g, o, c, err := newWorkerAssignSMVs(workersSize, getStateOwners); {
	case err != nil:
		return nil, nil, nil, err
	default:
		add, getOwners, allOwners, closeCollectOwners = a, g, o, c
	}

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := dutil.NewFuncJobWorker(ctx, dutil.SafeUint64ToInt64(workersSize)); {
	case err != nil:
		return nil, nil, nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

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

	return func(opindex uint64, subv SMV, addvs []SMV) error {
			var seq iter.Seq2[string, [2][]SMV]

			switch s, err := add(opindex, subv, addvs); {
			case err != nil:
				return err
			default:
				seq = s
			}

			for worker, vs := range seq {
				if err := newJob(func(context.Context) error {
					var s SMV
					if len(vs[0]) > 0 {
						s = vs[0][0]
					}

					_ = sendSMVs(worker, opindex, s, vs[1]) // NOTE ignore error

					return nil
				}); err != nil {
					return err
				}
			}

			return nil
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

					var cnewJob dutil.JobWorkerNewJobFunc
					var cjobDone func()
					var cjobWait dutil.InErrorFunc[error]

					switch n, d, w, err := dutil.NewFuncJobWorker(ctx, dutil.SafeUint64ToInt64(workersSize)); {
					case err != nil:
						return err
					default:
						cnewJob, cjobDone, cjobWait = n, d, w
					}

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
	getStateOwners GetStateOwnersFunc,
) (
	func(uint64, SMV, []SMV) (
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
			iter.Seq2[string, [2][]SMV],
			error,
		) {
			var addWorker dutil.AppendSliceAddFunc[string]
			var doneWorkers dutil.OutFunc[[]string]

			switch a, d, err := dutil.NewComparableUniqueAppendSlice[string](workersSize, 0); {
			case err != nil:
				return nil, err
			default:
				addWorker, doneWorkers = a, d
			}

			var msub map[string]SMV
			maffected := map[string][]SMV{}

			if subv != nil {
				msub = map[string]SMV{}

				for worker := range slices.Values(getStateOwners(subv.Key())) {
					msub[worker] = subv
					_, _ = addWorker(worker)
				}
			}

			for v := range slices.Values(addvs) {
				for worker := range slices.Values(getStateOwners(v.Key())) {
					i := maffected[worker]
					if i == nil {
						i = make([]SMV, 0, len(addvs))
					}

					i = append(i, v)

					maffected[worker] = i
					_, _ = addWorker(worker)
				}
			}

			affected := slices.Collect(maps.Keys(maffected))
			for subw := range msub {
				addOwner(subw, affected)
			}

			return func(yield func(string, [2][]SMV) bool) {
					defer func() {
						clear(msub)
						clear(maffected)
					}()

					for worker := range slices.Values(doneWorkers()) {
						sub, affected := msub[worker], maffected[worker]
						if !yield(worker, [2][]SMV{{sub}, affected}) {
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
