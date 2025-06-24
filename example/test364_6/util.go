package dop6

import (
	"context"
	"iter"
	"maps"
	"math"
	"slices"

	dop5 "github.com/spikeekips/mitum/example/test364_5"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

func FuncsJobWorker(ctx context.Context, fs ...dutil.InErrorFunc[context.Context]) error {
	if len(fs) < 1 {
		return nil
	}

	return dutil.FuncsJobWorker(ctx, fs...)
}

func NewFuncJobWorker(ctx context.Context, semSize int64) (
	dutil.JobWorkerNewJobFunc,
	func(),
	dutil.InErrorFunc[error],
) {
	n, d, w, err := dutil.NewFuncJobWorker(ctx, max(semSize, 1))
	if err != nil {
		panic(err)
	}

	return n, d, w
}

func NewFuncErrJobWorker(ctx context.Context, semSize int64, errf dutil.InErrorFunc[error]) (
	dutil.JobWorkerNewJobFunc,
	func(),
	dutil.InErrorFunc[error],
) {
	n, d, w, err := dutil.NewFuncErrJobWorker(ctx, max(semSize, 1), errf)
	if err != nil {
		panic(err)
	}

	return n, d, w
}

func newOpsWorkersNItems(workers, share, items uint64) (
	uint64,
	iter.Seq2[uint64 /* worker */, uint64 /* number of items */],
) {
	if workers < 1 || share < 1 || items < 1 {
		return 0, func(func(uint64, uint64) bool) {}
	}

	share = min(share, workers)

	total := items * share

	ni := uint64(math.Floor(float64(total) / float64(workers)))
	wl := total - (ni * workers)

	ow := wl
	if ni > 0 {
		ow = workers
	}

	return ow, func(yield func(uint64, uint64) bool) {
		for w := range ow {
			var n uint64

			switch {
			case w >= wl:
				n = ni
			default:
				n = ni + 1
			}

			if !yield(w, n) {
				return
			}
		}
	}
}

type opsWorkersItemsSeq = iter.Seq2[string /* worker */, func() (uint64, iter.Seq[uint64 /* opindex */])]

func newOpsWorkersItems(workers []string, share, items uint64) (
	[]string,
	opsWorkersItemsSeq,
) {
	if len(workers) < 1 || share < 1 || items < 1 {
		return nil, func(func(string, func() (uint64, iter.Seq[uint64])) bool) {}
	}

	nworkers := uint64(len(workers))

	share = min(share, nworkers)

	pos := func(worker, item uint64) uint64 {
		return uint64(math.Floor(
			float64((nworkers*item)+worker) / float64(share),
		))
	}

	ow, seq := newOpsWorkersNItems(nworkers, share, items)

	return workers[:ow], func(yield func(string, func() (uint64, iter.Seq[uint64])) bool) {
		for nw, n := range seq {
			if !yield(
				workers[nw],
				func() (uint64, iter.Seq[uint64]) {
					return n, func(yield2 func(uint64) bool) {
						for i := range n {
							if !yield2(pos(nw, i)) {
								return
							}
						}
					}
				},
			) {
				return
			}
		}
	}
}

func newFuncLazyCheckFailedWorkers(
	share uint64,
	blockUntil func(worker string) bool,
) (
	func(workers []string) (bool, error),
	dop5.CheckFailedWorkersFunc,
) {
	f := util.EmptyLocked[dop5.CheckFailedWorkersFunc]()
	pool := map[string]error{}
	mworkers := map[string]struct{}{}

	return func(workers []string) (bool, error) {
			var enough bool

			_, failed := f.Set(func(check dop5.CheckFailedWorkersFunc, isEmpty bool) (dop5.CheckFailedWorkersFunc, error) {
				if !isEmpty {
					return nil, util.ErrLockedSetIgnore
				}

				for worker := range slices.Values(workers) {
					mworkers[worker] = struct{}{}
				}

				switch i, err := dop5.NewFuncCheckFailedWorkers(uint64(len(workers)), share, blockUntil); {
				case err != nil:
					return nil, err
				default:
					check = i
				}

				defer clear(pool)

				for worker, err := range maps.All(pool) {
					if _, found := mworkers[worker]; !found {
						continue
					}

					switch ok, e, failed := check(worker, err); {
					case !ok:
					case failed != nil:
						enough = e

						return nil, failed
					case e:
						enough = e

						return nil, util.ErrLockedSetIgnore
					}
				}

				return check, nil
			})

			return enough, failed
		},
		func(worker string, err error) (bool, bool, error) {
			var ok, enough bool
			var failed error

			_, _ = f.Set(func(check dop5.CheckFailedWorkersFunc, isEmpty bool) (dop5.CheckFailedWorkersFunc, error) {
				if isEmpty {
					ok = true
					pool[worker] = err

					return nil, util.ErrLockedSetIgnore
				}

				if _, found := mworkers[worker]; !found {
					return nil, util.ErrLockedSetIgnore
				}

				ok, enough, failed = check(worker, err)

				return nil, util.ErrLockedSetIgnore
			})

			return ok, enough, failed
		}
}
