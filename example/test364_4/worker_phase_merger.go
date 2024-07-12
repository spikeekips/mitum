package main

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var localWorkerName = "local"

type (
	workerPhaseMergerInsertFunc       func(worker string, mergeOps, unmergeOps []uint64) error
	workerPhaseMergerWaitCallbackFunc func(newMerged, newUnmerged []uint64, reasons map[uint64]string, isDone bool) error
	workerPhaseMergerWaitFunc         func(workerPhaseMergerWaitCallbackFunc) error
	newPhaseMergerFunc                func(context.Context, *PhaseMergerArgs) (
		merge phaseMergerXMergeFunc,
		unmerge phaseMergerXMergeFunc,
		done func(),
		wait phaseMergerWaitFunc,
		_ error,
	)
	newPhaseMergerFuncs func() (
		*PhaseMergerArgs,
		newPhaseMergerFunc,
		error,
	)
)

type workerPhaseMergerArgs struct {
	*logging.Logging
	NewPhaseMergerFuncs newPhaseMergerFuncs
	EnoughWorkers       uint64 // NOTE include local worker
	FilterMergeOps      func(mergeOps, unmergeOps []uint64) (filteredMergeOps, filteredUnmergeOps []uint64)
	local               string
}

func newWorkerPhaseMergerArgs() *workerPhaseMergerArgs {
	return &workerPhaseMergerArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "worker-phase-merger")
		}),
		NewPhaseMergerFuncs: func() (
			*PhaseMergerArgs,
			newPhaseMergerFunc,
			error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("NewPhaseMergerFuncs")
		},
		FilterMergeOps: func([]uint64, []uint64) ([]uint64, []uint64) { return nil, nil },
	}
}

func newWorkerPhaseMerger(ctx context.Context, args *workerPhaseMergerArgs) (
	workerPhaseMergerInsertFunc,
	workerPhaseMergerWaitFunc,
	error,
) {
	if args.EnoughWorkers < 1 {
		return nil, nil, util.ErrInvalid.Errorf("EnoughWorkers should be at least 1")
	}

	pctx, pctxCancel := context.WithCancelCause(ctx)

	var merge, unmerge phaseMergerXMergeFunc

	mergeDoneCh := make(chan struct{}, 1)

	var mergeDone func()
	var mergeWait phaseMergerWaitFunc
	var insertOps func(mergeOps, unmergeOps []uint64) error

	switch pargs, newf, err := args.NewPhaseMergerFuncs(); {
	case err != nil:
		return nil, nil, err
	default:
		pargs.SetLogging(args.Logging)

		switch m, u, d, w, err := newf(pctx, pargs); {
		case err != nil:
			return nil, nil, err
		default:
			merge = m
			unmerge = u

			mergeDoneOnce := OnceFunc(func() error {
				close(mergeDoneCh)

				d()

				return nil
			})

			mergeDone = func() {
				_ = mergeDoneOnce()
			}

			mergeWait = w
			insertOps = newWorkerPhaseMergerInsertFunc(merge, unmerge)
		}
	}

	var checkWorker func(string, bool, func(_, _ bool) error) error
	var checkWorkerEnoughEmpty func() bool

	switch c, i, err := newIsEnoughkWokerPhaseMergerFuncs(args.EnoughWorkers); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker = c
		checkWorkerEnoughEmpty = i
	}

	insert := newWorkerPhaseMergerInsertPoolFuncs(
		pctx,
		checkWorker,
		func(worker string, mergeOps, unmergeOps []uint64, enough bool) error {
			l := args.Log().With().
				Str("worker", worker).
				Bool("enough", enough).
				Uints64("merge_ops", mergeOps).
				Uints64("unmerge_ops", unmergeOps).
				Logger()

			switch {
			case enough:
				defer func() {
					l.Debug().Msg("enough; merge done")

					mergeDone()
				}()
			default:
				l.Debug().Msg("worker checked")
			}

			switch {
			case worker == localWorkerName,
				len(mergeOps) < 1 && len(unmergeOps) < 1:
				return nil
			}

			fm, fu := args.FilterMergeOps(mergeOps, unmergeOps)

			l.Debug().
				Uints64("filtered_merge_ops", fm).
				Uints64("filtered_unmerge_ops", fu).
				Msg("insert merge/unmerge ops")

			if len(fm) < 1 && len(fu) < 1 {
				return nil
			}

			switch err := insertOps(fm, fu); {
			case err == nil,
				enough && errors.Is(err, util.ErrJobWorkerDone):
				return nil
			default:
				return err
			}
		},
	)

	return insert,
		func(f workerPhaseMergerWaitCallbackFunc) error {
			defer func() {
				_ = checkWorkerEnoughEmpty()
			}()

			if err := context.Cause(pctx); err != nil {
				return err
			}

			select {
			case <-pctx.Done():
				return context.Cause(pctx)
			case <-mergeDoneCh:
				var gerr error

				switch {
				case mergeWait != nil:
					gerr = mergeWait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string) error {
						if f == nil {
							return nil
						}

						return f(newMerged, newUnmerged, reasons, checkWorkerEnoughEmpty())
					})
				case f != nil:
					gerr = f(nil, nil, nil, checkWorkerEnoughEmpty())
				}

				pctxCancel(gerr)

				return gerr
			}
		},
		nil
}

func newIsEnoughkWokerPhaseMergerFuncs(
	enoughWorkers uint64,
) (
	_ func(worker string, isEmpty bool, _ func(enough, ok bool) error) error,
	_ func() (enoughEmpty bool),
	_ error,
) {
	m := map[string] /* isEmpty */ bool{}

	var done bool
	var enough, enoughEmpty bool
	var emptyCount uint64
	var lock sync.RWMutex

	check := func(worker string, isEmpty bool) (bool, bool, error) {
		if enough || done {
			return enough, false, nil
		}

		if _, found := m[worker]; found {
			return enough, false, util.ErrFound.Errorf(worker)
		}

		m[worker] = isEmpty

		if isEmpty {
			emptyCount++
		}

		// NOTE before local worker, pending merge done.
		if _, found := m[localWorkerName]; found && uint64(len(m)) >= enoughWorkers {
			enough = true
			enoughEmpty = emptyCount >= enoughWorkers

			done = true

			clear(m)
		}

		return enough, true, nil
	}

	return func(worker string, isEmpty bool, f func(enough, ok bool) error) error {
			if f == nil {
				return nil
			}

			lock.Lock()
			defer lock.Unlock()

			switch enough, ok, err := check(worker, isEmpty); {
			case err != nil:
				return err
			default:
				return f(enough, ok)
			}
		},
		func() bool {
			lock.RLock()
			defer lock.RUnlock()

			return enoughEmpty
		},
		nil
}

func newWorkerPhaseMergerInsertFunc(
	merge, unmerge phaseMergerXMergeFunc,
) func(mergeOps, unmergeOps []uint64) error {
	return func(mergeOps, unmergeOps []uint64) error {
		if err := util.TraverseSliceV1(mergeOps, func(_ int, opindex uint64) (bool, error) {
			return true, merge(opindex)
		}); err != nil {
			return err
		}

		return util.TraverseSliceV1(unmergeOps, func(_ int, opindex uint64) (bool, error) {
			return true, unmerge(opindex)
		})
	}
}

func newWorkerPhaseMergerInsertPoolFuncs( // FIXME remove
	ctx context.Context,
	checkWorker func(string, bool, func(_, _ bool) error) error,
	f func(worker string, mergeOps, unmergeOps []uint64, enough bool) error,
) workerPhaseMergerInsertFunc {
	pool := map[string][2][]uint64{}

	return func(worker string, mergeOps, unmergeOps []uint64) error {
		if err := context.Cause(ctx); err != nil {
			return err
		}

		return checkWorker(
			worker,
			len(mergeOps) < 1 && len(unmergeOps) < 1,
			func(enough, ok bool) error {
				switch {
				case !ok:
					return nil
				case !enough:
					pool[worker] = [2][]uint64{mergeOps, unmergeOps}

					return nil
				default:
					if err := TraverseMap(pool, func(worker string, v [2][]uint64) (bool, error) {
						return true, f(worker, v[0], v[1], false)
					}); err != nil {
						return err
					}

					clear(pool)
					pool = nil

					return f(worker, mergeOps, unmergeOps, enough)
				}
			},
		)
	}
}
