package main

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var localWorkerName = "local"

type (
	filterMergeOpsFunc                func(mergeOps, unmergeOps []uint64) (filteredMergeOps, filteredUnmergeOps []uint64)
	workerPhaseMergerInsertFunc       func(worker string, mergeOps, unmergeOps []uint64) error
	workerPhaseMergerWaitCallbackFunc func(newMerged, newUnmerged []uint64, reasons map[uint64]string, isDone bool) error
	workerPhaseMergerWaitFunc         func(workerPhaseMergerWaitCallbackFunc) error
	phaseMergerFunc                   func(
		context.Context,
		*phaseMergerArgs,
		phaseMergerGetStateMergeValuesFunc,
		phaseMergerGetMergers,
		extractStateMergeValueKeysFunc,
	) (
		merge, unmerge phaseMergerXMergeFunc,
		done func(),
		wait phaseMergerWaitFunc,
		_ error,
	)
	workerPhaseMergerFunc func(
		_ context.Context,
		minEnough uint64,
		_ phaseMergerGetStateMergeValuesFunc,
		_ phaseMergerGetMergers,
		_ extractStateMergeValueKeysFunc,
		_ filterMergeOpsFunc,
	) (
		workerPhaseMergerInsertFunc,
		workerPhaseMergerWaitFunc,
		error,
	)
)

type workerPhaseMergerArgs struct {
	*logging.Logging
}

func newWorkerPhaseMergerArgs() *workerPhaseMergerArgs {
	return &workerPhaseMergerArgs{
		Logging: NewModuleLogging("worker-phase-merger", nil),
	}
}

func newFuncWorkerPhaseMerger(
	args *workerPhaseMergerArgs,
	pargs *phaseMergerArgs,
) workerPhaseMergerFunc {
	return func(
		ctx context.Context,
		minEnough uint64,
		getStateMergeValues phaseMergerGetStateMergeValuesFunc,
		getMergers phaseMergerGetMergers,
		extractStateMergeValueKeys extractStateMergeValueKeysFunc,
		filterMergeOps filterMergeOpsFunc,
	) (
		workerPhaseMergerInsertFunc,
		workerPhaseMergerWaitFunc,
		error,
	) {
		if minEnough < 1 {
			return nil, nil, util.ErrInvalid.Errorf("min enough should be at least 1")
		}

		pctx, pctxCancel := ContextWithCancelCause(ctx)

		var merge, unmerge phaseMergerXMergeFunc

		_, mergeDoneDone, mergeDoneWait := NewDoneContextFunc(pctx)

		var mergeDone func()
		var mergeWait phaseMergerWaitFunc
		var insertOps func(mergeOps, unmergeOps []uint64) error

		switch m, u, d, w, err := newPhaseMerger(pctx, pargs, getStateMergeValues, getMergers, extractStateMergeValueKeys); {
		case err != nil:
			return nil, nil, err
		default:
			merge = m
			unmerge = u

			mergeDone = func() {
				mergeDoneDone(nil)
				d()
			}

			mergeWait = w
			insertOps = newWorkerPhaseMergerInsert(merge, unmerge)
		}

		var checkWorker func(string, bool, func(_, _ bool) error) error
		var checkWorkerEnoughEmpty func() bool

		switch c, i, err := newIsEnoughkWokerPhaseMergerFuncs(minEnough); {
		case err != nil:
			return nil, nil, err
		default:
			checkWorker = c
			checkWorkerEnoughEmpty = i
		}

		closef := func(err error) error {
			pctxCancel(err)
			_ = checkWorkerEnoughEmpty()

			return err
		}

		return func(worker string, mergeOps, unmergeOps []uint64) error {
				if err := context.Cause(pctx); err != nil {
					return errors.WithStack(err)
				}

				l := args.Log().With().Str("worker", worker).Logger()

				return checkWorker(
					worker,
					len(mergeOps) < 1 && len(unmergeOps) < 1,
					func(enough, ok bool) error {
						ll := l.With().
							Bool("ok", ok).
							Bool("enough", enough).
							Uints64("merge_ops", mergeOps).
							Uints64("unmerge_ops", unmergeOps).
							Logger()

						switch {
						case !ok:
							return nil
						case enough:
							defer func() {
								l.Debug().Msg("enough; merge done")

								mergeDone()
							}()
						default:
							ll.Debug().Msg("worker checked")
						}

						switch {
						case worker == localWorkerName,
							len(mergeOps) < 1 && len(unmergeOps) < 1:
							return nil
						}

						fm, fu := filterMergeOps(mergeOps, unmergeOps)

						ll.Debug().
							Uints64("filtered_merge_ops", fm).
							Uints64("filtered_unmerge_ops", fu).
							Msg("insert merge/unmerge ops")

						if len(fm) < 1 && len(fu) < 1 {
							return nil
						}

						return insertOps(fm, fu)
					},
				)
			},
			func(f workerPhaseMergerWaitCallbackFunc) error {
				return contextCauseError(pctx, closef, func(context.Context) error {
					if err := mergeDoneWait(); err != nil {
						return err
					}

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

					return gerr
				})
			},
			nil
	}
}

func newIsEnoughkWokerPhaseMergerFuncs(
	minEnough uint64,
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
		if _, found := m[localWorkerName]; found && uint64(len(m)) >= minEnough {
			enough = true
			enoughEmpty = emptyCount >= minEnough

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

func newWorkerPhaseMergerInsert(
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
