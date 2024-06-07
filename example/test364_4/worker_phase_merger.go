package main

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var localWorkerName = "local"

type (
	workerPhaseMergerInsertFunc func(worker string, mergeOps, unmergeOps []uint64) error
	workerPhaseMergerWaitFunc   func(phaseMergerWaitCallbackFunc) (done bool, _ error)
)

type workerPhaseMergerArgs struct {
	*logging.Logging
	NewPhaseMegerFuncs func(context.Context, *workerPhaseMergerArgs) (
		merge phaseMergerXMergeFunc,
		unmerge phaseMergerXMergeFunc,
		done func(),
		wait phaseMergerWaitFunc,
		_ error,
	)
	GetValuesFunc func(opindex uint64) ([]StateMergeValue, error)
	EnoughWorkers uint64 // NOTE include local worker
	FilterOp      func(opindex uint64) bool
}

func newWorkerPhaseMergerArgs() *workerPhaseMergerArgs {
	return &workerPhaseMergerArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "worker-phase-merger")
		}),
		NewPhaseMegerFuncs: func(ctx context.Context, args *workerPhaseMergerArgs) (
			phaseMergerXMergeFunc,
			phaseMergerXMergeFunc,
			func(),
			phaseMergerWaitFunc,
			error,
		) {
			return nil, nil, nil, nil, util.ErrNotImplemented.Errorf("NewPhaseMegerFuncs")
		},
		GetValuesFunc: func(uint64) ([]StateMergeValue, error) {
			return nil, util.ErrNotImplemented.Errorf("GetValuesFunc")
		},
		FilterOp: func(uint64) bool { return false },
	}
}

func newWorkerPhaseMerger(
	ctx context.Context,
	args *workerPhaseMergerArgs,
) (
	insert workerPhaseMergerInsertFunc,
	wait workerPhaseMergerWaitFunc,
	_ error,
) {
	if args.EnoughWorkers < 1 {
		return nil, nil, util.ErrInvalid.Errorf("EnoughWorkers should be at least 1")
	}

	pctx, pctxCancel := context.WithCancelCause(ctx)

	var merge, unmerge phaseMergerXMergeFunc

	mergeDoneCh := make(chan struct{}, 1)
	mergeDone := func() { close(mergeDoneCh) }

	var mergeWait phaseMergerWaitFunc
	var insertOps func(mergeOps, unmergeOps []uint64) error

	var newPhaseMergerOnce sync.Once

	insertf := func(worker string, mergeOps, unmergeOps []uint64) error {
		var gerr error

		newPhaseMergerOnce.Do(func() {
			switch m, u, d, w, err := args.NewPhaseMegerFuncs(pctx, args); {
			case err != nil:
				gerr = err
			default:
				merge = m
				unmerge = u
				mergeDone = func() {
					close(mergeDoneCh)

					d()
				}
				mergeWait = w
				insertOps = newWorkerPhaseMergerInsertFunc(merge, unmerge)
			}
		})

		if gerr != nil {
			return gerr
		}

		return insertOps(mergeOps, unmergeOps)
	}

	var checkWorker func(string, bool) (bool, bool)
	var checkWorkerDone func()
	var checkIsDone func() bool

	switch c, i, d, err := newIsEnoughkWokerPhaseMergerFuncs(args.EnoughWorkers); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker = c
		checkIsDone = i
		checkWorkerDone = d
	}

	filterOps := newFilterOpsFunc(args.FilterOp)

	var waitErr error
	var waitOnce sync.Once

	return func(worker string, mergeOps, unmergeOps []uint64) error {
			if err := context.Cause(pctx); err != nil {
				return err
			}

			l := args.Log().With().Str("worker", worker).Logger()

			switch enough, ok := checkWorker(worker, len(mergeOps) < 1 && len(unmergeOps) < 1); {
			case !ok:
				l.Debug().Msg("not ok")

				return nil
			case enough:
				defer func() {
					l.Debug().Msg("enough; merge done")

					mergeDone()
				}()
			}

			l.Debug().
				Uints64("merge_ops", mergeOps).
				Uints64("unmerge_ops", unmergeOps).
				Msg("insert")

			if len(mergeOps) < 1 && len(unmergeOps) < 1 {
				return nil
			}

			fm, fu := filterOps(mergeOps), filterOps(unmergeOps)

			l.Debug().
				Uints64("merge_ops", fm).
				Uints64("unmerge_ops", fu).
				Msg("insert; filtered")

			if len(fm) < 1 && len(fu) < 1 {
				return nil
			}

			if err := insertf(worker, fm, fu); err != nil {
				pctxCancel(err)

				return err
			}

			return nil
		},
		func(f phaseMergerWaitCallbackFunc) (bool, error) {
			waitOnce.Do(func() {
				defer checkWorkerDone()

				select {
				case <-pctx.Done():
					waitErr = context.Cause(pctx)
				case <-mergeDoneCh:
					defer pctxCancel(nil)

					switch {
					case mergeWait != nil:
						waitErr = mergeWait(f)
					case f != nil:
						waitErr = f(nil, nil, nil)
					}
				}
			})

			return checkIsDone(), waitErr
		},
		nil
}

func newIsEnoughkWokerPhaseMergerFuncs(
	enoughWorkers uint64,
) (
	check func(worker string, isEmpty bool) (enough, ok bool),
	isDonef func() bool,
	closef func(),
	_ error,
) {
	m := map[string] /* isEmpty */ bool{}

	var enough, isDone bool
	var doneCount uint64
	var lock sync.RWMutex
	var clearOnce sync.Once

	return func(worker string, isEmpty bool) (bool, bool) {
			lock.Lock()
			defer lock.Unlock()

			if enough {
				return enough, false
			}

			if _, found := m[worker]; found {
				return enough, false
			}

			m[worker] = isEmpty

			if isEmpty {
				doneCount++
			}

			// NOTE before local worker, pending merge done.
			if _, found := m[localWorkerName]; found && uint64(len(m)) >= enoughWorkers {
				enough = true
				isDone = doneCount >= enoughWorkers
			}

			return enough, true
		},
		func() bool {
			lock.RLock()
			defer lock.RUnlock()

			return isDone
		},
		func() {
			lock.Lock()
			defer lock.Unlock()

			clearOnce.Do(func() {
				clear(m)
			})
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

func newFilterOpsFunc(f func(uint64) bool) func([]uint64) []uint64 {
	return func(ops []uint64) []uint64 {
		switch l := len(ops); {
		case l < 1:
			return nil
		case l == 1:
			if f(ops[0]) {
				return ops
			}

			return nil
		}

		return util.FilterSlice(ops, f)
	}
}
