package main

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var localWorkerName = "local"

type workerPhaseMergerInsertFunc func(worker string, mergeOps, unmergeOps []uint64) error

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
	}
}

func newWorkerPhaseMerger(
	ctx context.Context,
	args *workerPhaseMergerArgs,
) (
	insert workerPhaseMergerInsertFunc,
	wait phaseMergerWaitFunc,
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

	var checkWorker func(string) (bool, bool)
	var checkWorkerDone func()

	switch c, d, err := newIsEnoughkWokerPhaseMergerFuncs(args.EnoughWorkers); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker = c
		checkWorkerDone = d
	}

	return func(worker string, mergeOps, unmergeOps []uint64) error {
			if err := context.Cause(pctx); err != nil {
				return err
			}

			l := args.Log().With().Str("worker", worker).Logger()

			switch enough, ok := checkWorker(worker); {
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

			if err := insertf(worker, mergeOps, unmergeOps); err != nil {
				pctxCancel(err)

				return err
			}

			return nil
		},
		func(f phaseMergerWaitCallbackFunc) error {
			defer checkWorkerDone()

			select {
			case <-pctx.Done():
				return context.Cause(pctx)
			case <-mergeDoneCh:
				defer pctxCancel(nil)

				switch {
				case mergeWait != nil:
					return mergeWait(f)
				case f != nil:
					return f(nil, nil, nil)
				default:
					return nil
				}
			}
		},
		nil
}

func newIsEnoughkWokerPhaseMergerFuncs(
	enoughWorkers uint64,
) (
	check func(worker string) (enough, ok bool),
	done func(),
	_ error,
) {
	m := map[string]struct{}{}

	var enough bool
	var lock sync.Mutex
	var clearOnce sync.Once

	return func(worker string) (bool, bool) {
			lock.Lock()
			defer lock.Unlock()

			if enough {
				return enough, false
			}

			if _, found := m[worker]; found {
				return enough, false
			}

			m[worker] = struct{}{}

			// NOTE before local worker, pending merge done.
			if _, found := m[localWorkerName]; found && uint64(len(m)) >= enoughWorkers {
				enough = true
			}

			return enough, true
		},
		func() {
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
