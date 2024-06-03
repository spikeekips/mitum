package main

import (
	"context"

	"github.com/spikeekips/mitum/util"
)

func newOneStepPhaseJobWorker(
	ctx context.Context,
	newJobWorker func(context.Context) (JobWorker, error),
	phase int,
	jobIDLocked func(jobID uint64, phase int) bool,
) (
	newJob PhaseJobWorkerNewJobFunc,
	jobDone func(),
	wait func() error,
	_ error,
) {
	var wk JobWorker

	switch i, err := newJobWorker(ctx); {
	case err != nil:
		return nil, nil, nil, err
	default:
		wk = i
	}

	var addNext util.CompactAppendSliceAddFunc[[2]interface{}]
	var doneNext util.CompactAppendSliceDoneFunc[[2]interface{}]

	switch a, d, err := util.CompactAppendSliceV1[[2]interface{}](1<<9, 1<<9); { // FIXME size from argument
	case err != nil:
		return nil, nil, nil, err
	default:
		defer func() {
			_ = d(nil)
		}()

		addNext = a
		doneNext = d
	}

	nextPhaseNewJob := func(jobID uint64, c PhaseJobWorkerCallback) error {
		addNext([2]interface{}{jobID, c})

		return nil
	}

	doNext := func() error {
		return doneNext(func(s [][2]interface{}) error {
			if len(s) < 1 {
				return nil
			}

			switch n, d, w, err := newOneStepPhaseJobWorker(ctx, newJobWorker, phase+1, jobIDLocked); {
			case err != nil:
				return err
			default:
				if err := util.TraverseSliceV1(s, func(_ int, i [2]interface{}) (bool, error) {
					return true, n(
						i[0].(uint64),
						i[1].(PhaseJobWorkerCallback),
					)
				}); err != nil {
					return err
				}

				d()

				return w()
			}
		})
	}

	return func(jobID uint64, c PhaseJobWorkerCallback) error {
			return wk.NewJob(func(ctx context.Context) error {
				if !jobIDLocked(jobID, phase) {
					return nil
				}

				return c(ctx, phase, nextPhaseNewJob)
			})
		},
		wk.JobDone,
		func() error {
			if err := wk.Wait(); err != nil {
				return err
			}

			return doNext()
		},
		nil
}

func NewOneStepPhaseJobWorker(
	ctx context.Context,
	newJobWorker func(context.Context) (JobWorker, error),
	mapSize uint64,
) (
	newJob PhaseJobWorkerNewJobFunc,
	jobDone func(),
	wait func() error,
	_ error,
) {
	if newJobWorker == nil {
		newJobWorker = func(ctx context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, 1<<9)
		}
	}

	var jobIDLocked *util.ShardedMap[uint64, int]

	switch i, err := util.NewShardedMap[uint64, int](mapSize, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		jobIDLocked = i
	}

	pctx, pctxCancel := context.WithCancelCause(ctx)

	var newJobFunc func(uint64, PhaseJobWorkerCallback) error
	var doneFunc func()
	var waitFunc func() error

	switch n, d, w, err := newOneStepPhaseJobWorker(
		pctx,
		newJobWorker,
		0,
		func(jobID uint64, phase int) (ok bool) {
			_, _, _ = jobIDLocked.Set(jobID, func(prev int, found bool) (int, error) {
				if found && phase <= prev {
					return 0, util.ErrLockedSetIgnore
				}

				ok = true

				return phase, nil
			})

			return ok
		},
	); {
	case err != nil:
		pctxCancel(err)

		return nil, nil, nil, err
	default:
		newJobFunc = n
		doneFunc = d
		waitFunc = w
	}

	go func() {
		<-pctx.Done()

		jobIDLocked.Close()
	}()

	return func(jobID uint64, c PhaseJobWorkerCallback) error {
			if err := newJobFunc(jobID, c); err != nil {
				pctxCancel(err)

				return err
			}

			return nil
		},
		doneFunc,
		func() error {
			err := waitFunc()
			pctxCancel(err)

			return err
		}, nil
}
