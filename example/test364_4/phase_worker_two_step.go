package main

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type TwoStepPhaseJobWorkerArgs struct {
	NewJobWorker               newJobWorkerFunc
	NewJobPoolSize             uint64
	NewJobPoolGrowSize         uint64
	JobIDMapSize               uint64
	NextPhaseNewJobsChanBuffer uint64
	*logging.Logging
}

func NewTwoStepPhaseJobWorkerArgs() *TwoStepPhaseJobWorkerArgs {
	return &TwoStepPhaseJobWorkerArgs{
		Logging:                    NewModuleLogging("phase-job-worker", nil),
		NewJobWorker:               NewFuncBaseJobWorker(1 << 9),
		NewJobPoolSize:             1 << 9,
		NewJobPoolGrowSize:         1 << 9,
		JobIDMapSize:               1 << 9,
		NextPhaseNewJobsChanBuffer: 1 << 9,
	}
}

func newTwoStepPhaseJobWorker(
	ctx context.Context,
	args *TwoStepPhaseJobWorkerArgs,
	phase int,
	prevWait func() error,
) (
	newJob phaseJobWorkerNewJobFunc,
	jobDone func(),
	wait func() error,
	_ error,
) {
	pctx, pctxCancel := context.WithCancelCause(ctx)

	var jobIDLocked *util.ShardedMap[uint64, int]

	switch i, err := util.NewShardedMap[uint64, int](args.JobIDMapSize, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		jobIDLocked = i
	}

	filterJobID := func(jobID uint64, phase int) (ok bool) {
		_, _, _ = jobIDLocked.Set(jobID, func(prev int, found bool) (int, error) {
			if found && phase == prev {
				return 0, util.ErrLockedSetIgnore
			}

			ok = true

			return phase, nil
		})

		return ok
	}

	l := args.Log().With().Int("phase", phase).Logger()

	var wk JobWorker

	switch i, err := args.NewJobWorker(pctx); {
	case err != nil:
		return nil, nil, nil, err
	default:
		wk = i
	}

	waitDone := NewFuncContextDoneFunc(pctx, func(context.Context) error {
		return wk.Wait()
	})

	var nextPhaseLock sync.RWMutex
	var nextPhaseNewJobFunc phaseJobWorkerNewJobFunc
	var nextPhaseWaitFunc func() error

	newNextPhase := func() error {
		nextPhaseLock.Lock()
		defer nextPhaseLock.Unlock()

		if nextPhaseNewJobFunc != nil {
			return nil
		}

		l.Debug().Int("phase", phase+1).Msg("new phase job worker")

		switch n, d, w, err := newTwoStepPhaseJobWorker(pctx, args, phase+1, waitDone); {
		case err != nil:
			return err
		default:
			nextPhaseNewJobFunc = n
			nextPhaseWaitFunc = func() error {
				d()

				return w()
			}

			return nil
		}
	}

	nextPhaseNewJobCh := make(chan [2]interface{}, args.NextPhaseNewJobsChanBuffer)

	nextPhaseNewJob := func(jobID uint64, c phaseJobWorkerCallback) error {
		if c == nil {
			return nil
		}

		nextPhaseNewJobCh <- [2]interface{}{jobID, c}

		l.Debug().Uint64("job_id", jobID).Msg("next phase new job to ch")

		return nil
	}

	nextPhaseNewJobDoneCh := make(chan struct{})

	go func() {
		if err := newPhaseJobWorkerNewJobsPool(
			pctx,
			args,
			phase+1,
			newNextPhase,
			func() phaseJobWorkerNewJobFunc {
				nextPhaseLock.RLock()
				defer nextPhaseLock.RUnlock()

				return nextPhaseNewJobFunc
			},
			func(f func(jobID uint64, c phaseJobWorkerCallback) error) error {
				for {
					select {
					case <-pctx.Done():
						return pctx.Err()
					case i, notClosed := <-nextPhaseNewJobCh:
						if !notClosed {
							return nil
						}

						if err := f(
							i[0].(uint64),
							i[1].(phaseJobWorkerCallback),
						); err != nil {
							return err
						}
					}
				}
			},
			prevWait,
		); err != nil {
			pctxCancel(err)
		}

		close(nextPhaseNewJobDoneCh)
	}()

	var closeOnce sync.Once

	closeCh := func() {
		closeOnce.Do(func() {
			close(nextPhaseNewJobCh)
		})
	}

	nextPhaseWait := func() error {
		nextPhaseLock.Lock()
		defer nextPhaseLock.Unlock()

		if nextPhaseWaitFunc == nil {
			return nil
		}

		return nextPhaseWaitFunc()
	}

	return func(jobID uint64, c phaseJobWorkerCallback) error {
			if c == nil {
				return nil
			}

			return wk.NewJob(func(ctx context.Context) error {
				if !filterJobID(jobID, phase) {
					return nil
				}

				return c(ctx, phase, nextPhaseNewJob)
			})
		},
		wk.JobDone,
		func() error {
			defer pctxCancel(nil)
			defer closeCh()
			defer jobIDLocked.Close()

			if err := waitDone(); err != nil {
				return err
			}

			closeCh()

			select {
			case <-pctx.Done():
				return errors.WithStack(context.Cause(pctx))
			case <-nextPhaseNewJobDoneCh:
			}

			return nextPhaseWait()
		},
		nil
}

func newPhaseJobWorkerNewJobsPool(
	ctx context.Context,
	args *TwoStepPhaseJobWorkerArgs,
	phase int,
	newNextPhaseFunc func() error,
	nextPhaseNewJobFunc func() phaseJobWorkerNewJobFunc,
	newJobs func(func(jobID uint64, c phaseJobWorkerCallback) error) error,
	prevDonef func() error,
) error {
	l := args.Log().With().Int("phase", phase).Logger()

	var addNext util.CompactAppendSliceAddFunc[[2]interface{}]
	var doneNext util.CompactAppendSliceDoneFunc[[2]interface{}]

	switch a, d, err := util.CompactAppendSliceV1[[2]interface{}](args.NewJobPoolSize, args.NewJobPoolGrowSize); {
	case err != nil:
		return err
	default:
		defer func() {
			_ = d(nil)
		}()

		addNext = a
		doneNext = d
	}

	var newNextPhaseLock sync.RWMutex
	var newNextPhaseOnce sync.Once
	var isPrevDone bool

	newNextPhase := func() error {
		var gerr error

		newNextPhaseOnce.Do(func() {
			if err := newNextPhaseFunc(); err != nil {
				gerr = err

				return
			}
		})

		return gerr
	}

	exhaust := func() error {
		var newJob phaseJobWorkerNewJobFunc

		return doneNext(func(s [][2]interface{}) error {
			if len(s) < 1 {
				return nil
			}

			if err := newNextPhase(); err != nil {
				return err
			}

			newJob = nextPhaseNewJobFunc()

			return util.TraverseSliceV1(s, func(_ int, i [2]interface{}) (bool, error) {
				return true, newJob(
					i[0].(uint64),
					i[1].(phaseJobWorkerCallback),
				)
			})
		})
	}

	var nextPhaseNewJobExhaustOnce sync.Once

	nextPhaseNewJob := func() (phaseJobWorkerNewJobFunc, error) {
		newNextPhaseLock.RLock()
		defer newNextPhaseLock.RUnlock()

		if !isPrevDone {
			return nil, nil
		}

		var gerr error

		nextPhaseNewJobExhaustOnce.Do(func() {
			gerr = exhaust()
		})

		if gerr != nil {
			return nil, gerr
		}

		return nextPhaseNewJobFunc(), nil
	}

	addNewJob := func(jobID uint64, c phaseJobWorkerCallback) error {
		switch f, err := nextPhaseNewJob(); {
		case err != nil:
			return err
		case f == nil:
			if i, _ := addNext([2]interface{}{jobID, c}); i != 1 {
				return errors.Errorf("not added, %d", jobID)
			}

			return nil
		default:
			return f(jobID, c)
		}
	}

	prevDone := NewFuncContextDoneFunc(
		ctx,
		func(context.Context) error {
			if err := prevDonef(); err != nil {
				return err
			}

			newNextPhaseLock.Lock()
			isPrevDone = true
			newNextPhaseLock.Unlock()

			return nil
		},
	)

	newJobsDone := NewFuncContextDoneFunc(
		ctx,
		func(context.Context) error {
			return newJobs(func(jobID uint64, c phaseJobWorkerCallback) error {
				return addNewJob(jobID, c)
			})
		},
	)

	go func() {
		_ = prevDone()
	}()

	if err := newJobsDone(); err != nil {
		return err
	}

	l.Debug().Msg("next phase new jobs done")

	if err := prevDone(); err != nil {
		return err
	}

	return exhaust()
}

func NewTwoStepPhaseJobWorker(
	ctx context.Context,
	args *TwoStepPhaseJobWorkerArgs,
) (
	newJob phaseJobWorkerNewJobFunc,
	jobDone func(),
	wait func() error,
	_ error,
) {
	return newTwoStepPhaseJobWorker(
		ctx,
		args,
		0,
		func() error { return nil },
	)
}
