package main

import (
	"context"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	phaseJobWorkerNewJobFunc func(jobID uint64, _ phaseJobWorkerCallback) error
	phaseJobWorkerCallback   func(_ context.Context, phase int, _ phaseJobWorkerNewJobFunc) error
	newPhaseJobWorkerFunc    func(context.Context) (
		newJob phaseJobWorkerNewJobFunc,
		jobDone func(),
		wait func() error,
		_ error,
	)
)

type phaseJobWorkerOption struct {
	*logging.Logging
	NewJobWorker newJobWorkerFunc
	JobIDMapSize uint64
}

func newPhaseJobWorkerOption() *phaseJobWorkerOption {
	return &phaseJobWorkerOption{
		Logging:      newModuleLogging("phase-job-worker", nil),
		NewJobWorker: newFuncJobWorker(1 << 9),
		JobIDMapSize: 1 << 9,
	}
}

func newPhaseJobWorker(
	ctx context.Context,
	option *phaseJobWorkerOption,
	phase int,
) (
	phaseJobWorkerNewJobFunc,
	func(),
	func() error,
	error,
) {
	var jobIDFilter func(uint64, int) bool
	var jobIDClose func()

	switch f, c, err := newFuncJobID(option.JobIDMapSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		jobIDFilter = f
		jobIDClose = c
	}

	l := option.Log().With().Int("phase", phase).Logger()

	var newJob jobWorkerNewJobFunc
	var jobDone jobWorkerJobDoneFunc
	var jobWait jobWorkerWaitFunc

	switch n, d, w, err := option.NewJobWorker(ctx); {
	case err != nil:
		return nil, nil, nil, err
	default:
		newJob = n
		jobDone = d
		jobWait = w
	}

	var nextPhaseNewJobf phaseJobWorkerNewJobFunc
	nextPhaseWait := func() error { return nil }

	newNextPhase := onceFunc(func() error {
		l.Debug().Int("phase", phase+1).Msg("new phase job worker")

		switch n, d, w, err := newPhaseJobWorker(ctx, option, phase+1); {
		case err != nil:
			return err
		default:
			nextPhaseNewJobf = n
			nextPhaseWait = func() error {
				d()

				return w()
			}

			return nil
		}
	})

	nextPhaseNewJob := func(jobID uint64, c phaseJobWorkerCallback) error {
		l.Debug().Uint64("job_id", jobID).Msg("next phase new job")

		if err := newNextPhase(); err != nil {
			return err
		}

		return nextPhaseNewJobf(jobID, c)
	}

	return func(jobID uint64, c phaseJobWorkerCallback) error {
			if c == nil {
				return nil
			}

			return newJob(func(ctx context.Context) error {
				return contextCauseError(ctx, dummyErrorFunc, func(ctx context.Context) error {
					if !jobIDFilter(jobID, phase) {
						return nil
					}

					return c(ctx, phase, nextPhaseNewJob)
				})
			})
		},
		jobDone,
		func() error {
			defer jobIDClose()

			return contextCauseError(ctx, dummyErrorFunc, func(context.Context) error {
				if err := jobWait(); err != nil {
					return err
				}

				return nextPhaseWait()
			})
		},
		nil
}

func newFuncPhaseJobWorker(
	ctx context.Context,
	option *phaseJobWorkerOption,
) (
	newJob phaseJobWorkerNewJobFunc,
	jobDone func(),
	wait func() error,
	_ error,
) {
	return newPhaseJobWorker(ctx, option, 0)
}

func newFuncJobID(mapSize uint64) (
	filter func(jobID uint64, phase int) (ok bool),
	closef func(),
	_ error,
) {
	var m util.LockedMap[uint64, int]

	switch i, err := util.NewShardedMap[uint64, int](mapSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i
	}

	return func(jobID uint64, phase int) (ok bool) {
			_, _, _ = m.Set(jobID, func(prev int, found bool) (int, error) {
				if found && phase == prev {
					return 0, util.ErrLockedSetIgnore
				}

				ok = true

				return phase, nil
			})

			return ok
		},
		m.Close,
		nil
}
