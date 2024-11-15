package main

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
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

func newFuncPhaseJobWorker(
	ctx context.Context,
	newJobWorker newJobWorkerFunc,
	jobIDMapSize uint64,
	phase int,
) (
	phaseJobWorkerNewJobFunc,
	func(),
	func() error,
	error,
) {
	log := newModuleLoggerFromContext(ctx, "newFuncPhaseJobWorker", func(zctx zerolog.Context) zerolog.Context {
		return zctx.Int("phase", phase)
	})

	var jobIDFilter func(uint64, int) bool
	var jobIDClose func()

	switch f, c, err := newFuncJobID(jobIDMapSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		jobIDFilter = f
		jobIDClose = c
	}

	var newJob jobWorkerNewJobFunc
	var jobDone func()
	var jobWait errorFunc

	switch n, d, w, err := newJobWorker(ctx); {
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
		log.Debug().Int("phase", phase+1).Msg("new phase job worker")

		switch n, d, w, err := newFuncPhaseJobWorker(ctx, newJobWorker, jobIDMapSize, phase+1); {
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
		log.Debug().Uint64("job_id", jobID).Msg("next phase new job")

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
				return contextCauseError(ctx, dummyErrorInFunc, func(ctx context.Context) error {
					if !jobIDFilter(jobID, phase) {
						return nil
					}

					return c(ctx, phase, nextPhaseNewJob)
				})
			})
		},
		jobDone,
		func() error {
			defer func() {
				_ = jobWait()
				_ = nextPhaseWait()

				jobIDClose()
			}()

			return contextCauseError(ctx, dummyErrorInFunc, func(context.Context) error {
				if err := jobWait(); err != nil {
					return err
				}

				return nextPhaseWait()
			})
		},
		nil
}

func newFuncJobID(mapSize uint64) (
	filter func(jobID uint64, phase int) (ok bool),
	closef func(),
	_ error,
) {
	var m util.LockedMap[uint64, int]

	switch i, err := util.NewLockedMap[uint64, int](mapSize, nil); {
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
