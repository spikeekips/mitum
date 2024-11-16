package main

import (
	"context"
	"sync"

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
	ctx = setContextValueLogger(ctx, log)

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

	nextPhaseNewJob, nextPhaseWait := newLockedPhaseJobWorker(ctx, newJobWorker, jobIDMapSize, phase+1)

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

func newLockedPhaseJobWorker(
	ctx context.Context,
	newJobWorker newJobWorkerFunc,
	jobIDMapSize uint64,
	phase int,
) (
	phaseJobWorkerNewJobFunc,
	errorFunc,
) {
	log := loadContextValueLogger(ctx)

	l := log.With().Int("phase", phase).Logger()

	newJobLocked := util.EmptyLocked[phaseJobWorkerNewJobFunc]()
	waitLocked := util.EmptyLocked[errorFunc]()

	once := sync.OnceValue(func() error {
		l.Debug().Msg("new phase job worker")

		switch n, d, w, err := newFuncPhaseJobWorker(ctx, newJobWorker, jobIDMapSize, phase); {
		case err != nil:
			return err
		default:
			newJobLocked.SetValue(n)
			waitLocked.SetValue(func() error {
				d()

				return w()
			})

			return nil
		}
	})

	return func(jobID uint64, c phaseJobWorkerCallback) error {
			l.Debug().Uint64("job_id", jobID).Msg("new job")

			if err := once(); err != nil {
				return err
			}

			if i, isEmpty := newJobLocked.Value(); !isEmpty {
				return i(jobID, c)
			}

			return nil
		},
		func() error {
			l.Debug().Msg("wait done")

			defer func() {
				newJobLocked.EmptyValue()
				waitLocked.EmptyValue()
			}()

			if i, isEmpty := waitLocked.Value(); !isEmpty {
				return i()
			}

			return nil
		}
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
