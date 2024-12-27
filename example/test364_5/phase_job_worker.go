package main

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	phaseJobWorkerNewJobFunc func(jobID uint64, _ phaseJobWorkerCallback) error
	phaseJobWorkerCallback   = func(_ context.Context, phase int, _ phaseJobWorkerNewJobFunc) error
	newPhaseJobWorkerFunc    = func(context.Context) (
		newJob phaseJobWorkerNewJobFunc,
		jobDone func(),
		wait dutil.ErrorFunc,
		_ error,
	)
)

func newFuncPhaseJobWorker(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	jobIDMapSize uint64,
	phase int,
) (
	phaseJobWorkerNewJobFunc,
	func(),
	dutil.ErrorFunc,
	error,
) {
	ctx, _ = dutil.UpdateModuleLoggerFromContext(ctx, "newFuncPhaseJobWorker", func(zctx zerolog.Context) zerolog.Context {
		return zctx.Int("phase", phase)
	})

	var jobIDFilter func(uint64, int) bool
	var jobIDClose func()

	switch f, c, err := newFuncJobID(jobIDMapSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		jobIDFilter, jobIDClose = f, c
	}

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.ErrorFunc

	switch n, d, w, err := newJobWorker(ctx); {
	case err != nil:
		return nil, nil, nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	nextPhaseNewJob, nextPhaseWait := newLockedPhaseJobWorker(ctx, newJobWorker, jobIDMapSize, phase+1)

	return func(jobID uint64, c phaseJobWorkerCallback) error {
			if c == nil {
				return nil
			}

			return newJob(func(ctx context.Context) error {
				if !jobIDFilter(jobID, phase) {
					return nil
				}

				return c(ctx, phase, nextPhaseNewJob)
			})
		},
		jobDone,
		func() error {
			defer func() {
				_ = jobWait()
				_ = nextPhaseWait()

				jobIDClose()
			}()

			if err := jobWait(); err != nil {
				return err
			}

			return nextPhaseWait()
		},
		nil
}

func newLockedPhaseJobWorker(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	jobIDMapSize uint64,
	phase int,
) (
	phaseJobWorkerNewJobFunc,
	dutil.ErrorFunc,
) {
	log := dutil.LoadContextValueLogger(ctx)

	l := log.With().Int("phase", phase).Logger()

	var fl sync.RWMutex
	var newJob phaseJobWorkerNewJobFunc
	var wait dutil.ErrorFunc

	once := sync.OnceValue(func() error {
		l.Debug().Msg("new phase job worker")

		switch n, d, w, err := newFuncPhaseJobWorker(ctx, newJobWorker, jobIDMapSize, phase); {
		case err != nil:
			return err
		default:
			fl.Lock()
			defer fl.Unlock()

			newJob = n
			wait = func() error {
				d()

				return w()
			}

			return nil
		}
	})

	return func(jobID uint64, c phaseJobWorkerCallback) error {
			l.Debug().Uint64("job_id", jobID).Msg("new job")

			if err := once(); err != nil {
				return err
			}

			return newJob(jobID, c)
		},
		func() error {
			fl.RLock()
			defer fl.RUnlock()

			l.Debug().Msg("wait done")

			if wait != nil {
				return wait()
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
