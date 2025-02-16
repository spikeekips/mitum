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

type concPhaseJobWorkerArgs struct {
	NewJobWorker newJobWorkerFunc
	JobIDMapSize uint64
	*logging.Logging
}

func newConcPhaseJobWorkerArgs() *concPhaseJobWorkerArgs {
	return &concPhaseJobWorkerArgs{
		Logging:      NewModuleLogging("phase-job-worker", nil),
		NewJobWorker: newFuncBaseJobWorker(1 << 9),
		JobIDMapSize: 1 << 9,
	}
}

func newConcPhaseJobWorker(ctx context.Context, args *concPhaseJobWorkerArgs, phase int) (
	newJob phaseJobWorkerNewJobFunc,
	jobDone func(),
	wait func() error,
	_ error,
) {
	pctx, pctxCancel := contextWithCancelCause(ctx)

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

	var nextPhaseNewJobFunc phaseJobWorkerNewJobFunc
	nextPhaseWait := func() error { return nil }

	newNextPhase := OnceFunc(func() error {
		l.Debug().Int("phase", phase+1).Msg("new phase job worker")

		switch n, d, w, err := newConcPhaseJobWorker(ctx, args, phase+1); {
		case err != nil:
			return err
		default:
			nextPhaseNewJobFunc = n
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

		return nextPhaseNewJobFunc(jobID, c)
	}

	closef := func(err error) error {
		pctxCancel(err)
		jobIDLocked.Close()

		return err
	}

	return func(jobID uint64, c phaseJobWorkerCallback) error {
			if c == nil {
				return nil
			}

			return wk.NewJob(func(ctx context.Context) error {
				return contextCauseError(ctx, ifErrErrorFunc(closef), func(ctx context.Context) error {
					if !filterJobID(jobID, phase) {
						return nil
					}

					return c(ctx, phase, nextPhaseNewJob)
				})
			})
		},
		wk.JobDone,
		func() error {
			return contextCauseError(pctx, closef, func(context.Context) error {
				if err := wk.Wait(); err != nil {
					return err
				}

				return nextPhaseWait()
			})
		},
		nil
}

func newFuncConcPhaseJobWorker(
	ctx context.Context,
	args *concPhaseJobWorkerArgs,
) (
	newJob phaseJobWorkerNewJobFunc,
	jobDone func(),
	wait func() error,
	_ error,
) {
	return newConcPhaseJobWorker(
		ctx,
		args,
		0,
	)
}
