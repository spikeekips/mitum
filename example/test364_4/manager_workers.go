package main

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type managerStartWorkersFunc func(
	_ context.Context,
	workers []string,
	maxFailed uint64,
) error

type managerStartWorkersArgs struct {
	*logging.Logging
	NewCheckWorkersFunc checkEnoughFailedWorkersFuncs
}

func newManagerStartWorkersArgs() *managerStartWorkersArgs {
	return &managerStartWorkersArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "manager-start-workers")
		}),
		NewCheckWorkersFunc: func(_, maxFailed uint64) (
			checkEnoughFailedWorkerFunc,
			func(),
			error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("NewCheckWorkers")
		},
	}
}

func newManagerStartWorkers(
	ctx context.Context,
	args *managerStartWorkersArgs,
	workers []string,
	maxFailed uint64,
	startWorker func(_ context.Context, worker string) error,
) error {
	wctx, wctxCancel := ContextWithCancelCause(ctx)
	defer wctxCancel(nil)

	var checkWorker checkEnoughFailedWorkerFunc

	switch c, d, err := args.NewCheckWorkersFunc(uint64(len(workers)), maxFailed); {
	case err != nil:
		return err
	default:
		defer d()

		checkWorker = c
	}

	var jw JobWorker

	switch i, err := NewBaseJobWorker(wctx, int64(len(workers))); {
	case err != nil:
		return err
	default:
		defer i.Close()

		jw = i
	}

	if err := util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
		return true, jw.NewJob(func(ctx context.Context) error {
			switch _, _, failed, err := checkWorker(worker, func() error {
				return startWorker(ctx, worker)
			}); {
			case failed:
				return err
			default:
				return nil
			}
		})
	}); err != nil {
		return err
	}

	jw.JobDone()

	return jw.Wait()
}
