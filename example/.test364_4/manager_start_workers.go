package main

import (
	"context"

	"github.com/spikeekips/mitum/util"
)

type managerStartWorkersFunc func(
	_ context.Context,
	workers []string,
	maxFailed uint64,
) error

func newManagerStartWorkers(
	ctx context.Context,
	workers []string,
	maxFailed uint64,
	newCheckWorkersFunc checkEnoughFailedWorkersFuncs,
	startWorker func(_ context.Context, worker string) error,
) error {
	wctx, wctxCancel := contextWithCancelCause(ctx)
	defer wctxCancel(nil)

	var checkWorker checkEnoughFailedWorkerFunc

	switch c, d, err := newCheckWorkersFunc(uint64(len(workers)), maxFailed); {
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
				return errCheckWorkersFailed.Wrap(err)
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
