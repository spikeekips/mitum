package main

import (
	"context"
	"slices"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testNewManagerStartWorkersFunc struct {
	bsaeTestManager
}

func (t *testNewManagerStartWorkersFunc) newArgs() *managerStartWorkersArgs {
	args := newManagerStartWorkersArgs()
	args.NewCheckWorkersFunc = func(minEnough, maxFailed uint64) (
		checkEnoughFailedWorkerFunc,
		func(),
		error,
	) {
		return newCheckEnoughFailedWorkersFunc(minEnough, maxFailed)
	}
	args.StartWorker = func(context.Context, string) error { return nil }

	return args
}

func (t *testNewManagerStartWorkersFunc) TestNew() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workers := t.workers(3)

	args := t.newArgs()

	addWorker, doneWorker, err := util.CompactAppendSliceV1[string](3)
	t.NoError(err)

	args.StartWorker = func(_ context.Context, worker string) error {
		if n, _ := addWorker(worker); n < 1 {
			return errors.Errorf("add worker, %q", worker)
		}

		return nil
	}

	f := newManagerStartWorkersFunc(args)

	t.NoError(f(ctx, workers, 0))

	doneWorker(func(s []string) error {
		slices.Sort(s)

		t.Equal(workers, s)

		return nil
	})
}

func (t *testNewManagerStartWorkersFunc) TestOverMaxFailed() {
	t.Run("under", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(333)
		failedWorkers := map[string]struct{}{
			workers[9]:  {},
			workers[12]: {},
		}
		okWorkers := slices.Clone(workers)
		okWorkers = slices.Delete(okWorkers, 12, 13)
		okWorkers = slices.Delete(okWorkers, 9, 10)
		slices.Sort(okWorkers)

		args := t.newArgs()

		addWorker, doneWorker, err := util.CompactAppendSliceV1[string](uint64(len(workers)))
		t.NoError(err)

		args.StartWorker = func(_ context.Context, worker string) error {
			if _, found := failedWorkers[worker]; found {
				return errors.Errorf("showme")
			}

			if n, _ := addWorker(worker); n < 1 {
				return errors.Errorf("add worker, %q", worker)
			}

			return nil
		}

		f := newManagerStartWorkersFunc(args)

		t.NoError(f(ctx, workers, 3))

		doneWorker(func(s []string) error {
			slices.Sort(s)

			t.Equal(okWorkers, s)

			return nil
		})
	})

	t.Run("over", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(333)
		failedWorkers := map[string]struct{}{
			workers[9]:  {},
			workers[12]: {},
			workers[13]: {},
		}

		args := t.newArgs()

		args.StartWorker = func(_ context.Context, worker string) error {
			if _, found := failedWorkers[worker]; found {
				return errors.Errorf("showme")
			}

			return nil
		}

		f := newManagerStartWorkersFunc(args)

		err := f(ctx, workers, 3)
		t.ErrorContains(err, "showme")
	})
}

func TestNewManagerStartWorkersFunc(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testNewManagerStartWorkersFunc))
}
