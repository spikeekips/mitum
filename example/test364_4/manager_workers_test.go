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

type testManagerStartWorkers struct {
	suite.Suite
	baseTestManager
}

func (t *testManagerStartWorkers) defaultStartWorker() func(context.Context, string) error {
	return func(context.Context, string) error { return nil }
}

func (t *testManagerStartWorkers) TestNew() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workers := t.workers(3)

	addWorker, doneWorker, err := util.CompactAppendSliceV1[string](3)
	t.NoError(err)

	startWorker := func(_ context.Context, worker string) error {
		if n, _ := addWorker(worker); n < 1 {
			return errors.Errorf("add worker, %q", worker)
		}

		return nil
	}

	t.NoError(newManagerStartWorkers(ctx, workers, 0, t.newCheckWorkersFunc(), startWorker))

	doneWorker(func(s []string) error {
		slices.Sort(s)

		t.Equal(workers, s)

		return nil
	})
}

func (t *testManagerStartWorkers) TestOverMaxFailed() {
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

		addWorker, doneWorker, err := util.CompactAppendSliceV1[string](uint64(len(workers)))
		t.NoError(err)

		startWorker := func(_ context.Context, worker string) error {
			if _, found := failedWorkers[worker]; found {
				return errors.Errorf("showme")
			}

			if n, _ := addWorker(worker); n < 1 {
				return errors.Errorf("add worker, %q", worker)
			}

			return nil
		}

		t.NoError(newManagerStartWorkers(ctx, workers, 3, t.newCheckWorkersFunc(), startWorker))

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

		startWorker := func(_ context.Context, worker string) error {
			if _, found := failedWorkers[worker]; found {
				return errors.Errorf("showme")
			}

			return nil
		}

		err := newManagerStartWorkers(ctx, workers, 3, t.newCheckWorkersFunc(), startWorker)
		t.ErrorContains(err, "showme")
	})
}

func TestManagerStartWorkers(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerStartWorkers))
}
