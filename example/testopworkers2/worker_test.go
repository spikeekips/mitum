package testopsworkers

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testRunSerialWorkers struct {
	logging.BaseTestSuite
}

func (t *testRunSerialWorkers) run(ctx context.Context, workersize int64) (
	newJob func(serialWorkerNewNextJobFunc) error,
	done func() error,
	cancel func(),
) {
	sw, err := NewSerialWorkers(
		func(ctx context.Context) (distributedWorker, error) {
			return util.NewErrgroupWorker(ctx, workersize)
		},
	)
	t.NoError(err)
	// sw.SetLogging(logging.TestLogging)

	return sw.New(ctx)
}

type subJobFunc func(context.Context, uint64, uint64) error

func (t *testRunSerialWorkers) runNPhases(
	ctx context.Context,
	workersize int64,
	maxdepth, jobs uint64,
	f subJobFunc,
) func() error {
	if f == nil {
		f = func(context.Context, uint64, uint64) error { return nil }
	}

	var jobf func(max, depth uint64, f subJobFunc) serialWorkerNewNextJobFunc

	jobf = func(max, depth uint64, f subJobFunc) serialWorkerNewNextJobFunc {
		return func(int) (serialWorkerNextJobFunc, bool, error) {
			return func(ctx context.Context, jobid uint64, phase int, add serialWorkerNewJobFunc) error {
				if err := f(ctx, jobid, depth); err != nil {
					return err
				}

				if depth < max-1 {
					<-time.After(time.Millisecond * 33)
					add(jobf(max, depth+1, f))
				}

				return nil
			}, true, nil
		}
	}

	newJob, done, _ := t.run(ctx, workersize)

	go func() {
		defer newJob(nil)

		for i := uint64(0); i < jobs; i++ {
			if err := newJob(jobf(maxdepth, 0, f)); err != nil {
				return
			}
		}
	}()

	return done
}

func (t *testRunSerialWorkers) runNPhasesAdd(ctx context.Context, workersize int64, maxdepth, jobs uint64) {
	sumed := util.NewSingleLockedMap[uint64, uint64]()

	done := t.runNPhases(ctx, workersize, maxdepth, jobs, func(_ context.Context, jobid, depth uint64) error {
		_, _, _ = sumed.Set(depth, func(i uint64, _ bool) (uint64, error) {
			return i + jobid + 1, nil
		})

		return nil
	})
	t.NoError(done())

	expected := func(maxdepth, jobs uint64) (s uint64) {
		for j := uint64(0); j < jobs; j++ {
			s += j + 1
		}

		return s
	}

	t.Equal(maxdepth, uint64(sumed.Len()))

	sumed.Traverse(func(_, v uint64) bool {
		t.Equal(expected(maxdepth, jobs), v, "%d", v)

		return true
	})
}

func (t *testRunSerialWorkers) testNPhasesCases(workersize int64, maxdepth, jobs uint64) {
	t.Run("ok", func() {
		t.runNPhasesAdd(context.Background(), workersize, maxdepth, jobs)
	})

	for d := uint64(0); d < maxdepth; d++ {
		d := d

		t.Run(fmt.Sprintf("job error in depth, %d", d), func() {
			done := t.runNPhases(context.Background(), workersize, maxdepth, jobs, func(_ context.Context, jobid, depth uint64) error {
				if depth == d && jobid == jobs-1 {
					return errors.Errorf("hohoho")
				}

				return nil
			})
			err := done()

			t.Error(err)
			t.True(errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "hohoho"))
		})
	}

	t.Run("more worker size and jobs", func() {
		t.runNPhasesAdd(context.Background(), workersize, maxdepth, jobs)
	})
}

func (t *testRunSerialWorkers) TestNPhases() {
	var cases [][2]uint64

	for i := uint64(1); i < 10; i++ {
		cases = append(cases, [2]uint64{i, 3})
	}

	cases = append(cases, [2]uint64{33, 33})

	for i := range cases {
		c := cases[i]

		t.Run(fmt.Sprintf("maxdepth=%d jobs=%d", c[0], c[1]), func() {
			t.testNPhasesCases(int64(c[1]), c[0], c[1])
		})
	}
}

func (t *testRunSerialWorkers) TestCancel() {
	ctx, cancel := context.WithCancel(context.Background())

	done := t.runNPhases(ctx, 3, 3, 33, func(ctx context.Context, jobid, depth uint64) error {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
			return nil
		}
	})

	<-time.After(time.Millisecond * 333)

	t.T().Log("cancel by force")
	cancel()

	err := done()
	t.Error(err)
	t.True(errors.Is(err, context.Canceled))
}

func TestRunSerialWorkers(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testRunSerialWorkers))
}
