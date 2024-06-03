package main

import (
	"cmp"
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testTwoStepPhaseJobWorker struct {
	suite.Suite
}

func (t *testTwoStepPhaseJobWorker) newArgs() *TwoStepPhaseJobWorkerArgs {
	args := NewTwoStepPhaseJobWorkerArgs()
	args.SetLogging(logging.TestNilLogging)

	return args
}

func (t *testTwoStepPhaseJobWorker) TestPrevDone() {
	var callbackFunc func(jobID uint64, depth int, f func(phase int) (uint64, error)) PhaseJobWorkerCallback

	callbackFunc = func(jobID uint64, depth int, f func(phase int) (uint64, error)) PhaseJobWorkerCallback {
		return func(_ context.Context, phase int, c PhaseJobWorkerNewJobFunc) error {
			t.T().Logf("> phase=%d jobid=%d", phase, jobID)

			next := jobID

			switch i, err := f(phase); {
			case err != nil:
				return err
			default:
				next = i
			}

			if phase < depth {
				t.T().Logf("< phase=%d jobid=%d", phase, next)

				return c(next, callbackFunc(next, depth, f))
			}

			return nil
		}
	}

	newCallbackFunc := func(f func(phase int) (uint64, error)) (
		func(jobID uint64, depth int, f func(phase int) (uint64, error)) PhaseJobWorkerCallback,
		func() [][2]interface{},
	) {
		add, done, _ := util.CompactAppendSliceV1[[2]interface{}](1<<9, 1<<9)

		called := func(jobID uint64, f func(int) (uint64, error)) func(int) (uint64, error) {
			return func(phase int) (uint64, error) {
				add([2]interface{}{phase, jobID})

				if f != nil {
					return f(phase)
				}

				return jobID, nil
			}
		}

		return func(jobID uint64, depth int, f func(phase int) (uint64, error)) PhaseJobWorkerCallback {
				return callbackFunc(jobID, depth, called(jobID, f))
			}, func() (v [][2]interface{}) {
				done(func(s [][2]interface{}) error {
					v = slices.Clone(s)

					return nil
				})

				return v
			}
	}

	cmpDone := func(a, b [2]interface{}) int {
		if i := cmp.Compare(a[0].(int), b[0].(int)); i != 0 {
			return i
		}

		return cmp.Compare(a[1].(uint64), b[1].(uint64))
	}

	t.Run("depth 0; pre closed prev done ch", func() {
		t.T().Log("prev ch closed")

		newJob, jobDone, wait, err := newTwoStepPhaseJobWorker(
			context.Background(),
			t.newArgs(),
			0,
			func() error { return nil },
		)
		t.NoError(err)

		callback, done := newCallbackFunc(nil)

		t.NoError(newJob(0, callback(0, 0, nil)))
		t.NoError(newJob(1, callback(1, 0, nil)))

		jobDone()

		t.NoError(wait())

		called := done()
		slices.SortFunc(called, cmpDone)

		t.Equal([][2]interface{}{{0, uint64(0)}, {0, uint64(1)}}, called)
	})

	t.Run("depth 0; close prev done ch after new job", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pch := make(chan struct{}, 1)

		newJob, jobDone, wait, err := newTwoStepPhaseJobWorker(
			ctx,
			t.newArgs(),
			0,
			func() error {
				select {
				case <-ctx.Done():
				case <-pch:
				}
				return nil
			},
		)
		t.NoError(err)

		callback, done := newCallbackFunc(nil)

		calledch := make(chan struct{}, 1)
		t.NoError(newJob(0, callback(0, 0, func(phase int) (uint64, error) {
			calledch <- struct{}{}

			return 0, nil
		})))
		t.NoError(newJob(1, callback(1, 0, nil)))

		<-calledch
		close(pch)
		t.T().Log("prev ch closed")

		jobDone()

		t.NoError(wait())

		called := done()
		slices.SortFunc(called, cmpDone)

		t.Equal([][2]interface{}{{0, uint64(0)}, {0, uint64(1)}}, called)
	})

	t.Run("depth 0; close pre done ch after job done", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pch := make(chan struct{}, 1)

		newJob, jobDone, wait, err := newTwoStepPhaseJobWorker(
			ctx,
			t.newArgs(),
			0,
			func() error {
				select {
				case <-ctx.Done():
				case <-pch:
				}
				return nil
			},
		)
		t.NoError(err)

		callback, done := newCallbackFunc(nil)

		t.NoError(newJob(0, callback(0, 0, nil)))
		t.NoError(newJob(1, callback(1, 0, nil)))

		jobDone()
		close(pch)
		t.T().Log("prev ch closed")

		t.NoError(wait())

		called := done()
		slices.SortFunc(called, cmpDone)

		t.Equal([][2]interface{}{{0, uint64(0)}, {0, uint64(1)}}, called)
	})

	t.Run("depth 0; not close pre done ch", func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		pch := make(chan struct{}, 1)

		newJob, jobDone, wait, err := newTwoStepPhaseJobWorker(
			ctx,
			t.newArgs(),
			0,
			func() error {
				select {
				case <-ctx.Done():
				case <-pch:
				}
				return nil
			},
		)
		t.NoError(err)

		callback, done := newCallbackFunc(nil)

		t.NoError(newJob(0, callback(0, 0, nil)))
		t.NoError(newJob(1, callback(1, 0, nil)))

		jobDone()

		err = wait()
		t.ErrorIs(err, context.DeadlineExceeded, err)

		called := done()
		slices.SortFunc(called, cmpDone)

		t.Equal([][2]interface{}{{0, uint64(0)}, {0, uint64(1)}}, called)
	})

	t.Run("depth 1; pre closed prev done ch", func() {
		t.T().Log("prev ch closed")

		newJob, jobDone, wait, err := newTwoStepPhaseJobWorker(
			context.Background(),
			t.newArgs(),
			0,
			func() error { return nil },
		)
		t.NoError(err)

		callback, done := newCallbackFunc(nil)

		t.NoError(newJob(0, callback(0, 1, nil)))
		t.NoError(newJob(1, callback(1, 1, nil)))

		jobDone()

		t.NoError(wait())

		called := done()
		slices.SortFunc(called, cmpDone)

		expected := [][2]interface{}{
			{0, uint64(0)},
			{0, uint64(1)},
			{1, uint64(0)},
			{1, uint64(1)},
		}

		t.Equal(expected, called)
	})

	t.Run("depth 1; close prev done ch after new job", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pch := make(chan struct{}, 1)

		newJob, jobDone, wait, err := newTwoStepPhaseJobWorker(
			ctx,
			t.newArgs(),
			0,
			func() error {
				select {
				case <-ctx.Done():
				case <-pch:
				}
				return nil
			},
		)
		t.NoError(err)

		callback, done := newCallbackFunc(nil)

		calledch := make(chan struct{}, 1)
		t.NoError(newJob(0, callback(0, 1, func(phase int) (uint64, error) {
			calledch <- struct{}{}

			return 0, nil
		})))
		t.NoError(newJob(1, callback(1, 1, nil)))

		<-calledch
		close(pch)
		t.T().Log("prev ch closed")

		jobDone()

		t.NoError(wait())

		called := done()
		slices.SortFunc(called, cmpDone)

		expected := [][2]interface{}{
			{0, uint64(0)},
			{0, uint64(1)},
			{1, uint64(0)},
			{1, uint64(1)},
		}

		t.Equal(expected, called)
	})

	t.Run("depth 1; close pre done ch after job done", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pch := make(chan struct{}, 1)

		newJob, jobDone, wait, err := newTwoStepPhaseJobWorker(
			ctx,
			t.newArgs(),
			0,
			func() error {
				select {
				case <-ctx.Done():
				case <-pch:
				}
				return nil
			},
		)
		t.NoError(err)

		callback, done := newCallbackFunc(nil)

		t.NoError(newJob(0, callback(0, 1, nil)))
		t.NoError(newJob(1, callback(1, 1, nil)))

		jobDone()
		close(pch)
		t.T().Log("prev ch closed")

		t.NoError(wait())

		called := done()
		slices.SortFunc(called, cmpDone)

		expected := [][2]interface{}{
			{0, uint64(0)},
			{0, uint64(1)},
			{1, uint64(0)},
			{1, uint64(1)},
		}

		t.Equal(expected, called)
	})

	t.Run("depth 1; not close pre done ch", func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		pch := make(chan struct{}, 1)

		newJob, jobDone, wait, err := newTwoStepPhaseJobWorker(
			ctx,
			t.newArgs(),
			0,
			func() error {
				select {
				case <-ctx.Done():
				case <-pch:
				}
				return nil
			},
		)
		t.NoError(err)

		callback, done := newCallbackFunc(nil)

		t.NoError(newJob(0, callback(0, 1, nil)))
		t.NoError(newJob(1, callback(1, 1, nil)))

		jobDone()

		err = wait()
		t.ErrorIs(err, context.DeadlineExceeded)

		called := done()
		slices.SortFunc(called, cmpDone)

		expected := [][2]interface{}{
			{0, uint64(0)},
			{0, uint64(1)},
			//{1, uint64(0)},
			//{1, uint64(1)},
		}

		t.Equal(expected, called)
	})
}

func (t *testTwoStepPhaseJobWorker) runPhases(
	ctx context.Context,
	jobs uint64,
	c func() (jobID uint64, _ PhaseJobWorkerCallback),
) error {
	newJob, jobDone, wait, err := newTwoStepPhaseJobWorker(
		ctx,
		t.newArgs(),
		0,
		func() error { return nil },
	)
	t.NoError(err)

	for i := uint64(0); i < jobs; i++ {
		j, k := c()
		if err := newJob(j, k); err != nil {
			return err
		}
	}

	jobDone()

	return wait()
}

func (t *testTwoStepPhaseJobWorker) TestAdd() {
	t.Run("0 phase", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var jobID, sum uint64

		t.runPhases(ctx, 9, func() (uint64, PhaseJobWorkerCallback) {
			jobID++

			return jobID, func(_ context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
				atomic.AddUint64(&sum, 1)

				return nil
			}
		})

		t.Equal(uint64(9), sum)
	})

	t.Run("n phase", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		jobID := util.EmptyLocked[uint64]()
		var sum uint64

		depth := 9

		newJobID := func() (j uint64) {
			jobID.Set(func(prev uint64, isempty bool) (uint64, error) {
				if !isempty {
					prev++
				}

				j = prev

				return prev, nil
			})

			return j
		}

		var f func() func(_ context.Context, phase int, _ PhaseJobWorkerNewJobFunc) error

		f = func() func(_ context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
			return func(_ context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
				id := newJobID()

				// t.T().Logf("phase=%d jobID=%d", phase, j)
				atomic.AddUint64(&sum, id)

				if phase == depth {
					return nil
				}

				return j(id, f())
			}
		}

		t.NoError(t.runPhases(ctx, 9, func() (uint64, PhaseJobWorkerCallback) {
			id := newJobID()

			atomic.AddUint64(&sum, id)

			return id, func(_ context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
				id := newJobID()

				// t.T().Logf("phase=%d jobID=%d", phase, j)
				atomic.AddUint64(&sum, id)

				return j(id, f())
			}
		}))

		t.Equal(uint64(4851), sum, sum)
	})
}

func TestTwoStepPhaseJobWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testTwoStepPhaseJobWorker))
}
