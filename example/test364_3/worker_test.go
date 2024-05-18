package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"golang.org/x/sync/semaphore"
)

type testRunSerialWorkers struct {
	logging.BaseTestSuite
}

func (t *testRunSerialWorkers) run(ctx context.Context, workersize int64) (
	newJob func(serialWorkerNewNextJobFunc) error,
	done func() error,
	cancel func(),
) {
	sw, err := NewSerialWorker(
		func(ctx context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, workersize)
		},
	)
	t.NoError(err)
	// sw.SetLogging(logging.TestLogging)

	return sw.New(ctx)
}

type subJobFunc func(context.Context, uint64) error

func (t *testRunSerialWorkers) runNPhases(
	ctx context.Context,
	workersize int64,
	maxdepth, jobs uint64,
	f subJobFunc,
) func() error {
	if f == nil {
		f = func(context.Context, uint64) error { return nil }
	}

	var jobf func(max, depth uint64, f subJobFunc) serialWorkerNewNextJobFunc

	jobf = func(max, depth uint64, f subJobFunc) serialWorkerNewNextJobFunc {
		return func(int) (serialWorkerNextJobFunc, bool, error) {
			return func(ctx context.Context, phase int, add serialWorkerNewJobFunc) error {
				if err := f(ctx, depth); err != nil {
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

	var jobid int64
	ld := util.EmptyLocked[uint64]()

	done := t.runNPhases(ctx, workersize, maxdepth, jobs, func(_ context.Context, depth uint64) error {
		_, _, _ = sumed.Set(depth, func(i uint64, _ bool) (uint64, error) {
			j := atomic.LoadInt64(&jobid)

			if i, _ := ld.Value(); i != depth {
				atomic.AddInt64(&jobid, j*-1)
				j = 0
				ld.SetValue(depth)
			}

			defer atomic.AddInt64(&jobid, 1)

			return i + uint64(j) + 1, nil
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
		e := expected(maxdepth, jobs)
		t.Equal(e, v, "expected=%d actual=%d", e, v)

		return true
	})
}

func (t *testRunSerialWorkers) testNPhasesCases(workersize int64, maxdepth, jobs uint64) {
	t.Run("ok", func() {
		t.runNPhasesAdd(context.Background(), workersize, maxdepth, jobs)
	})

	for d := uint64(0); d < maxdepth; d++ {
		d := d

		var jobid int64
		ld := util.EmptyLocked[uint64]()

		t.Run(fmt.Sprintf("job error in depth, %d", d), func() {
			done := t.runNPhases(context.Background(), workersize, maxdepth, jobs, func(_ context.Context, depth uint64) error {
				_, err := ld.Set(func(i uint64, _ bool) (uint64, error) {
					j := atomic.LoadInt64(&jobid)

					if i != depth {
						atomic.AddInt64(&jobid, j*-1)
						j = 0
					}

					defer atomic.AddInt64(&jobid, 1)

					if depth == d && uint64(j) == jobs-1 {
						return 0, errors.Errorf("hohoho")
					}

					return depth, nil
				})

				return err
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

	done := t.runNPhases(ctx, 3, 3, 33, func(ctx context.Context, depth uint64) error {
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

type testJobWorker struct {
	suite.Suite
}

func (t *testJobWorker) TestNoError() {
	var l uint64 = 10
	returnch := make(chan interface{}, l)

	callback := func(j interface{}) ContextWorkerCallback {
		return func(ctx context.Context) error {
			if j == nil {
				return nil
			}

			select {
			case <-time.After(time.Millisecond * 10):
				returnch <- j
			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		}
	}

	wk, _ := NewBaseJobWorker(context.Background(), int64(l))
	defer wk.Cancel()

	go func() {
		for i := uint64(0); i < l; i++ {
			if err := wk.NewJob(callback(i)); err != nil {
				break
			}
		}

		wk.Done()
	}()

	t.NoError(wk.Wait())

	close(returnch)

	e := make([]uint64, l)
	for i := uint64(0); i < l; i++ {
		e[i] = i
	}
	r := make([]uint64, l)

	for i := range returnch {
		j := i.(uint64)
		r[j] = j
	}

	slices.Sort(r)

	t.Equal(e, r)
}

func (t *testJobWorker) TestError() {
	t.Run("error", func() {
		var l uint64 = 10

		var called uint64
		callback := func(j interface{}) ContextWorkerCallback {
			return func(ctx context.Context) error {
				if j == nil {
					return nil
				}

				if i := j.(uint64); i == 3 {
					return errors.Errorf("error:%d", j)
				}

				select {
				case <-time.After(time.Second * 3):
					atomic.AddUint64(&called, 1)
				case <-ctx.Done():
					return ctx.Err()
				}

				return nil
			}
		}

		wk, _ := NewBaseJobWorker(context.Background(), int64(l))
		defer wk.Cancel()

		go func() {
			for i := uint64(0); i < l; i++ {
				if err := wk.NewJob(callback(i)); err != nil {
					break
				}
			}

			wk.Done()
		}()

		err := wk.Wait()
		t.NotNil(err)
		t.ErrorContains(err, "error:3")

		c := atomic.LoadUint64(&called)
		t.True(c < 1, "called=%d", c)
	})

	t.Run("error, no cancel ", func() {
		wk, _ := NewBaseJobWorker(context.Background(), 1)
		defer wk.Cancel()

		var jobCount uint64
		callback := func(context.Context) error {
			if atomic.LoadUint64(&jobCount) == 1 {
				return errors.Errorf("findme")
			}

			atomic.AddUint64(&jobCount, 1)

			return nil
		}

		errch := make(chan error, 4)
		go func() {
			defer wk.Done()

			for i := 0; i < 4; i++ {
				if err := wk.NewJob(callback); err != nil {
					errch <- err
				}
			}

			errch <- nil
		}()

		switch err := <-errch; {
		case err == nil:
			t.Fail("expected error")
		case strings.Contains(err.Error(), "findme"):
		case errors.Is(err, context.Canceled):
		default:
			t.Fail("expected context.Canceled or findme")
		}

		err := wk.Wait()
		t.Error(err)
		t.ErrorContains(err, "findme")
	})

	t.Run("error and canceled", func() {
		wk, _ := NewBaseJobWorker(context.Background(), 1)
		defer wk.Cancel()

		var jobCount uint64
		callback := func(context.Context) error {
			switch jobCount := atomic.LoadUint64(&jobCount); {
			case jobCount == 1:
				return errors.Errorf("findme")
			case jobCount > 1:
				go wk.Cancel()
			}

			atomic.AddUint64(&jobCount, 1)

			return nil
		}

		errch := make(chan error, 1)
		go func() {
			defer wk.Done()

			for i := 0; i < 4; i++ {
				if err := wk.NewJob(callback); err != nil {
					errch <- err

					return
				}
			}

			errch <- nil
		}()

		switch err := <-errch; {
		case err == nil:
			t.Fail("expected error")
		case strings.Contains(err.Error(), "findme"):
		case errors.Is(err, context.Canceled):
		default:
			t.Fail("expected context.Canceled or findme")
		}

		err := wk.Wait()
		t.Error(err)
		t.ErrorContains(err, "findme")
	})
}

func (t *testJobWorker) TestDeadlineError() {
	var l int64 = 10

	var called uint64
	callback := func() ContextWorkerCallback {
		return func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second * 10):
				atomic.AddUint64(&called, 1)
			}

			return nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	wk, _ := NewBaseJobWorker(ctx, l)
	defer wk.Cancel()

	go func() {
		for i := int64(0); i < l; i++ {
			if err := wk.NewJob(callback()); err != nil {
				break
			}
		}

		wk.Done()
	}()

	err := wk.Wait()
	t.Error(err)
	t.True(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))

	c := atomic.LoadUint64(&called)
	t.T().Logf("called: %d", c)
	t.True(c < 1)
}

func (t *testJobWorker) TestCancel() {
	t.Run("cancel after NewJob", func() {
		var l uint64 = 10

		callback := func(j interface{}) ContextWorkerCallback {
			return func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Millisecond * 900):
				}

				return nil
			}
		}

		wk, _ := NewBaseJobWorker(context.Background(), int64(l))
		defer wk.Cancel()

		go func() {
			for i := uint64(0); i < l; i++ {
				_ = wk.NewJob(callback(i))

				if i == 3 {
					go wk.Cancel()
				}
			}

			wk.Done()
		}()

		err := wk.Wait()
		t.Error(err)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel before wait", func() {
		wk, _ := NewBaseJobWorker(context.Background(), 1)
		defer wk.Cancel()

		wk.Cancel()

		err := wk.Wait()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel after wait", func() {
		wk, _ := NewBaseJobWorker(context.Background(), 1)
		defer wk.Cancel()

		t.NoError(wk.NewJob(func(ctx context.Context) error {
			select {
			case <-time.After(time.Minute):
			case <-ctx.Done():
			}

			return nil
		}))

		errch := make(chan error, 1)
		go func() {
			errch <- wk.Wait()
		}()

		<-time.After(time.Second * 2)
		wk.Cancel()

		err := <-errch
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testJobWorker) TestCancelBeforeRun() {
	var l int64 = 33

	var called int64

	callback := func(ctx context.Context) error {
		if atomic.LoadInt64(&called) < 5 {
			atomic.AddInt64(&called, 1)

			return nil
		}

		select {
		case <-time.After(time.Second):
			atomic.AddInt64(&called, 1)
		case <-ctx.Done():
			return ctx.Err()
		}

		return nil
	}

	wk, _ := NewBaseJobWorker(context.Background(), int64(l))
	defer wk.Cancel()

	enoughch := make(chan bool)
	go func() {
		for i := int64(0); i < l; i++ {
			if i == 5 {
				enoughch <- true
			}

			if err := wk.NewJob(callback); err != nil {
				return
			}
		}

		wk.Done()
	}()

	<-enoughch
	wk.Cancel()

	err := wk.Wait()
	t.Error(err)
	t.ErrorIs(err, context.Canceled)

	c := atomic.LoadInt64(&called)
	t.T().Logf("called=%d, total=%d", c, l)
	t.True(c < l, "called=%d, total=%d", c, l)
}

func (t *testJobWorker) TestDone() {
	t.Run("NewJob() after Done()", func() {
		wk, _ := NewBaseJobWorker(context.Background(), 3)
		defer wk.Cancel()

		jobdonech := make(chan struct{}, 1)
		t.NoError(wk.NewJob(func(context.Context) error {
			jobdonech <- struct{}{}

			return nil
		}))
		<-jobdonech

		wk.Done()

		err := wk.NewJob(func(context.Context) error {
			return nil
		})
		t.Error(err)
		t.ErrorIs(err, util.ErrJobWorkerDone)

		t.NoError(wk.Wait())
	})
}

func (t *testJobWorker) TestLazyWait() {
	t.Run("wait", func() {
		wk, _ := NewBaseJobWorker(context.Background(), 3)
		defer wk.Cancel()

		jobdonech := make(chan struct{}, 1)
		t.NoError(wk.NewJob(func(context.Context) error {
			<-time.After(time.Second * 2)

			jobdonech <- struct{}{}

			return nil
		}))

		wk.Done()

		t.NoError(wk.LazyWait())

		select {
		case <-time.After(time.Second):
			t.Fail("wait job done")
		case <-jobdonech:
		}
	})

	t.Run("no wait", func() {
		wk, _ := NewBaseJobWorker(context.Background(), 3)
		defer wk.Cancel()

		var called int

		t.NoError(wk.NewJob(func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second * 33):
				called++

				return nil
			}
		}))

		wk.Done()

		waitch := make(chan error, 1)
		go func() {
			waitch <- wk.LazyWait()
		}()

		<-time.After(time.Second * 2)
		wk.Cancel()

		t.Equal(0, called)
	})
}

func TestJobWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testJobWorker))
}

func TestNJobWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	sem := semaphore.NewWeighted(33)
	for i := 0; i < 99; i++ {
		_ = sem.Acquire(context.Background(), 1)

		go func() {
			defer sem.Release(1)

			suite.Run(t, new(testJobWorker))
		}()
	}

	_ = sem.Acquire(context.Background(), 33)
}

type testErrCallbackJobWorker struct {
	suite.Suite
}

func (t *testErrCallbackJobWorker) TestError() {
	t.Run("job error", func() {
		errch := make(chan error, 1)
		wk, err := NewErrCallbackJobWorker(context.Background(), 3, func(err error) {
			errch <- err
		})
		t.NoError(err)

		t.NoError(wk.NewJob(func(context.Context) error {
			return errors.Errorf("findme")
		}))
		wk.Done()
		t.NoError(wk.Wait())

		err = <-errch
		t.Error(err)
		t.ErrorContains(err, "findme")
	})

	t.Run("no error", func() {
		errch := make(chan error, 1)
		wk, err := NewErrCallbackJobWorker(context.Background(), 3, func(err error) {
			errch <- err
		})
		t.NoError(err)

		t.NoError(wk.NewJob(func(context.Context) error {
			return nil
		}))
		wk.Done()
		t.NoError(wk.Wait())
		errch <- nil

		t.NoError(<-errch)
	})
}

func TestErrCallbackJobWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	sem := semaphore.NewWeighted(33)
	for i := 0; i < 99; i++ {
		_ = sem.Acquire(context.Background(), 1)

		go func() {
			defer sem.Release(1)

			suite.Run(t, new(testErrCallbackJobWorker))
		}()
	}

	_ = sem.Acquire(context.Background(), 33)
}
