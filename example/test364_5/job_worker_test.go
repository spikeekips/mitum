package main

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"golang.org/x/sync/semaphore"
)

type testJobWorker struct {
	suite.Suite
}

func (t *testJobWorker) TestNoError() {
	var l uint64 = 10
	returnch := make(chan interface{}, l)

	callback := func(j interface{}) inErrorFunc[context.Context] {
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

	newJob, jobDone, wait, _ := newFuncJobWorker(context.Background(), int64(l))

	go func() {
		for i := uint64(0); i < l; i++ {
			if err := newJob(callback(i)); err != nil {
				break
			}
		}

		jobDone()
	}()

	t.NoError(wait())

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
		callback := func(j interface{}) inErrorFunc[context.Context] {
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

		newJob, jobDone, wait, _ := newFuncJobWorker(context.Background(), int64(l))

		go func() {
			for i := uint64(0); i < l; i++ {
				if err := newJob(callback(i)); err != nil {
					break
				}
			}

			jobDone()
		}()

		err := wait()
		t.NotNil(err)
		t.ErrorContains(err, "error:3")

		c := atomic.LoadUint64(&called)
		t.True(c < 1, "called=%d", c)
	})

	t.Run("error, no cancel", func() {
		newJob, jobDone, wait, _ := newFuncJobWorker(context.Background(), 1)

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
			defer jobDone()

			for i := 0; i < 4; i++ {
				if err := newJob(callback); err != nil {
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

		err := wait()
		t.Error(err)
		t.ErrorContains(err, "findme")
	})

	t.Run("error and canceled", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newJob, jobDone, wait, _ := newFuncJobWorker(ctx, 1)

		var jobCount uint64
		callback := func(context.Context) error {
			switch jobCount := atomic.LoadUint64(&jobCount); {
			case jobCount == 1:
				return errors.Errorf("findme")
			case jobCount > 1:
				cancel()
			}

			atomic.AddUint64(&jobCount, 1)

			return nil
		}

		errch := make(chan error, 1)
		go func() {
			defer jobDone()

			for i := 0; i < 4; i++ {
				if err := newJob(callback); err != nil {
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

		err := wait()
		t.Error(err)
		t.ErrorContains(err, "findme")
	})
}

func (t *testJobWorker) TestDeadlineError() {
	var l int64 = 10

	var called uint64
	callback := func() inErrorFunc[context.Context] {
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

	newJob, jobDone, wait, _ := newFuncJobWorker(ctx, l)

	go func() {
		for i := int64(0); i < l; i++ {
			if err := newJob(callback()); err != nil {
				break
			}
		}

		jobDone()
	}()

	err := wait()
	t.Error(err)
	t.True(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))

	c := atomic.LoadUint64(&called)
	t.T().Logf("called: %d", c)
	t.True(c < 1)
}

func (t *testJobWorker) TestCancel() {
	t.Run("cancel after NewJob", func() {
		var l uint64 = 10

		callback := func(j interface{}) inErrorFunc[context.Context] {
			return func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Millisecond * 900):
				}

				return nil
			}
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newJob, jobDone, wait, _ := newFuncJobWorker(ctx, int64(l))

		go func() {
			for i := uint64(0); i < l; i++ {
				_ = newJob(callback(i))

				if i == 3 {
					cancel()
				}
			}

			jobDone()
		}()

		err := wait()
		t.Error(err)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel before wait", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, wait, _ := newFuncJobWorker(ctx, 1)

		cancel()

		err := wait()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel after wait", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newJob, _, wait, _ := newFuncJobWorker(ctx, 1)

		t.NoError(newJob(func(ctx context.Context) error {
			select {
			case <-time.After(time.Minute):
			case <-ctx.Done():
			}

			return nil
		}))

		errch := make(chan error, 1)
		go func() {
			errch <- wait()
		}()

		cancel()

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	newJob, jobDone, wait, _ := newFuncJobWorker(ctx, int64(l))

	enoughch := make(chan bool)
	go func() {
		for i := int64(0); i < l; i++ {
			if i == 5 {
				enoughch <- true
			}

			if err := newJob(callback); err != nil {
				return
			}
		}

		jobDone()
	}()

	<-enoughch
	cancel()

	err := wait()
	t.Error(err)
	t.ErrorIs(err, context.Canceled)

	c := atomic.LoadInt64(&called)
	t.T().Logf("called=%d, total=%d", c, l)
	t.True(c < l, "called=%d, total=%d", c, l)
}

func (t *testJobWorker) TestDone() {
	t.Run("NewJob() after Done()", func() {
		newJob, jobDone, wait, _ := newFuncJobWorker(context.Background(), 3)

		jobdonech := make(chan struct{}, 1)
		t.NoError(newJob(func(context.Context) error {
			jobdonech <- struct{}{}

			return nil
		}))
		<-jobdonech

		jobDone()

		err := newJob(func(context.Context) error {
			return nil
		})
		t.Error(err)
		t.ErrorIs(err, errJobWorkerDone)

		t.NoError(wait())
	})
}

func (t *testJobWorker) TestWait() {
	t.Run("wait", func() {
		newJob, jobDone, wait, _ := newFuncJobWorker(context.Background(), 3)

		jobdonech := make(chan struct{}, 1)
		t.NoError(newJob(func(context.Context) error {
			jobdonech <- struct{}{}

			return nil
		}))

		jobDone()

		t.NoError(wait())

		select {
		case <-time.After(time.Second):
			t.Fail("wait job done")
		case <-jobdonech:
		}
	})

	t.Run("no wait", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		newJob, jobDone, wait, _ := newFuncJobWorker(ctx, 3)

		var called int

		t.NoError(newJob(func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Minute):
				called++

				return nil
			}
		}))

		jobDone()

		waitch := make(chan error, 1)
		go func() {
			waitch <- wait()
		}()

		<-time.After(time.Second)
		cancel()

		select {
		case <-time.After(time.Second):
			t.Fail("wait")
		case err := <-waitch:
			t.True(errors.Is(err, context.Canceled))
		}

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

type testErrJobWorker struct {
	suite.Suite
}

func (t *testErrJobWorker) TestError() {
	t.Run("job error; no returned error", func() {
		errch := make(chan error, 1)
		newJob, jobDone, wait, err := newFuncErrJobWorker(context.Background(), 3, func(err error) error {
			errch <- err

			return nil
		})
		t.NoError(err)

		t.NoError(newJob(func(context.Context) error {
			return errors.Errorf("findme")
		}))
		jobDone()
		t.NoError(wait())

		err = <-errch
		t.Error(err)
		t.ErrorContains(err, "findme")
	})

	t.Run("job error; returned error", func() {
		newJob, jobDone, wait, err := newFuncErrJobWorker(context.Background(), 3, func(err error) error {
			return err
		})
		t.NoError(err)

		t.NoError(newJob(func(context.Context) error {
			return errors.Errorf("findme")
		}))

		jobDone()

		err = wait()
		t.ErrorContains(err, "findme")
	})

	t.Run("no error", func() {
		errch := make(chan error, 1)
		newJob, jobDone, wait, err := newFuncErrJobWorker(context.Background(), 3, func(err error) error {
			errch <- err

			return nil
		})
		t.NoError(err)

		t.NoError(newJob(func(context.Context) error {
			return nil
		}))
		jobDone()
		t.NoError(wait())
		errch <- nil

		t.NoError(<-errch)
	})
}

func TestErrJobWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	sem := semaphore.NewWeighted(33)
	for i := 0; i < 99; i++ {
		_ = sem.Acquire(context.Background(), 1)

		go func() {
			defer sem.Release(1)

			suite.Run(t, new(testErrJobWorker))
		}()
	}

	_ = sem.Acquire(context.Background(), 33)
}
