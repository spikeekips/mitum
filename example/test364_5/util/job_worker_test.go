package util

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
	BaseSuite
}

func (t *testJobWorker) TestNoError() {
	var l int64 = 10
	returnch := make(chan interface{}, l)

	callback := func(j interface{}) InErrorFunc[context.Context] {
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

	newJob, jobDone, wait, _ := NewFuncJobWorker(context.Background(), l)

	go func() {
		for i := int64(0); i < l; i++ {
			if err := newJob(callback(i)); err != nil {
				break
			}
		}

		jobDone()
	}()

	t.NoError(wait(nil))

	close(returnch)

	e := make([]int64, l)
	for i := int64(0); i < l; i++ {
		e[i] = i
	}
	r := make([]int64, l)

	for i := range returnch {
		j := i.(int64)
		r[j] = j
	}

	slices.Sort(r)

	t.Equal(e, r)
}

func (t *testJobWorker) TestError() {
	t.Run("error", func() {
		var l int64 = 10

		var called uint64
		callback := func(j interface{}) InErrorFunc[context.Context] {
			return func(ctx context.Context) error {
				if j == nil {
					return nil
				}

				if i := j.(int64); i == 3 {
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

		newJob, jobDone, wait, _ := NewFuncJobWorker(context.Background(), l)

		go func() {
			for i := int64(0); i < l; i++ {
				if err := newJob(callback(i)); err != nil {
					break
				}
			}

			jobDone()
		}()

		err := wait(nil)
		t.NotNil(err)
		t.ErrorContains(err, "error:3")

		c := atomic.LoadUint64(&called)
		t.True(c < 1, "called=%d", c)
	})

	t.Run("error, no cancel", func() {
		newJob, jobDone, wait, _ := NewFuncJobWorker(context.Background(), 1)

		var jobCount uint64
		callback := func(context.Context) error {
			if atomic.LoadUint64(&jobCount) == 1 {
				return errors.New("findme")
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

		err := wait(nil)
		t.Error(err)
		t.ErrorContains(err, "findme")
	})

	t.Run("error and canceled", func() {
		ctx, cancel := t.Ctx()

		newJob, jobDone, wait, _ := NewFuncJobWorker(ctx, 1)

		var jobCount uint64
		callback := func(context.Context) error {
			switch jobCount := atomic.LoadUint64(&jobCount); {
			case jobCount == 1:
				return errors.New("findme")
			case jobCount > 1:
				cancel(nil)
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

		err := wait(nil)
		t.Error(err)
		t.ErrorContains(err, "findme")
	})
}

func (t *testJobWorker) TestDeadlineError() {
	var l int64 = 10

	var called uint64
	callback := func() InErrorFunc[context.Context] {
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

	newJob, jobDone, wait, _ := NewFuncJobWorker(ctx, l)

	go func() {
		for i := int64(0); i < l; i++ {
			if err := newJob(callback()); err != nil {
				break
			}
		}

		jobDone()
	}()

	err := wait(nil)
	t.Error(err)
	t.True(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))

	c := atomic.LoadUint64(&called)
	t.T().Logf("called: %d", c)
	t.True(c < 1)
}

func (t *testJobWorker) TestCancel() {
	t.Run("cancel after NewJob", func() {
		var l int64 = 10

		callback := func(j interface{}) InErrorFunc[context.Context] {
			return func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Millisecond * 900):
				}

				return nil
			}
		}

		ctx, cancel := t.Ctx()

		newJob, jobDone, wait, _ := NewFuncJobWorker(ctx, l)

		go func() {
			for i := int64(0); i < l; i++ {
				_ = newJob(callback(i))

				if i == 3 {
					cancel(nil)
				}
			}

			jobDone()
		}()

		err := wait(nil)
		t.Error(err)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel before wait", func() {
		ctx, cancel := t.Ctx()

		_, _, wait, _ := NewFuncJobWorker(ctx, 1)

		cancel(nil)

		err := wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel after wait", func() {
		ctx, cancel := t.Ctx()

		newJob, _, wait, _ := NewFuncJobWorker(ctx, 1)

		t.NoError(newJob(func(ctx context.Context) error {
			select {
			case <-time.After(time.Minute):
			case <-ctx.Done():
			}

			return nil
		}))

		errch := make(chan error, 1)
		go func() {
			errch <- wait(nil)
		}()

		cancel(nil)

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

	ctx, cancel := t.Ctx()

	newJob, jobDone, wait, _ := NewFuncJobWorker(ctx, l)

	enoughch := make(chan bool)
	go func() {
		for i := int64(0); i < l; i++ {
			if i == 5 {
				enoughch <- true
			}

			newJob(callback)
		}

		jobDone()
	}()

	<-enoughch
	cancel(nil)

	err := wait(nil)
	t.Error(err)
	t.ErrorIs(err, context.Canceled)

	c := atomic.LoadInt64(&called)
	t.T().Logf("called=%d, total=%d", c, l)
	t.True(c < l, "called=%d, total=%d", c, l)
}

func (t *testJobWorker) TestDone() {
	t.Run("NewJob() after Done()", func() {
		newJob, jobDone, wait, _ := NewFuncJobWorker(context.Background(), 3)

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
		t.ErrorIs(err, ErrJobWorkerDone)

		t.NoError(wait(nil))
	})

	t.Run("under async", func() {
		newJob, jobDone, wait, _ := NewFuncJobWorker(context.Background(), 3)

		jobdonech := make(chan struct{}, 1)
		t.NoError(newJob(func(context.Context) error {
			jobdonech <- struct{}{}

			return nil
		}))
		<-jobdonech

		go func() {
			for i := range 3333 {
				_ = newJob(func(context.Context) error {
					if i%333 == 0 {
						jobDone()
					}

					return nil
				})
			}
		}()

		t.NoError(wait(nil))
	})
}

func (t *testJobWorker) TestWait() {
	t.Run("wait cancel; before job finished", func() {
		newJob, jobDone, wait, err := NewFuncJobWorker(context.Background(), 3)
		t.NoError(err)

		jobdonech := make(chan struct{})
		t.NoError(newJob(func(context.Context) error {
			jobdonech <- struct{}{}

			return nil
		}))

		jobDone()

		err = wait(errors.New("showme"))
		t.ErrorContains(err, "showme")

		select {
		case <-time.After(time.Second):
			t.Fail("wait job done")
		case <-jobdonech:
		}
	})

	t.Run("wait cancel; after job finished", func() {
		newJob, jobDone, wait, err := NewFuncJobWorker(context.Background(), 3)
		t.NoError(err)

		jobdonech := make(chan struct{})
		t.NoError(newJob(func(context.Context) error {
			jobdonech <- struct{}{}

			return nil
		}))

		jobDone()
		<-jobdonech

		err = wait(errors.New("showme"))
		t.ErrorContains(err, "showme")
	})

	t.Run("wait", func() {
		newJob, jobDone, wait, _ := NewFuncJobWorker(context.Background(), 3)

		var called int64
		t.NoError(newJob(func(context.Context) error {
			atomic.AddInt64(&called, 1)

			return nil
		}))

		jobDone()

		t.NoError(wait(nil))
		t.Equal(int64(1), atomic.LoadInt64(&called))
	})

	t.Run("no wait", func() {
		ctx, cancel := t.Ctx()

		newJob, jobDone, wait, _ := NewFuncJobWorker(ctx, 3)

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
			waitch <- wait(nil)
		}()

		<-time.After(time.Second)
		cancel(nil)

		select {
		case <-time.After(time.Second):
			t.Fail("wait")
		case err := <-waitch:
			t.True(errors.Is(err, context.Canceled))
		}

		t.Equal(0, called)
	})

	t.Run("nil wait and the error wait", func() {
		_, _, wait, _ := NewFuncJobWorker(context.Background(), 3)

		errch := make(chan error)
		go func() {
			errch <- wait(nil)
		}()

		select {
		case <-time.After(time.Second):
		case <-errch:
			t.Fail("unexpected wait")
		}

		err := wait(errors.New("showme"))
		t.ErrorContains(err, "showme")

		select {
		case <-time.After(time.Second):
		case err := <-errch:
			t.ErrorContains(err, "showme")
		}
	})

	t.Run("nil wait and the error wait", func() {
		_, jobDone, wait, _ := NewFuncJobWorker(context.Background(), 3)

		jobDone()
		t.NoError(wait(nil))

		err := wait(errors.New("showme"))
		t.ErrorIs(err, context.Canceled)
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
		var gerr atomic.Value

		newJob, jobDone, wait, err := NewFuncErrJobWorker(context.Background(), 3, func(err error) error {
			gerr.Store(err)

			return nil
		})
		t.NoError(err)

		t.NoError(newJob(func(context.Context) error {
			return errors.New("findme")
		}))
		jobDone()
		t.NoError(wait(nil))

		err, ok := gerr.Load().(error)
		t.True(ok)
		t.ErrorContains(err, "findme")
	})

	t.Run("job error; returned error", func() {
		newJob, jobDone, wait, err := NewFuncErrJobWorker(context.Background(), 3, func(err error) error {
			return err
		})
		t.NoError(err)

		t.NoError(newJob(func(context.Context) error {
			return errors.New("findme")
		}))

		jobDone()

		err = wait(nil)
		t.ErrorContains(err, "findme")
	})

	t.Run("no error", func() {
		var gerr atomic.Value

		newJob, jobDone, wait, err := NewFuncErrJobWorker(context.Background(), 3, func(err error) error {
			gerr.Store(err)

			return nil
		})
		t.NoError(err)

		t.NoError(newJob(func(context.Context) error {
			return nil
		}))
		jobDone()
		t.NoError(wait(nil))

		t.Nil(gerr.Load())
	})
}

func TestErrJobWorkerAsync(t *testing.T) {
	defer goleak.VerifyNone(t)

	sem := semaphore.NewWeighted(99)
	for range 99 {
		_ = sem.Acquire(context.Background(), 1)

		go func() {
			defer sem.Release(1)

			suite.Run(t, new(testErrJobWorker))
		}()
	}

	_ = sem.Acquire(context.Background(), 99)
}

func TestErrJobWorker(t *testing.T) {
	for range 99 {
		func() {
			defer goleak.VerifyNone(t)

			suite.Run(t, new(testErrJobWorker))
		}()
	}
}
