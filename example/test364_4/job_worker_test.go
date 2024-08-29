package main

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
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
	defer wk.Close()

	go func() {
		for i := uint64(0); i < l; i++ {
			if err := wk.NewJob(callback(i)); err != nil {
				break
			}
		}

		wk.JobDone()
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
		defer wk.Close()

		go func() {
			for i := uint64(0); i < l; i++ {
				if err := wk.NewJob(callback(i)); err != nil {
					break
				}
			}

			wk.JobDone()
		}()

		err := wk.Wait()
		t.NotNil(err)
		t.ErrorContains(err, "error:3")

		c := atomic.LoadUint64(&called)
		t.True(c < 1, "called=%d", c)
	})

	t.Run("error, no cancel ", func() {
		wk, _ := NewBaseJobWorker(context.Background(), 1)
		defer wk.Close()

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
			defer wk.JobDone()

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
		defer wk.Close()

		var jobCount uint64
		callback := func(context.Context) error {
			switch jobCount := atomic.LoadUint64(&jobCount); {
			case jobCount == 1:
				return errors.Errorf("findme")
			case jobCount > 1:
				go wk.Close()
			}

			atomic.AddUint64(&jobCount, 1)

			return nil
		}

		errch := make(chan error, 1)
		go func() {
			defer wk.JobDone()

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
	defer wk.Close()

	go func() {
		for i := int64(0); i < l; i++ {
			if err := wk.NewJob(callback()); err != nil {
				break
			}
		}

		wk.JobDone()
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
		defer wk.Close()

		go func() {
			for i := uint64(0); i < l; i++ {
				_ = wk.NewJob(callback(i))

				if i == 3 {
					wk.Close()
				}
			}

			wk.JobDone()
		}()

		err := wk.Wait()
		t.Error(err)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel before wait", func() {
		wk, _ := NewBaseJobWorker(context.Background(), 1)
		defer wk.Close()

		wk.Close()

		err := wk.Wait()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel after wait", func() {
		wk, _ := NewBaseJobWorker(context.Background(), 1)
		defer wk.Close()

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
		wk.Close()

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
	defer wk.Close()

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

		wk.JobDone()
	}()

	<-enoughch
	wk.Close()

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
		defer wk.Close()

		jobdonech := make(chan struct{}, 1)
		t.NoError(wk.NewJob(func(context.Context) error {
			jobdonech <- struct{}{}

			return nil
		}))
		<-jobdonech

		wk.JobDone()

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
		defer wk.Close()

		jobdonech := make(chan struct{}, 1)
		t.NoError(wk.NewJob(func(context.Context) error {
			<-time.After(time.Second * 2)

			jobdonech <- struct{}{}

			return nil
		}))

		wk.JobDone()

		t.NoError(wk.Wait())

		select {
		case <-time.After(time.Second):
			t.Fail("wait job done")
		case <-jobdonech:
		}
	})

	t.Run("no wait", func() {
		wk, _ := NewBaseJobWorker(context.Background(), 3)
		defer wk.Close()

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

		wk.JobDone()

		waitch := make(chan error, 1)
		go func() {
			waitch <- wk.Wait()
		}()

		<-time.After(time.Second * 2)
		wk.Close()

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
		wk.JobDone()
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
		wk.JobDone()
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
