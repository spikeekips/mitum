package util

import (
	"context"
	"strings"
	"sync"
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
	returnch := make(chan uint64, l)

	callback := func(j interface{}) ContextWorkerCallback {
		return func(ctx context.Context, i uint64) error {
			if j == nil {
				return nil
			}

			select {
			case <-time.After(time.Millisecond * 10):
				returnch <- i
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
		r[i] = i
	}

	t.Equal(e, r)
}

func (t *testJobWorker) TestError() {
	t.Run("error", func() {
		var l uint64 = 10

		var called uint64
		callback := func(j interface{}) ContextWorkerCallback {
			return func(ctx context.Context, i uint64) error {
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

		callback := func(_ context.Context, jobCount uint64) error {
			if jobCount == 1 {
				return errors.Errorf("findme")
			}

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

		callback := func(_ context.Context, jobCount uint64) error {
			switch {
			case jobCount == 1:
				return errors.Errorf("findme")
			case jobCount > 1:
				go wk.Cancel()
			}

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
		return func(ctx context.Context, _ uint64) error {
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
			return func(ctx context.Context, _ uint64) error {
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

		t.NoError(wk.NewJob(func(ctx context.Context, _ uint64) error {
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

	callback := func(ctx context.Context, jobCount uint64) error {
		if jobCount < 5 {
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

		jobdonech := make(chan uint64, 1)
		t.NoError(wk.NewJob(func(_ context.Context, jobCount uint64) error {
			jobdonech <- jobCount

			return nil
		}))
		t.Equal(uint64(0), <-jobdonech)

		wk.Done()

		err := wk.NewJob(func(context.Context, uint64) error {
			return nil
		})
		t.Error(err)
		t.ErrorIs(err, ErrJobWorkerDone)

		t.NoError(wk.Wait())
	})
}

func (t *testJobWorker) TestLazyWait() {
	t.Run("wait", func() {
		wk, _ := NewBaseJobWorker(context.Background(), 3)
		defer wk.Cancel()

		jobdonech := make(chan uint64, 1)
		t.NoError(wk.NewJob(func(_ context.Context, jobCount uint64) error {
			<-time.After(time.Second * 2)

			jobdonech <- jobCount

			return nil
		}))

		wk.Done()

		t.NoError(wk.LazyWait())

		t.Equal(uint64(0), <-jobdonech)
	})

	t.Run("no wait", func() {
		wk, _ := NewBaseJobWorker(context.Background(), 3)
		defer wk.Cancel()

		var called int

		t.NoError(wk.NewJob(func(ctx context.Context, _ uint64) error {
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

		t.NoError(wk.NewJob(func(context.Context, uint64) error {
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

		t.NoError(wk.NewJob(func(context.Context, uint64) error {
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

type testBatchWork struct {
	suite.Suite
}

func (t *testBatchWork) TestSimple() {
	size := int64(14)
	limit := int64(3)

	founds := make([]byte, size)
	foundlasts := make([]uint64, size/limit+1)

	lastlock := &sync.Mutex{}
	var l uint64
	t.NoError(BatchWork(context.Background(), size, limit,
		func(_ context.Context, last uint64) error {
			lastlock.Lock()
			defer lastlock.Unlock()

			foundlasts[l] = last
			l++

			return nil
		},
		func(_ context.Context, i, last uint64) error {
			founds[i] = 'f'

			return nil
		},
	))

	for i := range founds {
		t.NotNil(founds[i])
	}

	t.Equal(int64(5), size/limit+1)

	for i := int64(0); i <= size/limit; i++ {
		j := (i+1)*limit - 1
		if j > size-1 {
			j = size - 1
		}

		t.Equal(uint64(j), foundlasts[i])
	}
}

func TestBatchWork(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testBatchWork))
}
