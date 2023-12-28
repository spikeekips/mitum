package util

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testContextDaemon struct {
	suite.Suite
}

func (t *testContextDaemon) TestNew() {
	stoppedchan := make(chan time.Time, 2)
	ed := NewContextDaemon(func(ctx context.Context) error {
		<-ctx.Done()

		stoppedchan <- time.Now()

		return nil
	})
	t.NoError(ed.Start(context.Background()))

	t.True(ed.IsStarted())

	err := ed.Start(context.Background())
	t.ErrorIs(err, ErrDaemonAlreadyStarted)

	<-time.After(time.Millisecond * 100)

	timeStopping := time.Now()
	t.NoError(ed.Stop())
	t.False(ed.IsStarted())

	timeStopped := <-stoppedchan
	t.True(timeStopped.Sub(timeStopping) > 0)

	err = ed.Stop()
	t.ErrorIs(err, ErrDaemonAlreadyStopped)
}

func (t *testContextDaemon) TestFuncStopped() {
	ed := NewContextDaemon(func(ctx context.Context) error {
		<-time.After(time.Millisecond * 100)

		return errors.Errorf("show me")
	})
	t.NoError(ed.Start(context.Background()))
	defer ed.Stop()

	t.True(ed.IsStarted())

	<-time.After(time.Millisecond * 300)
	t.False(ed.IsStarted())
}

func (t *testContextDaemon) TestStop() {
	stopAfter := time.Second
	ed := NewContextDaemon(func(ctx context.Context) error {
		<-time.After(stopAfter)

		return nil
	})
	timeStopping := time.Now()
	<-ed.Wait(context.Background())
	t.False(ed.IsStarted())

	t.True(time.Since(timeStopping) > stopAfter)

	// stop again
	t.ErrorIs(ed.Stop(), ErrDaemonAlreadyStopped)
}

func (t *testContextDaemon) TestStartAgain() {
	startedch := make(chan struct{}, 1)
	resultchan := make(chan error, 1)
	ed := NewContextDaemon(func(ctx context.Context) error {
		startedch <- struct{}{}

		<-ctx.Done()

		resultchan <- nil

		return nil
	})
	t.NoError(ed.Start(context.Background()))
	t.True(ed.IsStarted())

	t.NoError(ed.Stop())
	select {
	case <-time.After(time.Second):
		t.Fail("wait to stop, but failed")
		return
	case <-resultchan:
	}

	t.NoError(ed.Start(context.Background()))
	<-startedch
	t.True(ed.IsStarted())

	t.NoError(ed.Stop())

	select {
	case <-time.After(time.Second * 3):
		t.Fail("wait to stop, but failed")
		return
	case <-resultchan:
	}
}

func (t *testContextDaemon) TestWait() {
	ed := NewContextDaemon(func(_ context.Context) error {
		return errors.Errorf("show me")
	})

	err := <-ed.Wait(context.Background())
	t.ErrorContains(err, "show me")
	t.ErrorIs(ed.Stop(), ErrDaemonAlreadyStopped)

	ed = NewContextDaemon(func(_ context.Context) error {
		<-time.After(time.Second * 2)

		return errors.Errorf("show me")
	})

	done := make(chan error)
	go func() {
		done <- <-ed.Wait(context.Background())
	}()

	<-time.After(time.Second)
	t.True(ed.IsStarted())

	err = <-done
	t.ErrorContains(err, "show me")
}

func (t *testContextDaemon) TestStartWithContext() {
	resultchan := make(chan error, 1)
	ed := NewContextDaemon(func(ctx context.Context) error {
		<-ctx.Done()

		resultchan <- errors.Errorf("find me")

		return nil
	})

	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	t.NoError(ed.Start(ctx))
	err := <-resultchan

	t.True(time.Since(started) < time.Second*2)

	t.ErrorContains(err, "find me")
	<-time.After(time.Second)
	t.False(ed.IsStarted())
}

func (t *testContextDaemon) TestStopInGoroutine() {
	ed := NewContextDaemon(func(ctx context.Context) error {
		<-ctx.Done()

		return nil
	})

	t.NoError(ed.Start(context.Background()))

	var wg sync.WaitGroup
	wg.Add(4)
	for i := 0; i < 4; i++ {
		func() {
			defer wg.Done()

			_ = ed.Stop()
		}()
	}
	wg.Wait()

	t.False(ed.IsStarted())
}

func TestContextDaemon(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testContextDaemon))
}
