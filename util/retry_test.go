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

type testRetry struct {
	suite.Suite
}

func (t *testRetry) TestNoError() {
	var called int
	err := Retry(context.Background(), func() (bool, error) {
		called++

		return false, nil
	}, 3, 0)
	t.NoError(err)
	t.Equal(1, called)
}

func (t *testRetry) TestError() {
	t.Run("false error", func() {
		var called int
		err := Retry(context.Background(), func() (bool, error) {
			called++

			if called < 2 {
				return true, errors.Errorf("findme")
			}

			return false, errors.Errorf("showme")
		}, 3, 1)
		t.Contains(err.Error(), "showme")
		t.Equal(2, called)
	})

	t.Run("limited error", func() {
		var called int
		err := Retry(context.Background(), func() (bool, error) {
			called++

			return true, errors.Errorf("showme: %d", called)
		}, 3, 1)
		t.Contains(err.Error(), "showme: 3")
		t.Equal(3, called)
	})
}

func (t *testRetry) TestCancel() {
	calledch := make(chan struct{}, 1)
	var calledonce sync.Once

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	donech := make(chan error)
	go func() {
		donech <- Retry(ctx, func() (bool, error) {
			calledonce.Do(func() {
				calledch <- struct{}{}
			})

			return true, errors.Errorf("showme")
		}, 100, time.Millisecond*600)
	}()

	<-calledch
	cancel()

	select {
	case <-time.After(time.Second * 3):
		t.NoError(errors.Errorf("failed to cancel"))
	case err := <-donech:
		t.True(errors.Is(err, context.Canceled))
	}
}

func TestRetry(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testRetry))
}
