package launch

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testLongRunningMemberlistJoin struct {
	suite.Suite
}

func (t *testLongRunningMemberlistJoin) TestJoin() {
	var joined int64

	l := NewLongRunningMemberlistJoin(
		func() error {
			if atomic.LoadInt64(&joined) > 0 {
				return nil
			}

			return errors.Errorf("heheheh")
		},
		func() bool {
			return atomic.LoadInt64(&joined) > 0
		},
	)
	l.interval = time.Millisecond * 300

	ch0, err := l.Join()
	t.NoError(err)
	ch1, err := l.Join()
	t.NoError(err)

	t.Run("check done channels not closed", func() {
		select {
		case <-time.After(time.Second * 1):
		case <-ch0:
			t.NoError(errors.Errorf("second join was finished unexpectedly"))
		}

		select {
		case <-time.After(time.Second * 1):
		case <-ch1:
			t.NoError(errors.Errorf("second join was finished unexpectedly"))
		}
	})

	t.Run("after joined, done channels should be closed", func() {
		_ = atomic.AddInt64(&joined, 1)

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("wait to done ch0, but failed"))
		case err := <-ch0:
			t.NoError(err)
		}

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("wait to done ch1, but failed"))
		case err := <-ch1:
			t.NoError(err)
		}
	})
}

func (t *testLongRunningMemberlistJoin) TestCancel() {
	l := NewLongRunningMemberlistJoin(
		func() error {
			return errors.Errorf("heheheh")
		},
		func() bool {
			return false
		},
	)
	l.interval = time.Millisecond * 300

	ch, err := l.Join()
	t.NoError(err)

	t.True(l.Cancel())

	select {
	case <-time.After(time.Second * 1):
		t.NoError(errors.Errorf("wait to done, but failed"))
	case err := <-ch:
		t.True(errors.Is(err, context.Canceled))
	}
}

func TestLongRunningMemberlistJoin(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testLongRunningMemberlistJoin))
}
