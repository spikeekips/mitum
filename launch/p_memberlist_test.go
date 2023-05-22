package launch

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testLongRunningMemberlistJoin struct {
	suite.Suite
}

func (t *testLongRunningMemberlistJoin) TestJoin() {
	var joined int64

	l := NewLongRunningMemberlistJoin(
		func([]quicstream.UDPConnInfo) (bool, error) {
			if atomic.LoadInt64(&joined) > 0 {
				return true, nil
			}

			return false, errors.Errorf("heheheh")
		},
		func() bool {
			return atomic.LoadInt64(&joined) > 0
		},
	)
	l.interval = time.Millisecond * 300

	ch0 := l.Join()
	ch1 := l.Join()

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
		case <-ch0:
		}

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("wait to done ch1, but failed"))
		case <-ch1:
		}
	})
}

func (t *testLongRunningMemberlistJoin) TestCancel() {
	l := NewLongRunningMemberlistJoin(
		func([]quicstream.UDPConnInfo) (bool, error) {
			return false, errors.Errorf("heheheh")
		},
		func() bool {
			return false
		},
	)
	l.interval = time.Millisecond * 300

	ch := l.Join()

	t.NoError(l.Cancel())

	select {
	case <-time.After(time.Second * 1):
		t.NoError(errors.Errorf("wait to done, but failed"))
	case <-ch:
	}
}

func TestLongRunningMemberlistJoin(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testLongRunningMemberlistJoin))
}
