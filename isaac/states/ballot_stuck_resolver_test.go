package isaacstates

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

var (
	findMissingBallotsf    = func(context.Context, base.StagePoint, bool) ([]base.Address, bool, error) { return nil, true, nil }
	requestMissingBallotsf = func(context.Context, base.StagePoint, []base.Address) error { return nil }
	voteSuffrageVotingf    = func(context.Context, base.StagePoint, []base.Address) (base.Voteproof, error) { return nil, nil }
)

type testDefaultBallotStuckResolver struct {
	suite.Suite
}

func (t *testDefaultBallotStuckResolver) TestNew() {
	r := NewDefaultBallotStuckResolver(
		time.Second*3,
		time.Second*3,
		time.Millisecond*300,
		findMissingBallotsf,
		requestMissingBallotsf,
		voteSuffrageVotingf,
	)

	_ = (interface{})(r).(BallotStuckResolver)
}

func (t *testDefaultBallotStuckResolver) TestCancel() {
	t.Run("cancel before wait", func() {
		point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

		r := NewDefaultBallotStuckResolver(
			time.Second*3,
			time.Second,
			time.Millisecond*300,
			findMissingBallotsf,
			requestMissingBallotsf,
			voteSuffrageVotingf,
		)

		t.True(r.NewPoint(context.Background(), point))
		r.Cancel(point)
	})

	t.Run("cancel after wait", func() {
		point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

		r := NewDefaultBallotStuckResolver(
			time.Nanosecond,
			time.Second,
			time.Millisecond*100,
			func(context.Context, base.StagePoint, bool) ([]base.Address, bool, error) {
				return []base.Address{base.RandomAddress("")}, true, nil
			},
			requestMissingBallotsf,
			voteSuffrageVotingf,
		)
		defer r.Clean()

		r.SetLogging(logging.TestNilLogging)

		t.True(r.NewPoint(context.Background(), point))
		<-time.After(time.Millisecond * 300)
		r.Cancel(point)
	})

	t.Run("cancel by another point", func() {
		point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

		votedch := make(chan struct{}, 1)

		r := NewDefaultBallotStuckResolver(
			time.Nanosecond,
			time.Second,
			time.Millisecond*300,
			func(context.Context, base.StagePoint, bool) ([]base.Address, bool, error) {
				return []base.Address{base.RandomAddress("")}, true, nil
			},
			requestMissingBallotsf,
			func(context.Context, base.StagePoint, []base.Address) (base.Voteproof, error) {
				votedch <- struct{}{}

				return nil, nil
			},
		)
		defer r.Clean()

		t.True(r.NewPoint(context.Background(), point))

		newpoint := base.NewStagePoint(base.RawPoint(33, 1), base.StageINIT)
		t.True(r.NewPoint(context.Background(), newpoint))

		select {
		case <-time.After(time.Millisecond * 600):
		case <-votedch:
			t.NoError(errors.Errorf("should be cancled before suffrage voting"))
		}

		r.Cancel(newpoint)

		t.Equal(newpoint, r.point)
	})

	t.Run("cancel by old point", func() {
		point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

		votedch := make(chan struct{}, 1)

		r := NewDefaultBallotStuckResolver(
			time.Nanosecond,
			time.Second,
			time.Millisecond*300,
			func(context.Context, base.StagePoint, bool) ([]base.Address, bool, error) {
				return []base.Address{base.RandomAddress("")}, true, nil
			},
			requestMissingBallotsf,
			func(context.Context, base.StagePoint, []base.Address) (base.Voteproof, error) {
				votedch <- struct{}{}

				return nil, nil
			},
		)
		defer r.Clean()

		t.True(r.NewPoint(context.Background(), point))

		<-time.After(time.Millisecond * 600)
		oldpoint := base.NewStagePoint(point.Point.PrevRound(), base.StageINIT)

		r.Cancel(oldpoint)

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("wait suffrage voting, but failed"))
		case <-votedch:
		}

		r.Cancel(point)
		t.Equal(point, r.point)
	})
}

func (t *testDefaultBallotStuckResolver) TestClean() {
	t.Run("clean", func() {
		point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

		votedch := make(chan struct{}, 1)

		r := NewDefaultBallotStuckResolver(
			time.Nanosecond,
			time.Second,
			time.Millisecond*300,
			func(context.Context, base.StagePoint, bool) ([]base.Address, bool, error) {
				return []base.Address{base.RandomAddress("")}, true, nil
			},
			requestMissingBallotsf,
			func(context.Context, base.StagePoint, []base.Address) (base.Voteproof, error) {
				votedch <- struct{}{}

				return nil, nil
			},
		)

		t.True(r.NewPoint(context.Background(), point))

		<-time.After(time.Millisecond * 600)

		r.Clean()

		select {
		case <-time.After(time.Second * 2):
		case <-votedch:
			t.NoError(errors.Errorf("should be cancled before suffrage voting"))
		}

		t.Equal(base.ZeroStagePoint, r.point)
	})
}

func (t *testDefaultBallotStuckResolver) TestNomoreGatherMissingBallots() {
	point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

	var n int64

	findMissingBallotsf := func(context.Context, base.StagePoint, bool) ([]base.Address, bool, error) {
		defer atomic.AddInt64(&n, 1)

		if atomic.LoadInt64(&n) < 2 {
			return []base.Address{base.RandomAddress("")}, true, nil
		}

		return nil, true, nil // no more missing nodes
	}

	votedch := make(chan struct{}, 1)

	r := NewDefaultBallotStuckResolver(
		time.Nanosecond,
		time.Second*3,
		time.Millisecond*10,
		findMissingBallotsf,
		requestMissingBallotsf,
		func(context.Context, base.StagePoint, []base.Address) (base.Voteproof, error) {
			votedch <- struct{}{}

			return nil, nil
		},
	)
	defer r.Clean()

	t.True(r.NewPoint(context.Background(), point))

	select {
	case <-time.After(time.Second * 2):
	case <-votedch:
		t.NoError(errors.Errorf("should be cancled before suffrage voting"))
	}
}

func (t *testDefaultBallotStuckResolver) TestNomoreMissingNodesInSuffrageVoting() {
	point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

	var n int64

	findMissingBallotsf := func(context.Context, base.StagePoint, bool) ([]base.Address, bool, error) {
		defer atomic.AddInt64(&n, 1)

		if atomic.LoadInt64(&n) < 3 {
			return []base.Address{base.RandomAddress("")}, true, nil
		}

		return nil, true, nil // no more missing nodes
	}

	votedch := make(chan struct{}, 1)

	r := NewDefaultBallotStuckResolver(
		time.Nanosecond,
		time.Millisecond*600,
		time.Millisecond*300,
		findMissingBallotsf,
		requestMissingBallotsf,
		func(context.Context, base.StagePoint, []base.Address) (base.Voteproof, error) {
			votedch <- struct{}{}

			return nil, nil
		},
	)
	defer r.Clean()

	t.True(r.NewPoint(context.Background(), point))

	<-votedch

	select {
	case <-time.After(time.Second * 2):
	case <-votedch:
		t.NoError(errors.Errorf("should be cancled before suffrage voting"))
	}
}

func (t *testDefaultBallotStuckResolver) TestNextRound() {
	point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

	r := NewDefaultBallotStuckResolver(
		time.Nanosecond,
		time.Second,
		time.Millisecond*300,
		func(context.Context, base.StagePoint, bool) ([]base.Address, bool, error) {
			return []base.Address{base.RandomAddress("")}, true, nil
		},
		requestMissingBallotsf,
		func(context.Context, base.StagePoint, []base.Address) (base.Voteproof, error) {
			return isaac.NewINITVoteproof(point.Point), nil
		},
	)
	defer r.Clean()

	t.True(r.NewPoint(context.Background(), point))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("wait next round, but failed"))
	case vp := <-r.Voteproof():
		t.NotNil(vp)
	}
}

func TestDefaultBallotStuckResolver(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testDefaultBallotStuckResolver))
}
