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

type testBallotStuckResolver struct {
	suite.Suite
}

func (t *testBallotStuckResolver) TestCancel() {
	t.Run("cancel before wait", func() {
		point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

		r := newBallotStuckResolver(
			time.Second*3,
			time.Millisecond*300,
			func(context.Context, base.StagePoint) ([]base.Address, error) { return nil, nil },
			func(context.Context, base.StagePoint, []base.Address) error { return nil },
			func(context.Context, base.Point, []base.Address) error { return nil },
			func(context.Context, base.Point) (base.Voteproof, error) { return nil, nil },
			func(context.Context, base.Point, base.Voteproof) error { return nil },
		)

		t.True(r.newPoint(context.Background(), point))
		r.cancel(point)
	})

	t.Run("cancel after wait", func() {
		point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

		r := newBallotStuckResolver(
			time.Nanosecond,
			time.Millisecond*100,
			func(context.Context, base.StagePoint) ([]base.Address, error) {
				return []base.Address{base.RandomAddress("")}, nil
			},
			func(context.Context, base.StagePoint, []base.Address) error { return nil },
			func(context.Context, base.Point, []base.Address) error { return nil },
			func(context.Context, base.Point) (base.Voteproof, error) { return nil, nil },
			func(context.Context, base.Point, base.Voteproof) error { return nil },
		)

		r.SetLogging(logging.TestNilLogging)

		t.True(r.newPoint(context.Background(), point))
		<-time.After(time.Millisecond * 300)
		r.cancel(point)
	})

	t.Run("cancel by another point", func() {
		point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

		votedch := make(chan struct{}, 1)

		r := newBallotStuckResolver(
			time.Nanosecond,
			time.Millisecond*300,
			func(context.Context, base.StagePoint) ([]base.Address, error) {
				return []base.Address{base.RandomAddress("")}, nil
			},
			func(context.Context, base.StagePoint, []base.Address) error { return nil },
			func(context.Context, base.Point, []base.Address) error {
				votedch <- struct{}{}

				return nil
			},
			func(context.Context, base.Point) (base.Voteproof, error) { return nil, nil },
			func(context.Context, base.Point, base.Voteproof) error { return nil },
		)

		t.True(r.newPoint(context.Background(), point))

		newpoint := base.NewStagePoint(base.RawPoint(33, 1), base.StageINIT)
		t.True(r.newPoint(context.Background(), newpoint))

		select {
		case <-time.After(time.Millisecond * 600):
		case <-votedch:
			t.NoError(errors.Errorf("should be cancled before suffrage voting"))
		}

		r.cancel(newpoint)

		t.Equal(newpoint, r.point)
	})

	t.Run("cancel by old point", func() {
		point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

		votedch := make(chan struct{}, 1)

		r := newBallotStuckResolver(
			time.Nanosecond,
			time.Millisecond*300,
			func(context.Context, base.StagePoint) ([]base.Address, error) {
				return []base.Address{base.RandomAddress("")}, nil
			},
			func(context.Context, base.StagePoint, []base.Address) error { return nil },
			func(context.Context, base.Point, []base.Address) error {
				votedch <- struct{}{}

				return nil
			},
			func(context.Context, base.Point) (base.Voteproof, error) { return nil, nil },
			func(context.Context, base.Point, base.Voteproof) error { return nil },
		)

		t.True(r.newPoint(context.Background(), point))

		<-time.After(time.Millisecond * 600)
		oldpoint := base.NewStagePoint(point.Point.PrevRound(), base.StageINIT)

		r.cancel(oldpoint)

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("wait suffrage voting, but failed"))
		case <-votedch:
		}

		r.cancel(point)
		t.Equal(point, r.point)
	})
}

func (t *testBallotStuckResolver) TestClean() {
	t.Run("clean", func() {
		point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

		votedch := make(chan struct{}, 1)

		r := newBallotStuckResolver(
			time.Nanosecond,
			time.Millisecond*300,
			func(context.Context, base.StagePoint) ([]base.Address, error) {
				return []base.Address{base.RandomAddress("")}, nil
			},
			func(context.Context, base.StagePoint, []base.Address) error { return nil },
			func(context.Context, base.Point, []base.Address) error {
				votedch <- struct{}{}

				return nil
			},
			func(context.Context, base.Point) (base.Voteproof, error) { return nil, nil },
			func(context.Context, base.Point, base.Voteproof) error { return nil },
		)

		t.True(r.newPoint(context.Background(), point))

		<-time.After(time.Millisecond * 600)

		r.clean()

		select {
		case <-time.After(time.Second * 2):
		case <-votedch:
			t.NoError(errors.Errorf("should be cancled before suffrage voting"))
		}

		t.Equal(base.ZeroStagePoint, r.point)
	})
}

func (t *testBallotStuckResolver) TestNomoreGatherMissingBallots() {
	point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

	var n int64

	findMissingBallotsf := func(context.Context, base.StagePoint) ([]base.Address, error) {
		defer atomic.AddInt64(&n, 1)

		if atomic.LoadInt64(&n) < 2 {
			return []base.Address{base.RandomAddress("")}, nil
		}

		return nil, nil // no more missing nodes
	}

	votedch := make(chan struct{}, 1)

	r := newBallotStuckResolver(
		time.Nanosecond,
		time.Millisecond*300,
		findMissingBallotsf,
		func(context.Context, base.StagePoint, []base.Address) error { return nil },
		func(context.Context, base.Point, []base.Address) error {
			votedch <- struct{}{}

			return nil
		},
		func(context.Context, base.Point) (base.Voteproof, error) { return nil, nil },
		func(context.Context, base.Point, base.Voteproof) error { return nil },
	)

	t.True(r.newPoint(context.Background(), point))

	select {
	case <-time.After(time.Second * 2):
	case <-votedch:
		t.NoError(errors.Errorf("should be cancled before suffrage voting"))
	}
}

func (t *testBallotStuckResolver) TestNomoreMissingNodesInSuffrageVoting() {
	point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

	var n int64

	findMissingBallotsf := func(context.Context, base.StagePoint) ([]base.Address, error) {
		defer atomic.AddInt64(&n, 1)

		if atomic.LoadInt64(&n) < 6 {
			return []base.Address{base.RandomAddress("")}, nil
		}

		return nil, nil // no more missing nodes
	}

	votedch := make(chan struct{}, 1)

	r := newBallotStuckResolver(
		time.Nanosecond,
		time.Millisecond*300,
		findMissingBallotsf,
		func(context.Context, base.StagePoint, []base.Address) error { return nil },
		func(context.Context, base.Point, []base.Address) error {
			votedch <- struct{}{}

			return nil
		},
		func(context.Context, base.Point) (base.Voteproof, error) { return nil, nil },
		func(context.Context, base.Point, base.Voteproof) error { return nil },
	)

	t.True(r.newPoint(context.Background(), point))

	<-votedch

	select {
	case <-time.After(time.Second * 2):
	case <-votedch:
		t.NoError(errors.Errorf("should be cancled before suffrage voting"))
	}
}

func (t *testBallotStuckResolver) TestNextRound() {
	point := base.NewStagePoint(base.RawPoint(33, 0), base.StageINIT)

	nextroundch := make(chan struct{}, 1)

	r := newBallotStuckResolver(
		time.Nanosecond,
		time.Millisecond*300,
		func(context.Context, base.StagePoint) ([]base.Address, error) {
			return []base.Address{base.RandomAddress("")}, nil
		},
		func(context.Context, base.StagePoint, []base.Address) error { return nil },
		func(context.Context, base.Point, []base.Address) error {
			return nil
		},
		func(context.Context, base.Point) (base.Voteproof, error) {
			return isaac.NewINITVoteproof(point.Point), nil
		},
		func(context.Context, base.Point, base.Voteproof) error {
			nextroundch <- struct{}{}

			return nil
		},
	)

	t.True(r.newPoint(context.Background(), point))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("wait next round, but failed"))
	case <-nextroundch:
	}
}

func TestBallotStuckResolver(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testBallotStuckResolver))
}
