package isaacstates

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testJoiningHandler struct {
	isaac.BaseTestBallots
}

func (t *testJoiningHandler) newState(suf base.Suffrage) (*JoiningHandler, func()) {
	local := t.Local
	policy := t.Policy

	st := NewJoiningHandler(
		local,
		policy,
		nil,
		func(base.Height) base.Suffrage { return suf },
		func() (base.Manifest, bool, error) {
			return nil, false, errors.Errorf("empty manifest")
		},
	)
	_ = st.SetLogging(logging.TestNilLogging)
	_ = st.setTimers(util.NewTimers([]util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastACCEPTBallot,
	}, false))

	st.broadcastBallotFunc = func(bl base.Ballot) error {
		return nil
	}
	st.switchStateFunc = func(switchContext) error {
		return nil
	}

	return st, func() {
		deferred, err := st.exit(nil)
		t.NoError(err)
		deferred()
	}
}

func (t *testJoiningHandler) TestNew() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closef := t.newState(suf)
	defer closef()

	_, ok := (interface{})(st).(handler)
	t.True(ok)

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	st.getLastManifest = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	sctx := newJoiningSwitchContext(StateBooting, nil)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	_, ivp := t.VoteproofsPair(point, point.Next(), manifest.Hash(), nil, nil, nodes)
	err = st.newVoteproof(ivp)

	var ssctx consensusSwitchContext
	t.True(errors.As(err, &ssctx))
	base.EqualVoteproof(t.Assert(), ivp, ssctx.ivp)
}

func (t *testJoiningHandler) TestFailedLastManifest() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	t.Run("with error", func() {
		st, closef := t.newState(suf)
		defer closef()

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(sctx)
		t.NoError(err)
		deferred()

		point := base.RawPoint(33, 0)
		_, ivp := t.VoteproofsPair(point, point.Next(), nil, nil, nil, nodes)
		err = st.newVoteproof(ivp)

		var ssctx brokenSwitchContext
		t.True(errors.As(err, &ssctx))
		t.Contains(err.Error(), "failed to get last manifest")
	})

	t.Run("not found", func() {
		st, closef := t.newState(suf)
		defer closef()

		st.getLastManifest = func() (base.Manifest, bool, error) {
			return nil, false, nil
		}

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(sctx)
		t.NoError(err)
		deferred()

		point := base.RawPoint(33, 0)
		_, ivp := t.VoteproofsPair(point, point.Next(), nil, nil, nil, nodes)
		err = st.newVoteproof(ivp)

		var ssctx syncingSwitchContext
		t.True(errors.As(err, &ssctx))
		t.Equal(ivp.Point().Height()-1, ssctx.height)
	})
}

func (t *testJoiningHandler) TestInvalidINITVoteproof() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	t.Run("lower height", func() {
		st, closef := t.newState(suf)
		defer closef()

		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
		st.getLastManifest = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(sctx)
		t.NoError(err)
		deferred()

		_, ivp := t.VoteproofsPair(point.Decrease().Decrease(), point.Decrease(), nil, nil, nil, nodes)
		t.NoError(st.newVoteproof(ivp))
	})

	t.Run("higher height", func() {
		st, closef := t.newState(suf)
		defer closef()

		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
		st.getLastManifest = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(sctx)
		t.NoError(err)
		deferred()

		_, ivp := t.VoteproofsPair(point.Next(), point.Next().Next(), nil, nil, nil, nodes)
		err = st.newVoteproof(ivp)

		var ssctx syncingSwitchContext
		t.True(errors.As(err, &ssctx))
		t.Equal(ivp.Point().Height()-1, ssctx.height)
	})

	t.Run("previous block does not match", func() {
		st, closef := t.newState(suf)
		defer closef()

		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

		st.getLastManifest = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(sctx)
		t.NoError(err)
		deferred()

		_, ivp := t.VoteproofsPair(point, point.Next(), nil, nil, nil, nodes)
		err = st.newVoteproof(ivp)

		var ssctx syncingSwitchContext
		t.True(errors.As(err, &ssctx))
		t.Equal(ivp.Point().Height()-1, ssctx.height)
	})
}

func (t *testJoiningHandler) TestInvalidACCEPTVoteproof() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	t.Run("lower height", func() {
		st, closef := t.newState(suf)
		defer closef()

		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
		st.getLastManifest = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(sctx)
		t.NoError(err)
		deferred()

		avp, _ := t.VoteproofsPair(point, point.Next(), nil, nil, nil, nodes)
		t.NoError(st.newVoteproof(avp))
	})

	t.Run("higher height", func() {
		st, closef := t.newState(suf)
		defer closef()

		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
		st.getLastManifest = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(sctx)
		t.NoError(err)
		deferred()

		avp, _ := t.VoteproofsPair(point.Next().Next(), point.Next().Next().Next(), nil, nil, nil, nodes)
		err = st.newVoteproof(avp)

		var ssctx syncingSwitchContext
		t.True(errors.As(err, &ssctx))
		t.Equal(avp.Point().Height(), ssctx.height)
	})

	t.Run("higher height, but draw", func() {
		st, closef := t.newState(suf)
		defer closef()

		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
		st.getLastManifest = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(sctx)
		t.NoError(err)
		deferred()

		avp, _ := t.VoteproofsPair(point.Next().Next(), point.Next().Next().Next(), nil, nil, nil, nodes)
		avp.SetResult(base.VoteResultDraw)
		err = st.newVoteproof(avp)

		var ssctx syncingSwitchContext
		t.True(errors.As(err, &ssctx))
		t.Equal(avp.Point().Height()-1, ssctx.height)
	})
}

func (t *testJoiningHandler) TestINITVoteproofNextRound() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closef := t.newState(suf)
	defer closef()

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	st.getLastManifest = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		if bl.Point().Point.Equal(point.Next().NextRound()) {
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = isaac.DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.PRPool.Get(p), nil
		}
	})

	sctx := newJoiningSwitchContext(StateBooting, nil)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	_, ivp := t.VoteproofsPair(point, point.Next(), manifest.Hash(), nil, nil, nodes)
	ivp.SetResult(base.VoteResultDraw)

	t.NoError(st.newVoteproof(ivp))

	t.T().Log("wait next round init ballot")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.Next().NextRound(), bl.Point().Point)

		rbl, ok := bl.(base.INITBallot)
		t.True(ok)

		t.True(manifest.Hash().Equal(rbl.BallotSignedFact().BallotFact().PreviousBlock()))
	}
}

func (t *testJoiningHandler) TestACCEPTVoteproofNextRound() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closef := t.newState(suf)
	defer closef()

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	st.getLastManifest = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		if bl.Point().Point.Equal(point.Next().NextRound()) {
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = isaac.DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.PRPool.Get(p), nil
		}
	})

	sctx := newJoiningSwitchContext(StateBooting, nil)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	avp, _ := t.VoteproofsPair(point.Next(), point.Next().Next(), manifest.Hash(), nil, nil, nodes)
	avp.SetResult(base.VoteResultDraw)

	t.NoError(st.newVoteproof(avp))

	t.T().Log("wait next round init ballot")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.Next().NextRound(), bl.Point().Point)

		rbl, ok := bl.(base.INITBallot)
		t.True(ok)

		t.True(manifest.Hash().Equal(rbl.BallotSignedFact().BallotFact().PreviousBlock()))
	}
}

func (t *testJoiningHandler) TestLastINITVoteproofNextRound() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closef := t.newState(suf)
	defer closef()

	st.waitFirstVoteproof = time.Nanosecond

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	st.getLastManifest = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		if bl.Point().Point.Equal(point.Next().NextRound()) {
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = isaac.DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.PRPool.Get(p), nil
		}
	})

	sctx := newJoiningSwitchContext(StateBooting, nil)

	_, ivp := t.VoteproofsPair(point, point.Next(), manifest.Hash(), nil, nil, nodes)
	ivp.SetResult(base.VoteResultDraw)
	t.True(st.setLastVoteproof(ivp))

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	t.T().Log("wait next round init ballot")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.Next().NextRound(), bl.Point().Point)

		rbl, ok := bl.(base.INITBallot)
		t.True(ok)

		t.True(manifest.Hash().Equal(rbl.BallotSignedFact().BallotFact().PreviousBlock()))
	}
}

func (t *testJoiningHandler) TestLastACCEPTVoteproofNextRound() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closef := t.newState(suf)
	defer closef()

	st.waitFirstVoteproof = time.Nanosecond

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	st.getLastManifest = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		if bl.Point().Point.Equal(point.Next().NextRound()) {
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = isaac.DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.PRPool.Get(p), nil
		}
	})

	sctx := newJoiningSwitchContext(StateBooting, nil)

	avp, _ := t.VoteproofsPair(point.Next(), point.Next().Next(), manifest.Hash(), nil, nil, nodes)
	avp.SetResult(base.VoteResultDraw)
	st.setLastVoteproof(avp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	t.T().Log("wait next round init ballot")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.Next().NextRound(), bl.Point().Point)

		rbl, ok := bl.(base.INITBallot)
		t.True(ok)

		t.True(manifest.Hash().Equal(rbl.BallotSignedFact().BallotFact().PreviousBlock()))
	}
}

func TestJoiningHandler(t *testing.T) {
	suite.Run(t, new(testJoiningHandler))
}
