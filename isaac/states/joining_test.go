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
	params := t.LocalParams

	newhandler := NewNewJoiningHandlerType(
		local,
		params,
		nil,
		func() (base.Manifest, bool, error) {
			return nil, false, errors.Errorf("empty manifest")
		},
		func(base.Node, base.Height) (base.Suffrage, bool, error) {
			return suf, suf.ExistsPublickey(local.Address(), local.Publickey()), nil
		},
		func(base.Ballot) (bool, error) { return true, nil },
		func(context.Context, base.Suffrage) error { return nil },
		func(time.Duration) error { return nil },
	)
	_ = newhandler.SetLogging(logging.TestNilLogging)
	_ = newhandler.setTimers(util.NewTimers([]util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastACCEPTBallot,
	}, false))

	i, err := newhandler.new()
	t.NoError(err)

	st := i.(*JoiningHandler)

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

	st.lastManifest = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	sctx := newJoiningSwitchContext(StateBooting, nil)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	_, ivp := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), nil, nil, nodes)
	err = st.newVoteproof(ivp)

	var ssctx consensusSwitchContext
	t.True(errors.As(err, &ssctx))
	base.EqualVoteproof(t.Assert(), ivp, ssctx.ivp)
}

func (t *testJoiningHandler) TestLocalNotInSuffrage() {
	suf, _ := isaac.NewTestSuffrage(2) // NOTE local is not in suffrage

	st, closef := t.newState(suf)
	defer closef()

	_, ok := (interface{})(st).(handler)
	t.True(ok)

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	st.lastManifest = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	sctx := newJoiningSwitchContext(StateBooting, nil)

	_, err := st.enter(sctx)
	t.Error(err)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(manifest.Height(), ssctx.height)
}

func (t *testJoiningHandler) TestFailedLastManifest() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	t.Run("with error", func() {
		st, closef := t.newState(suf)
		defer closef()

		sctx := newJoiningSwitchContext(StateBooting, nil)

		_, err := st.enter(sctx)
		t.Error(err)
		t.ErrorContains(err, "failed to enter joining state")
		t.ErrorContains(err, "empty manifest")
	})

	t.Run("new voteproof with error", func() {
		st, closef := t.newState(suf)
		defer closef()

		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

		orig := st.lastManifest
		st.lastManifest = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(sctx)
		t.NoError(err)
		deferred()

		st.lastManifest = orig

		_, ivp := t.VoteproofsPair(point, point.NextHeight(), nil, nil, nil, nodes)
		err = st.newVoteproof(ivp)

		var ssctx baseErrorSwitchContext
		t.True(errors.As(err, &ssctx))
		t.Equal(ssctx.next(), StateBroken)
		t.ErrorContains(err, "failed to get last manifest")
	})

	t.Run("not found", func() {
		st, closef := t.newState(suf)
		defer closef()

		st.lastManifest = func() (base.Manifest, bool, error) {
			return nil, false, nil
		}

		sctx := newJoiningSwitchContext(StateBooting, nil)

		_, err := st.enter(sctx)
		t.Error(err)

		t.ErrorContains(err, "last manifest not found")
	})
}

func (t *testJoiningHandler) TestInvalidINITVoteproof() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	t.Run("lower height", func() {
		st, closef := t.newState(suf)
		defer closef()

		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
		st.lastManifest = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(sctx)
		t.NoError(err)
		deferred()

		_, ivp := t.VoteproofsPair(point.PrevHeight().PrevHeight(), point.PrevHeight(), nil, nil, nil, nodes)
		t.NoError(st.newVoteproof(ivp))
	})

	t.Run("higher height", func() {
		st, closef := t.newState(suf)
		defer closef()

		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
		st.lastManifest = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(sctx)
		t.NoError(err)
		deferred()

		_, ivp := t.VoteproofsPair(point.NextHeight(), point.NextHeight().NextHeight(), nil, nil, nil, nodes)
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

		st.lastManifest = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(sctx)
		t.NoError(err)
		deferred()

		_, ivp := t.VoteproofsPair(point, point.NextHeight(), nil, nil, nil, nodes)
		err = st.newVoteproof(ivp)

		var ssctx syncingSwitchContext
		t.True(errors.As(err, &ssctx))
		t.Equal(ivp.Point().Height()-1, ssctx.height)
	})
}

func (t *testJoiningHandler) TestFirstVoteproof() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)
	prpool := t.PRPool

	st, closef := t.newState(suf)
	defer closef()

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	st.lastManifest = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}
	st.waitFirstVoteproof = 1

	st.proposalSelector = isaac.DummyProposalSelector(func(_ context.Context, p base.Point) (base.ProposalSignedFact, error) {
		return prpool.Get(p), nil
	})

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		if bl.Point().Point.Equal(point.NextHeight()) {
			ballotch <- bl
		}

		return nil
	}

	avp, _ := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), nil, nil, nodes)
	sctx := newJoiningSwitchContext(StateBooting, avp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait init ballot for next block"))

		return
	case bl := <-ballotch:
		rbl, ok := bl.(base.INITBallot)
		t.True(ok)

		t.True(manifest.Hash().Equal(rbl.BallotSignedFact().BallotFact().PreviousBlock()))
	}
}

func (t *testJoiningHandler) TestInvalidACCEPTVoteproof() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	t.Run("lower height", func() {
		st, closef := t.newState(suf)
		defer closef()

		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
		st.lastManifest = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(sctx)
		t.NoError(err)
		deferred()

		avp, _ := t.VoteproofsPair(point, point.NextHeight(), nil, nil, nil, nodes)
		t.NoError(st.newVoteproof(avp))
	})

	t.Run("higher height", func() {
		st, closef := t.newState(suf)
		defer closef()

		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
		st.lastManifest = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(sctx)
		t.NoError(err)
		deferred()

		avp, _ := t.VoteproofsPair(point.NextHeight().NextHeight(), point.NextHeight().NextHeight().NextHeight(), nil, nil, nil, nodes)
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
		st.lastManifest = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(sctx)
		t.NoError(err)
		deferred()

		avp, _ := t.VoteproofsPair(point.NextHeight().NextHeight(), point.NextHeight().NextHeight().NextHeight(), nil, nil, nil, nodes)
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

	st.lastManifest = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.voteFunc = func(bl base.Ballot) (bool, error) {
		if bl.Point().Point.Equal(point.NextHeight().NextRound()) {
			ballotch <- bl
		}

		return true, nil
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

	_, ivp := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), nil, nil, nodes)
	ivp.SetResult(base.VoteResultDraw)

	t.NoError(st.newVoteproof(ivp))

	t.T().Log("wait next round init ballot")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.NextHeight().NextRound(), bl.Point().Point)

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

	st.lastManifest = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.voteFunc = func(bl base.Ballot) (bool, error) {
		if bl.Point().Point.Equal(point.NextHeight().NextRound()) {
			ballotch <- bl
		}

		return true, nil
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

	avp, _ := t.VoteproofsPair(point.NextHeight(), point.NextHeight().NextHeight(), manifest.Hash(), nil, nil, nodes)
	avp.SetResult(base.VoteResultDraw)

	t.NoError(st.newVoteproof(avp))

	t.T().Log("wait next round init ballot")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.NextHeight().NextRound(), bl.Point().Point)

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

	st.lastManifest = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.voteFunc = func(bl base.Ballot) (bool, error) {
		if bl.Point().Point.Equal(point.NextHeight().NextRound()) {
			ballotch <- bl
		}

		return true, nil
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

	_, ivp := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), nil, nil, nodes)
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
		t.Equal(point.NextHeight().NextRound(), bl.Point().Point)

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

	st.lastManifest = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.voteFunc = func(bl base.Ballot) (bool, error) {
		if bl.Point().Point.Equal(point.NextHeight().NextRound()) {
			ballotch <- bl
		}

		return true, nil
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

	avp, _ := t.VoteproofsPair(point.NextHeight(), point.NextHeight().NextHeight(), manifest.Hash(), nil, nil, nodes)
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
		t.Equal(point.NextHeight().NextRound(), bl.Point().Point)

		rbl, ok := bl.(base.INITBallot)
		t.True(ok)

		t.True(manifest.Hash().Equal(rbl.BallotSignedFact().BallotFact().PreviousBlock()))
	}
}

func (t *testJoiningHandler) TestINITVoteproofNextRoundButNotInConsensusNodes() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closef := t.newState(suf)
	defer closef()

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	st.lastManifest = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.voteFunc = func(bl base.Ballot) (bool, error) {
		return false, errNotInConsensusNodes.Errorf("hehehe")
	}

	switchch := make(chan switchContext, 1)
	st.switchStateFunc = func(sctx switchContext) error {
		switchch <- sctx

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

	_, ivp := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), nil, nil, nodes)
	ivp.SetResult(base.VoteResultDraw)

	t.NoError(st.newVoteproof(ivp))

	t.T().Log("wait next round init ballot")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case <-ballotch:
		t.NoError(errors.Errorf("unexpected next round init ballot"))
	case sctx := <-switchch:
		var ssctx syncingSwitchContext
		t.True(errors.As(sctx, &ssctx))
		t.Equal(ivp.Point().Height()-1, ssctx.height)
	}
}

func TestJoiningHandler(t *testing.T) {
	suite.Run(t, new(testJoiningHandler))
}
