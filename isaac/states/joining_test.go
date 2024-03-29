package isaacstates

import (
	"context"
	"sync/atomic"
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

func (t *testJoiningHandler) newargs(suf base.Suffrage) *JoiningHandlerArgs {
	local := t.Local
	params := t.LocalParams

	args := NewJoiningHandlerArgs()

	args.IntervalBroadcastBallot = params.IntervalBroadcastBallot
	args.LastManifestFunc = func() (base.Manifest, bool, error) {
		return nil, false, errors.Errorf("empty manifest")
	}
	args.NodeInConsensusNodesFunc = func(base.Node, base.Height) (base.Suffrage, bool, error) {
		if suf == nil {
			return nil, false, nil
		}

		return suf, suf.ExistsPublickey(local.Address(), local.Publickey()), nil
	}
	args.VoteFunc = func(base.Ballot) (bool, error) { return true, nil }
	args.JoinMemberlistFunc = func(context.Context, base.Suffrage) error { return nil }
	args.LeaveMemberlistFunc = func() error { return nil }
	args.SuffrageVotingFindFunc = func(context.Context, base.Height, base.Suffrage) ([]base.SuffrageExpelOperation, error) {
		return nil, nil
	}
	args.WaitFirstVoteproof = func() time.Duration {
		return params.IntervalBroadcastBallot()*2 + params.WaitPreparingINITBallot()
	}

	return args
}

func (t *testJoiningHandler) newState(args *JoiningHandlerArgs) (*JoiningHandler, func()) {
	local := t.Local
	params := t.LocalParams

	newhandler := NewNewJoiningHandlerType(params.NetworkID(), local, args)
	_ = newhandler.SetLogging(logging.TestNilLogging)

	timers, err := util.NewSimpleTimers(2, time.Millisecond*33)
	t.NoError(err)

	i, err := newhandler.new()
	t.NoError(err)

	st := i.(*JoiningHandler)

	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		return nil
	})
	st.bbt = newBallotBroadcastTimers(timers, func(_ context.Context, bl base.Ballot) error {
		return st.ballotBroadcaster.Broadcast(bl)
	}, args.IntervalBroadcastBallot())
	t.NoError(st.bbt.Start(context.Background()))

	st.switchStateFunc = func(switchContext) error {
		return nil
	}

	return st, func() {
		deferred, err := st.exit(nil)
		t.NoError(err)
		deferred()

		_ = st.bbt.Stop()
	}
}

func (t *testJoiningHandler) TestNew() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	args := t.newargs(suf)
	args.LastManifestFunc = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	st, closef := t.newState(args)
	defer closef()

	_, ok := (interface{})(st).(handler)
	t.True(ok)

	sctx := newJoiningSwitchContext(StateBooting, nil)

	deferred, err := st.enter(StateBooting, sctx)
	t.NoError(err)
	deferred()

	_, ivp := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), nil, nil, nodes)
	err = st.newVoteproof(ivp)

	var ssctx consensusSwitchContext
	t.ErrorAs(err, &ssctx)
	base.EqualVoteproof(t.Assert(), ivp, ssctx.vp)
}

func (t *testJoiningHandler) TestLocalNotInSuffrage() {
	suf, _ := isaac.NewTestSuffrage(2) // NOTE local is not in suffrage

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	args := t.newargs(suf)
	args.LastManifestFunc = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	st, closef := t.newState(args)
	defer closef()

	_, ok := (interface{})(st).(handler)
	t.True(ok)

	sctx := newJoiningSwitchContext(StateBooting, nil)

	_, err := st.enter(StateBooting, sctx)
	t.Error(err)

	var ssctx SyncingSwitchContext
	t.ErrorAs(err, &ssctx)
	t.Equal(manifest.Height(), ssctx.height)
}

func (t *testJoiningHandler) TestFailedLastManifest() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	t.Run("with error", func() {
		args := t.newargs(nil)
		st, closef := t.newState(args)
		defer closef()

		sctx := newJoiningSwitchContext(StateBooting, nil)

		_, err := st.enter(StateBooting, sctx)
		t.Error(err)
		t.ErrorContains(err, "enter joining state")
		t.ErrorContains(err, "empty manifest")
	})

	t.Run("new voteproof with error", func() {
		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

		args := t.newargs(suf)

		var called int64
		args.LastManifestFunc = func() (base.Manifest, bool, error) {
			if atomic.LoadInt64(&called) > 0 {
				return nil, false, errors.Errorf("empty manifest")
			}

			atomic.AddInt64(&called, 1)

			return manifest, true, nil
		}

		st, closef := t.newState(args)
		defer closef()

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(StateBooting, sctx)
		t.NoError(err)
		deferred()

		_, ivp := t.VoteproofsPair(point, point.NextHeight(), nil, nil, nil, nodes)
		err = st.newVoteproof(ivp)

		var ssctx baseErrorSwitchContext
		t.ErrorAs(err, &ssctx)
		t.Equal(ssctx.next(), StateBroken)
		t.ErrorContains(err, "get last manifest")
	})

	t.Run("not found", func() {
		args := t.newargs(nil)
		args.LastManifestFunc = func() (base.Manifest, bool, error) {
			return nil, false, nil
		}

		st, closef := t.newState(args)
		defer closef()

		sctx := newJoiningSwitchContext(StateBooting, nil)

		_, err := st.enter(StateBooting, sctx)
		t.Error(err)

		t.ErrorContains(err, "last manifest not found")
	})
}

func (t *testJoiningHandler) TestINITVoteproof() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	t.Run("lower height", func() {
		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

		args := t.newargs(suf)
		args.LastManifestFunc = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		st, closef := t.newState(args)
		defer closef()

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(StateBooting, sctx)
		t.NoError(err)
		deferred()

		_, ivp := t.VoteproofsPair(point.PrevHeight().PrevHeight(), point.PrevHeight(), nil, nil, nil, nodes)
		t.NoError(st.newVoteproof(ivp))
	})

	t.Run("higher height", func() {
		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

		args := t.newargs(suf)
		args.LastManifestFunc = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		st, closef := t.newState(args)
		defer closef()

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(StateBooting, sctx)
		t.NoError(err)
		deferred()

		_, ivp := t.VoteproofsPair(point.NextHeight(), point.NextHeight().NextHeight(), nil, nil, nil, nodes)
		err = st.newVoteproof(ivp)

		var ssctx SyncingSwitchContext
		t.ErrorAs(err, &ssctx)
		t.Equal(ivp.Point().Height()-1, ssctx.height)
	})

	t.Run("previous block does not match", func() {
		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

		args := t.newargs(suf)
		args.LastManifestFunc = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		st, closef := t.newState(args)
		defer closef()

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(StateBooting, sctx)
		t.NoError(err)
		deferred()

		_, ivp := t.VoteproofsPair(point, point.NextHeight(), nil, nil, nil, nodes)
		err = st.newVoteproof(ivp)

		var ssctx SyncingSwitchContext
		t.ErrorAs(err, &ssctx)
		t.Equal(ivp.Point().Height()-1, ssctx.height)
	})
}

func (t *testJoiningHandler) TestFirstVoteproof() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	args := t.newargs(suf)
	args.LastManifestFunc = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	prpool := t.PRPool
	args.ProposalSelectFunc = func(_ context.Context, p base.Point, _ util.Hash, _ time.Duration) (base.ProposalSignFact, error) {
		return prpool.Get(p), nil
	}
	args.WaitFirstVoteproof = func() time.Duration { return 1 }
	args.WaitPreparingINITBallot = func() time.Duration {
		return time.Second * 2
	}

	st, closef := t.newState(args)
	defer closef()

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		if bl.Point().Point.Equal(point.NextHeight()) {
			ballotch <- bl
		}

		return nil
	})

	avp, _ := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), nil, nil, nodes)
	sctx := newJoiningSwitchContext(StateBooting, avp)

	deferred, err := st.enter(StateBooting, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 5):
		t.Fail("timeout to wait init ballot for next block")

		return
	case bl := <-ballotch:
		rbl, ok := bl.(base.INITBallot)
		t.True(ok)

		t.True(manifest.Hash().Equal(rbl.BallotSignFact().BallotFact().PreviousBlock()))
	}
}

func (t *testJoiningHandler) TestInvalidACCEPTVoteproof() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	t.Run("lower height", func() {
		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

		args := t.newargs(suf)
		args.LastManifestFunc = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		st, closef := t.newState(args)
		defer closef()

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(StateBooting, sctx)
		t.NoError(err)
		deferred()

		avp, _ := t.VoteproofsPair(point, point.NextHeight(), nil, nil, nil, nodes)
		t.NoError(st.newVoteproof(avp))
	})

	t.Run("higher height", func() {
		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

		args := t.newargs(suf)
		args.LastManifestFunc = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		st, closef := t.newState(args)
		defer closef()

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(StateBooting, sctx)
		t.NoError(err)
		deferred()

		avp, _ := t.VoteproofsPair(point.NextHeight().NextHeight(), point.NextHeight().NextHeight().NextHeight(), nil, nil, nil, nodes)
		err = st.newVoteproof(avp)

		var ssctx SyncingSwitchContext
		t.ErrorAs(err, &ssctx)
		t.Equal(avp.Point().Height(), ssctx.height)
	})

	t.Run("higher height, but draw", func() {
		point := base.RawPoint(33, 0)
		manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

		args := t.newargs(suf)
		args.LastManifestFunc = func() (base.Manifest, bool, error) {
			return manifest, true, nil
		}

		st, closef := t.newState(args)
		defer closef()

		sctx := newJoiningSwitchContext(StateBooting, nil)

		deferred, err := st.enter(StateBooting, sctx)
		t.NoError(err)
		deferred()

		avp, _ := t.VoteproofsPair(point.NextHeight().NextHeight(), point.NextHeight().NextHeight().NextHeight(), nil, nil, nil, nodes)
		avp.SetResult(base.VoteResultDraw)
		err = st.newVoteproof(avp)

		var ssctx SyncingSwitchContext
		t.ErrorAs(err, &ssctx)
		t.Equal(avp.Point().Height()-1, ssctx.height)
	})
}

func (t *testJoiningHandler) TestINITVoteproofNextRound() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	args := t.newargs(suf)
	args.LastManifestFunc = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	st, closef := t.newState(args)
	defer closef()

	sctx := newJoiningSwitchContext(StateBooting, nil)

	deferred, err := st.enter(StateBooting, sctx)
	t.NoError(err)
	deferred()

	_, ivp := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), nil, nil, nodes)
	ivp.SetResult(base.VoteResultDraw)

	err = st.newVoteproof(ivp)
	t.Error(err)

	var ssctx consensusSwitchContext
	t.ErrorAs(err, &ssctx)
	base.EqualVoteproof(t.Assert(), ivp, ssctx.vp)
}

func (t *testJoiningHandler) TestACCEPTVoteproofNextRound() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	args := t.newargs(suf)
	args.LastManifestFunc = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	st, closef := t.newState(args)
	defer closef()

	sctx := newJoiningSwitchContext(StateBooting, nil)

	deferred, err := st.enter(StateBooting, sctx)
	t.NoError(err)
	deferred()

	avp, _ := t.VoteproofsPair(point.NextHeight(), point.NextHeight().NextHeight(), manifest.Hash(), nil, nil, nodes)
	avp.SetResult(base.VoteResultDraw)

	err = st.newVoteproof(avp)

	var ssctx consensusSwitchContext
	t.ErrorAs(err, &ssctx)
	base.EqualVoteproof(t.Assert(), avp, ssctx.vp)
}

func (t *testJoiningHandler) TestLastINITVoteproofNextRound() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	args := t.newargs(suf)
	args.LastManifestFunc = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}
	args.WaitFirstVoteproof = func() time.Duration { return 1 }

	st, closef := t.newState(args)
	defer closef()

	switchch := make(chan switchContext, 1)
	st.switchStateFunc = func(sctx switchContext) error {
		switchch <- sctx

		return nil
	}

	sctx := newJoiningSwitchContext(StateBooting, nil)

	_, ivp := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), nil, nil, nodes)
	ivp.SetResult(base.VoteResultDraw)
	t.True(st.setLastVoteproof(ivp))

	deferred, err := st.enter(StateBooting, sctx)
	t.NoError(err)
	deferred()

	t.T().Log("wait to switch state")
	select {
	case <-time.After(time.Second * 2):
		t.Fail("timeout to wait next round init ballot")

		return
	case sctx := <-switchch:
		var ssctx consensusSwitchContext
		t.ErrorAs(sctx, &ssctx)
		base.EqualVoteproof(t.Assert(), ivp, ssctx.vp)
	}
}

func (t *testJoiningHandler) TestLastACCEPTVoteproofNextRound() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	args := t.newargs(suf)
	args.LastManifestFunc = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}
	args.WaitFirstVoteproof = func() time.Duration { return 1 }

	st, closef := t.newState(args)
	defer closef()

	switchch := make(chan switchContext, 1)
	st.switchStateFunc = func(sctx switchContext) error {
		switchch <- sctx

		return nil
	}

	sctx := newJoiningSwitchContext(StateBooting, nil)

	avp, _ := t.VoteproofsPair(point.NextHeight(), point.NextHeight().NextHeight(), manifest.Hash(), nil, nil, nodes)
	avp.SetResult(base.VoteResultDraw)
	st.setLastVoteproof(avp)

	deferred, err := st.enter(StateBooting, sctx)
	t.NoError(err)
	deferred()

	t.T().Log("wait to switch state")
	select {
	case <-time.After(time.Second * 2):
		t.Fail("timeout to wait next round init ballot")

		return
	case sctx := <-switchch:
		var ssctx consensusSwitchContext
		t.ErrorAs(sctx, &ssctx)
		base.EqualVoteproof(t.Assert(), avp, ssctx.vp)
	}
}

func (t *testJoiningHandler) TestEnterButNotInConsensusNodes() {
	suf, _ := isaac.NewTestSuffrage(2, t.Local)

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	args := t.newargs(suf)
	args.LastManifestFunc = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}
	args.NodeInConsensusNodesFunc = func(node base.Node, height base.Height) (base.Suffrage, bool, error) {
		return nil, false, nil
	}

	st, closef := t.newState(args)
	defer closef()

	sctx := newJoiningSwitchContext(StateBooting, nil)

	_, err := st.enter(StateBooting, sctx)
	t.Error(err)

	var ssctx SyncingSwitchContext
	t.ErrorAs(err, &ssctx)
	t.Equal(point.Height(), ssctx.height)
}

func (t *testJoiningHandler) TestStuckINITVoteproof() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	args := t.newargs(suf)
	args.LastManifestFunc = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	st, closef := t.newState(args)
	defer closef()
	_, ok := (interface{})(st).(handler)
	t.True(ok)

	sctx := newJoiningSwitchContext(StateBooting, nil)

	deferred, err := st.enter(StateBooting, sctx)
	t.NoError(err)
	deferred()

	t.T().Log("new stuck init voteproof")

	expelnode := nodes[2]

	_, origivp := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), nil, nil, nodes)

	expels := t.Expels(point.Height(), []base.Address{expelnode.Address()}, nodes[:2])
	expelfacts := make([]util.Hash, len(expels))
	for i := range expels {
		expelfacts[i] = expels[i].Fact().Hash()
	}

	sfs := util.FilterSlice(origivp.SignFacts(), func(i base.BallotSignFact) bool {
		return !i.Node().Equal(expelnode.Address())
	})

	stuckivp := isaac.NewINITStuckVoteproof(origivp.Point().Point)
	stuckivp.
		SetMajority(origivp.Majority()).
		SetSignFacts(sfs)
	stuckivp.SetExpels(expels)
	stuckivp.Finish()

	err = st.newVoteproof(stuckivp)

	var ssctx consensusSwitchContext
	t.ErrorAs(err, &ssctx)
	base.EqualVoteproof(t.Assert(), stuckivp, ssctx.vp)
}

func (t *testJoiningHandler) TestStuckACCEPTVoteproof() {
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	point := base.RawPoint(33, 0)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	args := t.newargs(suf)
	args.LastManifestFunc = func() (base.Manifest, bool, error) {
		return manifest, true, nil
	}

	st, closef := t.newState(args)
	defer closef()

	switchch := make(chan switchContext, 1)
	st.switchStateFunc = func(sctx switchContext) error {
		switchch <- sctx

		return nil
	}

	sctx := newJoiningSwitchContext(StateBooting, nil)

	deferred, err := st.enter(StateBooting, sctx)
	t.NoError(err)
	deferred()

	expelnode := nodes[2]

	origavp, _ := t.VoteproofsPair(point.NextHeight(), point.NextHeight().NextHeight(), valuehash.RandomSHA256(), nil, nil, nodes)

	expels := t.Expels(point.Height(), []base.Address{expelnode.Address()}, nodes[:2])
	expelfacts := make([]util.Hash, len(expels))
	for i := range expels {
		expelfacts[i] = expels[i].Fact().Hash()
	}

	sfs := util.FilterSlice(origavp.SignFacts(), func(i base.BallotSignFact) bool {
		return !i.Node().Equal(expelnode.Address())
	})

	stuckivp := isaac.NewACCEPTStuckVoteproof(origavp.Point().Point)
	stuckivp.
		SetMajority(origavp.Majority()).
		SetSignFacts(sfs)
	stuckivp.SetExpels(expels)
	stuckivp.Finish()

	t.T().Log("new stuck accept voteproof")

	err = st.newVoteproof(stuckivp)
	t.Error(err)

	var ssctx consensusSwitchContext
	t.ErrorAs(err, &ssctx)
	base.EqualVoteproof(t.Assert(), stuckivp, ssctx.vp)
}

func TestJoiningHandler(t *testing.T) {
	suite.Run(t, new(testJoiningHandler))
}
