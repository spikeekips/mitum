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

type baseTestConsensusHandler struct {
	isaac.BaseTestBallots
}

func (t *baseTestConsensusHandler) newargs(previous base.Manifest, suf base.Suffrage) *ConsensusHandlerArgs {
	local := t.Local

	args := NewConsensusHandlerArgs()
	args.ProposalProcessors = isaac.NewProposalProcessors(nil, nil)
	args.GetManifestFunc = func(base.Height) (base.Manifest, error) { return previous, nil }
	args.NodeInConsensusNodesFunc = func(base.Node, base.Height) (base.Suffrage, bool, error) {
		if suf == nil {
			return nil, false, nil
		}

		return suf, suf.ExistsPublickey(local.Address(), local.Publickey()), nil
	}
	args.VoteFunc = func(base.Ballot) (bool, error) { return true, nil }
	args.SuffrageVotingFindFunc = func(context.Context, base.Height, base.Suffrage) ([]base.SuffrageWithdrawOperation, error) {
		return nil, nil
	}

	return args
}

func (t *baseTestConsensusHandler) newState(args *ConsensusHandlerArgs) (*ConsensusHandler, func()) {
	newhandler := NewNewConsensusHandlerType(t.Local, t.LocalParams, args)
	_ = newhandler.SetLogging(logging.TestNilLogging)
	_ = newhandler.setTimers(util.NewTimers([]util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastSuffrageConfirmBallot,
		timerIDBroadcastACCEPTBallot,
	}, false))

	i, err := newhandler.new()
	t.NoError(err)

	st := i.(*ConsensusHandler)

	return st, func() {
		st.timers.Stop()

		deferred, err := st.exit(nil)
		t.NoError(err)
		deferred()
	}
}

func (t *baseTestConsensusHandler) newStateWithINITVoteproof(point base.Point, suf base.Suffrage) (
	*ConsensusHandler,
	func(),
	*isaac.DummyProposalProcessor,
	base.INITVoteproof,
) {
	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())

	prpool := t.PRPool
	fact := prpool.GetFact(point)

	args := t.newargs(previous, suf)

	pp := isaac.NewDummyProposalProcessor()
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return nil, errors.Errorf("process error")
	}

	args.ProposalProcessors.SetMakeNew(pp.Make)
	args.ProposalProcessors.SetGetProposal(func(_ context.Context, facthash util.Hash) (base.ProposalSignFact, error) {
		return prpool.ByHash(facthash)
	})

	args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ time.Duration) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			pr := prpool.ByPoint(p)
			if pr != nil {
				return pr, nil
			}
			return nil, util.ErrNotFound.Call()
		}
	}

	st, closef := t.newState(args)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(base.Ballot) error {
		return nil
	})
	st.switchStateFunc = func(switchContext) error {
		return nil
	}

	nodes := make([]isaac.LocalNode, suf.Len())
	sn := suf.Nodes()
	for i := range sn {
		nodes[i] = sn[i].(isaac.LocalNode)
	}

	avp, ivp := t.VoteproofsPair(point.PrevHeight(), point, nil, nil, fact.Hash(), nodes)
	t.True(st.setLastVoteproof(avp))
	t.True(st.setLastVoteproof(ivp))

	return st, closef, pp, ivp
}

type testConsensusHandler struct {
	baseTestConsensusHandler
}

func (t *testConsensusHandler) TestFailedToFetchProposal() {
	point := base.RawPoint(33, 0)

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	args := t.newargs(previous, suf)
	args.ProposalProcessors = isaac.NewProposalProcessors(nil, func(context.Context, util.Hash) (base.ProposalSignFact, error) {
		return nil, util.ErrNotFound.Call()
	})
	args.ProposalProcessors.SetRetryLimit(1).SetRetryInterval(1)

	prpool := t.PRPool
	args.ProposalSelectFunc = func(_ context.Context, p base.Point, _ time.Duration) (base.ProposalSignFact, error) {
		return prpool.Get(p), nil
	}

	newhandler := NewNewConsensusHandlerType(t.Local, t.LocalParams, args)

	i, err := newhandler.new()
	t.NoError(err)

	st := i.(*ConsensusHandler)

	_, ok := (interface{})(st).(handler)
	t.True(ok)

	defer func() {
		deferred, err := st.exit(nil)
		t.NoError(err)
		deferred()
	}()

	st.switchStateFunc = func(switchContext) error { return nil }

	_ = st.setTimers(util.NewTimers([]util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastSuffrageConfirmBallot,
		timerIDBroadcastACCEPTBallot,
	}, false))

	defer st.timers.Stop()

	avp, ivp := t.VoteproofsPair(point.PrevHeight(), point, nil, nil, nil, nodes)
	t.True(st.setLastVoteproof(avp))
	t.True(st.setLastVoteproof(ivp))

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		ballotch <- bl

		return nil
	})

	st.params = t.LocalParams.SetWaitPreparingINITBallot(time.Nanosecond)

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	t.Run("intended wrong accept ballot", func() {
		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("timeout to wait next round init ballot"))

			return
		case bl := <-ballotch:
			t.NoError(bl.IsValid(t.LocalParams.NetworkID()))
			abl, ok := bl.(base.ACCEPTBallot)
			t.True(ok)

			t.Equal(ivp.Point().Point, abl.Point().Point)
		}
	})
}

func (t *testConsensusHandler) TestInvalidVoteproofs() {
	point := base.RawPoint(22, 0)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	t.Run("empty init voteproof", func() {
		st, closef, _, _ := t.newStateWithINITVoteproof(point, suf)
		defer closef()

		sctx, _ := newConsensusSwitchContext(StateJoining, nil)

		deferred, err := st.enter(StateJoining, sctx)
		t.Nil(deferred)
		t.Error(err)
		t.ErrorContains(err, "empty init voteproof")
	})

	t.Run("draw result of init voteproof", func() {
		st, closef, _, _ := t.newStateWithINITVoteproof(point, suf)
		defer closef()

		point := base.RawPoint(33, 0)
		avp, ivp := t.VoteproofsPair(point.PrevHeight(), point, nil, nil, nil, nodes)
		ivp.SetResult(base.VoteResultDraw).Finish()
		st.setLastVoteproof(avp)
		st.setLastVoteproof(ivp)

		sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

		deferred, err := st.enter(StateJoining, sctx)
		t.NotNil(deferred)
		t.NoError(err)
	})

	t.Run("empty majority of init voteproof", func() {
		st, closef, _, _ := t.newStateWithINITVoteproof(point, suf)
		defer closef()

		point := base.RawPoint(33, 0)
		avp, ivp := t.VoteproofsPair(point.PrevHeight(), point, nil, nil, nil, nodes)
		ivp.SetMajority(nil).Finish()
		st.setLastVoteproof(avp)
		st.setLastVoteproof(ivp)

		sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

		deferred, err := st.enter(StateJoining, sctx)
		t.NotNil(deferred)
		t.NoError(err)
	})
}

func (t *testConsensusHandler) TestExit() {
	point := base.RawPoint(33, 44)
	suf, _ := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		ballotch <- bl

		return nil
	})

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferredenter, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferredenter()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait accept ballot"))

		return
	case bl := <-ballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))

		abl, ok := bl.(base.ACCEPTBallot)
		t.True(ok)

		t.Equal(ivp.Point().Point, abl.Point().Point)
		t.True(ivp.BallotMajority().Proposal().Equal(abl.BallotSignFact().BallotFact().Proposal()))
	}

	t.NotNil(st.args.ProposalProcessors.Processor())

	deferredexit, err := st.exit(nil)
	t.NoError(err)
	t.NotNil(deferredexit)

	t.Nil(st.args.ProposalProcessors.Processor())
}

func (t *testConsensusHandler) TestProcessingProposalAfterEntered() {
	point := base.RawPoint(33, 44)
	suf, _ := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		ballotch <- bl

		return nil
	})

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait accept ballot"))

		return
	case bl := <-ballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))

		abl, ok := bl.(base.ACCEPTBallot)
		t.True(ok)

		t.Equal(ivp.Point().Point, abl.Point().Point)
		t.True(ivp.BallotMajority().Proposal().Equal(abl.BallotSignFact().BallotFact().Proposal()))
	}
}

func (t *testConsensusHandler) TestEnterWithDrawINITVoteproof() {
	point := base.RawPoint(33, 44)
	suf, _ := isaac.NewTestSuffrage(2, t.Local)

	t.LocalParams = t.LocalParams.SetWaitPreparingINITBallot(time.Nanosecond)

	st, closefunc, _, origivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	prpool := t.PRPool
	st.args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ time.Duration) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return prpool.Get(p), nil
		}
	}

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		ballotch <- bl

		return nil
	})

	ivp := origivp.(isaac.INITVoteproof)
	_ = ivp.SetMajority(nil).Finish()

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait accept ballot"))

		return
	case bl := <-ballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))

		abl, ok := bl.(base.INITBallot)
		t.True(ok)

		t.Equal(ivp.Point().Point.NextRound(), abl.Point().Point)
	}
}

func (t *testConsensusHandler) TestEnterWithDrawACCEPTVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	t.LocalParams.SetWaitPreparingINITBallot(time.Nanosecond)

	st, closefunc, _, _ := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	prpool := t.PRPool
	st.args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ time.Duration) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return prpool.Get(p), nil
		}
	}

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		ballotch <- bl

		return nil
	})

	avp, _ := t.VoteproofsPair(point, point.NextHeight(), nil, nil, nil, nodes)
	_ = avp.SetMajority(nil).Finish()
	t.True(st.setLastVoteproof(avp))

	sctx, _ := newConsensusSwitchContext(StateJoining, avp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait accept ballot"))

		return
	case bl := <-ballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))

		abl, ok := bl.(base.INITBallot)
		t.True(ok)

		t.Equal(avp.Point().Point.NextRound(), abl.Point().Point)
	}
}

func (t *testConsensusHandler) TestFailedProcessingProposalProcessingFailed() {
	point := base.RawPoint(33, 44)
	suf, _ := isaac.NewTestSuffrage(1, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return nil, errors.Errorf("hahaha")
	}

	var i int
	st.args.ProposalProcessors.SetGetProposal(func(_ context.Context, facthash util.Hash) (base.ProposalSignFact, error) {
		if i < 1 {
			i++
			return nil, errors.Errorf("findme")
		}

		return t.PRPool.ByHash(facthash)
	})

	sctxch := make(chan switchContext, 1)
	st.switchStateFunc = func(sctx switchContext) error {
		sctxch <- sctx
		return nil
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait switch context"))

		return
	case sctx := <-sctxch:
		t.True(sctx.ok(StateConsensus))
		t.Equal(StateBroken, sctx.next())
		t.ErrorContains(sctx, "hahaha")
	}
}

func (t *testConsensusHandler) TestProcessingProposalWithACCEPTVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	avp, _ := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), t.PRPool.Hash(point), nil, nodes)

	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		st.setLastVoteproof(avp)

		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save block"))

		return
	case ravp := <-savedch:
		base.EqualVoteproof(t.Assert(), avp, ravp)
	}
}

func (t *testConsensusHandler) TestProcessingProposalWithDrawACCEPTVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	avp, _ := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), nil, nil, nodes)
	avp.SetResult(base.VoteResultDraw).Finish()

	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		st.setLastVoteproof(avp)

		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
	case <-savedch:
		t.NoError(errors.Errorf("to save block should be ignored"))
	}

	t.Nil(st.args.ProposalProcessors.Processor())
}

func (t *testConsensusHandler) TestProcessingProposalWithWrongNewBlockACCEPTVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	avp, _ := t.VoteproofsPair(point, point.NextHeight(), nil, nil, nil, nodes) // random new block hash

	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		st.setLastVoteproof(avp)

		return manifest, nil
	}

	sctxch := make(chan switchContext, 1)
	st.switchStateFunc = func(sctx switchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)
	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait to switch syncing state"))
	case err := <-sctxch:
		var ssctx SyncingSwitchContext
		t.True(errors.As(err, &ssctx))

		t.Equal(avp.Point().Height(), ssctx.height)

		t.Nil(st.args.ProposalProcessors.Processor())
	}
}

func (t *testConsensusHandler) TestWithBallotbox() {
	point := base.RawPoint(33, 44)
	suf, _ := isaac.NewTestSuffrage(0, t.Local)

	t.LocalParams = t.LocalParams.SetWaitPreparingINITBallot(time.Nanosecond)

	th := base.Threshold(100)
	box := NewBallotbox(
		t.Local.Address(),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)

	testctx, testdone := context.WithCancel(context.Background())

	st, closef, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer func() {
		testdone()
		st.timers.Stop()

		closef()
	}()

	manifests := util.NewSingleLockedMap(base.NilHeight, (base.Manifest)(nil))
	getmanifest := func(height base.Height) base.Manifest {
		i, _, _ := manifests.GetOrCreate(height, func() (base.Manifest, error) {
			manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())

			return manifest, nil
		})

		return i
	}

	processdelay := time.Millisecond * 100
	pp.Processerr = func(_ context.Context, fact base.ProposalFact, _ base.INITVoteproof) (base.Manifest, error) {
		<-time.After(processdelay)

		return getmanifest(fact.Point().Height()), nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp

		return nil
	}

	st.args.VoteFunc = func(bl base.Ballot) (bool, error) {
		voted, err := box.Vote(bl, th)
		if err != nil {
			return false, errors.WithStack(err)
		}

		return voted, nil
	}

	prpool := t.PRPool
	st.args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ time.Duration) (base.ProposalSignFact, error) {
		var pr base.ProposalSignFact

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-testctx.Done():
			return nil, testctx.Err()
		default:
			pr = prpool.Get(p)

		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-testctx.Done():
			return nil, testctx.Err()
		default:
			return pr, nil
		}
	}

	go func() {
	end:
		for {
			select {
			case <-testctx.Done():
				break end
			case vp := <-box.Voteproof():
				_ = st.newVoteproof(vp)
			}
		}
	}()

	target := point
	for range make([]struct{}, 33) {
		target = target.NextHeight()
	}

	wait := processdelay * time.Duration((target.Height()-point.Height()).Int64()*10)
	after := time.After(wait)
	t.T().Logf("> trying to create blocks up to %q; will wait %q", target, wait)

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

end:
	for {
		select {
		case <-after:
			t.NoError(errors.Errorf("failed to wait new blocks"))
		case avp := <-savedch:
			t.T().Logf("new block saved: %q", avp.Point())

			if avp.Point().Point.Equal(target) {
				t.T().Logf("< all new blocks saved, %q", target)

				break end
			}
		}
	}
}

func (t *testConsensusHandler) TestEmptySuffrageNextBlock() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	st.args.NodeInConsensusNodesFunc = func(_ base.Node, height base.Height) (base.Suffrage, bool, error) {
		switch {
		case height <= point.Height():
			return suf, true, nil
		default:
			return nil, false, nil
		}
	}

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}
	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	prpool := t.PRPool
	st.args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ time.Duration) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return prpool.Get(p), nil
		}
	}

	sctxch := make(chan switchContext, 1)
	st.switchStateFunc = func(sctx switchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	avp, _ := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), t.PRPool.Hash(point), t.PRPool.Hash(point.NextHeight()), nodes)
	t.NoError(st.newVoteproof(avp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	t.T().Log("wait to switch broken state")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait"))

		return
	case sctx := <-sctxch:
		var ssctx baseErrorSwitchContext
		t.True(errors.As(sctx, &ssctx))
		t.Equal(ssctx.next(), StateBroken)
		t.ErrorContains(ssctx, "empty suffrage")
	}
}

func (t *testConsensusHandler) TestOutOfSuffrage() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)
	newsuf, _ := isaac.NewTestSuffrage(2)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	st.args.NodeInConsensusNodesFunc = func(_ base.Node, height base.Height) (base.Suffrage, bool, error) {
		if height == point.Height() {
			return suf, true, nil
		}

		return newsuf, newsuf.ExistsPublickey(t.Local.Address(), t.Local.Publickey()), nil
	}

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}
	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	st.args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ time.Duration) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.PRPool.Get(p), nil
		}
	}

	sctxch := make(chan switchContext, 1)
	st.switchStateFunc = func(sctx switchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	avp, _ := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), t.PRPool.Hash(point), t.PRPool.Hash(point.NextHeight()), nodes)
	t.NoError(st.newVoteproof(avp), "%+v", err)

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	t.T().Log("wait to switch syncing state")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait to switch state syncing"))

		return
	case sctx := <-sctxch:
		var ssctx SyncingSwitchContext
		t.True(errors.As(sctx, &ssctx))
		t.Equal(point.Height(), ssctx.height)
	}
}

func (t *testConsensusHandler) TestEnterButEmptySuffrage() {
	point := base.RawPoint(33, 44)
	suf, _ := isaac.NewTestSuffrage(2)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()
	st.args.NodeInConsensusNodesFunc = func(base.Node, base.Height) (base.Suffrage, bool, error) {
		return nil, false, nil
	}

	sctxch := make(chan switchContext, 1)
	st.switchStateFunc = func(sctx switchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	_, err := st.enter(StateJoining, sctx)
	t.Error(err)
	t.ErrorContains(err, "empty suffrage of init voteproof")
}

func (t *testConsensusHandler) TestEnterButNotInSuffrage() {
	point := base.RawPoint(33, 44)
	suf, _ := isaac.NewTestSuffrage(2)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	sctxch := make(chan switchContext, 1)
	st.switchStateFunc = func(sctx switchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	_, err := st.enter(StateJoining, sctx)

	var ssctx SyncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(point.Height(), ssctx.height)
}

func TestConsensusHandler(t *testing.T) {
	suite.Run(t, new(testConsensusHandler))
}
