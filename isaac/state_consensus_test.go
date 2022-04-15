package isaac

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type baseTestConsensusHandler struct {
	BaseTestBallots
}

func (t *baseTestConsensusHandler) newState(previous base.Manifest, suf suffrage) (*ConsensusHandler, func()) {
	local := t.Local
	policy := t.Policy

	st := NewConsensusHandler(
		local,
		policy,
		nil,
		func(base.Height) (base.Manifest, error) { return previous, nil },
		func(base.Height) base.Suffrage { return suf },
		newProposalProcessors(nil, nil),
	)
	_ = st.SetLogging(logging.TestNilLogging)
	_ = st.setTimers(util.NewTimers([]util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastACCEPTBallot,
	}, false))

	return st, func() {
		deferred, err := st.exit(nil)
		t.NoError(err)
		deferred()
	}
}

func (t *baseTestConsensusHandler) newStateWithINITVoteproof(point base.Point, suf suffrage) (
	*ConsensusHandler,
	func(),
	*DummyProposalProcessor,
	base.INITVoteproof,
) {
	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	st, closef := t.newState(previous, suf)

	prpool := t.prpool
	fact := prpool.getfact(point)

	pp := NewDummyProposalProcessor()
	pp.processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return nil, errors.Errorf("process error")
	}

	st.pps.makenew = pp.make
	st.pps.getproposal = func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error) {
		return prpool.byHash(facthash)
	}

	st.broadcastBallotFunc = func(bl base.Ballot) error {
		return nil
	}
	st.switchStateFunc = func(stateSwitchContext) error {
		return nil
	}
	st.proposalSelector = DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			pr := prpool.byPoint(p)
			if pr != nil {
				return pr, nil
			}
			return nil, util.NotFoundError.Call()
		}
	})

	nodes := make([]LocalNode, suf.Len())
	sn := suf.Nodes()
	for i := range sn {
		nodes[i] = sn[i].(LocalNode)
	}

	_, ivp := t.VoteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	t.True(st.setLastVoteproof(ivp))

	return st, closef, pp, ivp
}

type testConsensusHandler struct {
	baseTestConsensusHandler
}

func (t *testConsensusHandler) TestNew() {
	point := base.RawPoint(33, 0)

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	suf, nodes := NewTestSuffrage(2, t.Local)

	st := NewConsensusHandler(
		t.Local,
		t.Policy,
		nil,
		func(base.Height) (base.Manifest, error) { return previous, nil },
		func(base.Height) base.Suffrage { return suf },
		newProposalProcessors(nil, func(context.Context, util.Hash) (base.ProposalSignedFact, error) {
			return nil, util.NotFoundError.Call()
		}),
	)
	_ = st.SetLogging(logging.TestNilLogging)

	_, ok := (interface{})(st).(stateHandler)
	t.True(ok)

	defer func() {
		deferred, err := st.exit(nil)
		t.NoError(err)
		deferred()
	}()

	st.switchStateFunc = func(stateSwitchContext) error { return nil }

	_ = st.setTimers(util.NewTimers([]util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastACCEPTBallot,
	}, false))

	_, ivp := t.VoteproofsPair(point.Decrease(), point, nil, nil, nil, nodes)
	t.True(st.setLastVoteproof(ivp))

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()
}

func (t *testConsensusHandler) TestInvalidVoteproofs() {
	point := base.RawPoint(22, 0)
	suf, nodes := NewTestSuffrage(2, t.Local)

	t.Run("empty init voteproof", func() {
		st, closef, _, _ := t.newStateWithINITVoteproof(point, suf)
		defer closef()

		sctx := newConsensusSwitchContext(StateJoining, nil)

		deferred, err := st.enter(sctx)
		t.Nil(deferred)
		t.Error(err)
		t.Contains(err.Error(), "empty init voteproof")
	})

	t.Run("draw result of init voteproof", func() {
		st, closef, _, _ := t.newStateWithINITVoteproof(point, suf)
		defer closef()

		point := base.RawPoint(33, 0)
		_, ivp := t.VoteproofsPair(point.Decrease(), point, nil, nil, nil, nodes)
		ivp.SetResult(base.VoteResultDraw).finish()

		sctx := newConsensusSwitchContext(StateJoining, ivp)

		deferred, err := st.enter(sctx)
		t.Nil(deferred)
		t.Error(err)
		t.Contains(err.Error(), "wrong vote result")
	})

	t.Run("empty majority of init voteproof", func() {
		st, closef, _, _ := t.newStateWithINITVoteproof(point, suf)
		defer closef()

		point := base.RawPoint(33, 0)
		_, ivp := t.VoteproofsPair(point.Decrease(), point, nil, nil, nil, nodes)
		ivp.SetMajority(nil).finish()

		sctx := newConsensusSwitchContext(StateJoining, ivp)

		deferred, err := st.enter(sctx)
		t.NotNil(deferred)
		t.NoError(err)
	})
}

func (t *testConsensusHandler) TestExit() {
	point := base.RawPoint(33, 44)
	suf, _ := NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		ballotch <- bl

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferredenter, err := st.enter(sctx)
	t.NoError(err)
	deferredenter()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait accept ballot"))

		return
	case bl := <-ballotch:
		t.NoError(bl.IsValid(t.Policy.NetworkID()))

		abl, ok := bl.(base.ACCEPTBallot)
		t.True(ok)

		t.Equal(ivp.Point().Point, abl.Point().Point)
		t.True(ivp.BallotMajority().Proposal().Equal(abl.BallotSignedFact().BallotFact().Proposal()))
	}

	t.NotNil(st.pps.p)

	deferredexit, err := st.exit(nil)
	t.NoError(err)
	t.NotNil(deferredexit)

	t.Nil(st.pps.p)
}

func (t *testConsensusHandler) TestProcessingProposalAfterEntered() {
	point := base.RawPoint(33, 44)
	suf, _ := NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		ballotch <- bl

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait accept ballot"))

		return
	case bl := <-ballotch:
		t.NoError(bl.IsValid(t.Policy.NetworkID()))

		abl, ok := bl.(base.ACCEPTBallot)
		t.True(ok)

		t.Equal(ivp.Point().Point, abl.Point().Point)
		t.True(ivp.BallotMajority().Proposal().Equal(abl.BallotSignedFact().BallotFact().Proposal()))
	}
}

func (t *testConsensusHandler) TestFailedProcessingProposalFetchFactFailed() {
	point := base.RawPoint(33, 44)
	suf, _ := NewTestSuffrage(1, t.Local)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	st.pps.getproposal = func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error) {
		return nil, util.NotFoundError.Errorf("fact not found")
	}
	st.pps.retrylimit = 1
	st.pps.retryinterval = 1

	sctxch := make(chan stateSwitchContext, 1)
	st.switchStateFunc = func(sctx stateSwitchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait switch context"))

		return
	case sctx := <-sctxch:
		t.Equal(StateConsensus, sctx.from())
		t.Equal(StateBroken, sctx.next())
		t.Contains(sctx.Error(), "failed to get proposal fact")
	}
}

func (t *testConsensusHandler) TestFailedProcessingProposalProcessingFailed() {
	point := base.RawPoint(33, 44)
	suf, _ := NewTestSuffrage(1, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	pp.processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return nil, errors.Errorf("hahaha")
	}

	var i int
	st.pps.getproposal = func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error) {
		if i < 1 {
			i++
			return nil, errors.Errorf("findme")
		}

		return t.prpool.byHash(facthash)
	}

	sctxch := make(chan stateSwitchContext, 1)
	st.switchStateFunc = func(sctx stateSwitchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait switch context"))

		return
	case sctx := <-sctxch:
		t.Equal(StateConsensus, sctx.from())
		t.Equal(StateBroken, sctx.next())
		t.Contains(sctx.Error(), "hahaha")
	}
}

func (t *testConsensusHandler) TestProcessingProposalWithACCEPTVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	avp, _ := t.VoteproofsPair(point, point.Next(), manifest.Hash(), t.prpool.hash(point), nil, nodes)

	pp.processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		st.setLastVoteproof(avp)

		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
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
	suf, nodes := NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	avp, _ := t.VoteproofsPair(point, point.Next(), manifest.Hash(), nil, nil, nodes)
	avp.SetResult(base.VoteResultDraw).finish()

	pp.processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		st.setLastVoteproof(avp)

		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
	case <-savedch:
		t.NoError(errors.Errorf("to save block should be ignored"))
	}

	t.Nil(st.pps.processor())
}

func (t *testConsensusHandler) TestProcessingProposalWithWrongNewBlockACCEPTVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	avp, _ := t.VoteproofsPair(point, point.Next(), nil, nil, nil, nodes) // random new block hash

	pp.processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		st.setLastVoteproof(avp)

		return manifest, nil
	}

	sctxch := make(chan stateSwitchContext, 1)
	st.switchStateFunc = func(sctx stateSwitchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait to switch syncing state"))
	case nsctx := <-sctxch:
		var ssctx syncingSwitchContext
		t.True(errors.As(nsctx, &ssctx))

		t.Equal(avp.Point().Height(), ssctx.height)
	}

	t.Nil(st.pps.processor())
}

func (t *testConsensusHandler) TestWithBallotbox() {
	point := base.RawPoint(33, 44)
	suf, _ := NewTestSuffrage(0, t.Local)

	box := NewBallotbox(
		func(base.Height) base.Suffrage {
			return suf
		},
		base.Threshold(100),
	)

	testctx, testdone := context.WithCancel(context.Background())

	st, closef, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer func() {
		testdone()
		st.timers.Stop()

		closef()
	}()

	st.policy.SetWaitProcessingProposal(time.Millisecond * 100)

	manifests := util.NewLockedMap()
	getmanifest := func(height base.Height) base.Manifest {
		i, _, _ := manifests.Get(height, func() (interface{}, error) {
			manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())

			t.T().Logf("new manifest processed: height=%d hash=%q proposal=%q", height, manifest.Hash(), manifest.Proposal())

			return manifest, nil
		})

		return i.(base.Manifest)
	}

	processdelay := time.Millisecond * 100
	pp.processerr = func(_ context.Context, fact base.ProposalFact, _ base.INITVoteproof) (base.Manifest, error) {
		<-time.After(processdelay)

		return getmanifest(fact.Point().Height()), nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp

		return nil
	}

	st.broadcastBallotFunc = func(bl base.Ballot) error {
		_, err := box.Vote(bl)
		// t.T().Logf("voted: point=%q, node=%q, voted=%v", bl.Point(), bl.SignedFact().Node(), voted)
		if err != nil {
			return err
		}

		return nil
	}
	prpool := t.prpool
	st.proposalSelector = DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		var pr base.ProposalSignedFact

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-testctx.Done():
			return nil, testctx.Err()
		default:
			pr = prpool.get(p)

		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-testctx.Done():
			return nil, testctx.Err()
		default:
			t.T().Logf("new proposal created: point=%q fact hash=%q", pr.Point(), pr.Fact().Hash())

			return pr, nil
		}
	})

	go func() {
	end:
		for {
			select {
			case <-testctx.Done():
				break end
			case vp := <-box.Voteproof():
				if err := st.newVoteproof(vp); err != nil {
					t.T().Logf("error: failed to new voteproof: %+v", err)
				}
			}
		}
	}()

	target := point
	for range make([]struct{}, 33) {
		target = target.Next()
	}

	wait := processdelay * time.Duration((target.Height()-point.Height()).Int64()*10)
	after := time.After(wait)
	t.T().Logf("> trying to create blocks up to %q; will wait %q", target, wait)

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
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
	suf, nodes := NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	st.getSuffrage = func(height base.Height) base.Suffrage {
		switch {
		case height <= point.Height():
			return suf
		default:
			return nil
		}
	}

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}
	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.prpool.get(p), nil
		}
	})

	sctxch := make(chan stateSwitchContext, 1)
	st.switchStateFunc = func(sctx stateSwitchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	avp, _ := t.VoteproofsPair(point, point.Next(), manifest.Hash(), t.prpool.hash(point), t.prpool.hash(point.Next()), nodes)
	t.NoError(st.newVoteproof(avp))

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
		t.NoError(errors.Errorf("timeout to wait next init ballot"))

		return
	case sctx := <-sctxch:
		var ssctx brokenSwitchContext
		t.True(errors.As(sctx, &ssctx))
		t.Contains(ssctx.Error(), "empty suffrage")
	}
}

func (t *testConsensusHandler) TestOutOfSuffrage() {
	point := base.RawPoint(33, 44)
	suf, nodes := NewTestSuffrage(2, t.Local)
	newsuf, _ := NewTestSuffrage(2)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	st.getSuffrage = func(height base.Height) base.Suffrage {
		if height == point.Height() {
			return suf
		}

		return newsuf
	}

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}
	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.prpool.get(p), nil
		}
	})

	sctxch := make(chan stateSwitchContext, 1)
	st.switchStateFunc = func(sctx stateSwitchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	avp, _ := t.VoteproofsPair(point, point.Next(), manifest.Hash(), t.prpool.hash(point), t.prpool.hash(point.Next()), nodes)
	t.NoError(st.newVoteproof(avp))

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
		t.NoError(errors.Errorf("timeout to wait next init ballot"))

		return
	case sctx := <-sctxch:
		var ssctx syncingSwitchContext
		t.True(errors.As(sctx, &ssctx))
		t.Equal(point.Height()+1, ssctx.height)
	}
}

func (t *testConsensusHandler) TestEnterButEmptySuffrage() {
	point := base.RawPoint(33, 44)
	suf, _ := NewTestSuffrage(2)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()
	st.getSuffrage = func(base.Height) base.Suffrage {
		return nil
	}

	sctxch := make(chan stateSwitchContext, 1)
	st.switchStateFunc = func(sctx stateSwitchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	_, err := st.enter(sctx)

	var ssctx brokenSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Contains(ssctx.Error(), "empty suffrage")
}

func (t *testConsensusHandler) TestEnterButNotInSuffrage() {
	point := base.RawPoint(33, 44)
	suf, _ := NewTestSuffrage(2)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	sctxch := make(chan stateSwitchContext, 1)
	st.switchStateFunc = func(sctx stateSwitchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	_, err := st.enter(sctx)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(point.Height(), ssctx.height)
}

func TestConsensusHandler(t *testing.T) {
	suite.Run(t, new(testConsensusHandler))
}
