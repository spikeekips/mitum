package isaac

import (
	"context"
	"sync/atomic"
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
	baseTestStateHandler
}

func (t *baseTestConsensusHandler) newState() (*ConsensusHandler, func()) {
	local := t.local
	policy := t.policy

	st := NewConsensusHandler(
		local,
		policy,
		nil,
		newProposalProcessors(nil, nil),
	)
	_ = st.SetLogging(logging.TestNilLogging)
	_ = st.setTimers(util.NewTimers([]util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastACCEPTBallot,
	}, false))

	return st, func() {
		deferred, err := st.exit()
		t.NoError(err)
		t.NoError(deferred())
	}
}

func (t *baseTestConsensusHandler) newStateWithINITVoteproof(point base.Point, nodes []*LocalNode) (
	*ConsensusHandler,
	func(),
	*DummyProposalProcessor,
	base.INITVoteproof,
) {
	st, closef := t.newState()

	prpool := t.prpool
	fact := prpool.getfact(point)

	pp := NewDummyProposalProcessor()
	pp.processerr = func(context.Context, base.ProposalFact) (base.Manifest, error) {
		return nil, errors.Errorf("process error")
	}

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		return prpool.factByHash(facthash)
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		ballotch <- bl

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

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	t.True(st.setLastVoteproof(ivp))

	return st, closef, pp, ivp
}

type testConsensusHandler struct {
	baseTestConsensusHandler
}

func (t *testConsensusHandler) TestNew() {
	nodes := t.nodes(3)

	st := NewConsensusHandler(
		t.local,
		t.policy,
		nil,
		newProposalProcessors(nil, func(context.Context, util.Hash) (base.ProposalFact, error) {
			return nil, util.NotFoundError.Call()
		}),
	)
	_ = st.SetLogging(logging.TestNilLogging)

	defer func() {
		deferred, err := st.exit()
		t.NoError(err)
		t.NoError(deferred())
	}()

	st.switchStateFunc = func(stateSwitchContext) error { return nil }

	_ = st.setTimers(util.NewTimers([]util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastACCEPTBallot,
	}, false))

	point := base.RawPoint(33, 0)
	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, nil, nodes)
	t.True(st.setLastVoteproof(ivp))

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())
}

func (t *testConsensusHandler) TestInvalidVoteproofs() {
	point := base.RawPoint(22, 0)
	nodes := t.nodes(3)

	t.Run("empty init voteproof", func() {
		st, closef, _, _ := t.newStateWithINITVoteproof(point, nodes)
		defer closef()

		sctx := newConsensusSwitchContext(StateJoining, nil)

		deferred, err := st.enter(sctx)
		t.Nil(deferred)
		t.Error(err)
		t.Contains(err.Error(), "empty init voteproof")
	})

	t.Run("draw result of init voteproof", func() {
		st, closef, _, _ := t.newStateWithINITVoteproof(point, nodes)
		defer closef()

		point := base.RawPoint(33, 0)
		_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, nil, nodes)
		ivp.SetResult(base.VoteResultDraw).finish()

		sctx := newConsensusSwitchContext(StateJoining, ivp)

		deferred, err := st.enter(sctx)
		t.Nil(deferred)
		t.Error(err)
		t.Contains(err.Error(), "wrong vote result")
	})

	t.Run("empty majority of init voteproof", func() {
		st, closef, _, _ := t.newStateWithINITVoteproof(point, nodes)
		defer closef()

		point := base.RawPoint(33, 0)
		_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, nil, nodes)
		ivp.SetMajority(nil).finish()

		sctx := newConsensusSwitchContext(StateJoining, ivp)

		deferred, err := st.enter(sctx)
		t.NotNil(deferred)
		t.NoError(err)
	})
}

func (t *testConsensusHandler) TestExit() {
	point := base.RawPoint(33, 44)
	nodes := t.nodes(3)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, nodes)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.processerr = func(context.Context, base.ProposalFact) (base.Manifest, error) {
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
	t.NoError(deferredenter())

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait accept ballot"))

		return
	case bl := <-ballotch:
		t.NoError(bl.IsValid(t.policy.NetworkID()))

		abl, ok := bl.(base.ACCEPTBallot)
		t.True(ok)

		t.Equal(ivp.Point().Point, abl.Point().Point)
		t.True(ivp.BallotMajority().Proposal().Equal(abl.BallotSignedFact().BallotFact().Proposal()))
	}

	t.NotNil(st.pps.p)

	deferredexit, err := st.exit()
	t.NoError(err)
	t.NotNil(deferredexit)

	t.Nil(st.pps.p)
}

func (t *testConsensusHandler) TestProcessingProposalAfterEntered() {
	point := base.RawPoint(33, 44)
	nodes := t.nodes(3)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, nodes)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.processerr = func(context.Context, base.ProposalFact) (base.Manifest, error) {
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
	t.NoError(deferred())

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait accept ballot"))

		return
	case bl := <-ballotch:
		t.NoError(bl.IsValid(t.policy.NetworkID()))

		abl, ok := bl.(base.ACCEPTBallot)
		t.True(ok)

		t.Equal(ivp.Point().Point, abl.Point().Point)
		t.True(ivp.BallotMajority().Proposal().Equal(abl.BallotSignedFact().BallotFact().Proposal()))
	}
}

func (t *testConsensusHandler) TestFailedProcessingProposalFetchFactFailed() {
	point := base.RawPoint(33, 44)
	nodes := t.nodes(2)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, nodes)
	defer closefunc()

	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		return nil, util.NotFoundError.Errorf("fact not found")
	}

	sctxch := make(chan stateSwitchContext, 1)
	st.switchStateFunc = func(sctx stateSwitchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

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
	nodes := t.nodes(2)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, nodes)
	defer closefunc()

	pp.processerr = func(context.Context, base.ProposalFact) (base.Manifest, error) {
		return nil, errors.Errorf("hahaha")
	}

	var i int
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		if i < 1 {
			i++
			return nil, RetryProposalProcessorError.Errorf("findme")
		}

		return t.prpool.factByHash(facthash)
	}

	sctxch := make(chan stateSwitchContext, 1)
	st.switchStateFunc = func(sctx stateSwitchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

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

func (t *testConsensusHandler) TestFailedProcessingProposalProcessingFailedRetry() {
	point := base.RawPoint(33, 44)
	nodes := t.nodes(2)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, nodes)
	defer closefunc()

	var c int64
	pp.processerr = func(context.Context, base.ProposalFact) (base.Manifest, error) {
		if i := atomic.LoadInt64(&c); i < 1 {
			atomic.AddInt64(&c, 1)

			return nil, RetryProposalProcessorError.Errorf("findme")
		}

		return nil, errors.Errorf("hahaha")
	}

	var g int64
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		if i := atomic.LoadInt64(&g); i < 1 {
			atomic.AddInt64(&g, 1)

			return nil, RetryProposalProcessorError.Errorf("findme")
		}

		return t.prpool.factByHash(facthash)
	}

	sctxch := make(chan stateSwitchContext, 1)
	st.switchStateFunc = func(sctx stateSwitchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	select {
	case <-time.After(time.Second * 5):
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
	nodes := t.nodes(3)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, nodes)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	avp, _ := t.voteproofsPair(point, point.Next(), manifest.Hash(), t.prpool.hash(point), nil, nodes)

	pp.processerr = func(context.Context, base.ProposalFact) (base.Manifest, error) {
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
	t.NoError(deferred())

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save block"))

		return
	case ravp := <-savedch:
		base.CompareVoteproof(t.Assert(), avp, ravp)
	}
}

func (t *testConsensusHandler) TestProcessingProposalWithDrawACCEPTVoteproof() {
	point := base.RawPoint(33, 44)
	nodes := t.nodes(3)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, nodes)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	avp, _ := t.voteproofsPair(point, point.Next(), manifest.Hash(), nil, nil, nodes)
	avp.SetResult(base.VoteResultDraw).finish()

	pp.processerr = func(context.Context, base.ProposalFact) (base.Manifest, error) {
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
	t.NoError(deferred())

	select {
	case <-time.After(time.Second * 2):
	case <-savedch:
		t.NoError(errors.Errorf("to save block should be ignored"))
	}

	t.Nil(st.pps.processor())
}

func (t *testConsensusHandler) TestProcessingProposalWithWrongNewBlockACCEPTVoteproof() {
	point := base.RawPoint(33, 44)
	nodes := t.nodes(3)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, nodes)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	avp, _ := t.voteproofsPair(point, point.Next(), nil, nil, nil, nodes) // random new block hash

	pp.processerr = func(context.Context, base.ProposalFact) (base.Manifest, error) {
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
	t.NoError(deferred())

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
	suf, nodes := newTestSuffrage(0, t.local)
	point := base.RawPoint(33, 44)

	box := NewBallotbox(
		func(base.Height) base.Suffrage {
			return suf
		},
		base.Threshold(100),
	)

	testctx, testdone := context.WithCancel(context.Background())

	st, closef, pp, ivp := t.newStateWithINITVoteproof(point, nodes)
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
	pp.processerr = func(_ context.Context, fact base.ProposalFact) (base.Manifest, error) {
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
	t.NoError(deferred())

end:
	for {
		select {
		case <-after:
			t.NoError(errors.Errorf("failed to wait new blocks"))
		case avp := <-savedch:
			t.T().Logf("new block saved: %q", avp.Point())

			if avp.Point().Point == target {
				t.T().Logf("< all new blocks saved, %q", target)

				break end
			}
		}
	}
}

func TestConsensusHandler(t *testing.T) {
	suite.Run(t, new(testConsensusHandler))
}
