package states

import (
	"testing"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testConsensusHandler struct {
	baseTestStateHandler
}

func (t *testConsensusHandler) voteproofsPair(point base.Point, pr, nextpr util.Hash, nodes []*LocalNode) (ACCEPTVoteproof, INITVoteproof) {
	if pr == nil {
		pr = valuehash.RandomSHA256()
	}
	if nextpr == nil {
		nextpr = valuehash.RandomSHA256()
	}

	newblock := valuehash.RandomSHA256()

	afact := t.newACCEPTBallotFact(point.Decrease(), pr, newblock)
	avp, err := t.newACCEPTVoteproof(afact, t.local, nodes)
	t.NoError(err)

	ifact := t.newINITBallotFact(point, newblock, nextpr)
	ivp, err := t.newINITVoteproof(ifact, t.local, nodes)
	t.NoError(err)

	return avp, ivp
}

func (t *testConsensusHandler) TestNew() {
	nodes := t.nodes(3)

	st := NewConsensusHandler(
		t.local,
		t.policy,
		t.proposalMaker(t.local, t.policy),
		func(base.Height) base.Suffrage {
			return nil
		},
		newProposalProcessors(nil, func(util.Hash) (base.ProposalFact, error) {
			return nil, util.NotFoundError.Call()
		}),
	)
	_ = st.SetLogging(logging.TestNilLogging)

	defer func() {
		deferred, err := st.exit()
		t.NoError(err)
		t.NoError(deferred())
	}()

	st.switchStateFunc = func(stateSwitchContext) {}

	_ = st.setTimers(util.NewTimers([]util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDPrepareProposal,
	}, false))

	avp, ivp := t.voteproofsPair(base.NewPoint(base.Height(33), base.Round(0)), nil, nil, nodes)

	sctx := newConsensusSwitchContext(StateJoining, avp, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())
}

func (t *testConsensusHandler) TestInvalidVoteproofs() {
	nodes := t.nodes(3)

	st := NewConsensusHandler(
		t.local,
		t.policy,
		t.proposalMaker(t.local, t.policy),
		func(base.Height) base.Suffrage {
			return nil
		},
		newProposalProcessors(nil, nil),
	)
	_ = st.setTimers(util.NewTimers(nil, true))

	defer func() {
		deferred, err := st.exit()
		t.NoError(err)
		t.NoError(deferred())
	}()

	t.Run("empty accept voteproof", func() {
		_, ivp := t.voteproofsPair(base.NewPoint(base.Height(33), base.Round(0)), nil, nil, nodes)

		sctx := newConsensusSwitchContext(StateJoining, nil, ivp)

		deferred, err := st.enter(sctx)
		t.Nil(deferred)
		t.Error(err)
		t.Contains(err.Error(), "empty accept voteproof")
	})

	t.Run("empty init voteproof", func() {
		avp, _ := t.voteproofsPair(base.NewPoint(base.Height(33), base.Round(0)), nil, nil, nodes)

		sctx := newConsensusSwitchContext(StateJoining, avp, nil)

		deferred, err := st.enter(sctx)
		t.Nil(deferred)
		t.Error(err)
		t.Contains(err.Error(), "empty init voteproof")
	})

	t.Run("wrong prev block hash", func() {
		pr := valuehash.RandomSHA256()
		newblock := valuehash.RandomSHA256()

		afact := t.newACCEPTBallotFact(base.NewPoint(base.Height(33), base.Round(0)), pr, newblock)
		avp, err := t.newACCEPTVoteproof(afact, t.local, nodes)
		t.NoError(err)

		ifact := t.newINITBallotFact(afact.Point().Point.Next(), valuehash.RandomSHA256(), valuehash.RandomSHA256())
		ivp, err := t.newINITVoteproof(ifact, t.local, nodes)
		t.NoError(err)

		sctx := newConsensusSwitchContext(StateJoining, avp, ivp)

		deferred, err := st.enter(sctx)
		t.Nil(deferred)
		t.Error(err)
		t.Contains(err.Error(), "wrong previous block hash")
	})

	t.Run("wrong result of accept voteproof", func() {
		avp, ivp := t.voteproofsPair(base.NewPoint(base.Height(33), base.Round(0)), nil, nil, nodes)
		avp.SetResult(base.VoteResultDraw)

		sctx := newConsensusSwitchContext(StateJoining, avp, ivp)

		deferred, err := st.enter(sctx)
		t.Nil(deferred)
		t.Error(err)
		t.Contains(err.Error(), "wrong result of accept voteproof")
	})

	t.Run("wrong result of init voteproof", func() {
		avp, ivp := t.voteproofsPair(base.NewPoint(base.Height(33), base.Round(0)), nil, nil, nodes)
		ivp.SetResult(base.VoteResultDraw)

		sctx := newConsensusSwitchContext(StateJoining, avp, ivp)

		deferred, err := st.enter(sctx)
		t.Nil(deferred)
		t.Error(err)
		t.Contains(err.Error(), "wrong result of init voteproof")
	})

	t.Run("empty majority of accept voteproof", func() {
		avp, ivp := t.voteproofsPair(base.NewPoint(base.Height(33), base.Round(0)), nil, nil, nodes)
		avp.SetMajority(nil)

		sctx := newConsensusSwitchContext(StateJoining, avp, ivp)

		deferred, err := st.enter(sctx)
		t.Nil(deferred)
		t.Error(err)
		t.Contains(err.Error(), "wrong majority of accept voteproof")
	})

	t.Run("wrong result of init voteproof", func() {
		avp, ivp := t.voteproofsPair(base.NewPoint(base.Height(33), base.Round(0)), nil, nil, nodes)
		ivp.SetMajority(nil)

		sctx := newConsensusSwitchContext(StateJoining, avp, ivp)

		deferred, err := st.enter(sctx)
		t.Nil(deferred)
		t.Error(err)
		t.Contains(err.Error(), "wrong majority of init voteproof")
	})

	t.Run("heights mismatch", func() {
		pr := valuehash.RandomSHA256()
		newblock := valuehash.RandomSHA256()

		afact := t.newACCEPTBallotFact(base.NewPoint(base.Height(33), base.Round(0)), pr, newblock)
		avp, err := t.newACCEPTVoteproof(afact, t.local, nodes)
		t.NoError(err)

		ifact := t.newINITBallotFact(afact.Point().Point, valuehash.RandomSHA256(), valuehash.RandomSHA256())
		ivp, err := t.newINITVoteproof(ifact, t.local, nodes)
		t.NoError(err)

		sctx := newConsensusSwitchContext(StateJoining, avp, ivp)

		deferred, err := st.enter(sctx)
		t.Nil(deferred)
		t.Error(err)
		t.Contains(err.Error(), "wrong heights")
	})
}

func (t *testConsensusHandler) TestProcessingProposalAfterEntered() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.newProposalFact(point, nil)

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest, nil)

	st := NewConsensusHandler(
		t.local,
		t.policy,
		t.proposalMaker(t.local, t.policy),
		func(base.Height) base.Suffrage {
			return nil
		},
		newProposalProcessors(
			pp.make,
			func(facthash util.Hash) (base.ProposalFact, error) {
				if facthash.Equal(fact.Hash()) {
					return fact, nil
				}

				return nil, util.NotFoundError.Errorf("fact not found")
			}),
	)
	_ = st.SetLogging(logging.TestLogging)

	defer func() {
		deferred, err := st.exit()
		t.NoError(err)
		t.NoError(deferred())
	}()

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		ballotch <- bl

		return nil
	}

	_ = st.setTimers(util.NewTimers([]util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDPrepareProposal,
	}, false))

	avp, ivp := t.voteproofsPair(point, nil, fact.Hash(), nodes)
	sctx := newConsensusSwitchContext(StateJoining, avp, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	select {
	case <-time.After(time.Second * 2):
	case bl := <-ballotch:
		t.NoError(bl.IsValid(t.policy.NetworkID()))

		abl, ok := bl.(base.ACCEPTBallot)
		t.True(ok)

		t.Equal(ivp.Point().Point, abl.Point().Point)
		t.True(ivp.BallotMajority().Proposal().Equal(abl.BallotSignedFact().BallotFact().Proposal()))
	}
}

func (t *testConsensusHandler) TestFailedProcessingProposal() {
}

func TestConsensusHandler(t *testing.T) {
	suite.Run(t, new(testConsensusHandler))
}
