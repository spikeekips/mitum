package isaacstates

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
)

type baseBallotHandler struct {
	*baseHandler
	proposalSelector    isaac.ProposalSelector
	broadcastBallotFunc func(base.Ballot) error
	voteFunc            func(base.Ballot) (bool, error)
}

func newBaseBallotHandler(
	state StateType,
	local base.LocalNode,
	params *isaac.LocalParams,
	proposalSelector isaac.ProposalSelector,
) *baseBallotHandler {
	return &baseBallotHandler{
		baseHandler:      newBaseHandler(state, local, params),
		proposalSelector: proposalSelector,
		broadcastBallotFunc: func(base.Ballot) error {
			return nil
		},
		voteFunc: func(base.Ballot) (bool, error) { return false, errors.Errorf("not voted") },
	}
}

func (st *baseBallotHandler) new() *baseBallotHandler {
	return &baseBallotHandler{
		baseHandler:         st.baseHandler.new(),
		proposalSelector:    st.proposalSelector,
		broadcastBallotFunc: st.broadcastBallotFunc,
		voteFunc:            st.voteFunc,
	}
}

func (st *baseBallotHandler) setStates(sts *States) {
	st.baseHandler.setStates(sts)

	st.broadcastBallotFunc = func(bl base.Ballot) error {
		return st.sts.broadcastBallot(bl)
	}
}

func (st *baseBallotHandler) prepareNextRound(vp base.Voteproof, prevBlock util.Hash) (base.INITBallot, error) {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger() //nolint:goconst //...

	point := vp.Point().Point.NextRound()

	l.Debug().Object("point", point).Msg("preparing next round")

	// NOTE find next proposal
	pr, err := st.proposalSelector.Select(st.ctx, point)
	if err != nil {
		l.Error().Err(err).Msg("failed to select proposal")

		return nil, newBrokenSwitchContext(st.stt, err)
	}

	l.Debug().Interface("proposal", pr).Msg("proposal selected")

	e := util.StringErrorFunc("failed to move to next round")

	// FIXME collect suffrage withdraw operations

	fact := isaac.NewINITBallotFact(
		point,
		prevBlock,
		pr.Fact().Hash(),
		nil,
	)
	sf := isaac.NewINITBallotSignFact(st.local.Address(), fact)

	if err := sf.Sign(st.local.Privatekey(), st.params.NetworkID()); err != nil {
		return nil, newBrokenSwitchContext(st.stt, e(err, "failed to make next round init ballot"))
	}

	return isaac.NewINITBallot(vp, sf, nil), nil
}

func (st *baseBallotHandler) prepareNextBlock(
	avp base.ACCEPTVoteproof, nodeInConsensusNodesFunc isaac.NodeInConsensusNodesFunc,
) (base.INITBallot, error) {
	e := util.StringErrorFunc("failed to prepare next block")

	point := avp.Point().Point.NextHeight()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Object("point", point).Logger()

	switch suf, found, err := nodeInConsensusNodesFunc(st.local, point.Height()); {
	case errors.Is(err, storage.ErrNotFound):
		return nil, newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	case err != nil:
		return nil, newBrokenSwitchContext(st.stt, err)
	case suf == nil || suf.Len() < 1:
		l.Debug().Msg("empty suffrage of next block; moves to broken state")

		return nil, newBrokenSwitchContext(st.stt, util.ErrNotFound.Errorf("empty suffrage"))
	case !found:
		l.Debug().Msg("local is not in consensus nodes at next block; moves to syncing state")

		return nil, newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	case suf.ExistsPublickey(st.local.Address(), st.local.Publickey()):
		l.Debug().Msg("local is in consensus nodes and in suffrage")
	default:
		l.Debug().Msg("local is in consensus nodes, but not in suffrage")
	}

	// NOTE find next proposal
	pr, err := st.proposalSelector.Select(st.ctx, point)

	switch {
	case err == nil:
	case errors.Is(err, context.Canceled):
		l.Debug().Err(err).Msg("canceled to select proposal; ignore")

		return nil, nil
	default:
		l.Error().Err(err).Msg("failed to select proposal")

		return nil, e(err, "")
	}

	l.Debug().Interface("proposal", pr).Msg("proposal selected")

	// NOTE broadcast next init ballot
	fact := isaac.NewINITBallotFact(
		point,
		avp.BallotMajority().NewBlock(),
		pr.Fact().Hash(),
		nil,
	)
	sf := isaac.NewINITBallotSignFact(st.local.Address(), fact)

	if err := sf.Sign(st.local.Privatekey(), st.params.NetworkID()); err != nil {
		return nil, e(err, "failed to make next init ballot")
	}

	return isaac.NewINITBallot(avp, sf, nil), nil
}

func (st *baseBallotHandler) broadcastINITBallot(bl base.Ballot, initialWait time.Duration) error {
	return st.broadcastBallot(bl, timerIDBroadcastINITBallot, initialWait)
}

func (st *baseBallotHandler) broadcastACCEPTBallot(bl base.Ballot, initialWait time.Duration) error {
	return st.broadcastBallot(bl, timerIDBroadcastACCEPTBallot, initialWait)
}

func (st *baseBallotHandler) broadcastBallot(
	bl base.Ballot,
	timerid util.TimerID,
	initialWait time.Duration,
) error {
	iw := initialWait
	if iw < 1 {
		iw = time.Nanosecond
	}

	l := st.Log().With().
		Stringer("ballot_hash", bl.SignFact().Fact().Hash()).
		Dur("initial_wait", iw).
		Logger()
	l.Debug().Interface("ballot", bl).Object("point", bl.Point()).Msg("trying to broadcast ballot")

	e := util.StringErrorFunc("failed to broadcast ballot")

	ct := util.NewContextTimer(
		timerid,
		st.params.IntervalBroadcastBallot(),
		func(int) (bool, error) {
			if err := st.broadcastBallotFunc(bl); err != nil {
				l.Error().Err(err).Msg("failed to broadcast ballot; keep going")
			}

			return true, nil
		},
	).SetInterval(func(i int, d time.Duration) time.Duration {
		if i < 1 {
			return iw
		}

		return d
	})

	if err := st.timers.SetTimer(ct); err != nil {
		return e(err, "")
	}

	return nil
}

func (st *baseBallotHandler) vote(bl base.Ballot) (bool, error) {
	return st.voteFunc(bl)
}

var errFailedToVoteNotInConsensus = util.NewError("failed to vote; local not in consensus nodes")

func preventVotingWithEmptySuffrage(
	voteFunc func(base.Ballot) (bool, error),
	node base.Node,
	nodeInConsensusNodes isaac.NodeInConsensusNodesFunc,
) func(base.Ballot) (bool, error) {
	return func(bl base.Ballot) (bool, error) {
		e := util.StringErrorFunc("failed to vote")

		suf, found, err := nodeInConsensusNodes(node, bl.Point().Height())

		switch {
		case err != nil:
		case suf == nil || len(suf.Nodes()) < 1:
			return false, e(nil, "empty suffrage")
		case !found:
			return false, e(errFailedToVoteNotInConsensus.Errorf("ballot=%q", bl.Point()), "")
		}

		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return false, e(err, "")
		}

		return voteFunc(bl)
	}
}
