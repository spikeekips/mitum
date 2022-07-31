package isaacstates

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type baseHandler struct {
	local            base.LocalNode
	ctx              context.Context //nolint:containedctx //...
	proposalSelector isaac.ProposalSelector
	*logging.Logging
	policy               *isaac.NodePolicy
	setLastVoteproofFunc func(base.Voteproof) bool
	cancel               func()
	lastVoteproofFunc    func() LastVoteproofs
	sts                  *States
	timers               *util.Timers
	switchStateFunc      func(switchContext) error
	broadcastBallotFunc  func(base.Ballot) error
	voteFunc             func(base.Ballot) (bool, error)
	stt                  StateType
}

func newBaseHandler(
	state StateType,
	local base.LocalNode,
	policy *isaac.NodePolicy,
	proposalSelector isaac.ProposalSelector,
) *baseHandler {
	lvps := NewLastVoteproofsHandler()

	return &baseHandler{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", fmt.Sprintf("state-handler-%s", state))
		}),
		stt:              state,
		local:            local,
		policy:           policy,
		proposalSelector: proposalSelector,
		broadcastBallotFunc: func(base.Ballot) error {
			return nil
		},
		lastVoteproofFunc: func() LastVoteproofs {
			return lvps.Last()
		},
		setLastVoteproofFunc: func(vp base.Voteproof) bool {
			return lvps.Set(vp)
		},
		voteFunc: func(base.Ballot) (bool, error) { return false, errors.Errorf("not voted") },
	}
}

func (st *baseHandler) new() *baseHandler {
	return &baseHandler{
		Logging:              st.Logging,
		local:                st.local,
		stt:                  st.stt,
		policy:               st.policy,
		sts:                  st.sts,
		timers:               st.timers,
		broadcastBallotFunc:  st.broadcastBallotFunc,
		lastVoteproofFunc:    st.lastVoteproofFunc,
		proposalSelector:     st.proposalSelector,
		setLastVoteproofFunc: st.setLastVoteproofFunc,
		switchStateFunc:      st.switchStateFunc,
		voteFunc:             st.voteFunc,
	}
}

func (st *baseHandler) enter(switchContext) (func(), error) { //nolint:unparam //...
	st.ctx, st.cancel = context.WithCancel(context.Background())

	return func() {}, nil
}

func (st *baseHandler) exit(switchContext) (func(), error) { //nolint:unparam //...
	st.cancel()

	return func() {}, nil
}

func (*baseHandler) newVoteproof(base.Voteproof) error {
	return nil
}

func (st *baseHandler) state() StateType {
	return st.stt
}

func (st *baseHandler) lastVoteproofs() LastVoteproofs {
	return st.lastVoteproofFunc()
}

func (st *baseHandler) setLastVoteproof(vp base.Voteproof) bool {
	return st.setLastVoteproofFunc(vp)
}

func (st *baseHandler) switchState(sctx switchContext) {
	elem := reflect.ValueOf(sctx)
	p := reflect.New(elem.Type())
	p.Elem().Set(elem)

	if i, ok := p.Interface().(interface{ setFrom(StateType) }); ok {
		i.setFrom(st.stt)
	}

	nsctx := p.Elem().Interface().(switchContext) //nolint:forcetypeassert //...

	l := st.Log().With().Dict("next_state", switchContextLog(nsctx)).Logger()

	switch err := st.switchStateFunc(nsctx); {
	case err == nil:
		l.Debug().Msg("state switched")
	case errors.Is(err, ignoreSwithingStateError):
		l.Error().Err(err).Msg("failed to switch state; ignore")
	case nsctx.next() == StateBroken:
		l.Error().Err(err).Msg("failed to switch state; panic")

		panic(err)
	default:
		l.Error().Err(err).Msg("failed to switch state; moves to broken")

		go st.switchState(newBrokenSwitchContext(st.stt, err))
	}
}

func (st *baseHandler) setStates(sts *States) {
	st.sts = sts

	st.switchStateFunc = func(sctx switchContext) error {
		return st.sts.newState(sctx)
	}

	st.broadcastBallotFunc = func(bl base.Ballot) error {
		return st.sts.broadcastBallot(bl)
	}

	st.timers = st.sts.timers

	st.lastVoteproofFunc = func() LastVoteproofs {
		return st.sts.lastVoteproof()
	}
	st.setLastVoteproofFunc = func(vp base.Voteproof) bool {
		return st.sts.setLastVoteproof(vp)
	}
}

func (st *baseHandler) setNewVoteproof(vp base.Voteproof) (LastVoteproofs, base.Voteproof) {
	lvps := st.lastVoteproofs()

	if st.sts == nil && !lvps.IsNew(vp) {
		return LastVoteproofs{}, nil
	}

	_ = st.setLastVoteproof(vp)

	return lvps, vp
}

func (st *baseHandler) broadcastBallot(
	bl base.Ballot,
	timerid util.TimerID,
	initialWait time.Duration,
) error {
	iw := initialWait
	if iw < 1 {
		iw = time.Nanosecond
	}

	l := st.Log().With().
		Stringer("ballot_hash", bl.SignedFact().Fact().Hash()).
		Dur("initial_wait", iw).
		Logger()
	l.Debug().Interface("ballot", bl).Object("point", bl.Point()).Msg("trying to broadcast ballot")

	e := util.StringErrorFunc("failed to broadcast ballot")

	ct := util.NewContextTimer(
		timerid,
		st.policy.IntervalBroadcastBallot(),
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

func (st *baseHandler) broadcastINITBallot(bl base.Ballot, initialWait time.Duration) error {
	return st.broadcastBallot(bl, timerIDBroadcastINITBallot, initialWait)
}

func (st *baseHandler) broadcastACCEPTBallot(bl base.Ballot, initialWait time.Duration) error {
	return st.broadcastBallot(bl, timerIDBroadcastACCEPTBallot, initialWait)
}

func (st *baseHandler) prepareNextRound(vp base.Voteproof, prevBlock util.Hash) (base.INITBallot, error) {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

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

	fact := isaac.NewINITBallotFact(
		point,
		prevBlock,
		pr.Fact().Hash(),
	)
	sf := isaac.NewINITBallotSignedFact(st.local.Address(), fact)

	if err := sf.Sign(st.local.Privatekey(), st.policy.NetworkID()); err != nil {
		return nil, newBrokenSwitchContext(st.stt, e(err, "failed to make next round init ballot"))
	}

	return isaac.NewINITBallot(vp, sf), nil
}

func (st *baseHandler) prepareNextBlock(
	avp base.ACCEPTVoteproof, nodeInConsensusNodesFunc isaac.NodeInConsensusNodesFunc,
) (base.INITBallot, error) {
	e := util.StringErrorFunc("failed to prepare next block")

	point := avp.Point().Point.NextHeight()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Object("point", point).Logger()

	switch suf, found, err := nodeInConsensusNodesFunc(st.local, point.Height()); {
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
	)
	sf := isaac.NewINITBallotSignedFact(st.local.Address(), fact)

	if err := sf.Sign(st.local.Privatekey(), st.policy.NetworkID()); err != nil {
		return nil, e(err, "failed to make next init ballot")
	}

	return isaac.NewINITBallot(avp, sf), nil
}

func (st *baseHandler) vote(bl base.Ballot) (bool, error) {
	return st.voteFunc(bl)
}

var errNotInConsensusNodes = util.NewError("failed to vote; local not in consensus nodes")

func preventVotingWithEmptySuffrage(
	voteFunc func(base.Ballot) (bool, error),
	node base.Node,
	nodeInConsensusNodes isaac.NodeInConsensusNodesFunc,
) func(base.Ballot) (bool, error) {
	return func(bl base.Ballot) (bool, error) {
		e := util.StringErrorFunc("failed to vote")

		switch suf, found, err := nodeInConsensusNodes(node, bl.Point().Height()); {
		case err != nil:
			return false, e(err, "failed to get suffrage for ballot")
		case suf == nil || len(suf.Nodes()) < 1:
			return false, e(nil, "empty suffrage")
		case !found:
			return false, e(errNotInConsensusNodes.Errorf("ballot=%q", bl.Point()), "")
		default:
			return voteFunc(bl)
		}
	}
}
