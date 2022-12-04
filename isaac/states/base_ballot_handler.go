package isaacstates

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type SuffrageVotingFindFunc func(context.Context, base.Height, base.Suffrage) ([]base.SuffrageWithdrawOperation, error)

type baseBallotHandler struct {
	*baseHandler
	proposalSelector    isaac.ProposalSelector
	broadcastBallotFunc func(base.Ballot) error
	voteFunc            func(base.Ballot) (bool, error)
	svf                 SuffrageVotingFindFunc
}

func newBaseBallotHandler(
	state StateType,
	local base.LocalNode,
	params *isaac.LocalParams,
	proposalSelector isaac.ProposalSelector,
	svf SuffrageVotingFindFunc,
) *baseBallotHandler {
	if svf == nil {
		svf = func(context.Context, base.Height, base.Suffrage) ( //revive:disable-line:modifies-parameter
			[]base.SuffrageWithdrawOperation, error,
		) {
			return nil, nil
		}
	}

	return &baseBallotHandler{
		baseHandler:      newBaseHandler(state, local, params),
		proposalSelector: proposalSelector,
		broadcastBallotFunc: func(base.Ballot) error {
			return nil
		},
		voteFunc: func(base.Ballot) (bool, error) { return false, errors.Errorf("not voted") },
		svf:      svf,
	}
}

func (st *baseBallotHandler) new() *baseBallotHandler {
	return &baseBallotHandler{
		baseHandler:         st.baseHandler.new(),
		proposalSelector:    st.proposalSelector,
		svf:                 st.svf,
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

func (st *baseBallotHandler) prepareNextRound(
	vp base.Voteproof,
	prevBlock util.Hash,
	nodeInConsensusNodesFunc isaac.NodeInConsensusNodesFunc,
) (base.INITBallot, error) {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger() //nolint:goconst //...

	point := vp.Point().Point.NextRound()

	l.Debug().Object("point", point).Msg("preparing next round")

	var withdrawfacts []util.Hash
	var withdraws []base.SuffrageWithdrawOperation

	switch suf, found, err := nodeInConsensusNodesFunc(st.local, point.Height()); {
	case errors.Is(err, storage.ErrNotFound):
	case err != nil:
		l.Error().Err(err).Msg("failed to get suffrage")

		return nil, err
	case suf == nil || suf.Len() < 1:
		l.Debug().Msg("empty suffrage of next block; moves to broken state")

		return nil, err
	case !found:
		l.Debug().Msg("local is not in consensus nodes")

		return nil, nil
	default:
		l.Debug().
			Bool("in_suffrage", suf.ExistsPublickey(st.local.Address(), st.local.Publickey())).
			Msg("local is in consensus nodes and is in suffrage?")

		// NOTE collect suffrage withdraw operations
		withdraws, withdrawfacts, err = st.findWithdraws(point.Height(), suf)
		if err != nil {
			return nil, err
		}
	}

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
		withdrawfacts,
	)
	sf := isaac.NewINITBallotSignFact(fact)

	if err := sf.NodeSign(st.local.Privatekey(), st.params.NetworkID(), st.local.Address()); err != nil {
		return nil, newBrokenSwitchContext(st.stt, e(err, "failed to make next round init ballot"))
	}

	return isaac.NewINITBallot(vp, sf, withdraws), nil
}

func (st *baseBallotHandler) prepareNextBlock(
	avp base.ACCEPTVoteproof,
	nodeInConsensusNodesFunc isaac.NodeInConsensusNodesFunc,
) (base.INITBallot, error) {
	e := util.StringErrorFunc("failed to prepare next block")

	point := avp.Point().Point.NextHeight()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Object("point", point).Logger()

	var withdrawfacts []util.Hash
	var withdraws []base.SuffrageWithdrawOperation

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
	default:
		l.Debug().
			Bool("in_suffrage", suf.ExistsPublickey(st.local.Address(), st.local.Publickey())).
			Msg("local is in consensus nodes and is in suffrage?")

		// NOTE collect suffrage withdraw operations
		withdraws, withdrawfacts, err = st.findWithdraws(point.Height(), suf)
		if err != nil {
			return nil, err
		}
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
		withdrawfacts,
	)
	sf := isaac.NewINITBallotSignFact(fact)

	if err := sf.NodeSign(st.local.Privatekey(), st.params.NetworkID(), st.local.Address()); err != nil {
		return nil, e(err, "failed to make next init ballot")
	}

	return isaac.NewINITBallot(avp, sf, withdraws), nil
}

func (st *baseBallotHandler) prepareACCEPTBallot(
	ivp base.INITVoteproof,
	manifest base.Manifest,
	initialWait time.Duration,
) error {
	e := util.StringErrorFunc("failed to prepare accept ballot")

	// NOTE add SuffrageWithdrawOperations into ballot from init voteproof
	var withdrawfacts []util.Hash
	var withdraws []base.SuffrageWithdrawOperation

	if i, ok := ivp.(isaac.WithdrawVoteproof); ok {
		withdraws = i.Withdraws()

		withdrawfacts = make([]util.Hash, len(withdraws))

		for i := range withdraws {
			withdrawfacts[i] = withdraws[i].WithdrawFact().Hash()
		}
	}

	afact := isaac.NewACCEPTBallotFact(
		ivp.Point().Point,
		ivp.BallotMajority().Proposal(),
		manifest.Hash(),
		withdrawfacts,
	)
	signfact := isaac.NewACCEPTBallotSignFact(afact)

	if err := signfact.NodeSign(st.local.Privatekey(), st.params.NetworkID(), st.local.Address()); err != nil {
		return e(err, "")
	}

	bl := isaac.NewACCEPTBallot(ivp, signfact, withdraws)

	go func() {
		<-time.After(initialWait)

		switch _, err := st.vote(bl); {
		case err == nil:
		case errors.Is(err, errFailedToVoteNotInConsensus):
			st.Log().Debug().Err(err).Msg("failed to vote accept ballot; moves to syncing state")

			go st.switchState(newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1))
		default:
			st.Log().Error().Err(err).Msg("failed to vote accept ballot; moves to broken state")

			go st.switchState(newBrokenSwitchContext(StateConsensus, err))
		}
	}()

	if err := st.broadcastACCEPTBallot(bl, initialWait); err != nil {
		return e(err, "failed to broadcast accept ballot")
	}

	if err := st.timers.StartTimers([]util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastSuffrageConfirmBallot,
		timerIDBroadcastACCEPTBallot,
	}, true); err != nil {
		return e(err, "failed to start timers for broadcasting accept ballot")
	}

	return nil
}

func (st *baseBallotHandler) prepareSuffrageConfirmBallot(vp base.Voteproof) {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	if _, ok := vp.(isaac.WithdrawVoteproof); !ok {
		l.Error().Msg("expected WithdrawVoteproof for suffrage sign voting")

		return
	}

	ifact := vp.Majority().(isaac.INITBallotFact) //nolint:forcetypeassert //...
	withdrawfacts := ifact.WithdrawFacts()

	fact := isaac.NewSuffrageConfirmBallotFact(
		vp.Point().Point,
		ifact.PreviousBlock(),
		ifact.Proposal(),
		withdrawfacts,
	)

	sf := isaac.NewINITBallotSignFact(fact)

	if err := sf.NodeSign(st.local.Privatekey(), st.params.NetworkID(), st.local.Address()); err != nil {
		go st.switchState(
			newBrokenSwitchContext(st.stt, errors.WithMessage(err, "failed to make suffrage confirm ballot")),
		)

		return
	}

	bl := isaac.NewINITBallot(vp, sf, nil)

	go func() {
		switch _, err := st.vote(bl); {
		case err == nil:
		case errors.Is(err, errFailedToVoteNotInConsensus):
			st.Log().Debug().Err(err).Msg("failed to vote suffrage confirm ballot; moves to syncing state")

			go st.switchState(newSyncingSwitchContext(StateConsensus, bl.Point().Height()-1))
		default:
			st.Log().Debug().Err(err).Msg("failed to vote suffrage confirm ballot; moves to broken state")

			go st.switchState(newBrokenSwitchContext(StateConsensus, err))
		}
	}()

	if err := st.broadcastSuffrageConfirmBallot(bl); err != nil {
		l.Error().Err(err).Msg("failed to prepare suffrage confirm ballot")

		return
	}

	l.Debug().Interface("ballot", bl).Msg("suffrage confirm ballot broadcasted")
}

func (st *baseBallotHandler) broadcastINITBallot(
	bl base.Ballot,
	interval func(int, time.Duration) time.Duration,
) error {
	return broadcastBallot(
		bl,
		st.timers,
		timerIDBroadcastINITBallot,
		st.broadcastBallotFunc,
		st.Logging,
		interval,
	)
}

func (st *baseBallotHandler) broadcastACCEPTBallot(bl base.Ballot, initialWait time.Duration) error {
	if initialWait < 1 {
		initialWait = time.Nanosecond //revive:disable-line:modifies-parameter
	}

	return broadcastBallot(
		bl,
		st.timers,
		timerIDBroadcastACCEPTBallot,
		st.broadcastBallotFunc,
		st.Logging,
		func(i int, _ time.Duration) time.Duration {
			if i < 1 {
				return initialWait
			}

			return st.params.IntervalBroadcastBallot()
		},
	)
}

func (st *baseBallotHandler) broadcastSuffrageConfirmBallot(bl base.INITBallot) error {
	if err := broadcastBallot(
		bl,
		st.timers,
		timerIDBroadcastSuffrageConfirmBallot,
		st.broadcastBallotFunc,
		st.Logging,
		func(i int, _ time.Duration) time.Duration {
			lvp := st.lastVoteproofs().Cap()
			if lvp.Point().Height() > bl.Point().Height() {
				return 0
			}

			if i < 1 {
				return time.Nanosecond
			}

			return st.params.IntervalBroadcastBallot()
		},
	); err != nil {
		return err
	}

	return st.timers.StartTimers(
		[]util.TimerID{
			timerIDBroadcastINITBallot,
			timerIDBroadcastSuffrageConfirmBallot,
			timerIDBroadcastACCEPTBallot,
		},
		true,
	)
}

func (st *baseBallotHandler) vote(bl base.Ballot) (bool, error) {
	return st.voteFunc(bl)
}

func (st *baseBallotHandler) findWithdraws(height base.Height, suf base.Suffrage) (
	withdraws []base.SuffrageWithdrawOperation,
	withdrawfacts []util.Hash,
	_ error,
) {
	ops, err := st.svf(context.Background(), height, suf)
	if err != nil {
		return nil, nil, err
	}

	if len(ops) < 1 {
		return nil, nil, nil
	}

	withdrawfacts = make([]util.Hash, len(ops))

	for i := range ops {
		withdrawfacts[i] = ops[i].WithdrawFact().Hash()
	}

	withdraws = ops

	return withdraws, withdrawfacts, nil
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

func broadcastBallot(
	bl base.Ballot,
	timers *util.Timers,
	timerid util.TimerID,
	broadcastBallotFunc func(base.Ballot) error,
	log *logging.Logging,
	interval func(int, time.Duration) time.Duration,
) error {
	l := log.Log().With().
		Stringer("ballot_hash", bl.SignFact().Fact().Hash()).
		Logger()
	l.Debug().Interface("ballot", bl).Object("point", bl.Point()).Msg("trying to broadcast ballot")

	e := util.StringErrorFunc("failed to broadcast ballot")

	ct := util.NewContextTimer(
		timerid,
		time.Nanosecond,
		func(i int) (bool, error) {
			if err := broadcastBallotFunc(bl); err != nil {
				l.Error().Err(err).Msg("failed to broadcast ballot; keep going")

				return true, nil
			}

			l.Debug().Msg("ballot broadcasted")

			return true, nil
		},
	)
	_ = ct.SetLogging(log)

	if err := timers.SetTimer(ct.SetInterval(interval)); err != nil {
		return e(err, "")
	}

	return nil
}
