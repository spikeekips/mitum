package isaacstates

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type ConsensusHandler struct {
	*baseHandler
	getManifest func(base.Height) (base.Manifest, error)
	getSuffrage func(base.Height) base.Suffrage
	pps         *isaac.ProposalProcessors
}

func NewConsensusHandler(
	local base.LocalNode,
	policy isaac.NodePolicy,
	proposalSelector isaac.ProposalSelector,
	getManifest func(base.Height) (base.Manifest, error),
	getSuffrage func(base.Height) base.Suffrage,
	pps *isaac.ProposalProcessors,
) *ConsensusHandler {
	return &ConsensusHandler{
		baseHandler: newBaseHandler(StateConsensus, local, policy, proposalSelector),
		getManifest: getManifest,
		getSuffrage: getSuffrage,
		pps:         pps,
	}
}

func (st *ConsensusHandler) enter(i switchContext) (func(), error) {
	e := util.StringErrorFunc("failed to enter consensus state")

	deferred, err := st.baseHandler.enter(i)
	if err != nil {
		return nil, e(err, "")
	}

	var sctx consensusSwitchContext
	switch j, ok := i.(consensusSwitchContext); {
	case !ok:
		return nil, e(nil, "invalid stateSwitchContext, not for consensus state; %T", i)
	case j.ivp == nil:
		return nil, e(nil, "invalid stateSwitchContext, empty init voteproof")
	case j.ivp.Result() != base.VoteResultMajority:
		return nil, e(nil, "invalid stateSwitchContext, wrong vote result of init voteproof, %q", j.ivp.Result())
	default:
		sctx = j
	}

	switch ok, err := st.isLocalInSuffrage(sctx.ivp.Point().Height()); {
	case err != nil:
		return nil, newBrokenSwitchContext(StateEmpty, e(err, "local not in suffrage for next block"))
	case !ok:
		st.Log().Debug().
			Dict("state_context", switchContextLog(sctx)).
			Object("height", sctx.ivp.Point().Height()).
			Msg("local is not in suffrage at entering consensus state; moves to syncing state")

		return nil, newSyncingSwitchContext(StateEmpty, sctx.ivp.Point().Height())
	}

	return func() {
		deferred()

		go st.processProposal(sctx.ivp)
	}, nil
}

func (st *ConsensusHandler) exit(sctx switchContext) (func(), error) {
	e := util.StringErrorFunc("failed to exit from consensus state")

	deferred, err := st.baseHandler.exit(sctx)
	if err != nil {
		return nil, e(err, "")
	}

	if err := st.pps.Close(); err != nil {
		return nil, e(err, "failed to close proposal processors")
	}

	return func() {
		deferred()

		var timers []util.TimerID
		if sctx != nil {
			switch sctx.next() {
			case StateJoining, StateHandover:
				timers = []util.TimerID{timerIDBroadcastINITBallot, timerIDBroadcastACCEPTBallot}
			}
		}

		if len(timers) < 1 {
			if err := st.timers.StopTimersAll(); err != nil {
				st.Log().Error().Err(err).Msg("failed to stop timers; ignore")
			}
		} else if err := st.timers.StartTimers([]util.TimerID{
			timerIDBroadcastINITBallot,
			timerIDBroadcastACCEPTBallot,
		}, true); err != nil {
			st.Log().Error().Err(err).Msg("failed to start timers; ignore")
		}
	}, nil
}

func (st *ConsensusHandler) processProposal(ivp base.INITVoteproof) {
	facthash := ivp.BallotMajority().Proposal()
	l := st.Log().With().Stringer("fact", facthash).Logger()
	l.Debug().Msg("tyring to process proposal")

	e := util.StringErrorFunc("failed to process proposal")

	manifest, err := st.processProposalInternal(ivp)
	switch {
	case err != nil:
		err = e(err, "")
		l.Error().Err(err).Msg("failed to process proposal; moves to broken state")

		go st.switchState(newBrokenSwitchContext(StateConsensus, err))

		return
	case manifest == nil:
		l.Debug().Msg("failed to process proposal; empty manifest; ignore")

		return
	}

	l.Debug().Msg("proposal processed")

	eavp := st.lastVoteproof().accept()
	if eavp == nil || !eavp.Point().Point.Equal(ivp.Point().Point) {
		return
	}

	ll := l.With().Dict("accept_voteproof", base.VoteproofLog(eavp)).Logger()

	switch { // NOTE check last accept voteproof is the execpted
	case eavp.Result() != base.VoteResultMajority:
		if err := st.pps.Close(); err != nil {
			ll.Error().Err(e(err, "failed to close processor")).
				Msg("expected accept voteproof is not majority result; cancel processor, but failed")

			return
		}

		ll.Debug().Msg("expected accept voteproof is not majority result; ignore")

		return
	case !manifest.Hash().Equal(eavp.BallotMajority().NewBlock()):
		if err := st.pps.Close(); err != nil {
			ll.Error().Err(e(err, "failed to close processor")).
				Msg("expected accept voteproof has different new block; cancel processor, but failed")

			return
		}

		ll.Debug().Msg("expected accept voteproof has different new block; moves to syncing")

		go st.switchState(newSyncingSwitchContext(StateConsensus, eavp.Point().Height()))

		return
	default:
		ll.Debug().Msg("proposal processed and expected voteproof found")
	}

	var sctx switchContext
	switch err := st.saveBlock(eavp); {
	case err == nil:
	case errors.As(err, &sctx):
	default:
		sctx = newBrokenSwitchContext(StateConsensus, errors.Wrap(err, "failed to save proposal"))
	}

	if sctx != nil {
		go st.switchState(sctx)
	}
}

func (st *ConsensusHandler) processProposalInternal(ivp base.INITVoteproof) (base.Manifest, error) {
	e := util.StringErrorFunc("failed to process proposal")

	facthash := ivp.BallotMajority().Proposal()

	started := time.Now()

	var previous base.Manifest
	switch m, err := st.getManifest(ivp.Point().Height() - 1); {
	case err != nil:
		return nil, e(err, "")
	default:
		previous = m
	}

	switch manifest, err := st.pps.Process(st.ctx, facthash, previous, ivp); {
	case err != nil:
		st.Log().Error().Err(err).Msg("failed to process proposal")

		if errors.Is(err, context.Canceled) {
			return nil, nil
		}

		if err0 := st.pps.Close(); err0 != nil {
			return nil, e(err0, "failed to close proposal processors")
		}

		return nil, err
	case manifest == nil:
		return nil, nil
	default:
		st.Log().Debug().Msg("proposal processed")

		initialWait := time.Nanosecond
		if d := time.Since(started); d < st.policy.WaitProcessingProposal() {
			initialWait = st.policy.WaitProcessingProposal() - d
		}

		afact := isaac.NewACCEPTBallotFact(ivp.Point().Point, facthash, manifest.Hash())
		signedFact := isaac.NewACCEPTBallotSignedFact(st.local.Address(), afact)
		if err := signedFact.Sign(st.local.Privatekey(), st.policy.NetworkID()); err != nil {
			return nil, e(err, "")
		}

		bl := isaac.NewACCEPTBallot(ivp, signedFact)
		if err := st.broadcastACCEPTBallot(bl, true, initialWait); err != nil {
			return nil, e(err, "failed to broadcast accept ballot")
		}

		if err := st.timers.StartTimers([]util.TimerID{
			timerIDBroadcastINITBallot,
			timerIDBroadcastACCEPTBallot,
		}, true); err != nil {
			return nil, e(err, "failed to start timers for broadcasting accept ballot")
		}

		return manifest, nil
	}
}

func (st *ConsensusHandler) newVoteproof(vp base.Voteproof) error {
	e := util.StringErrorFunc("failed to handle new voteproof")

	var lvps lastVoteproofs
	switch l, v, err := st.baseHandler.setNewVoteproof(vp); {
	case err != nil:
		return e(err, "")
	case v == nil:
		return nil
	default:
		lvps = l
	}

	switch vp.Point().Stage() {
	case base.StageINIT:
		return st.newINITVoteproof(vp.(base.INITVoteproof), lvps)
	case base.StageACCEPT:
		return st.newACCEPTVoteproof(vp.(base.ACCEPTVoteproof), lvps)
	default:
		return e(nil, "invalid voteproof received, %T", vp)
	}
}

func (st *ConsensusHandler) newINITVoteproof(ivp base.INITVoteproof, lvps lastVoteproofs) error {
	c := lvps.cap()
	l := st.Log().With().
		Dict("init_voteproof", base.VoteproofLog(ivp)).
		Dict("last_voteproof", base.VoteproofLog(c)).
		Logger()

	l.Debug().Msg("new init voteproof received")

	switch c.Point().Stage() {
	case base.StageINIT:
		return st.newINITVoteproofWithLastINITVoteproof(ivp, lvps)
	case base.StageACCEPT:
		return st.newINITVoteproofWithLastACCEPTVoteproof(ivp, lvps)
	}

	return nil
}

func (st *ConsensusHandler) newACCEPTVoteproof(avp base.ACCEPTVoteproof, lvps lastVoteproofs) error {
	lvp := lvps.cap()
	l := st.Log().With().
		Dict("accept_voteproof", base.VoteproofLog(avp)).
		Dict("last_voteproof", base.VoteproofLog(lvp)).
		Logger()

	l.Debug().Msg("new accept voteproof received")

	switch lvp.Point().Stage() {
	case base.StageINIT:
		return st.newACCEPTVoteproofWithLastINITVoteproof(avp, lvps)
	case base.StageACCEPT:
		return st.newACCEPTVoteproofWithLastACCEPTVoteproof(avp, lvps)
	}

	return nil
}

func (st *ConsensusHandler) newINITVoteproofWithLastINITVoteproof(
	ivp base.INITVoteproof, lvps lastVoteproofs,
) error {
	livp := lvps.cap().(base.INITVoteproof)

	switch {
	case ivp.Point().Height() > livp.Point().Height(): // NOTE higher height; moves to syncing state
		return newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1)
	case livp.Result() == base.VoteResultMajority:
		return nil
	case ivp.Result() == base.VoteResultMajority: // NOTE new init voteproof has same height, but higher round
		lavp := lvps.accept()

		l := st.Log().With().
			Dict("init_voteproof", base.VoteproofLog(ivp)).
			Dict("last_init_voteproof", base.VoteproofLog(livp)).
			Dict("last_accept_voteproof", base.VoteproofLog(lavp)).
			Logger()

		if lavp == nil {
			return newBrokenSwitchContext(StateConsensus, errors.Errorf("empty last accept voteproof"))
		}

		if m := lavp.BallotMajority(); m == nil || !ivp.BallotMajority().PreviousBlock().Equal(m.NewBlock()) {
			// NOTE local stored block is different with other nodes
			l.Debug().
				Stringer("previous_block", ivp.BallotMajority().PreviousBlock()).
				Stringer("new_block", m.NewBlock()).
				Msg("previous block does not match with last accept voteproof; moves to syncing")

			return newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1)
		}

		go st.processProposal(ivp)

		return nil
	default:
		// NOTE new init voteproof draw; next round
		go st.nextRound(ivp, lvps)

		return nil
	}
}

func (st *ConsensusHandler) newINITVoteproofWithLastACCEPTVoteproof(
	ivp base.INITVoteproof, lvps lastVoteproofs,
) error {
	lavp := lvps.cap().(base.ACCEPTVoteproof)

	l := st.Log().With().
		Dict("init_voteproof", base.VoteproofLog(ivp)).
		Dict("last_accept_voteproof", base.VoteproofLog(lavp)).
		Logger()

	switch expectedheight := lavp.Point().Height() + 1; {
	case ivp.Point().Height() > expectedheight:
		return newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1)
	case ivp.Result() == base.VoteResultDraw:
		// NOTE new init voteproof draw; next round
		go st.nextRound(ivp, lvps)

		return nil
	default:
		if m := lavp.BallotMajority(); m == nil || !ivp.BallotMajority().PreviousBlock().Equal(m.NewBlock()) {
			// NOTE local stored block is different with other nodes
			l.Debug().
				Stringer("previous_block", ivp.BallotMajority().PreviousBlock()).
				Stringer("new_block", m.NewBlock()).
				Msg("previous block does not match with last accept voteproof; moves to syncing")

			return newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1)
		}
	}

	go st.processProposal(ivp)

	return nil
}

func (st *ConsensusHandler) newACCEPTVoteproofWithLastINITVoteproof(
	avp base.ACCEPTVoteproof, lvps lastVoteproofs,
) error {
	livp := lvps.cap().(base.INITVoteproof)
	switch {
	case avp.Point().Point.Equal(livp.Point().Point): // NOTE expected accept voteproof
		if avp.Result() == base.VoteResultMajority {
			return st.saveBlock(avp)
		}

		go st.nextRound(avp, lvps)

		return nil
	case avp.Point().Height() > livp.Point().Height():
	case avp.Result() == base.VoteResultDraw:
		go st.nextRound(avp, lvps)

		return nil
	}

	return newSyncingSwitchContext(StateConsensus, avp.Point().Height())
}

func (st *ConsensusHandler) newACCEPTVoteproofWithLastACCEPTVoteproof(
	avp base.ACCEPTVoteproof, lvps lastVoteproofs,
) error {
	lavp := lvps.cap().(base.ACCEPTVoteproof)
	switch {
	case avp.Point().Height() > lavp.Point().Height():
		return newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	case lavp.Result() == base.VoteResultMajority:
		return nil
	case avp.Result() == base.VoteResultDraw:
		go st.nextRound(avp, lvps)

		return nil
	default:
		return newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	}
}

func (st *ConsensusHandler) nextRound(vp base.Voteproof, lvps lastVoteproofs) {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	prevBlock := lvps.previousBlockForNextRound(vp)
	if prevBlock == nil {
		l.Debug().Msg("failed to find previous block from last voteproofs; ignore to move next round")

		return
	}

	st.baseHandler.nextRound(vp, prevBlock)
}

func (st *ConsensusHandler) nextBlock(avp base.ACCEPTVoteproof) {
	point := avp.Point().Point.Next()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Object("point", point).Logger()

	switch ok, err := st.isLocalInSuffrage(point.Height()); {
	case err != nil:
		l.Debug().Object("height", point.Height()).Msg("empty suffrage of next block; moves to broken state")

		go st.switchState(newBrokenSwitchContext(
			StateConsensus, errors.Wrap(err, "local not in suffrage for next block")))

		return
	case !ok:
		l.Debug().
			Object("height", point.Height()).
			Msg("local is not in suffrage at next block; moves to syncing state")

		go st.switchState(newSyncingSwitchContext(StateConsensus, point.Height()))

		return
	}

	// NOTE find next proposal
	pr, err := st.proposalSelector.Select(st.ctx, point)
	switch {
	case err == nil:
	case errors.Is(err, context.Canceled):
		l.Debug().Err(err).Msg("canceled to select proposal; ignore")

		return
	default:
		l.Error().Err(err).Msg("failed to select proposal")

		go st.switchState(newBrokenSwitchContext(StateConsensus, err))

		return
	}

	l.Debug().Interface("proposal", pr).Msg("proposal selected")

	// NOTE broadcast next init ballot
	e := util.StringErrorFunc("failed to prepare next block")
	fact := isaac.NewINITBallotFact(
		point,
		avp.BallotMajority().NewBlock(),
		pr.Fact().Hash(),
	)
	sf := isaac.NewINITBallotSignedFact(st.local.Address(), fact)

	if err := sf.Sign(st.local.Privatekey(), st.policy.NetworkID()); err != nil {
		go st.switchState(newBrokenSwitchContext(StateConsensus, e(err, "failed to make next init ballot")))

		return
	}

	bl := isaac.NewINITBallot(avp, sf)
	if err := st.broadcastINITBallot(bl, true); err != nil {
		go st.switchState(newBrokenSwitchContext(StateConsensus, e(err, "failed to broadcast next init ballot")))
	}

	if err := st.timers.StartTimers([]util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastACCEPTBallot,
	}, true); err != nil {
		l.Error().Err(e(err, "")).Msg("failed to start timers for broadcasting next init ballot")

		return
	}

	l.Debug().Interface("ballot", bl).Msg("next init ballot broadcasted")
}

func (st *ConsensusHandler) saveBlock(avp base.ACCEPTVoteproof) error {
	facthash := avp.BallotMajority().Proposal()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Logger()
	ll := l.With().Stringer("fact", facthash).Logger()

	ll.Debug().Msg("expected accept voteproof; trying to save proposal")

	switch err := st.pps.Save(context.Background(), facthash, avp); {
	case err == nil:
		ll.Debug().Msg("processed proposal saved; moves to next block")

		go st.nextBlock(avp)

		return nil
	case errors.Is(err, isaac.NotProposalProcessorProcessedError):
		l.Debug().Msg("no processed proposal; moves to syncing state")

		return newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	default:
		ll.Error().Err(err).Msg("failed to save proposal; moves to broken state")

		return newBrokenSwitchContext(StateConsensus, err)
	}
}

func (st *ConsensusHandler) isLocalInSuffrage(height base.Height) (bool /* in suffrage */, error) {
	suf := st.getSuffrage(height)
	switch {
	case suf == nil:
		return false, errors.Errorf("empty suffrage")
	case !suf.Exists(st.local.Address()):
		return false, nil
	default:
		return true, nil
	}
}

type consensusSwitchContext struct {
	baseSwitchContext
	ivp base.INITVoteproof
}

func newConsensusSwitchContext(from StateType, ivp base.INITVoteproof) consensusSwitchContext {
	return consensusSwitchContext{
		baseSwitchContext: newBaseSwitchContext(from, StateConsensus),
		ivp:               ivp,
	}
}
