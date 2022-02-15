package isaac

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var (
	timerIDBroadcastINITBallot   = util.TimerID("broadcast-init-ballot")
	timerIDBroadcastACCEPTBallot = util.TimerID("broadcast-accept-ballot")
	timerIDPrepareProposal       = util.TimerID("prepare-proposal")
)

/*

ConsensusHandler handles the consensus state mainly. Consensus state does,

- to join suffrage network; the suffrage network consists of multiple nodes and they make blocks by consensus process.
- to store block by voting within suffrage network.

* ConsensusHandler starts with voteproofs, init and accept voteproof.
* When starts, it prepares proposal.
* During entering consensus state, if local is removed from suffrage, moves to
 syncing state.
* If failed to prepare proposal, moves to broken state

To prepare init ballot,

* When preparing accept ballot, proposal also be generated and saved.
* Node selects proposer and tries to fetch proposal from proposer
  - if failed, tries from another proposer

*/
type ConsensusHandler struct {
	*baseStateHandler
	local            base.LocalNode
	policy           base.Policy
	proposalSelector ProposalSelector
	getSuffrage      func(base.Height) base.Suffrage
	pps              *proposalProcessors
}

func NewConsensusHandler(
	local base.LocalNode,
	policy base.Policy,
	proposalSelector ProposalSelector,
	getSuffrage func(base.Height) base.Suffrage,
	pps *proposalProcessors,
) *ConsensusHandler {
	return &ConsensusHandler{
		baseStateHandler: newBaseStateHandler(StateConsensus),
		local:            local,
		policy:           policy,
		proposalSelector: proposalSelector,
		getSuffrage:      getSuffrage,
		pps:              pps,
	}
}

func (st *ConsensusHandler) SetLogging(l *logging.Logging) *logging.Logging {
	_ = st.baseStateHandler.SetLogging(l)

	return st.Logging.SetLogging(l)
}

func (st *ConsensusHandler) enter(i stateSwitchContext) (func() error, error) {
	e := util.StringErrorFunc("failed to enter consensus state")

	deferred, err := st.baseStateHandler.enter(i)
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
		_ = st.setLastVoteproof(j.ivp)

		sctx = j
	}

	return func() error {
		if err := deferred(); err != nil {
			return e(err, "")
		}

		go st.processProposal(sctx.ivp)

		return nil
	}, nil
}

func (st *ConsensusHandler) exit() (func() error, error) {
	e := util.StringErrorFunc("failed to exit from consensus state")

	deferred, err := st.baseStateHandler.exit()
	if err != nil {
		return nil, e(err, "")
	}

	if err := st.pps.close(); err != nil {
		return nil, e(err, "failed to close proposal processors")
	}

	// NOTE stop timers
	return func() error {
		if err := deferred(); err != nil {
			return e(err, "")
		}

		if err := st.timers().StopTimers([]util.TimerID{
			timerIDBroadcastINITBallot,
			timerIDBroadcastACCEPTBallot,
			timerIDPrepareProposal,
		}); err != nil {
			return e(err, "")
		}

		return nil
	}, nil
}

func (st *ConsensusHandler) newProposal(pr base.ProposalFact) error {
	e := util.StringErrorFunc("failed to handle new proposal")

	if err := st.baseStateHandler.newProposal(pr); err != nil {
		return e(err, "")
	}

	return nil
}

func (st *ConsensusHandler) processProposal(ivp base.INITVoteproof) {
	facthash := ivp.BallotMajority().Proposal()
	l := st.Log().With().Stringer("fact", facthash).Logger()
	l.Debug().Msg("tyring to process proposal")

	e := util.StringErrorFunc("failed to process proposal")

	manifest, err := st.processProposalInternal(ivp)
	if err != nil {
		err = e(err, "")
		l.Error().Err(err).Msg("failed to process proposal; moves to broken state")

		go st.switchState(newBrokenSwitchContext(StateConsensus, err))

		return
	}

	l.Debug().Msg("proposal processed")

	eavp := st.lastACCEPTVoteproof()
	if eavp == nil || eavp.Point().Point != ivp.Point().Point {
		return
	}

	ll := l.With().Dict("accept_voteproof", base.VoteproofLog(eavp)).Logger()

	switch { // NOTE check last accept voteproof is the execpted
	case eavp.Result() != base.VoteResultMajority:
		if err := st.pps.close(); err != nil {
			ll.Error().Err(e(err, "failed to close processor")).
				Msg("expected accept voteproof is not majority result; cancel processor, but failed")

			return
		}

		ll.Debug().Msg("expected accept voteproof is not majority result; ignore")

		return
	case !manifest.Hash().Equal(eavp.BallotMajority().NewBlock()):
		if err := st.pps.close(); err != nil {
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

	l.Debug().Msg("expected accept voteproof found")
	switch err := st.pps.save(context.Background(), facthash, eavp); {
	case err == nil:
		l.Debug().Msg("processed proposal saved")
	case errors.Is(err, NotProposalProcessorProcessedError):
		l.Debug().Msg("no processed proposal; ignore")
	default:
		l.Error().Err(err).Msg("failed to save proposal; moves to broken state")

		go st.switchState(newBrokenSwitchContext(StateConsensus, err))
	}
}

func (st *ConsensusHandler) processProposalInternal(ivp base.INITVoteproof) (base.Manifest, error) {
	e := util.StringErrorFunc("failed to process proposal")

	facthash := ivp.BallotMajority().Proposal()

	ch := make(chan proposalProcessResult)
	defer close(ch)

	if err := st.pps.process(context.Background(), facthash, ch); err != nil {
		return nil, e(err, "")
	}

	r := <-ch

	st.Log().Trace().
		Stringer("fact", facthash).
		AnErr("result_error", r.err).
		Dict("manifest", base.ManifestLog(r.manifest)).
		Msg("proposal processed")

	switch {
	case r.err != nil:
		return nil, r.err
	case r.manifest == nil:
		return nil, nil
	default:
		afact := NewACCEPTBallotFact(r.fact.Point().Point, facthash, r.manifest.Hash())
		signedFact := NewACCEPTBallotSignedFact(st.local.Address(), afact)
		if err := signedFact.Sign(st.local.Privatekey(), st.policy.NetworkID()); err != nil {
			return nil, e(err, "")
		}

		bl := NewACCEPTBallot(ivp, signedFact)
		if err := st.broadcastBallot(bl, true); err != nil {
			return nil, e(err, "failed to broadcast accept ballot")
		}
	}

	return r.manifest, nil
}

func (st *ConsensusHandler) newVoteproof(vp base.Voteproof) error {
	e := util.StringErrorFunc("failed to handle new voteproof")

	if err := st.baseStateHandler.newVoteproof(vp); err != nil {
		return e(err, "")
	}

	lvp := st.lastVoteproof()
	l := st.Log().With().
		Dict("voteproof", base.VoteproofLog(vp)).
		Dict("last_voteproof", base.VoteproofLog(lvp)).
		Logger()

	if vp.Point().Compare(lvp.Point()) < 1 {
		l.Debug().Msg("new voteproof received, but old; ignore")

		return nil
	}

	switch vp.Point().Stage() {
	case base.StageINIT:
		return st.newINITVoteproof(vp.(base.INITVoteproof))
	case base.StageACCEPT:
		return st.newACCEPTVoteproof(vp.(base.ACCEPTVoteproof))
	default:
		return e(nil, "invalid voteproof received, %T", vp)
	}
}

func (st *ConsensusHandler) newINITVoteproof(ivp base.INITVoteproof) error {
	lvp := st.lastVoteproof()

	l := st.Log().With().
		Dict("init_voteproof", base.VoteproofLog(ivp)).
		Dict("last_voteproof", base.VoteproofLog(lvp)).
		Logger()

	_ = st.setLastVoteproof(ivp)

	l.Debug().Msg("new init voteproof received")

	switch lvp.Point().Stage() {
	case base.StageINIT:
		return st.newINITVoteproofWithLastINITVoteproof(ivp, lvp.(base.INITVoteproof))
	case base.StageACCEPT:
		return st.newINITVoteproofWithLastACCEPTVoteproof(ivp, lvp.(base.ACCEPTVoteproof))
	}

	return nil
}

func (st *ConsensusHandler) newACCEPTVoteproof(avp base.ACCEPTVoteproof) error {
	lvp := st.lastVoteproof()

	l := st.Log().With().
		Dict("accept_voteproof", base.VoteproofLog(avp)).
		Dict("last_voteproof", base.VoteproofLog(lvp)).
		Logger()

	l.Debug().Msg("new accept voteproof received")

	_ = st.setLastVoteproof(avp)

	switch lvp.Point().Stage() {
	case base.StageINIT:
		return st.newACCEPTVoteproofWithLastINITVoteproof(avp, lvp.(base.INITVoteproof))
	case base.StageACCEPT:
		return st.newACCEPTVoteproofWithLastACCEPTVoteproof(avp, lvp.(base.ACCEPTVoteproof))
	}

	return nil
}

func (st *ConsensusHandler) newINITVoteproofWithLastINITVoteproof(ivp, livp base.INITVoteproof) error {
	switch {
	case ivp.Point().Height() > livp.Point().Height(): // NOTE higher height; moves to syncing state
		return newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1)
	case livp.Result() == base.VoteResultMajority:
		return nil
	case ivp.Result() == base.VoteResultMajority: // NOTE new init voteproof has same height, but higher round
		lavp := st.lastACCEPTVoteproof()

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
		go st.nextRound(ivp)

		return nil
	}
}

func (st *ConsensusHandler) newINITVoteproofWithLastACCEPTVoteproof(
	ivp base.INITVoteproof, lavp base.ACCEPTVoteproof,
) error {
	l := st.Log().With().
		Dict("init_voteproof", base.VoteproofLog(ivp)).
		Dict("last_accept_voteproof", base.VoteproofLog(lavp)).
		Logger()

	switch expectedheight := lavp.Point().Height() + 1; {
	case ivp.Point().Height() > expectedheight:
		return newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1)
	case ivp.Result() == base.VoteResultDraw:
		// NOTE new init voteproof draw; next round
		go st.nextRound(ivp)

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
	avp base.ACCEPTVoteproof, livp base.INITVoteproof,
) error {
	switch {
	case avp.Point().Point == livp.Point().Point: // NOTE expected accept voteproof
		if avp.Result() == base.VoteResultMajority {
			go st.saveBlock(avp)

			return nil
		}

		go st.nextRound(avp)

		return nil
	case avp.Point().Height() > livp.Point().Height():
	case avp.Result() == base.VoteResultDraw:
		go st.nextRound(avp)

		return nil
	}

	return newSyncingSwitchContext(StateConsensus, avp.Point().Height())
}

func (st *ConsensusHandler) newACCEPTVoteproofWithLastACCEPTVoteproof(
	avp base.ACCEPTVoteproof, lavp base.ACCEPTVoteproof,
) error {
	switch {
	case avp.Point().Height() > lavp.Point().Height():
		return newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	case lavp.Result() == base.VoteResultMajority:
		return nil
	case avp.Result() == base.VoteResultDraw:
		go st.nextRound(avp)

		return nil
	default:
		return newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	}
}

func (st *ConsensusHandler) nextRound(vp base.Voteproof) {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	point := vp.Point().Point.NextRound()

	l.Debug().Object("point", point).Msg("preparing next round")

	// NOTE find next proposal
	pr, err := st.proposalSelector.Select(point) // BLOCK save selected proposal
	if err != nil {
		l.Error().Err(err).Msg("failed to select proposal")

		go st.switchState(newBrokenSwitchContext(StateConsensus, err))

		return
	}

	l.Debug().Interface("proposal", pr).Msg("proposal selected")

	e := util.StringErrorFunc("failed to move to next round")

	fact := NewINITBallotFact(
		point,
		st.lastINITMajorityVoteproof().BallotMajority().PreviousBlock(),
		pr.SignedFact().Fact().Hash(),
	)
	sf := NewINITBallotSignedFact(st.local.Address(), fact)

	if err := sf.Sign(st.local.Privatekey(), st.policy.NetworkID()); err != nil {
		go st.switchState(newBrokenSwitchContext(StateConsensus, e(err, "failed to make next round init ballot")))

		return
	}

	bl := NewINITBallot(vp, sf)
	if err := st.broadcastBallot(bl, true); err != nil {
		go st.switchState(newBrokenSwitchContext(StateConsensus, e(err, "failed to broadcast next round init ballot")))
	}

	l.Debug().Interface("ballot", bl).Msg("next round init ballot broadcasted")
}

func (st *ConsensusHandler) nextBlock(avp base.ACCEPTVoteproof) {
	point := avp.Point().Point.Next()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Object("point", point).Logger()

	// NOTE find next proposal
	pr, err := st.proposalSelector.Select(point) // BLOCK save selected proposal
	if err != nil {
		l.Error().Err(err).Msg("failed to select proposal")

		go st.switchState(newBrokenSwitchContext(StateConsensus, err))

		return
	}

	l.Debug().Interface("proposal", pr).Msg("proposal selected")

	// NOTE broadcast next init ballot
	e := util.StringErrorFunc("failed to prepare next block")
	fact := NewINITBallotFact(
		point,
		avp.BallotMajority().NewBlock(),
		pr.SignedFact().Fact().Hash(),
	)
	sf := NewINITBallotSignedFact(st.local.Address(), fact)

	if err := sf.Sign(st.local.Privatekey(), st.policy.NetworkID()); err != nil {
		go st.switchState(newBrokenSwitchContext(StateConsensus, e(err, "failed to make next init ballot")))

		return
	}

	bl := NewINITBallot(avp, sf)
	if err := st.broadcastBallot(bl, true); err != nil {
		go st.switchState(newBrokenSwitchContext(StateConsensus, e(err, "failed to broadcast next init ballot")))
	}

	l.Debug().Interface("ballot", bl).Msg("next init ballot broadcasted")
}

func (st *ConsensusHandler) saveBlock(avp base.ACCEPTVoteproof) {
	facthash := avp.BallotMajority().Proposal()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Logger()
	ll := l.With().Stringer("fact", facthash).Logger()

	ll.Debug().Msg("expected accept voteproof; trying to save proposal")

	switch err := st.pps.save(context.Background(), facthash, avp); {
	case err == nil:
		ll.Debug().Msg("processed proposal saved; moves to next block")

		go st.nextBlock(avp)
	case errors.Is(err, NotProposalProcessorProcessedError):
		l.Debug().Msg("no processed proposal; moves to syncing state")

		go st.switchState(newSyncingSwitchContext(StateConsensus, avp.Point().Height()))
	default:
		ll.Error().Err(err).Msg("failed to save proposal; moves to broken state")

		go st.switchState(newBrokenSwitchContext(StateConsensus, err))
	}
}

type consensusSwitchContext struct {
	baseStateSwitchContext
	ivp base.INITVoteproof
}

func newConsensusSwitchContext(from StateType, ivp base.INITVoteproof) consensusSwitchContext {
	return consensusSwitchContext{
		baseStateSwitchContext: newBaseStateSwitchContext(from, StateConsensus),
		ivp:                    ivp,
	}
}
