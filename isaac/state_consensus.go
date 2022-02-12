package isaac

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

var (
	timerIDBroadcastINITBallot = util.TimerID("broadcast-init-ballot")
	timerIDPrepareProposal     = util.TimerID("preppare-proposal")
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
	ivp              *util.Locked
	avp              *util.Locked
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
		ivp:              util.NewLocked(nil),
		avp:              util.NewLocked(nil),
		getSuffrage:      getSuffrage,
		pps:              pps,
	}
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
		return nil, e(nil, "invalid stateSwitchContext, empty init voteproof in stateSwitchContext")
	default:
		_ = st.ivp.SetValue(j.ivp)

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
			timerIDPrepareProposal,
		}); err != nil {
			return e(err, "")
		}

		return nil
	}, nil
}

func (st *ConsensusHandler) lastINITVoteproof() base.INITVoteproof {
	i := st.ivp.Value()
	if i == nil {
		return nil
	}

	return i.(base.INITVoteproof)
}

func (st *ConsensusHandler) lastACCEPTVoteproof() base.ACCEPTVoteproof {
	i := st.avp.Value()
	if i == nil {
		return nil
	}

	return i.(base.ACCEPTVoteproof)
}

func (st *ConsensusHandler) newVoteproof(vp base.Voteproof) error {
	e := util.StringErrorFunc("failed to handle new voteproof")

	if err := st.baseStateHandler.newVoteproof(vp); err != nil {
		return e(err, "")
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

	if err := st.processProposalInternal(ivp); err != nil {
		l.Error().Err(err).Msg("failed to process proposal; moves to broken state")

		go st.switchState(newBrokenSwitchContext(StateConsensus, err))

		return
	}

	l.Debug().Msg("proposal processed")

	eavp := st.lastACCEPTVoteproof()
	switch { // NOTE check last accept voteproof is the execpted
	case eavp == nil:
		return
	case eavp.Point().Point != ivp.Point().Point:
		return
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

func (st *ConsensusHandler) processProposalInternal(ivp base.INITVoteproof) error {
	e := util.StringErrorFunc("failed to process proposal")

	facthash := ivp.BallotMajority().Proposal()

	ch := make(chan proposalProcessResult)
	defer close(ch)

	if err := st.pps.process(context.Background(), facthash, ch); err != nil {
		return e(err, "")
	}

	r := <-ch

	st.Log().Trace().
		Stringer("fact", facthash).
		AnErr("result_error", r.err).
		Dict("manifest", base.ManifestLog(r.manifest)).
		Msg("proposal processed")

	switch {
	case r.err != nil:
		return r.err
	case r.manifest == nil:
		return nil
	default:
		afact := NewACCEPTBallotFact(r.fact.Point().Point, facthash, r.manifest.Hash())
		signedFact := NewACCEPTBallotSignedFact(st.local.Address(), afact)
		if err := signedFact.Sign(st.local.Privatekey(), st.policy.NetworkID()); err != nil {
			return e(err, "")
		}

		bl := NewACCEPTBallot(ivp, signedFact)
		if err := st.broadcastBallot(bl, true); err != nil {
			return e(err, "failed to broadcast accept ballot")
		}
	}

	return nil
}

func (st *ConsensusHandler) newINITVoteproof(ivp base.INITVoteproof) error {
	// BLOCK set last init voteproof; only VoteResultMajority

	return nil
}

func (st *ConsensusHandler) newACCEPTVoteproof(avp base.ACCEPTVoteproof) error {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Logger()

	// NOTE check accept voteproof is the expected, if not moves to syncing state
	ivp := st.lastINITVoteproof()
	switch c := avp.Point().Point.Compare(ivp.Point().Point); {
	case c < 0:
		l.Debug().Msg("old voteproof received; ignored")

		return nil
	case c > 0:
		l.Debug().Dict("init_voteproof", base.VoteproofLog(ivp)).Msg("higher voteproof received; moves to sync")

		return newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	}

	if avp.Result() == base.VoteResultDraw { // NOTE draw, starts next round
		l.Debug().Msg("expected new accept voteproof received, but draw, moves to next round")

		go st.nextRound(avp)

		return nil
	}

	_ = st.avp.SetValue(avp)

	l.Debug().Msg("expected new accept voteproof received")

	go func() {
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
	}()

	return nil
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
		st.lastINITVoteproof().BallotMajority().PreviousBlock(),
		pr.SignedFact().Fact().Hash(),
	)
	sf := NewINITBallotSignedFact(st.local.Address(), fact)

	if err := sf.Sign(st.local.Privatekey(), st.policy.NetworkID()); err != nil {
		go st.switchState(newBrokenSwitchContext(StateConsensus, e(err, "failed to make next init ballot")))

		return
	}

	bl := NewINITBallot(vp, sf)
	if err := st.broadcastBallot(bl, true); err != nil {
		go st.switchState(newBrokenSwitchContext(StateConsensus, e(err, "failed to broadcast accept ballot")))
	}

	l.Debug().Interface("ballot", bl).Msg("next round init ballot broadcasted")
}

func (st *ConsensusHandler) nextBlock(avp base.ACCEPTVoteproof) {
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
