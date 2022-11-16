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

type ConsensusHandler struct {
	*baseBallotHandler
	pps                  *isaac.ProposalProcessors
	getManifest          func(base.Height) (base.Manifest, error)
	nodeInConsensusNodes isaac.NodeInConsensusNodesFunc
	whenNewBlockSaved    func(base.Height)
}

type NewConsensusHandlerType struct {
	*ConsensusHandler
}

func NewNewConsensusHandlerType(
	local base.LocalNode,
	params *isaac.LocalParams,
	proposalSelector isaac.ProposalSelector,
	pps *isaac.ProposalProcessors,
	getManifest func(base.Height) (base.Manifest, error),
	nodeInConsensusNodes isaac.NodeInConsensusNodesFunc,
	voteFunc func(base.Ballot) (bool, error),
	whenNewBlockSaved func(base.Height),
	svf SuffrageVotingFindFunc,
) *NewConsensusHandlerType {
	baseBallotHandler := newBaseBallotHandler(StateConsensus, local, params, proposalSelector, svf)

	if voteFunc != nil {
		baseBallotHandler.voteFunc = preventVotingWithEmptySuffrage(voteFunc, local, nodeInConsensusNodes)
	}

	return &NewConsensusHandlerType{
		ConsensusHandler: &ConsensusHandler{
			baseBallotHandler:    baseBallotHandler,
			pps:                  pps,
			getManifest:          getManifest,
			nodeInConsensusNodes: nodeInConsensusNodes,
			whenNewBlockSaved:    whenNewBlockSaved,
		},
	}
}

func (h *NewConsensusHandlerType) new() (handler, error) {
	return &ConsensusHandler{
		baseBallotHandler:    h.baseBallotHandler.new(),
		getManifest:          h.getManifest,
		nodeInConsensusNodes: h.nodeInConsensusNodes,
		whenNewBlockSaved:    h.whenNewBlockSaved,
		pps:                  h.pps,
	}, nil
}

func (st *ConsensusHandler) enter(from StateType, i switchContext) (func(), error) {
	e := util.StringErrorFunc("failed to enter consensus state")

	deferred, err := st.baseBallotHandler.enter(from, i)
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

	switch suf, found, err := st.nodeInConsensusNodes(st.local, sctx.ivp.Point().Height()); { //nolint:govet //...
	case errors.Is(err, storage.ErrNotFound):
		st.Log().Debug().
			Dict("state_context", switchContextLog(sctx)).
			Interface("height", sctx.ivp.Point().Height()).
			Msg("suffrage not found at entering consensus state; moves to syncing state")

		return nil, newSyncingSwitchContext(StateConsensus, sctx.ivp.Point().Height())
	case err != nil:
		return nil, e(err, "")
	case suf == nil || suf.Len() < 1:
		return nil, e(nil, "empty suffrage of init voteproof")
	case !found:
		st.Log().Debug().
			Dict("state_context", switchContextLog(sctx)).
			Interface("height", sctx.ivp.Point().Height()).
			Msg("local is not in consensus nodes at entering consensus state; moves to syncing state")

		return nil, newSyncingSwitchContext(StateConsensus, sctx.ivp.Point().Height())
	}

	process := func() {}

	// NOTE suffrage sign voting
	switch keep, err := st.checkSuffrageVoting(sctx.ivp); {
	case err != nil:
		return nil, e(err, "")
	case keep:
		f, err := st.processProposal(sctx.ivp)
		if err != nil {
			return nil, e(err, "")
		}

		process = f
	}

	return func() {
		deferred()

		process()
	}, nil
}

func (st *ConsensusHandler) exit(sctx switchContext) (func(), error) {
	e := util.StringErrorFunc("failed to exit from consensus state")

	deferred, err := st.baseBallotHandler.exit(sctx)
	if err != nil {
		return nil, e(err, "")
	}

	if err := st.pps.Cancel(); err != nil {
		return nil, e(err, "failed to cancel proposal processors")
	}

	return deferred, nil
}

func (st *ConsensusHandler) processProposalFunc(ivp base.INITVoteproof) (func(context.Context) error, error) {
	facthash := ivp.BallotMajority().Proposal()
	l := st.Log().With().Stringer("fact", facthash).Logger()
	l.Debug().Msg("trying to process proposal")

	e := util.StringErrorFunc("failed to process proposal")

	var process isaac.ProcessorProcessFunc

	switch i, err := st.processProposalInternal(ivp); {
	case err == nil:
		if i == nil {
			l.Debug().Msg("empty manifest; ignore")

			return nil, nil
		}

		process = i
	case errors.Is(err, isaac.ErrNotProposalProcessorProcessed):
		go st.nextRound(ivp, ivp.BallotMajority().PreviousBlock())

		return nil, nil
	default:
		err = e(err, "")

		l.Error().Err(err).Msg("failed to process proposal; moves to broken state")

		return nil, newBrokenSwitchContext(StateConsensus, err)
	}

	return func(ctx context.Context) error {
		manifest, err := process(ctx)

		switch {
		case err != nil:
			return e(err, "")
		case manifest == nil:
			return nil
		}

		eavp := st.lastVoteproofs().ACCEPT()

		if err := st.prepareACCEPTBallot(ivp, manifest, time.Nanosecond); err != nil {
			l.Error().Err(err).Msg("failed to prepare accept ballot")

			return e(err, "")
		}

		if eavp == nil || !eavp.Point().Point.Equal(ivp.Point().Point) {
			return nil
		}

		ll := l.With().Dict("accept_voteproof", base.VoteproofLog(eavp)).Logger()

		var sctx switchContext

		switch saved, err := st.handleACCEPTVoteproofAfterProcessingProposal(manifest, eavp); {
		case saved:
			ll.Debug().Msg("new block saved by accept voteproof after processing proposal")
		case err == nil:
			return nil
		case errors.As(err, &sctx):
		default:
			ll.Error().Err(err).Msg("failed to save new block by accept voteproof after processing proposal")

			sctx = newBrokenSwitchContext(StateConsensus, errors.Wrap(err, "failed to save proposal"))
		}

		return sctx
	}, nil
}

func (st *ConsensusHandler) processProposal(ivp base.INITVoteproof) (func(), error) {
	f, err := st.processProposalFunc(ivp)

	switch {
	case err != nil:
		return nil, err
	case f == nil:
		return func() {}, nil
	}

	return func() {
		var sctx switchContext

		switch err := f(st.ctx); {
		case err == nil:
		case errors.As(err, &sctx):
			go st.switchState(sctx)
		default:
			go st.switchState(newBrokenSwitchContext(StateConsensus, err))
		}
	}, nil
}

func (st *ConsensusHandler) processProposalInternal(ivp base.INITVoteproof) (isaac.ProcessorProcessFunc, error) {
	e := util.StringErrorFunc("failed to process proposal")

	facthash := ivp.BallotMajority().Proposal()

	var previous base.Manifest

	switch m, err := st.getManifest(ivp.Point().Height() - 1); {
	case err != nil:
		return nil, e(err, "")
	default:
		previous = m
	}

	switch process, err := st.pps.Process(st.ctx, facthash, previous, ivp); {
	case err != nil:
		return nil, e(err, "")
	case process == nil:
		return nil, nil
	default:
		return func(ctx context.Context) (base.Manifest, error) {
			switch manifest, err := process(ctx); {
			case err != nil:
				st.Log().Error().Err(err).Msg("failed to process proposal")

				if errors.Is(err, context.Canceled) {
					return nil, err
				}

				if err0 := st.pps.Cancel(); err0 != nil {
					return nil, e(err0, "failed to cancel proposal processors")
				}

				return nil, err
			case manifest == nil:
				st.Log().Debug().Msg("empty manifest; already processed")

				return nil, nil
			default:
				st.Log().Debug().Msg("proposal processed")

				return manifest, nil
			}
		}, nil
	}
}

func (st *ConsensusHandler) handleACCEPTVoteproofAfterProcessingProposal(
	manifest base.Manifest, avp base.ACCEPTVoteproof,
) (saved bool, _ error) {
	l := st.Log().With().Dict("accept_voteproof", base.VoteproofLog(avp)).Logger()

	switch { // NOTE check last accept voteproof is the execpted
	case avp.Result() != base.VoteResultMajority:
		if err := st.pps.Cancel(); err != nil {
			l.Error().Err(err).
				Msg("expected accept voteproof is not majority result; cancel processor, but failed")

			return false, err
		}

		l.Debug().Msg("expected accept voteproof is not majority result; ignore")

		return false, nil
	case !manifest.Hash().Equal(avp.BallotMajority().NewBlock()):
		if err := st.pps.Cancel(); err != nil {
			l.Error().Err(err).
				Msg("expected accept voteproof has different new block; cancel processor, but failed")

			return false, err
		}

		l.Debug().Msg("expected accept voteproof has different new block; moves to syncing")

		return false, newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	default:
		l.Debug().Msg("proposal processed and expected voteproof found")
	}

	var sctx switchContext

	switch i, err := st.saveBlock(avp); {
	case err == nil:
		saved = i
	case errors.As(err, &sctx):
	default:
		sctx = newBrokenSwitchContext(StateConsensus, errors.Wrap(err, "failed to save proposal"))
	}

	return saved, sctx
}

func (st *ConsensusHandler) prepareINITBallot(
	bl base.INITBallot,
	ids []util.TimerID,
	initialWait time.Duration,
	interval func(int, time.Duration) time.Duration,
) error {
	go func() {
		<-time.After(initialWait)

		switch _, err := st.vote(bl); {
		case err == nil:
		case errors.Is(err, errFailedToVoteNotInConsensus):
			st.Log().Debug().Err(err).Msg("failed to vote init ballot; moves to syncing state")

			go st.switchState(newSyncingSwitchContext(StateConsensus, bl.Point().Height()-1))
		default:
			st.Log().Debug().Err(err).Msg("failed to vote init ballot; moves to broken state")

			go st.switchState(newBrokenSwitchContext(StateConsensus, err))
		}
	}()

	if err := st.broadcastINITBallot(bl, interval); err != nil {
		return err
	}

	return st.timers.StartTimers(ids, true)
}

func (st *ConsensusHandler) prepareSIGNBallot(bl base.INITBallot) error {
	go func() {
		switch _, err := st.vote(bl); {
		case err == nil:
		case errors.Is(err, errFailedToVoteNotInConsensus):
			st.Log().Debug().Err(err).Msg("failed to vote sign ballot; moves to syncing state")

			go st.switchState(newSyncingSwitchContext(StateConsensus, bl.Point().Height()-1))
		default:
			st.Log().Debug().Err(err).Msg("failed to vote sign ballot; moves to broken state")

			go st.switchState(newBrokenSwitchContext(StateConsensus, err))
		}
	}()

	if err := broadcastBallot(
		bl,
		st.timers,
		timerIDBroadcastSIGNBallot,
		st.broadcastBallotFunc,
		st.Log(),
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
			timerIDBroadcastSIGNBallot,
			timerIDBroadcastACCEPTBallot,
		},
		true,
	)
}

func (st *ConsensusHandler) prepareACCEPTBallot(
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
	signfact := isaac.NewACCEPTBallotSignFact(st.local.Address(), afact)

	if err := signfact.Sign(st.local.Privatekey(), st.params.NetworkID()); err != nil {
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
		timerIDBroadcastSIGNBallot,
		timerIDBroadcastACCEPTBallot,
	}, true); err != nil {
		return e(err, "failed to start timers for broadcasting accept ballot")
	}

	return nil
}

func (st *ConsensusHandler) newVoteproof(vp base.Voteproof) error {
	var lvps LastVoteproofs

	switch i, v, isnew := st.baseBallotHandler.setNewVoteproof(vp); {
	case v == nil, !isnew:
		return nil
	default:
		lvps = i
	}

	e := util.StringErrorFunc("failed to handle new voteproof")

	switch vp.Point().Stage() {
	case base.StageINIT:
		return st.newINITVoteproof(vp.(base.INITVoteproof), lvps) //nolint:forcetypeassert //...
	case base.StageACCEPT:
		return st.newACCEPTVoteproof(vp.(base.ACCEPTVoteproof), lvps) //nolint:forcetypeassert //...
	default:
		return e(nil, "invalid voteproof received, %T", vp)
	}
}

func (st *ConsensusHandler) newINITVoteproof(ivp base.INITVoteproof, lvps LastVoteproofs) error {
	c := lvps.Cap()
	l := st.Log().With().
		Dict("init_voteproof", base.VoteproofLog(ivp)).
		Dict("last_voteproof", base.VoteproofLog(c)).
		Logger()

	l.Debug().Msg("new init voteproof received")

	switch c.Point().Stage() { //nolint:exhaustive //...
	case base.StageINIT:
		return st.newINITVoteproofWithLastINITVoteproof(ivp, lvps)
	case base.StageACCEPT:
		return st.newINITVoteproofWithLastACCEPTVoteproof(ivp, lvps)
	}

	return nil
}

func (st *ConsensusHandler) newACCEPTVoteproof(avp base.ACCEPTVoteproof, lvps LastVoteproofs) error {
	lvp := lvps.Cap()
	l := st.Log().With().
		Dict("accept_voteproof", base.VoteproofLog(avp)).
		Dict("last_voteproof", base.VoteproofLog(lvp)).
		Logger()

	l.Debug().Msg("new accept voteproof received")

	switch lvp.Point().Stage() { //nolint:exhaustive //...
	case base.StageINIT:
		return st.newACCEPTVoteproofWithLastINITVoteproof(avp, lvps)
	case base.StageACCEPT:
		return st.newACCEPTVoteproofWithLastACCEPTVoteproof(avp, lvps)
	}

	return nil
}

func (st *ConsensusHandler) newINITVoteproofWithLastINITVoteproof(
	ivp base.INITVoteproof, lvps LastVoteproofs,
) error {
	livp := lvps.Cap().(base.INITVoteproof) //nolint:forcetypeassert //...

	l := st.Log().With().
		Dict("last_init_voteproof", base.VoteproofLog(livp)).
		Logger()

	switch {
	case ivp.Point().Height() > livp.Point().Height(): // NOTE higher height; moves to syncing state
		l.Debug().Msg("higher init voteproof; moves to syncing state")

		return newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1)
	case livp.Result() == base.VoteResultMajority:
	case ivp.Result() != base.VoteResultMajority: // NOTE new init voteproof has same height, but higher round
		l.Debug().Msg("new init voteproof draw; moves to next round")

		go st.nextRound(ivp, lvps.PreviousBlockForNextRound(ivp))

		return nil
	}

	lavp := lvps.ACCEPT()

	ll := st.Log().With().
		Dict("init_voteproof", base.VoteproofLog(ivp)).
		Dict("last_accept_voteproof", base.VoteproofLog(lavp)).
		Logger()

	if lavp == nil {
		ll.Debug().Msg("empty last accept voteproof; moves to broken state")

		return newBrokenSwitchContext(StateConsensus, errors.Errorf("empty last accept voteproof"))
	}

	if m := lavp.BallotMajority(); m == nil || !ivp.BallotMajority().PreviousBlock().Equal(m.NewBlock()) {
		// NOTE local stored block is different with other nodes
		ll.Debug().
			Stringer("previous_block", ivp.BallotMajority().PreviousBlock()).
			Stringer("new_block", m.NewBlock()).
			Msg("previous block does not match with last accept voteproof; moves to syncing")

		return newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1)
	}

	switch keep, err := st.checkSuffrageVoting(ivp); {
	case err != nil:
		return err
	case !keep:
		return nil
	default:
		process, err := st.processProposal(ivp)
		if err != nil {
			return err
		}

		go process()

		return nil
	}
}

func (st *ConsensusHandler) newINITVoteproofWithLastACCEPTVoteproof(
	ivp base.INITVoteproof, lvps LastVoteproofs,
) error {
	lavp := lvps.Cap().(base.ACCEPTVoteproof) //nolint:forcetypeassert //...

	l := st.Log().With().
		Dict("init_voteproof", base.VoteproofLog(ivp)).
		Dict("last_accept_voteproof", base.VoteproofLog(lavp)).
		Logger()

	switch expectedheight := lavp.Point().Height() + 1; {
	case ivp.Point().Height() > expectedheight:
		l.Debug().Msg("higher init voteproof; moves to syncing state")

		return newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1)
	case ivp.Result() == base.VoteResultDraw:
		l.Debug().Msg("new init voteproof draw; moves to next round")

		go st.nextRound(ivp, lvps.PreviousBlockForNextRound(ivp))

		return nil
	default:
		if m := lavp.BallotMajority(); m == nil || !ivp.BallotMajority().PreviousBlock().Equal(m.NewBlock()) {
			// NOTE local stored block is different with other nodes
			l.Debug().
				Stringer("previous_block", ivp.BallotMajority().PreviousBlock()).
				Interface("majority", m).
				Msg("previous block does not match with last accept voteproof; moves to syncing")

			return newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1)
		}
	}

	// NOTE suffrage sign voting
	switch keep, err := st.checkSuffrageVoting(ivp); {
	case err != nil:
		return err
	case !keep:
		return nil
	default:
		process, err := st.processProposal(ivp)
		if err != nil {
			return err
		}

		go process()

		return nil
	}
}

func (st *ConsensusHandler) newACCEPTVoteproofWithLastINITVoteproof(
	avp base.ACCEPTVoteproof, lvps LastVoteproofs,
) error {
	livp := lvps.Cap().(base.INITVoteproof) //nolint:forcetypeassert //...

	switch {
	case avp.Point().Point.Equal(livp.Point().Point): // NOTE expected accept voteproof
		if avp.Result() == base.VoteResultMajority {
			_, err := st.saveBlock(avp)

			return err
		}

		go st.nextRound(avp, lvps.PreviousBlockForNextRound(avp))

		return nil
	case avp.Point().Height() > livp.Point().Height():
	case avp.Result() == base.VoteResultDraw:
		go st.nextRound(avp, lvps.PreviousBlockForNextRound(avp))

		return nil
	}

	return newSyncingSwitchContext(StateConsensus, avp.Point().Height())
}

func (st *ConsensusHandler) newACCEPTVoteproofWithLastACCEPTVoteproof(
	avp base.ACCEPTVoteproof, lvps LastVoteproofs,
) error {
	lavp := lvps.Cap().(base.ACCEPTVoteproof) //nolint:forcetypeassert //...

	l := st.Log().With().
		Dict("last_accept_voteproof", base.VoteproofLog(lavp)).
		Logger()

	switch {
	case avp.Point().Height() > lavp.Point().Height():
		l.Debug().Msg("higher accept voteproof; moves to syncing state")

		return newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	case avp.Result() == base.VoteResultDraw:
		l.Debug().Msg("new accept voteproof draw; moves to next round")

		go st.nextRound(avp, lvps.PreviousBlockForNextRound(avp))

		return nil
	default:
		return newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	}
}

func (st *ConsensusHandler) nextRound(vp base.Voteproof, previousBlock util.Hash) {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	started := time.Now()

	if previousBlock == nil {
		l.Debug().Msg("failed to find previous block from last voteproofs; ignore to move next round")

		return
	}

	var sctx switchContext
	var bl base.INITBallot

	switch i, err := st.prepareNextRound(vp, previousBlock, st.nodeInConsensusNodes); {
	case err == nil:
		if i == nil {
			return
		}

		bl = i
	case errors.As(err, &sctx):
		go st.switchState(sctx)

		return
	default:
		l.Debug().Err(err).Msg("failed to prepare next round; moves to broken state")

		go st.switchState(newBrokenSwitchContext(StateConsensus, err))

		return
	}

	initialWait := time.Nanosecond
	if d := time.Since(started); d < st.params.WaitPreparingINITBallot() {
		initialWait = st.params.WaitPreparingINITBallot() - d // FIXME remove duration?
	}

	if err := st.prepareINITBallot(
		bl,
		[]util.TimerID{
			timerIDBroadcastINITBallot,
			timerIDBroadcastSIGNBallot,
			timerIDBroadcastACCEPTBallot,
		},
		initialWait,
		func(i int, _ time.Duration) time.Duration {
			lvp := st.lastVoteproofs().Cap()
			if bl.Point().Compare(lvp.Point()) < 0 &&
				bl.Point().Height() == lvp.Point().Height() &&
				lvp.Point().Stage() == base.StageINIT &&
				lvp.Result() == base.VoteResultMajority {
				return 0
			}

			if i < 1 {
				return initialWait
			}

			return st.params.IntervalBroadcastBallot()
		},
	); err != nil {
		l.Error().Err(err).Msg("failed to prepare init ballot for next round")

		return
	}

	l.Debug().Interface("ballot", bl).Msg("init ballot broadcasted for next round")
}

func (st *ConsensusHandler) nextBlock(avp base.ACCEPTVoteproof) {
	point := avp.Point().Point.NextHeight()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Object("point", point).Logger()

	started := time.Now()

	var sctx switchContext
	var bl base.INITBallot

	switch i, err := st.prepareNextBlock(avp, st.nodeInConsensusNodes); {
	case err == nil:
		if i == nil {
			return
		}

		bl = i
	case errors.As(err, &sctx):
		go st.switchState(sctx)

		return
	default:
		l.Debug().Err(err).Msg("failed to prepare next block; moves to broken state")

		go st.switchState(newBrokenSwitchContext(StateConsensus, err))

		return
	}

	initialWait := time.Nanosecond
	if d := time.Since(started); d < st.params.WaitPreparingINITBallot() {
		initialWait = st.params.WaitPreparingINITBallot() - d
	}

	if err := st.prepareINITBallot(
		bl,
		[]util.TimerID{
			timerIDBroadcastINITBallot,
			timerIDBroadcastACCEPTBallot,
		},
		initialWait,
		func(i int, _ time.Duration) time.Duration {
			if i < 1 {
				return initialWait
			}

			return st.params.IntervalBroadcastBallot()
		},
	); err != nil {
		l.Error().Err(err).Msg("failed to prepare init ballot for next block")

		return
	}

	l.Debug().Interface("ballot", bl).Msg("next init ballot broadcasted")
}

func (st *ConsensusHandler) suffrageSIGNVoting(vp base.Voteproof) {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	if _, ok := vp.(isaac.WithdrawVoteproof); !ok {
		l.Error().Msg("expected WithdrawVoteproof for suffrage sign voting")

		return
	}

	ifact := vp.Majority().(isaac.INITBallotFact) //nolint:forcetypeassert //...
	withdrawfacts := ifact.WithdrawFacts()

	fact := isaac.NewSIGNBallotFact(
		vp.Point().Point,
		ifact.PreviousBlock(),
		ifact.Proposal(),
		withdrawfacts,
	)

	sf := isaac.NewINITBallotSignFact(st.local.Address(), fact)

	if err := sf.Sign(st.local.Privatekey(), st.params.NetworkID()); err != nil {
		go st.switchState(newBrokenSwitchContext(st.stt, errors.WithMessage(err, "failed to make sign ballot")))

		return
	}

	bl := isaac.NewINITBallot(vp, sf, nil)

	if err := st.prepareSIGNBallot(bl); err != nil {
		l.Error().Err(err).Msg("failed to prepare sign ballot")

		return
	}

	l.Debug().Interface("ballot", bl).Msg("sign ballot broadcasted")
}

func (st *ConsensusHandler) saveBlock(avp base.ACCEPTVoteproof) (bool, error) {
	facthash := avp.BallotMajority().Proposal()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Logger()
	ll := l.With().Stringer("fact", facthash).Logger()

	ll.Debug().Msg("expected accept voteproof; trying to save proposal")

	switch err := st.pps.Save(context.Background(), facthash, avp); {
	case err == nil:
		ll.Debug().Msg("processed proposal saved; moves to next block")

		go st.whenNewBlockSaved(avp.Point().Height())
		go st.nextBlock(avp)

		return true, nil
	case errors.Is(err, isaac.ErrProcessorAlreadySaved):
		l.Debug().Msg("already saved")

		return false, nil
	case errors.Is(err, isaac.ErrNotProposalProcessorProcessed):
		l.Debug().Msg("no processed proposal; moves to syncing state")

		return false, newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	default:
		ll.Error().Err(err).Msg("failed to save proposal; moves to broken state")

		return false, newBrokenSwitchContext(StateConsensus, err)
	}
}

func (st *ConsensusHandler) checkSuffrageVoting(ivp base.INITVoteproof) (bool, error) {
	switch wvp, ok := ivp.(isaac.WithdrawVoteproof); {
	case !ok:
		return true, nil
	case len(wvp.Withdraws()) < 1:
		return true, nil
	}

	switch t := ivp.Majority().(type) {
	case isaac.INITBallotFact:
		go st.suffrageSIGNVoting(ivp)

		return false, nil
	case isaac.SIGNBallotFact:
		return true, nil
	default:
		return false, errors.Errorf("expected SIGNBallotFact, but %T", t)
	}
}

type consensusSwitchContext struct {
	ivp base.INITVoteproof
	baseSwitchContext
}

func newConsensusSwitchContext(from StateType, ivp base.INITVoteproof) consensusSwitchContext {
	return consensusSwitchContext{
		baseSwitchContext: newBaseSwitchContext(StateConsensus, switchContextOKFuncCheckFrom(from)),
		ivp:               ivp,
	}
}
