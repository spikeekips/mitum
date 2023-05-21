package isaacstates

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type HandoverHandlerArgs struct {
	voteproofHandlerArgs
}

func NewHandoverHandlerArgs() *HandoverHandlerArgs {
	return &HandoverHandlerArgs{
		voteproofHandlerArgs: newVoteproofHandlerArgs(),
	}
}

type HandoverHandler struct {
	*voteproofHandler
	args                  *HandoverHandlerArgs
	finishedWithVoteproof *util.Locked[bool]
}

type NewHandoverHandlerType struct {
	*HandoverHandler
}

func NewNewHandoverHandlerType(
	local base.LocalNode,
	params *isaac.LocalParams,
	args *HandoverHandlerArgs,
) *NewHandoverHandlerType {
	return &NewHandoverHandlerType{
		HandoverHandler: &HandoverHandler{
			voteproofHandler: newVoteproofHandler(StateHandover, local, params, &args.voteproofHandlerArgs),
			args:             args,
		},
	}
}

func (st *NewHandoverHandlerType) new() (handler, error) {
	nst := &HandoverHandler{
		voteproofHandler:      st.voteproofHandler.new(),
		args:                  st.args,
		finishedWithVoteproof: util.EmptyLocked[bool](),
	}

	nst.args.whenNewVoteproof = nst.whenNewVoteproof
	nst.args.whenNewBlockSaved = nst.whenNewBlockSaved
	nst.args.prepareACCEPTBallot = func(base.INITVoteproof, base.Manifest, time.Duration) error { return nil }
	nst.args.prepareNextRoundBallot = func(
		base.Voteproof, util.Hash, []util.TimerID, base.Suffrage, time.Duration,
	) error {
		return nil
	}
	nst.args.prepareSuffrageConfirmBallot = func(base.Voteproof) {}
	nst.args.prepareNextBlockBallot = func(base.ACCEPTVoteproof, []util.TimerID, base.Suffrage, time.Duration) error {
		return nil
	}
	nst.args.checkInState = nst.checkInState

	return nst, nil
}

func (st *HandoverHandler) enter(from StateType, i switchContext) (func(), error) {
	var vp base.Voteproof

	if vsctx, ok := i.(voteproofSwitchContext); ok {
		vp = vsctx.voteproof()
	}

	broker := st.handoverYBroker()
	if broker == nil {
		st.Log().Error().
			Interface("voteproof", vp).
			Msg("handover y broker not found; cancel handover; moves to syncing")

		return func() {}, newSyncingSwitchContextWithVoteproof(StateHandover, vp)
	}

	deferred, err := st.voteproofHandler.enter(from, i)
	if err != nil {
		return deferred, err
	}

	if st.allowedConsensus() {
		return func() {}, newSyncingSwitchContextWithVoteproof(StateHandover, vp)
	}

	return deferred, nil
}

func (st *HandoverHandler) exit(i switchContext) (func(), error) {
	deferred, err := st.voteproofHandler.exit(i)
	if err != nil {
		return deferred, err
	}

	if finished, _ := st.finishedWithVoteproof.Value(); finished {
		_ = st.setAllowConsensus(true)

		st.cleanHandovers()

		if st.sts != nil {
			_ = st.sts.args.Ballotbox.Count()
		}
	}

	return deferred, nil
}

func (st *HandoverHandler) whenNewVoteproof(vp base.Voteproof, lvps isaac.LastVoteproofs) error {
	l := st.Log().With().Interface("voteproof", vp).Logger()

	if finished, _ := st.finishedWithVoteproof.Value(); finished {
		return errIgnoreNewVoteproof.Errorf("handover already finished")
	}

	broker := st.handoverYBroker()
	if broker == nil {
		l.Error().Msg("handover y broker not found; cancel handover; moves to syncing")

		return newSyncingSwitchContextWithVoteproof(StateHandover, vp)
	}

	if ivp, ok := vp.(handoverFinishedVoteporof); ok {
		_ = st.finishedWithVoteproof.SetValue(true)

		if sctx := handoverIsReadyConsensusWithVoteproof(ivp, lvps); sctx != nil {
			return sctx
		}

		// NOTE not ready for consensus state?
		switch sctx, err := newConsensusSwitchContext(StateHandover, vp); {
		case err != nil:
			return err
		default:
			l.Error().Err(err).Msg("handover y broker; finished; moves to consensus")

			return sctx
		}
	}

	if isStagePointChallenge(vp) {
		if err := broker.sendStagePoint(st.ctx, vp.Point()); err != nil {
			l.Error().Msg("handover y broker; failed to send stage point; ignore")
		}
	}

	return nil
}

func (st *HandoverHandler) whenNewBlockSaved(bm base.BlockMap, avp base.ACCEPTVoteproof) {
	l := st.Log().With().Interface("blockmap", bm).Logger()

	broker := st.handoverYBroker()
	if broker == nil {
		l.Error().Msg("handover y broker not found; cancel handover; moves to syncing")

		go st.switchState(emptySyncingSwitchContext(StateHandover))

		return
	}

	if err := broker.sendBlockMap(st.ctx, avp.Point(), bm); err != nil {
		l.Error().Msg("handover y broker; failed to send blockmap")

		return
	}
}

func (st *HandoverHandler) checkInState(vp base.Voteproof) switchContext {
	var notInState bool

	switch {
	case st.handoverYBroker() == nil:
		st.Log().Debug().Msg("nil handover y broker")

		notInState = true
	case st.allowedConsensus():
		st.Log().Debug().Msg("allowed consensus")

		notInState = true
	}

	if notInState {
		return newSyncingSwitchContextWithVoteproof(StateHandover, vp)
	}

	return nil
}

type handoverSwitchContext struct {
	vp base.Voteproof
	baseSwitchContext
}

func newHandoverSwitchContext(from StateType, vp base.Voteproof) handoverSwitchContext {
	return handoverSwitchContext{
		baseSwitchContext: newBaseSwitchContext(from, StateHandover),
		vp:                vp,
	}
}

func newHandoverSwitchContextFromOther(sctx switchContext) handoverSwitchContext {
	var vp base.Voteproof

	switch t := sctx.(type) {
	case consensusSwitchContext:
		vp = t.vp
	case joiningSwitchContext:
		vp = t.vp
	}

	return newHandoverSwitchContext(sctx.from(), vp)
}

func (sctx handoverSwitchContext) voteproof() base.Voteproof {
	return sctx.vp
}

type handoverFinishedVoteporof struct {
	base.INITVoteproof
}

func handoverIsReadyConsensusWithVoteproof(ivp base.INITVoteproof, lvps isaac.LastVoteproofs) switchContext {
	if livp, ok := lvps.Cap().(base.INITVoteproof); ok {
		lavp := lvps.ACCEPT()

		switch {
		case ivp.Point().Height() > livp.Point().Height(): // NOTE higher height; moves to syncing state
			return newSyncingSwitchContextWithVoteproof(StateHandover, ivp)
		case ivp.Result() != base.VoteResultMajority: // NOTE new init voteproof has same height, but higher round
			return nil
		case lavp == nil:
			return newBrokenSwitchContext(StateHandover, errors.Errorf("empty last accept voteproof"))
		}

		if m := lavp.BallotMajority(); m == nil || !ivp.BallotMajority().PreviousBlock().Equal(m.NewBlock()) {
			return newSyncingSwitchContextWithVoteproof(StateHandover, ivp)
		}
	}

	if lavp, ok := lvps.Cap().(base.ACCEPTVoteproof); ok {
		switch expectedheight := lavp.Point().Height() + 1; {
		case ivp.Point().Height() > expectedheight:
			return newSyncingSwitchContextWithVoteproof(StateHandover, ivp)
		case ivp.Result() == base.VoteResultDraw:
			return nil
		default:
			if m := lavp.BallotMajority(); m == nil || !ivp.BallotMajority().PreviousBlock().Equal(m.NewBlock()) {
				return newSyncingSwitchContextWithVoteproof(StateHandover, ivp)
			}
		}
	}

	return nil
}
