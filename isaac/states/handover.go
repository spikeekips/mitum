package isaacstates

import (
	"time"

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
	args *HandoverHandlerArgs
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
		voteproofHandler: st.voteproofHandler.new(),
		args:             st.args,
	}

	nst.args.whenNewBlockSaved = nst.whenNewBlockSaved
	nst.args.prepareACCEPTBallot = func(base.INITVoteproof, base.Manifest, time.Duration) error { return nil }
	nst.args.prepareNextRoundBallot = func(base.Voteproof, util.Hash, []util.TimerID, base.Suffrage, time.Duration) error { return nil }
	nst.args.prepareSuffrageConfirmBallot = func(base.Voteproof) {}
	nst.args.prepareNextBlockBallot = func(base.ACCEPTVoteproof, []util.TimerID, base.Suffrage, time.Duration) error { return nil }
	nst.args.checkInState = nst.checkInState

	return nst, nil
}

func (st *HandoverHandler) whenNewVoteproof(vp base.Voteproof) error {
	l := st.Log().With().Interface("voteproof", vp).Logger()

	broker := st.sts.HandoverYBroker()
	if broker == nil {
		l.Error().Msg("handover y broker not found; cancel handover; moves to syncing")

		return newSyncingSwitchContextWithVoteproof(StateHandover, vp)
	}

	if _, ok := vp.(base.INITVoteproof); !ok {
		return nil
	}

	if _, ok := vp.(handoverFinishedVoteporof); ok {
		switch sctx, err := newConsensusSwitchContext(StateHandover, vp); {
		case err != nil:
			l.Error().Err(err).Msg("handover y broker; finished, but invalid consensus switch context; ignore")
		default:
			l.Error().Err(err).Msg("handover y broker; finished; moves to consensus")

			return sctx
		}
	}

	if err := broker.sendStagePoint(st.ctx, vp.Point()); err != nil {
		l.Error().Msg("handover y broker; failed to send stage point; ignore")
	}

	return nil
}

func (st *HandoverHandler) whenNewBlockSaved(bm base.BlockMap, avp base.ACCEPTVoteproof) {
	l := st.Log().With().Interface("blockmap", bm).Logger()

	broker := st.sts.HandoverYBroker()
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
	switch {
	case st.sts.HandoverYBroker() == nil,
		!st.allowedConsensus():
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

type handoverFinishedVoteporof struct {
	base.INITVoteproof
}
