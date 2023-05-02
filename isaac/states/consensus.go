package isaacstates

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
)

type ConsensusHandlerArgs struct {
	voteproofHandlerArgs
}

func NewConsensusHandlerArgs() *ConsensusHandlerArgs {
	return &ConsensusHandlerArgs{
		voteproofHandlerArgs: newVoteproofHandlerArgs(),
	}
}

type ConsensusHandler struct {
	*voteproofHandler
	args                *ConsensusHandlerArgs
	handoverXBrokerFunc func() *HandoverXBroker
}

type NewConsensusHandlerType struct {
	*ConsensusHandler
}

func NewNewConsensusHandlerType(
	local base.LocalNode,
	params *isaac.LocalParams,
	args *ConsensusHandlerArgs,
) *NewConsensusHandlerType {
	return &NewConsensusHandlerType{
		ConsensusHandler: &ConsensusHandler{
			voteproofHandler:    newVoteproofHandler(StateConsensus, local, params, &args.voteproofHandlerArgs),
			args:                args,
			handoverXBrokerFunc: func() *HandoverXBroker { return nil },
		},
	}
}

func (st *NewConsensusHandlerType) new() (handler, error) {
	nst := &ConsensusHandler{
		voteproofHandler:    st.voteproofHandler.new(),
		args:                st.args,
		handoverXBrokerFunc: st.handoverXBrokerFunc,
	}

	nst.args.checkInState = nst.checkInState

	return nst, nil
}

func (st *ConsensusHandler) whenNewVoteproof(vp base.Voteproof, _ isaac.LastVoteproofs) error {
	if err := st.handoverXBrokerSendVoteproof(vp); err != nil {
		st.Log().Error().Err(err).Interface("voteproof", vp).Msg("failed to send voteproof thru handover x broker")
	}

	return nil
}

func (st *ConsensusHandler) checkInState(vp base.Voteproof) switchContext {
	if st.allowedConsensus() {
		return nil
	}

	return newSyncingSwitchContextWithVoteproof(StateConsensus, vp)
}

func (st *ConsensusHandler) handoverXBroker() *HandoverXBroker {
	if st.sts == nil {
		return st.handoverXBrokerFunc()
	}

	return st.sts.HandoverXBroker()
}

func (st *ConsensusHandler) handoverXBrokerSendVoteproof(vp base.Voteproof) error {
	broker := st.handoverXBroker()
	if broker == nil {
		return nil
	}

	if _, err := broker.sendVoteproof(st.ctx, vp); err != nil {
		return err
	}

	return nil
}

type consensusSwitchContext struct {
	vp base.Voteproof
	baseSwitchContext
}

func newConsensusSwitchContext(from StateType, vp base.Voteproof) (consensusSwitchContext, error) {
	switch {
	case vp == nil:
		return consensusSwitchContext{}, errors.Errorf(
			"invalid voteproof for consensus switch context; empty init voteproof")
	case vp.Point().Stage() == base.StageINIT:
	case vp.Point().Stage() == base.StageACCEPT:
		if vp.Result() != base.VoteResultDraw {
			return consensusSwitchContext{}, errors.Errorf(
				"invalid voteproof for consensus switch context; vote result is not draw, %T", vp.Result())
		}
	}

	return consensusSwitchContext{
		baseSwitchContext: newBaseSwitchContext(from, StateConsensus),
		vp:                vp,
	}, nil
}

func (sctx consensusSwitchContext) voteproof() base.Voteproof {
	return sctx.vp
}
