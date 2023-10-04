package isaacstates

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type ConsensusHandlerArgs struct {
	IsEmptyProposalNoBlockFunc func() bool
	IsEmptyProposalFunc        func(base.ProposalSignFact) (bool, error)
	voteproofHandlerArgs
}

func NewConsensusHandlerArgs() *ConsensusHandlerArgs {
	return &ConsensusHandlerArgs{
		voteproofHandlerArgs:       newVoteproofHandlerArgs(),
		IsEmptyProposalNoBlockFunc: func() bool { return false },
		IsEmptyProposalFunc:        func(base.ProposalSignFact) (bool, error) { return false, nil },
	}
}

type ConsensusHandler struct {
	*voteproofHandler
	args *ConsensusHandlerArgs
}

type NewConsensusHandlerType struct {
	*ConsensusHandler
}

func NewNewConsensusHandlerType(
	networkID base.NetworkID,
	local base.LocalNode,
	args *ConsensusHandlerArgs,
) *NewConsensusHandlerType {
	origNewINITBallotFactFunc := args.NewINITBallotFactFunc
	args.NewINITBallotFactFunc = func(
		point base.Point,
		previousBlock util.Hash,
		proposal base.ProposalSignFact,
		expelfacts []util.Hash,
	) (base.INITBallotFact, error) {
		if len(expelfacts) > 0 || !args.IsEmptyProposalNoBlockFunc() {
			return origNewINITBallotFactFunc(point, previousBlock, proposal, expelfacts)
		}

		switch isempty, err := args.IsEmptyProposalFunc(proposal); {
		case err != nil:
			return nil, err
		case !isempty:
			return origNewINITBallotFactFunc(point, previousBlock, proposal, expelfacts)
		}

		// NOTE empty-proposal-init-ballot-fact
		return isaac.NewEmptyProposalINITBallotFact(
			point,
			previousBlock,
			proposal.Fact().Hash(),
		), nil
	}

	return &NewConsensusHandlerType{
		ConsensusHandler: &ConsensusHandler{
			voteproofHandler: newVoteproofHandler(StateConsensus, networkID, local, &args.voteproofHandlerArgs),
			args:             args,
		},
	}
}

func (st *NewConsensusHandlerType) new() (handler, error) {
	nst := &ConsensusHandler{
		voteproofHandler: st.voteproofHandler.new(),
		args:             st.args,
	}

	nst.args.whenNewVoteproof = nst.whenNewVoteproof
	nst.args.checkInState = nst.checkInState

	return nst, nil
}

func (st *ConsensusHandler) whenNewVoteproof(vp base.Voteproof, _ isaac.LastVoteproofs) error {
	broker := st.handoverXBroker()
	if broker == nil {
		return nil
	}

	switch isFinished, err := broker.sendVoteproof(st.ctx, vp); {
	case err != nil:
		return err
	case isFinished:
		return newSyncingSwitchContextWithVoteproof(StateConsensus, vp)
	default:
		return nil
	}
}

func (st *ConsensusHandler) checkInState(vp base.Voteproof) switchContext {
	if st.allowedConsensus() {
		return nil
	}

	return newSyncingSwitchContextWithVoteproof(StateConsensus, vp)
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
