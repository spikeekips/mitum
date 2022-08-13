package main

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
)

func (cmd *runCommand) prepareStates() error {
	cmd.getSuffrage = cmd.getSuffrageFunc()
	cmd.getManifest = cmd.getManifestFunc()
	cmd.proposalSelector = cmd.proposalSelectorFunc()
	cmd.getLastManifest = cmd.getLastManifestFunc()
	cmd.newProposalProcessor = cmd.newProposalProcessorFunc(cmd.enc)
	cmd.getProposal = cmd.getProposalFunc()
	cmd.nodeInConsensusNodes = cmd.nodeInConsensusNodesFunc()

	cmd.prepareLastSuffrageProofWatcher()

	cmd.ballotbox = isaacstates.NewBallotbox(cmd.getSuffrage, cmd.nodePolicy.Threshold())

	voteFunc := func(bl base.Ballot) (bool, error) {
		voted, err := cmd.ballotbox.Vote(bl)
		if err != nil {
			return false, err
		}

		return voted, nil
	}

	pps := isaac.NewProposalProcessors(cmd.newProposalProcessor, cmd.getProposal)
	_ = pps.SetLogging(log)

	lvps := isaacstates.NewLastVoteproofsHandler()

	states := isaacstates.NewStates(
		cmd.ballotbox,
		lvps,
		cmd.broadcastBallotFunc,
	)
	states.SetWhenStateSwitched(cmd.whenStateSwitched)

	syncinghandler := isaacstates.NewNewSyncingHandlerType(
		cmd.local, cmd.nodePolicy, cmd.proposalSelector, cmd.newSyncer(lvps), cmd.nodeInConsensusNodes,
	)
	syncinghandler.SetWhenFinished(func(base.Height) {
		cmd.ballotbox.Count()
	})

	states.
		SetHandler(isaacstates.StateBroken, isaacstates.NewNewBrokenHandlerType(cmd.local, cmd.nodePolicy)).
		SetHandler(isaacstates.StateStopped, isaacstates.NewNewStoppedHandlerType(cmd.local, cmd.nodePolicy)).
		SetHandler(
			isaacstates.StateBooting,
			isaacstates.NewNewBootingHandlerType(cmd.local, cmd.nodePolicy,
				cmd.getLastManifest, cmd.nodeInConsensusNodes),
		).
		SetHandler(
			isaacstates.StateJoining,
			isaacstates.NewNewJoiningHandlerType(
				cmd.local, cmd.nodePolicy, cmd.proposalSelector, cmd.getLastManifest, cmd.nodeInConsensusNodes,
				voteFunc, cmd.joinMemberlistForJoiningState,
			),
		).
		SetHandler(
			isaacstates.StateConsensus,
			isaacstates.NewNewConsensusHandlerType(
				cmd.local, cmd.nodePolicy, cmd.proposalSelector,
				cmd.getManifest, cmd.nodeInConsensusNodes, voteFunc, cmd.whenNewBlockSaved,
				pps,
			)).
		SetHandler(isaacstates.StateSyncing, syncinghandler)

	_ = states.SetLogging(log)

	// NOTE load last init, accept voteproof and last majority voteproof
	switch ivp, avp, found, err := cmd.pool.LastVoteproofs(); {
	case err != nil:
		return err
	case !found:
	default:
		_ = states.LastVoteproofsHandler().Set(ivp)
		_ = states.LastVoteproofsHandler().Set(avp)
	}

	cmd.states = states

	return nil
}
