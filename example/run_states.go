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
	_ = pps.SetLogging(logging)

	lvps := isaacstates.NewLastVoteproofsHandler()

	states := isaacstates.NewStates(
		cmd.ballotbox,
		lvps,
		cmd.broadcastBallotFunc,
	)

	whenNewBlockSaved := func(height base.Height) {
		cmd.ballotbox.Count()
	}

	syncinghandler := isaacstates.NewNewSyncingHandlerType(
		cmd.local, cmd.nodePolicy, cmd.proposalSelector, cmd.newSyncer(lvps), cmd.getSuffrage,
	)
	syncinghandler.SetWhenFinished(func(base.Height) {
		cmd.ballotbox.Count()
	})

	states.
		SetHandler(isaacstates.StateBroken, isaacstates.NewNewBrokenHandlerType(cmd.local, cmd.nodePolicy)).
		SetHandler(isaacstates.StateStopped, isaacstates.NewNewStoppedHandlerType(cmd.local, cmd.nodePolicy)).
		SetHandler(
			isaacstates.StateBooting,
			isaacstates.NewNewBootingHandlerType(cmd.local, cmd.nodePolicy, cmd.getLastManifest, cmd.getSuffrage),
		).
		SetHandler(
			isaacstates.StateJoining,
			isaacstates.NewNewJoiningHandlerType(
				cmd.local, cmd.nodePolicy, cmd.proposalSelector, cmd.getLastManifest, cmd.getSuffrage, voteFunc,
				cmd.joinMemberlistForJoiningState,
			),
		).
		SetHandler(
			isaacstates.StateConsensus,
			isaacstates.NewNewConsensusHandlerType(
				cmd.local, cmd.nodePolicy, cmd.proposalSelector,
				cmd.getManifest, cmd.getSuffrage, voteFunc, whenNewBlockSaved,
				pps,
			)).
		SetHandler(isaacstates.StateSyncing, syncinghandler)

	_ = states.SetLogging(logging)

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
