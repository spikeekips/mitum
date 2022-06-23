package main

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

func (cmd *runCommand) prepareStates() error {
	cmd.nodePolicy = isaac.DefaultNodePolicy(networkID)
	log.Info().
		Interface("node_policy", cmd.nodePolicy).
		Msg("node policy loaded")

	// FIXME implement isaacstates.NewSuffrageStateBuilder(cmd.nodePolicy.NetworkID(), )

	cmd.getSuffrage = cmd.getSuffrageFunc()
	// FIXME cmd.getSuffrageBooting   func(blockheight base.Height) (base.Suffrage, bool, error)
	cmd.getManifest = cmd.getManifestFunc()
	cmd.proposalSelector = cmd.proposalSelectorFunc()
	cmd.getLastManifest = cmd.getLastManifestFunc()
	cmd.newProposalProcessor = cmd.newProposalProcessorFunc(cmd.enc)
	cmd.getProposal = cmd.getProposalFunc()

	cmd.prepareSuffrageBuilder()

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
		func(ballot base.Ballot) error {
			e := util.StringErrorFunc("failed to broadcast ballot")

			b, err := cmd.enc.Marshal(ballot)
			if err != nil {
				return e(err, "")
			}

			id := valuehash.NewSHA256(ballot.HashBytes()).String()

			if err := launch.BroadcastThruMemberlist(cmd.memberlist, id, b); err != nil {
				return e(err, "")
			}

			return nil
		},
	) // FIXME set broadcast ballot func
	_ = states.SetLogging(logging)

	whenNewBlockSaved := func(height base.Height) {
		cmd.ballotbox.Count()
	}

	syncinghandler := isaacstates.NewSyncingHandler(
		cmd.local, cmd.nodePolicy, cmd.proposalSelector, cmd.newSyncer(lvps),
	)
	syncinghandler.SetWhenFinished(func(height base.Height) { // FIXME set later
	})

	states.
		SetHandler(isaacstates.NewBrokenHandler(cmd.local, cmd.nodePolicy)).
		SetHandler(isaacstates.NewStoppedHandler(cmd.local, cmd.nodePolicy)).
		SetHandler(isaacstates.NewBootingHandler(cmd.local, cmd.nodePolicy, cmd.getLastManifest, cmd.getSuffrage)).
		SetHandler(
			isaacstates.NewJoiningHandler(
				cmd.local, cmd.nodePolicy, cmd.proposalSelector, cmd.getLastManifest, cmd.getSuffrage, voteFunc,
			),
		).
		SetHandler(
			isaacstates.NewConsensusHandler(
				cmd.local, cmd.nodePolicy, cmd.proposalSelector,
				cmd.getManifest, cmd.getSuffrage, voteFunc, whenNewBlockSaved,
				pps,
			)).
		SetHandler(syncinghandler)

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
