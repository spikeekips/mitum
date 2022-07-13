package launch

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacoperation "github.com/spikeekips/mitum/isaac/operation"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

var Hinters = []encoder.DecodeDetail{
	// revive:disable-next-line:line-length-limit
	{Hint: DefaultNodeInfoHint, Instance: DefaultNodeInfo{}},
	{Hint: base.BaseOperationProcessReasonErrorHint, Instance: base.BaseOperationProcessReasonError{}},
	{Hint: base.BaseStateHint, Instance: base.BaseState{}},
	{Hint: base.MPrivatekeyHint, Instance: base.MPrivatekey{}},
	{Hint: base.MPublickeyHint, Instance: base.MPublickey{}},
	{Hint: base.OperationFixedtreeHint, Instance: base.OperationFixedtreeNode{}},
	{Hint: base.StateFixedtreeHint, Instance: fixedtree.BaseNode{}},
	{Hint: base.StringAddressHint, Instance: base.StringAddress{}},
	{Hint: isaac.ACCEPTBallotFactHint, Instance: isaac.ACCEPTBallotFact{}},
	{Hint: isaac.ACCEPTBallotHint, Instance: isaac.ACCEPTBallot{}},
	{Hint: isaac.ACCEPTBallotSignedFactHint, Instance: isaac.ACCEPTBallotSignedFact{}},
	{Hint: isaac.ACCEPTVoteproofHint, Instance: isaac.ACCEPTVoteproof{}},
	{Hint: isaac.FixedSuffrageCandidateLimiterRuleHint, Instance: isaac.FixedSuffrageCandidateLimiterRule{}},
	{Hint: isaac.INITBallotFactHint, Instance: isaac.INITBallotFact{}},
	{Hint: isaac.INITBallotHint, Instance: isaac.INITBallot{}},
	{Hint: isaac.INITBallotSignedFactHint, Instance: isaac.INITBallotSignedFact{}},
	{Hint: isaac.INITVoteproofHint, Instance: isaac.INITVoteproof{}},
	{Hint: isaac.ManifestHint, Instance: isaac.Manifest{}},
	{Hint: isaac.NetworkPolicyHint, Instance: isaac.NetworkPolicy{}},
	{Hint: isaac.NetworkPolicyStateValueHint, Instance: isaac.NetworkPolicyStateValue{}},
	{Hint: isaac.NodeHint, Instance: base.BaseNode{}},
	{Hint: isaac.NodePolicyHint, Instance: isaac.NodePolicy{}},
	{Hint: isaac.ProposalFactHint, Instance: isaac.ProposalFact{}},
	{Hint: isaac.ProposalSignedFactHint, Instance: isaac.ProposalSignedFact{}},
	{Hint: isaac.SuffrageCandidateHint, Instance: isaac.SuffrageCandidate{}},
	{Hint: isaac.SuffrageStateValueHint, Instance: isaac.SuffrageStateValue{}},
	{Hint: isaacblock.BlockMapHint, Instance: isaacblock.BlockMap{}},
	{Hint: isaacblock.SuffrageProofHint, Instance: isaacblock.SuffrageProof{}},
	{Hint: isaacnetwork.BlockMapRequestHeaderHint, Instance: isaacnetwork.BlockMapRequestHeader{}},
	{Hint: isaacnetwork.BlockMapItemRequestHeaderHint, Instance: isaacnetwork.BlockMapItemRequestHeader{}},
	{Hint: isaacnetwork.LastBlockMapRequestHeaderHint, Instance: isaacnetwork.LastBlockMapRequestHeader{}},
	{Hint: isaacnetwork.LastSuffrageProofRequestHeaderHint, Instance: isaacnetwork.LastSuffrageProofRequestHeader{}},
	{
		Hint:     isaacnetwork.NodeChallengeRequestHeaderHint,
		Instance: isaacnetwork.NodeChallengeRequestHeader{},
	},
	{Hint: isaacnetwork.OperationRequestHeaderHint, Instance: isaacnetwork.OperationRequestHeader{}},
	{Hint: isaacnetwork.ProposalRequestHeaderHint, Instance: isaacnetwork.ProposalRequestHeader{}},
	{Hint: isaacnetwork.RequestProposalRequestHeaderHint, Instance: isaacnetwork.RequestProposalRequestHeader{}},
	{Hint: isaacnetwork.ResponseHeaderHint, Instance: isaacnetwork.ResponseHeader{}},
	{Hint: isaacnetwork.SendOperationRequestHeaderHint, Instance: isaacnetwork.SendOperationRequestHeader{}},
	{Hint: isaacnetwork.SuffrageProofRequestHeaderHint, Instance: isaacnetwork.SuffrageProofRequestHeader{}},
	{Hint: isaacoperation.GenesisNetworkPolicyFactHint, Instance: isaacoperation.GenesisNetworkPolicyFact{}},
	{Hint: isaacoperation.GenesisNetworkPolicyHint, Instance: isaacoperation.GenesisNetworkPolicy{}},
	{Hint: isaacoperation.SuffrageCandidateFactHint, Instance: isaacoperation.SuffrageCandidateFact{}},
	{Hint: isaacoperation.SuffrageCandidateHint, Instance: isaacoperation.SuffrageCandidate{}},
	{Hint: isaacoperation.SuffrageGenesisJoinHint, Instance: isaacoperation.SuffrageGenesisJoin{}},
	{
		Hint:     isaacoperation.SuffrageGenesisJoinFactHint,
		Instance: isaacoperation.SuffrageGenesisJoinFact{},
	},
	{Hint: isaacoperation.SuffrageJoinHint, Instance: base.BaseOperation{}},
	{Hint: isaacoperation.SuffrageJoinFactHint, Instance: isaacoperation.SuffrageJoinFact{}},
	{Hint: quicmemberlist.NodeHint, Instance: quicmemberlist.BaseNode{}},
}

// FIXME set valid operation hinters for proposal processor.

func LoadHinters(enc encoder.Encoder) error {
	for i := range Hinters {
		if err := enc.Add(Hinters[i]); err != nil {
			return errors.Wrap(err, "failed to add to encoder")
		}
	}

	return nil
}
