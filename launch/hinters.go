package launch

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacoperation "github.com/spikeekips/mitum/isaac/operation"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
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
	{Hint: isaac.SuffrageCandidateStateValueHint, Instance: isaac.SuffrageCandidateStateValue{}},
	{Hint: isaac.SuffrageNodeStateValueHint, Instance: isaac.SuffrageNodeStateValue{}},
	{Hint: isaac.SuffrageNodesStateValueHint, Instance: isaac.SuffrageNodesStateValue{}},
	{Hint: isaac.SuffrageCandidatesStateValueHint, Instance: isaac.SuffrageCandidatesStateValue{}},
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
	{Hint: isaacnetwork.StateRequestHeaderHint, Instance: isaacnetwork.StateRequestHeader{}},
	{
		Hint:     isaacnetwork.ExistsInStateOperationRequestHeaderHint,
		Instance: isaacnetwork.ExistsInStateOperationRequestHeader{},
	},
	{Hint: isaacoperation.GenesisNetworkPolicyFactHint, Instance: isaacoperation.GenesisNetworkPolicyFact{}},
	{Hint: isaacoperation.GenesisNetworkPolicyHint, Instance: isaacoperation.GenesisNetworkPolicy{}},
	{Hint: isaacoperation.SuffrageCandidateHint, Instance: isaacoperation.SuffrageCandidate{}},
	{Hint: isaacoperation.SuffrageGenesisJoinHint, Instance: isaacoperation.SuffrageGenesisJoin{}},
	{Hint: isaacoperation.SuffrageDisjoinHint, Instance: isaacoperation.SuffrageDisjoin{}},
	{Hint: isaacoperation.SuffrageJoinHint, Instance: isaacoperation.SuffrageJoin{}},
	{
		Hint:     isaacoperation.SuffrageGenesisJoinFactHint,
		Instance: isaacoperation.SuffrageGenesisJoinFact{},
	},
	{Hint: PprofRequestHeaderHint, Instance: PprofRequestHeader{}},
	{Hint: quicmemberlist.NodeHint, Instance: quicmemberlist.BaseNode{}},
}

var SupportedProposalOperationFactHinters = []encoder.DecodeDetail{
	{Hint: isaacoperation.SuffrageCandidateFactHint, Instance: isaacoperation.SuffrageCandidateFact{}},
	{Hint: isaacoperation.SuffrageDisjoinFactHint, Instance: isaacoperation.SuffrageDisjoinFact{}},
	{Hint: isaacoperation.SuffrageJoinFactHint, Instance: isaacoperation.SuffrageJoinFact{}},
}

func LoadHinters(enc encoder.Encoder) error {
	for i := range Hinters {
		if err := enc.Add(Hinters[i]); err != nil {
			return errors.Wrap(err, "failed to add to encoder")
		}
	}

	for i := range SupportedProposalOperationFactHinters {
		if err := enc.Add(SupportedProposalOperationFactHinters[i]); err != nil {
			return errors.Wrap(err, "failed to add to encoder")
		}
	}

	return nil
}

func IsSupportedProposalOperationFactHintFunc() func(hintbytes []byte) bool {
	supportedProposalOperationFactTypeHintersBytes := make([][]byte, len(SupportedProposalOperationFactHinters))

	for i := range SupportedProposalOperationFactHinters {
		supportedProposalOperationFactTypeHintersBytes[i] = SupportedProposalOperationFactHinters[i].Hint.Type().Bytes()
	}

	return func(b []byte) bool {
		for i := range supportedProposalOperationFactTypeHintersBytes {
			if !bytes.HasPrefix(b, supportedProposalOperationFactTypeHintersBytes[i]) {
				continue
			}

			ht, err := hint.ParseHint(string(b))
			if err != nil {
				return false
			}

			return ht.IsCompatible(SupportedProposalOperationFactHinters[i].Hint)
		}

		return false
	}
}
