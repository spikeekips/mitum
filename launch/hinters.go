package launch

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacoperation "github.com/spikeekips/mitum/isaac/operation"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
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
	{Hint: isaac.ACCEPTBallotSignFactHint, Instance: isaac.ACCEPTBallotSignFact{}},
	{Hint: isaac.ACCEPTVoteproofHint, Instance: isaac.ACCEPTVoteproof{}},
	{Hint: isaac.ACCEPTWithdrawVoteproofHint, Instance: isaac.ACCEPTWithdrawVoteproof{}},
	{Hint: isaac.ACCEPTStuckVoteproofHint, Instance: isaac.ACCEPTStuckVoteproof{}},
	{Hint: isaac.FixedSuffrageCandidateLimiterRuleHint, Instance: isaac.FixedSuffrageCandidateLimiterRule{}},
	{Hint: isaac.INITBallotFactHint, Instance: isaac.INITBallotFact{}},
	{Hint: isaac.INITBallotHint, Instance: isaac.INITBallot{}},
	{Hint: isaac.INITBallotSignFactHint, Instance: isaac.INITBallotSignFact{}},
	{Hint: isaac.INITVoteproofHint, Instance: isaac.INITVoteproof{}},
	{Hint: isaac.INITWithdrawVoteproofHint, Instance: isaac.INITWithdrawVoteproof{}},
	{Hint: isaac.INITStuckVoteproofHint, Instance: isaac.INITStuckVoteproof{}},
	{Hint: isaac.LocalParamsHint, Instance: &isaac.LocalParams{}},
	{Hint: isaac.ManifestHint, Instance: isaac.Manifest{}},
	{Hint: isaac.NetworkPolicyHint, Instance: isaac.NetworkPolicy{}},
	{Hint: isaac.NetworkPolicyStateValueHint, Instance: isaac.NetworkPolicyStateValue{}},
	{Hint: isaac.NodeHint, Instance: base.BaseNode{}},
	{Hint: isaac.ProposalFactHint, Instance: isaac.ProposalFact{}},
	{Hint: isaac.ProposalSignFactHint, Instance: isaac.ProposalSignFact{}},
	{Hint: isaac.SuffrageConfirmBallotFactHint, Instance: isaac.SuffrageConfirmBallotFact{}},
	{Hint: isaac.SuffrageCandidateStateValueHint, Instance: isaac.SuffrageCandidateStateValue{}},
	{Hint: isaac.SuffrageCandidatesStateValueHint, Instance: isaac.SuffrageCandidatesStateValue{}},
	{Hint: isaac.SuffrageNodeStateValueHint, Instance: isaac.SuffrageNodeStateValue{}},
	{Hint: isaac.SuffrageNodesStateValueHint, Instance: isaac.SuffrageNodesStateValue{}},
	{Hint: isaac.SuffrageWithdrawOperationHint, Instance: isaac.SuffrageWithdrawOperation{}},
	{Hint: isaacblock.BlockMapHint, Instance: isaacblock.BlockMap{}},
	{Hint: isaacblock.SuffrageProofHint, Instance: isaacblock.SuffrageProof{}},
	{
		Hint:     isaacnetwork.ExistsInStateOperationRequestHeaderHint,
		Instance: isaacnetwork.ExistsInStateOperationRequestHeader{},
	},
	{Hint: isaacnetwork.BlockMapItemRequestHeaderHint, Instance: isaacnetwork.BlockMapItemRequestHeader{}},
	{Hint: isaacnetwork.BlockMapRequestHeaderHint, Instance: isaacnetwork.BlockMapRequestHeader{}},
	{Hint: isaacnetwork.LastBlockMapRequestHeaderHint, Instance: isaacnetwork.LastBlockMapRequestHeader{}},
	{Hint: isaacnetwork.LastSuffrageProofRequestHeaderHint, Instance: isaacnetwork.LastSuffrageProofRequestHeader{}},
	{Hint: isaacnetwork.NodeChallengeRequestHeaderHint, Instance: isaacnetwork.NodeChallengeRequestHeader{}},
	{Hint: isaacnetwork.NodeInfoHint, Instance: isaacnetwork.NodeInfo{}},
	{Hint: isaacnetwork.NodeInfoRequestHeaderHint, Instance: isaacnetwork.NodeInfoRequestHeader{}},
	{Hint: isaacnetwork.OperationRequestHeaderHint, Instance: isaacnetwork.OperationRequestHeader{}},
	{Hint: isaacnetwork.ProposalRequestHeaderHint, Instance: isaacnetwork.ProposalRequestHeader{}},
	{Hint: isaacnetwork.RequestProposalRequestHeaderHint, Instance: isaacnetwork.RequestProposalRequestHeader{}},
	{Hint: isaacnetwork.SendBallotsHeaderHint, Instance: isaacnetwork.SendBallotsHeader{}},
	{Hint: isaacnetwork.SendOperationRequestHeaderHint, Instance: isaacnetwork.SendOperationRequestHeader{}},
	{Hint: isaacnetwork.StateRequestHeaderHint, Instance: isaacnetwork.StateRequestHeader{}},
	{
		Hint:     isaacnetwork.SuffrageNodeConnInfoRequestHeaderHint,
		Instance: isaacnetwork.SuffrageNodeConnInfoRequestHeader{},
	},
	{Hint: isaacnetwork.SuffrageProofRequestHeaderHint, Instance: isaacnetwork.SuffrageProofRequestHeader{}},
	{Hint: isaacnetwork.SyncSourceConnInfoRequestHeaderHint, Instance: isaacnetwork.SyncSourceConnInfoRequestHeader{}},
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
	{Hint: isaacstates.MissingBallotsRequestsMessageHint, Instance: isaacstates.MissingBallotsRequestMessage{}},
	{Hint: quicmemberlist.CallbackBroadcastMessageHint, Instance: quicmemberlist.CallbackBroadcastMessage{}},
	{
		Hint:     quicmemberlist.CallbackBroadcastMessageHeaderHint,
		Instance: quicmemberlist.CallbackBroadcastMessageHeader{},
	},
	{Hint: quicmemberlist.MemberHint, Instance: quicmemberlist.BaseMember{}},
	{Hint: quicstream.DefaultResponseHeaderHint, Instance: quicstream.DefaultResponseHeader{}},
}

var SupportedProposalOperationFactHinters = []encoder.DecodeDetail{
	{Hint: isaac.SuffrageWithdrawFactHint, Instance: isaac.SuffrageWithdrawFact{}},
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

func IsSupportedProposalOperationFactHintFunc() func(hint.Hint) bool {
	return func(ht hint.Hint) bool {
		for i := range SupportedProposalOperationFactHinters {
			s := SupportedProposalOperationFactHinters[i].Hint
			if ht.Type() != s.Type() {
				continue
			}

			return ht.IsCompatible(s)
		}

		return false
	}
}
