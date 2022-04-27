package launch

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/isaac/blockdata"
	"github.com/spikeekips/mitum/isaac/nodenetwork"
	isaacoperation "github.com/spikeekips/mitum/isaac/operation"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/tree"
)

var Hinters = []encoder.DecodeDetail{
	{Hint: base.BaseOperationProcessReasonErrorHint, Instance: base.BaseOperationProcessReasonError{}},
	{Hint: base.BaseStateHint, Instance: base.BaseState{}},
	{Hint: base.MPrivatekeyHint, Instance: base.MPrivatekey{}},
	{Hint: base.MPublickeyHint, Instance: base.MPublickey{}},
	{Hint: base.OperationFixedTreeNodeHint, Instance: base.OperationFixedTreeNode{}},
	{Hint: base.StateFixedTreeNodeHint, Instance: base.StateFixedTreeNode{}},
	{Hint: base.StringAddressHint, Instance: base.StringAddress{}},
	{Hint: blockdata.BlockDataMapHint, Instance: blockdata.BlockDataMap{}},
	{Hint: isaac.ACCEPTBallotFactHint, Instance: isaac.ACCEPTBallotFact{}},
	{Hint: isaac.ACCEPTBallotHint, Instance: isaac.ACCEPTBallot{}},
	{Hint: isaac.ACCEPTBallotSignedFactHint, Instance: isaac.ACCEPTBallotSignedFact{}},
	{Hint: isaac.ACCEPTVoteproofHint, Instance: isaac.ACCEPTVoteproof{}},
	{Hint: isaac.INITBallotFactHint, Instance: isaac.INITBallotFact{}},
	{Hint: isaac.INITBallotHint, Instance: isaac.INITBallot{}},
	{Hint: isaac.INITBallotSignedFactHint, Instance: isaac.INITBallotSignedFact{}},
	{Hint: isaac.INITVoteproofHint, Instance: isaac.INITVoteproof{}},
	{Hint: isaac.ManifestHint, Instance: isaac.Manifest{}},
	{Hint: isaac.NodeHint, Instance: base.BaseNode{}},
	{Hint: isaac.NodePolicyHint, Instance: isaac.NodePolicy{}},
	{Hint: isaac.NetworkPolicyHint, Instance: isaac.NetworkPolicy{}},
	{Hint: isaac.NetworkPolicyStateValueHint, Instance: isaac.NetworkPolicyStateValue{}},
	{Hint: isaacoperation.GenesisNetworkPolicyFactHint, Instance: isaacoperation.GenesisNetworkPolicyFact{}},
	{Hint: isaacoperation.GenesisNetworkPolicyHint, Instance: isaacoperation.GenesisNetworkPolicy{}},
	{Hint: isaac.ProposalFactHint, Instance: isaac.ProposalFact{}},
	{Hint: isaac.ProposalSignedFactHint, Instance: isaac.ProposalSignedFact{}},
	{Hint: isaac.SuffrageCandidateStateNodeValueHint, Instance: isaac.SuffrageCandidateStateNodeValue{}},
	{Hint: isaacoperation.SuffrageGenesisJoinHint, Instance: isaacoperation.SuffrageGenesisJoin{}},
	{
		Hint:     isaacoperation.SuffrageGenesisJoinPermissionFactHint,
		Instance: isaacoperation.SuffrageGenesisJoinPermissionFact{},
	},
	{Hint: isaacoperation.SuffrageJoinHint, Instance: base.BaseOperation{}},
	{Hint: isaacoperation.SuffrageJoinPermissionFactHint, Instance: isaacoperation.SuffrageJoinPermissionFact{}},
	{Hint: isaac.SuffrageStateValueHint, Instance: isaac.SuffrageStateValue{}},
	{Hint: nodenetwork.ProposalBodyHint, Instance: nodenetwork.ProposalBody{}},
	{Hint: nodenetwork.RequestProposalBodyHint, Instance: nodenetwork.RequestProposalBody{}},
	{Hint: quictransport.NodeHint, Instance: quictransport.BaseNode{}},
	{Hint: quictransport.NodeMetaHint, Instance: quictransport.NodeMeta{}},
	{Hint: tree.FixedTreeHint, Instance: tree.FixedTree{}},
}

var LoadOperationHinters = []encoder.DecodeDetail{} // BLOCK apply to getOperation in ProposalProcessor

func LoadHinters(enc encoder.Encoder) error {
	for i := range Hinters {
		if err := enc.Add(Hinters[i]); err != nil {
			return errors.Wrap(err, "failed to add to encoder")
		}
	}

	return nil
}
