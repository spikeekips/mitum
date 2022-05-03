package isaac

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util"
)

type LastSuffrageFunc func(context.Context, quictransport.ConnInfo) (base.SuffrageInfo, bool, error)

type NodeNetworkClient interface {
	RequestProposal(
		context.Context, quictransport.ConnInfo, base.Point, base.Address, /* proposer */
	) (base.ProposalSignedFact, bool, error)
	Proposal(context.Context, quictransport.ConnInfo, util.Hash /* fact hash */) (base.ProposalSignedFact, bool, error)
	LastSuffrage(context.Context, quictransport.ConnInfo) (base.SuffrageInfo, bool, error)
	// BLOCK BlockDataMap(context.Context, quictransport.ConnInfo, base.Height)
	// (base.BlockDataMap, bool, error)

	// BLOCK BlockData(context.Context, quictransport.ConnInfo, base.Height,
	// base.BlockDataType) (base.BlockData, bool, error)
}
