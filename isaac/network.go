package isaac

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util"
)

type LastSuffrageFunc func(context.Context, quictransport.ConnInfo) (SuffrageProof, bool, error)

type NodeNetworkClient interface {
	RequestProposal(
		context.Context, quictransport.ConnInfo, base.Point, base.Address, /* proposer */
	) (base.ProposalSignedFact, bool, error)
	Proposal(context.Context, quictransport.ConnInfo, util.Hash /* fact hash */) (base.ProposalSignedFact, bool, error)
	// BLOCK LastSuffrageProof
	// BLOCK BlockMap(context.Context, quictransport.ConnInfo, base.Height)
	// (base.BlockMap, bool, error)

	// BLOCK Block(context.Context, quictransport.ConnInfo, base.Height,
	// base.BlockMapItemType) (base.Block, bool, error)
}
