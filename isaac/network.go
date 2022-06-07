package isaac

import (
	"context"
	"io"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util"
)

// revive:disable:line-length-limit
type NetworkClient interface {
	RequestProposal(_ context.Context, connInfo quictransport.ConnInfo, point base.Point, propser base.Address) (base.ProposalSignedFact, bool, error)
	Proposal(_ context.Context, connInfo quictransport.ConnInfo, facthash util.Hash) (base.ProposalSignedFact, bool, error)
	LastSuffrageProof(_ context.Context, connInfo quictransport.ConnInfo, state util.Hash) (_ base.SuffrageProof, updated bool, _ error)
	SuffrageProof(_ context.Context, connInfo quictransport.ConnInfo, suffrageheight base.Height) (_ base.SuffrageProof, found bool, _ error)
	LastBlockMap(_ context.Context, _ quictransport.ConnInfo, manifest util.Hash) (_ base.BlockMap, updated bool, _ error)
	BlockMap(context.Context, quictransport.ConnInfo, base.Height) (_ base.BlockMap, updated bool, _ error)
	BlockMapItem(context.Context, quictransport.ConnInfo, base.Height, base.BlockMapItemType) (io.ReadCloser, func() error, bool, error)
}

// revive:enable:line-length-limit
