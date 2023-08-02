package isaac

import (
	"context"
	"io"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
)

type NetworkClient interface { //nolint:interfacebloat //..
	// revive:disable:line-length-limit
	Operation(_ context.Context, _ quicstream.ConnInfo, operationhash util.Hash) (base.Operation, bool, error)
	SendOperation(context.Context, quicstream.ConnInfo, base.Operation) (bool, error)
	RequestProposal(_ context.Context, connInfo quicstream.ConnInfo, point base.Point, proposer base.Address, previousBlock util.Hash) (base.ProposalSignFact, bool, error)
	Proposal(_ context.Context, connInfo quicstream.ConnInfo, facthash util.Hash) (base.ProposalSignFact, bool, error)
	LastSuffrageProof(_ context.Context, connInfo quicstream.ConnInfo, state util.Hash) (lastheight base.Height, _ base.SuffrageProof, updated bool, _ error)
	SuffrageProof(_ context.Context, connInfo quicstream.ConnInfo, suffrageheight base.Height) (_ base.SuffrageProof, found bool, _ error)
	LastBlockMap(_ context.Context, _ quicstream.ConnInfo, manifest util.Hash) (_ base.BlockMap, updated bool, _ error)
	BlockMap(context.Context, quicstream.ConnInfo, base.Height) (_ base.BlockMap, updated bool, _ error)
	BlockMapItem(context.Context, quicstream.ConnInfo, base.Height, base.BlockMapItemType, func(io.Reader, bool) error) error
	NodeChallenge(_ context.Context, _ quicstream.ConnInfo, _ base.NetworkID, _ base.Address, _ base.Publickey, input []byte, me base.LocalNode) (base.Signature, error)
	SuffrageNodeConnInfo(context.Context, quicstream.ConnInfo) ([]NodeConnInfo, error)
	SyncSourceConnInfo(context.Context, quicstream.ConnInfo) ([]NodeConnInfo, error)
	State(_ context.Context, _ quicstream.ConnInfo, key string, _ util.Hash) (base.State, bool, error)
	ExistsInStateOperation(_ context.Context, _ quicstream.ConnInfo, facthash util.Hash) (bool, error)
	// revive:enable:line-length-limit
}

type NodeConnInfo interface {
	base.Node
	network.ConnInfo
	ConnInfo() quicstream.ConnInfo
}
