package isaac

import (
	"context"
	"io"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
)

// revive:disable:line-length-limit
type NetworkClient interface {
	Request(context.Context, quicstream.UDPConnInfo, NetworkHeader) (NetworkResponseHeader, interface{}, error)
	NewOperation(context.Context, quicstream.UDPConnInfo, base.Operation) (bool, error)
	RequestProposal(_ context.Context, connInfo quicstream.UDPConnInfo, point base.Point, propser base.Address) (base.ProposalSignedFact, bool, error)
	Proposal(_ context.Context, connInfo quicstream.UDPConnInfo, facthash util.Hash) (base.ProposalSignedFact, bool, error)
	LastSuffrageProof(_ context.Context, connInfo quicstream.UDPConnInfo, state util.Hash) (_ base.SuffrageProof, updated bool, _ error)
	SuffrageProof(_ context.Context, connInfo quicstream.UDPConnInfo, suffrageheight base.Height) (_ base.SuffrageProof, found bool, _ error)
	LastBlockMap(_ context.Context, _ quicstream.UDPConnInfo, manifest util.Hash) (_ base.BlockMap, updated bool, _ error)
	BlockMap(context.Context, quicstream.UDPConnInfo, base.Height) (_ base.BlockMap, updated bool, _ error)
	BlockMapItem(context.Context, quicstream.UDPConnInfo, base.Height, base.BlockMapItemType) (io.ReadCloser, func() error, bool, error)
	NodeChallenge(_ context.Context, _ quicstream.UDPConnInfo, _ base.NetworkID, _ base.Address, _ base.Publickey, input []byte) (base.Signature, error)
	SuffrageNodeConnInfo(context.Context, quicstream.UDPConnInfo) ([]NodeConnInfo, error)
	SyncSourceConnInfo(context.Context, quicstream.UDPConnInfo) ([]NodeConnInfo, error)
}

// revive:enable:line-length-limit

type NetworkHeader interface {
	util.IsValider
	HandlerPrefix() string
}

type NetworkResponseHeader interface {
	NetworkHeader
	Err() error
	OK() bool
}

type NodeConnInfo interface {
	base.Node
	network.ConnInfo
	UDPConnInfo() (quicstream.UDPConnInfo, error)
}
