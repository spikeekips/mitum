package isaacnetwork

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type QuicstreamHandlers struct {
	pool isaac.ProposalPool
	*baseNetwork
	proposalMaker         *isaac.ProposalMaker
	lastSuffrageProoff    func(suffragestate util.Hash) (base.SuffrageProof, bool, error)
	suffrageProoff        func(base.Height) (base.SuffrageProof, bool, error)
	lastBlockMapf         func(util.Hash) (base.BlockMap, bool, error)
	blockMapf             func(base.Height) (base.BlockMap, bool, error)
	blockMapItemf         func(base.Height, base.BlockMapItemType) (io.ReadCloser, bool, error)
	suffrageNodeConnInfof func() ([]isaac.NodeConnInfo, error)
	syncSourceConnInfof   func() ([]isaac.NodeConnInfo, error)
	local                 base.LocalNode
	nodepolicy            isaac.NodePolicy
}

func NewQuicstreamHandlers( // revive:disable-line:argument-limit
	local base.LocalNode,
	nodepolicy isaac.NodePolicy,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	idleTimeout time.Duration,
	pool isaac.ProposalPool,
	proposalMaker *isaac.ProposalMaker,
	lastSuffrageProoff func(util.Hash) (base.SuffrageProof, bool, error),
	suffrageProoff func(base.Height) (base.SuffrageProof, bool, error),
	lastBlockMapf func(util.Hash) (base.BlockMap, bool, error),
	blockMapf func(base.Height) (base.BlockMap, bool, error),
	blockMapItemf func(base.Height, base.BlockMapItemType) (io.ReadCloser, bool, error),
	suffrageNodeConnInfof func() ([]isaac.NodeConnInfo, error),
	syncSourceConnInfof func() ([]isaac.NodeConnInfo, error),
) *QuicstreamHandlers {
	return &QuicstreamHandlers{
		baseNetwork:           newBaseNetwork(encs, enc, idleTimeout),
		local:                 local,
		nodepolicy:            nodepolicy,
		pool:                  pool,
		proposalMaker:         proposalMaker,
		lastSuffrageProoff:    lastSuffrageProoff,
		suffrageProoff:        suffrageProoff,
		lastBlockMapf:         lastBlockMapf,
		blockMapf:             blockMapf,
		blockMapItemf:         blockMapItemf,
		suffrageNodeConnInfof: suffrageNodeConnInfof,
		syncSourceConnInfof:   syncSourceConnInfof,
	}
}

func (c *QuicstreamHandlers) ErrorHandler(_ net.Addr, _ io.Reader, w io.Writer, err error) error {
	if e := Response(w, NewResponseHeader(false, err), nil, c.enc); e != nil {
		return errors.Wrap(e, "failed to response error response")
	}

	return nil
}

func (c *QuicstreamHandlers) RequestProposal(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle request proposal")

	enc, hb, err := c.prehandle(r)
	if err != nil {
		return e(err, "")
	}

	var body RequestProposalRequestHeader
	if err = encoder.Decode(enc, hb, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	// FIXME if point is too old, returns error

	pr, err := c.getOrCreateProposal(body.point, body.proposer)

	header := NewResponseHeader(pr != nil, err)

	if err := Response(w, header, pr, enc); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) Proposal(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle get proposal")

	enc, hb, err := c.prehandle(r)
	if err != nil {
		return e(err, "")
	}

	var body ProposalRequestHeader
	if err = encoder.Decode(enc, hb, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	pr, found, err := c.pool.Proposal(body.proposal)

	header := NewResponseHeader(found, err)

	if err := Response(w, header, pr, enc); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) LastSuffrageProof(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle get last suffrage proof")

	enc, hb, err := c.prehandle(r)
	if err != nil {
		return e(err, "")
	}

	var body LastSuffrageProofRequestHeader
	if err = encoder.Decode(enc, hb, &body); err != nil {
		return e(err, "")
	}

	proof, updated, err := c.lastSuffrageProoff(body.State())
	header := NewResponseHeader(updated, err)

	if err := Response(w, header, proof, enc); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) SuffrageProof(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle get suffrage proof")

	enc, hb, err := c.prehandle(r)
	if err != nil {
		return e(err, "")
	}

	var body SuffrageProofRequestHeader
	if err = encoder.Decode(enc, hb, &body); err != nil {
		return e(err, "")
	}

	proof, found, err := c.suffrageProoff(body.Height())
	header := NewResponseHeader(found, err)

	if err := Response(w, header, proof, enc); err != nil {
		return e(err, "")
	}

	return nil
}

// LastBlockMap responds the last BlockMap to client; if there is no BlockMap,
// it returns nil BlockMap and not updated without error.
func (c *QuicstreamHandlers) LastBlockMap(_ net.Addr, r io.Reader, w io.Writer) error { //nolint:dupl //...
	e := util.StringErrorFunc("failed to handle request last BlockMap")

	enc, hb, err := c.prehandle(r)
	if err != nil {
		return e(err, "")
	}

	var body LastBlockMapRequestHeader
	if err = encoder.Decode(enc, hb, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	m, updated, err := c.lastBlockMapf(body.Manifest())
	header := NewResponseHeader(updated, err)

	if err := Response(w, header, m, enc); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) BlockMap(_ net.Addr, r io.Reader, w io.Writer) error { //nolint:dupl //...
	e := util.StringErrorFunc("failed to handle request BlockMap")

	enc, hb, err := c.prehandle(r)
	if err != nil {
		return e(err, "")
	}

	var body BlockMapRequestHeader
	if err = encoder.Decode(enc, hb, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	m, found, err := c.blockMapf(body.Height())
	header := NewResponseHeader(found, err)

	if err := Response(w, header, m, enc); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) BlockMapItem(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle request BlockMapItem")

	enc, hb, err := c.prehandle(r)
	if err != nil {
		return e(err, "")
	}

	var body BlockMapItemRequestHeader
	if err = encoder.Decode(enc, hb, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	itemr, found, err := c.blockMapItemf(body.Height(), body.Item())
	if itemr != nil {
		defer func() {
			_ = itemr.Close()
		}()
	}

	header := NewResponseHeader(found, err)

	if err := Response(w, header, nil, enc); err != nil {
		return e(err, "")
	}

	if itemr == nil {
		return nil
	}

	if _, err := io.Copy(w, itemr); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) NodeChallenge(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle NodeChallenge")

	enc, hb, err := c.prehandle(r)
	if err != nil {
		return e(err, "")
	}

	var body NodeChallengeRequestHeader
	if err = encoder.Decode(enc, hb, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	sig, err := c.local.Privatekey().Sign(util.ConcatBytesSlice(
		c.local.Address().Bytes(),
		c.nodepolicy.NetworkID(),
		body.Input(),
	))
	if err != nil {
		return e(err, "")
	}

	header := NewResponseHeader(true, nil)

	if err := Response(w, header, sig, enc); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) SuffrageNodeConnInfo(addr net.Addr, r io.Reader, w io.Writer) error {
	var body SuffrageNodeConnInfoRequestHeader

	if err := c.nodeConnInfos(addr, r, w, body, c.suffrageNodeConnInfof); err != nil {
		return errors.WithMessage(err, "failed to handle SuffrageNodeConnInfo")
	}

	return nil
}

func (c *QuicstreamHandlers) SyncSourceConnInfo(addr net.Addr, r io.Reader, w io.Writer) error {
	var body SyncSourceConnInfoRequestHeader

	if err := c.nodeConnInfos(addr, r, w, body, c.syncSourceConnInfof); err != nil {
		return errors.WithMessage(err, "failed to handle SyncSourceConnInfo")
	}

	return nil
}

func (c *QuicstreamHandlers) prehandle(r io.Reader) (encoder.Encoder, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.idleTimeout)
	defer cancel()

	return HandlerReadHead(ctx, c.encs, r)
}

func (c *QuicstreamHandlers) getOrCreateProposal(
	point base.Point,
	proposer base.Address,
) (base.ProposalSignedFact, error) {
	switch pr, found, err := c.pool.ProposalByPoint(point, proposer); {
	case err != nil:
		return nil, err
	case found:
		return pr, nil
	}

	if !proposer.Equal(c.local.Address()) {
		return nil, nil
	}

	// NOTE if proposer is local, create new one
	switch pr, err := c.proposalMaker.New(context.Background(), point); {
	case err != nil:
		return nil, err
	default:
		return pr, nil
	}
}

func (c *QuicstreamHandlers) nodeConnInfos(
	_ net.Addr, r io.Reader, w io.Writer,
	body isaac.NetworkHeader,
	f func() ([]isaac.NodeConnInfo, error),
) error {
	enc, hb, err := c.prehandle(r)
	if err != nil {
		return err
	}

	if err = encoder.Decode(enc, hb, &body); err != nil {
		return err
	}

	if err = body.IsValid(nil); err != nil {
		return err
	}

	cis, err := f()
	if err != nil {
		return err
	}

	header := NewResponseHeader(true, nil)

	return Response(w, header, cis, enc)
}
