package isaacnetwork

import (
	"context"
	"io"
	"net"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type QuicstreamHandlers struct {
	pool isaac.TempPoolDatabase
	*baseNetwork
	proposalMaker *isaac.ProposalMaker
	suffrageProof func(util.Hash) (isaac.SuffrageProof, bool, error)
	lastBlockMap  func(util.Hash) (base.BlockMap, bool, error)
	blockMap      func(base.Height) (base.BlockMap, bool, error)
	blockMapItem  func(base.Height, base.BlockMapItemType) (io.ReadCloser, bool, error)
	local         isaac.LocalNode
}

func NewQuicstreamHandlers(
	local isaac.LocalNode,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	pool isaac.TempPoolDatabase,
	proposalMaker *isaac.ProposalMaker,
	suffrageProof func(util.Hash) (isaac.SuffrageProof, bool, error),
	lastBlockMap func(util.Hash) (base.BlockMap, bool, error),
	blockMap func(base.Height) (base.BlockMap, bool, error),
	blockMapItem func(base.Height, base.BlockMapItemType) (io.ReadCloser, bool, error),
) *QuicstreamHandlers {
	return &QuicstreamHandlers{
		baseNetwork:   newBaseNetwork(encs, enc),
		local:         local,
		pool:          pool,
		proposalMaker: proposalMaker,
		suffrageProof: suffrageProof,
		lastBlockMap:  lastBlockMap,
		blockMap:      blockMap,
		blockMapItem:  blockMapItem,
	}
}

func (c *QuicstreamHandlers) ErrorHandler(_ net.Addr, _ io.Reader, w io.Writer, err error) error {
	if e := c.response(w, NewErrorResponseHeader(err), nil, c.enc); e != nil {
		return errors.Wrap(e, "failed to response error response")
	}

	return nil
}

func (c *QuicstreamHandlers) RequestProposal(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle request proposal")

	enc, err := c.readEncoder(r)
	if err != nil {
		return e(err, "")
	}

	var body RequestProposalRequestHeader
	if err = c.readHinter(r, enc, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	// BLOCK if point is too old, returns error

	pr, err := c.getOrCreateProposal(body.point, body.proposer)

	header := NewOKResponseHeader(pr != nil, err)

	if err := c.response(w, header, pr, enc); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) Proposal(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle get proposal")

	enc, err := c.readEncoder(r)
	if err != nil {
		return e(err, "")
	}

	var body ProposalRequestHeader
	if err = c.readHinter(r, enc, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	pr, found, err := c.pool.Proposal(body.proposal)

	header := NewOKResponseHeader(found, err)

	if err := c.response(w, header, pr, enc); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) SuffrageProof(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle get suffrage proof")

	enc, err := c.readEncoder(r)
	if err != nil {
		return e(err, "")
	}

	var body SuffrageProofRequestHeader
	if err = c.readHinter(r, enc, &body); err != nil {
		return e(err, "")
	}

	proof, found, err := c.suffrageProof(body.State())
	header := NewOKResponseHeader(found, err)

	if err := c.response(w, header, proof, enc); err != nil {
		return e(err, "")
	}

	return nil
}

// LastBlockMap responds the last BlockMap to client; if there is no BlockMap,
// it returns nil BlockMap and not updated without error.
func (c *QuicstreamHandlers) LastBlockMap(_ net.Addr, r io.Reader, w io.Writer) error { //nolint:dupl //...
	e := util.StringErrorFunc("failed to handle request last BlockMap")

	enc, err := c.readEncoder(r)
	if err != nil {
		return e(err, "")
	}

	var body LastBlockMapRequestHeader
	if err = c.readHinter(r, enc, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	m, updated, err := c.lastBlockMap(body.Manifest())
	header := NewOKResponseHeader(updated, err)

	if err := c.response(w, header, m, enc); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) BlockMap(_ net.Addr, r io.Reader, w io.Writer) error { //nolint:dupl //...
	e := util.StringErrorFunc("failed to handle request BlockMap")

	enc, err := c.readEncoder(r)
	if err != nil {
		return e(err, "")
	}

	var body BlockMapRequestHeader
	if err = c.readHinter(r, enc, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	m, found, err := c.blockMap(body.Height())
	header := NewOKResponseHeader(found, err)

	if err := c.response(w, header, m, enc); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) BlockMapItem(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle request BlockMapItem")

	enc, err := c.readEncoder(r)
	if err != nil {
		return e(err, "")
	}

	var body BlockMapItemRequestHeader
	if err = c.readHinter(r, enc, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	itemr, found, err := c.blockMapItem(body.Height(), body.Item())
	if itemr != nil {
		defer func() {
			_ = itemr.Close()
		}()
	}

	header := NewOKResponseHeader(found, err)

	if err := c.response(w, header, nil, enc); err != nil {
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

func (c *QuicstreamHandlers) getOrCreateProposal(
	point base.Point,
	proposer base.Address,
) (base.ProposalSignedFact, error) {
	switch pr, found, err := c.pool.ProposalByPoint(point, proposer); {
	case err != nil:
		return nil, errors.Wrap(err, "")
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
