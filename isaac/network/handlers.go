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
	blockReader   func(base.Height) (isaac.BlockReader, error)
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
	blockReader func(base.Height) (isaac.BlockReader, error),
) *QuicstreamHandlers {
	return &QuicstreamHandlers{
		baseNetwork:   newBaseNetwork(encs, enc),
		local:         local,
		pool:          pool,
		proposalMaker: proposalMaker,
		suffrageProof: suffrageProof,
		lastBlockMap:  lastBlockMap,
		blockMap:      blockMap,
		blockReader:   blockReader,
	}
}

func (c *QuicstreamHandlers) ErrorHandler(_ net.Addr, r io.Reader, w io.Writer, err error) error {
	if err := c.response(w, NewErrorResponseHeader(err), nil, c.enc); err != nil {
		return errors.Wrap(err, "failed to response error response")
	}

	return nil
}

func (c *QuicstreamHandlers) RequestProposal(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle request proposal")

	enc, err := c.readEncoder(r)
	if err != nil {
		return e(err, "")
	}

	var body RequestProposalBody
	if err := c.readHinter(r, enc, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	// BLOCK if point is too old, returns error

	pr, err := c.getOrCreateProposal(body.Point, body.Proposer)

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

	var body ProposalBody
	if err := c.readHinter(r, enc, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	pr, found, err := c.pool.Proposal(body.Proposal)

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

	var body SuffrageProofBody
	if err := c.readHinter(r, enc, &body); err != nil {
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
func (c *QuicstreamHandlers) LastBlockMap(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle request last BlockMap")

	enc, err := c.readEncoder(r)
	if err != nil {
		return e(err, "")
	}

	var body LastBlockMapBody
	if err := c.readHinter(r, enc, &body); err != nil {
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

func (c *QuicstreamHandlers) BlockMap(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle request BlockMap")

	enc, err := c.readEncoder(r)
	if err != nil {
		return e(err, "")
	}

	var body BlockMapBody
	if err := c.readHinter(r, enc, &body); err != nil {
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

	var body BlockMapItemBody
	if err := c.readHinter(r, enc, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	blockReader, err := c.blockReader(body.Height())
	if err != nil {
		return e(err, "")
	}

	reader, found, err := blockReader.Reader(body.Item())

	header := NewOKResponseHeader(found, err)

	if err := c.response(w, header, nil, enc); err != nil {
		return e(err, "")
	}

	if _, err := io.Copy(w, reader); err != nil {
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
