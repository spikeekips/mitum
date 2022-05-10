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

type (
	QuicstreamNodeNetworkHandlerLastSuffrageFunction func() (base.SuffrageInfo, bool, error)
)

type QuicstreamNodeNetworkHandlers struct {
	pool isaac.TempPoolDatabase
	*baseNodeNetwork
	proposalMaker *isaac.ProposalMaker
	lastSuffragef QuicstreamNodeNetworkHandlerLastSuffrageFunction
	local         isaac.LocalNode
}

func NewQuicstreamNodeNetworkHandlers(
	local isaac.LocalNode,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	pool isaac.TempPoolDatabase,
	proposalMaker *isaac.ProposalMaker,
	lastSuffragef QuicstreamNodeNetworkHandlerLastSuffrageFunction,
) *QuicstreamNodeNetworkHandlers {
	return &QuicstreamNodeNetworkHandlers{
		baseNodeNetwork: newBaseNodeNetwork(encs, enc),
		local:           local,
		pool:            pool,
		proposalMaker:   proposalMaker,
		lastSuffragef:   lastSuffragef,
	}
}

func (c *QuicstreamNodeNetworkHandlers) RequestProposal(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle request proposal")

	b, err := io.ReadAll(r)
	if err != nil {
		return e(err, "")
	}

	hinter, err := c.readHinter(b)
	switch {
	case err != nil:
		return e(err, "")
	case hinter == nil:
		return e(nil, "empty request body")
	}

	body, ok := hinter.(RequestProposalBody)
	switch {
	case !ok:
		return e(nil, "not RequestProposalBody: %T", hinter)
	default:
		if err = body.IsValid(nil); err != nil {
			return e(err, "")
		}
	}

	// BLOCK if point is too old, returns error

	pr, err := c.getOrCreateProposal(body.Point, body.Proposer)
	switch {
	case err != nil:
		return e(err, "")
	case pr == nil:
		return nil
	}

	if err := c.response(w, pr); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamNodeNetworkHandlers) getOrCreateProposal(
	point base.Point,
	proposer base.Address,
) (base.ProposalSignedFact, error) {
	// NOTE find proposal of this point
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

func (c *QuicstreamNodeNetworkHandlers) Proposal(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle get proposal")

	b, err := io.ReadAll(r)
	if err != nil {
		return e(err, "")
	}

	hinter, err := c.readHinter(b)
	switch {
	case err != nil:
		return e(err, "")
	case hinter == nil:
		return e(nil, "empty request body")
	}

	body, ok := hinter.(ProposalBody)
	switch {
	case !ok:
		return e(nil, "not ProposalBody: %T", hinter)
	default:
		if err = body.IsValid(nil); err != nil {
			return e(err, "")
		}
	}

	pr, found, err := c.pool.Proposal(body.Proposal)
	switch {
	case err != nil:
		return err
	case !found:
		return nil
	}

	if err := c.response(w, pr); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamNodeNetworkHandlers) LastSuffrage(_ net.Addr, _ io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle get last suffrage state")

	switch info, found, err := c.lastSuffragef(); {
	case err != nil:
		return e(err, "")
	case !found:
		return nil
	default:
		if err := c.response(w, info); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func (c *QuicstreamNodeNetworkHandlers) response(w io.Writer, i interface{}) error {
	switch b, err := c.marshal(i); {
	case err != nil:
		return errors.Wrap(err, "")
	default:
		if _, err := w.Write(b); err != nil {
			return errors.Wrap(err, "failed to response to client")
		}

		return nil
	}
}
