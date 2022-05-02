package isaacnodenetwork

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type baseNodeNetworkClientSend func(
	ctx context.Context,
	conninfo quictransport.ConnInfo,
	handlerprefix string,
	body []byte,
) (io.ReadCloser, error)

type baseNodeNetworkClient struct {
	*baseNodeNetwork
	send baseNodeNetworkClientSend
}

func newBaseNodeNetworkClient(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	send baseNodeNetworkClientSend,
) *baseNodeNetworkClient {
	return &baseNodeNetworkClient{
		baseNodeNetwork: newBaseNodeNetwork(encs, enc),
		send:            send,
	}
}

func (c *baseNodeNetworkClient) RequestProposal(
	ctx context.Context,
	ci quictransport.ConnInfo,
	point base.Point,
	proposer base.Address,
) (base.ProposalSignedFact, bool, error) {
	e := util.StringErrorFunc("failed to request proposal")

	body := NewRequestProposalBody(point, proposer)
	if err := body.IsValid(nil); err != nil {
		return nil, false, e(err, "invalid request body")
	}

	b, err := c.marshal(body)
	if err != nil {
		return nil, false, e(err, "failed to marshal body")
	}

	r, err := c.send(ctx, ci, HandlerPrefixRequestProposal, b)
	if err != nil {
		return nil, false, e(err, "failed to send request")
	}
	defer func() {
		_ = r.Close()
	}()

	rb, err := quicstream.ReadAll(ctx, r)
	switch {
	case err != nil:
		return nil, false, e(err, "failed to read stream")
	case len(rb) < 1:
		return nil, false, nil
	}

	switch i, err := c.loadProposal(rb); {
	case err != nil:
		return nil, false, e(err, "")
	default:
		return i, true, nil
	}
}

func (c *baseNodeNetworkClient) Proposal(
	ctx context.Context,
	ci quictransport.ConnInfo,
	pr util.Hash,
) (base.ProposalSignedFact, bool, error) {
	e := util.StringErrorFunc("failed to get proposal")

	body := NewProposalBody(pr)
	if err := body.IsValid(nil); err != nil {
		return nil, false, e(err, "invalid request body")
	}

	b, err := c.marshal(body)
	if err != nil {
		return nil, false, e(err, "failed to marshal body")
	}

	r, err := c.send(ctx, ci, HandlerPrefixProposal, b)
	if err != nil {
		return nil, false, e(err, "failed to send request")
	}

	rb, err := quicstream.ReadAll(ctx, r)
	switch {
	case err != nil:
		return nil, false, e(err, "failed to read stream")
	case len(rb) < 1:
		return nil, false, nil
	}

	switch i, err := c.loadProposal(rb); {
	case err != nil:
		return nil, false, e(err, "")
	default:
		return i, true, nil
	}
}

func (c *baseNodeNetworkClient) LastSuffrage(
	ctx context.Context,
	ci quictransport.ConnInfo,
) (base.Manifest, base.SuffrageStateValue, bool, error) {
	e := util.StringErrorFunc("failed to request proposal")

	r, err := c.send(ctx, ci, HandlerPrefixLastSuffrage, nil)
	if err != nil {
		return nil, nil, false, e(err, "failed to send request")
	}

	rb, err := quicstream.ReadAll(ctx, r)
	switch {
	case err != nil:
		return nil, nil, false, e(err, "failed to read stream")
	case len(rb) < 1:
		return nil, nil, false, nil
	}

	enc, raw, err := c.readEncoder(rb)
	if err != nil {
		return nil, nil, false, e(err, "")
	}

	l, err := enc.DecodeSlice(raw)
	switch {
	case err != nil:
		return nil, nil, false, e(err, "")
	case len(l) != 2:
		return nil, nil, false, e(nil, "invalid response")
	}

	manifest, ok := l[0].(base.Manifest)
	if !ok {
		return nil, nil, false, e(nil, "invalid response; not manifest")
	}

	stv, ok := l[1].(base.SuffrageStateValue)
	if !ok {
		return nil, nil, false, e(nil, "invalid response; not suffrage state value")
	}

	return manifest, stv, true, nil
}

func (c *baseNodeNetworkClient) loadProposal(b []byte) (base.ProposalSignedFact, error) {
	hinter, err := c.readHinter(b)
	switch {
	case err != nil:
		return nil, err
	case hinter == nil:
		return nil, errors.Errorf("empty proposal")
	}

	switch i, ok := hinter.(base.ProposalSignedFact); {
	case !ok:
		return nil, errors.Errorf("not ProposalSignedFact: %T", hinter)
	default:
		return i, nil
	}
}
