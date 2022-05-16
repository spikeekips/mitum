package isaacnetwork

import (
	"bytes"
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type baseNetworkClientSend func(
	ctx context.Context,
	conninfo quictransport.ConnInfo,
	handlerprefix string,
	body []byte,
) (io.ReadCloser, error)

type baseNetworkClient struct {
	*baseNetwork
	send baseNetworkClientSend
}

func newBaseNetworkClient(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	send baseNetworkClientSend,
) *baseNetworkClient {
	return &baseNetworkClient{
		baseNetwork: newBaseNetwork(encs, enc),
		send:        send,
	}
}

func (c *baseNetworkClient) RequestProposal(
	ctx context.Context,
	ci quictransport.ConnInfo,
	point base.Point,
	proposer base.Address,
) (base.ProposalSignedFact, bool, error) {
	e := util.StringErrorFunc("failed to request proposal")

	body := NewRequestProposalBody(point, proposer)

	if err := body.IsValid(nil); err != nil {
		return nil, false, e(err, "")
	}

	b, err := c.marshal(body, c.enc)
	if err != nil {
		return nil, false, e(err, "failed to marshal body")
	}

	r, err := c.send(ctx, ci, HandlerPrefixRequestProposal, b)
	if err != nil {
		return nil, false, e(err, "failed to send request")
	}

	defer func() {
		_ = r.Close() //nolint:errcheck //...
	}()

	h, enc, err := c.loadOKHeader(ctx, r)

	switch {
	case err != nil:
		return nil, false, e(err, "failed to read stream")
	case h.Err() != nil:
		return nil, false, e(h.Err(), "")
	case !h.OK():
		return nil, false, nil
	default:
		switch i, err := c.loadProposal(r, enc); {
		case err != nil:
			return nil, false, e(err, "")
		default:
			return i, true, nil
		}
	}
}

func (c *baseNetworkClient) Proposal(
	ctx context.Context,
	ci quictransport.ConnInfo,
	pr util.Hash,
) (base.ProposalSignedFact, bool, error) {
	e := util.StringErrorFunc("failed to get proposal")

	body := NewProposalBody(pr)

	if err := body.IsValid(nil); err != nil {
		return nil, false, e(err, "")
	}

	b, err := c.marshal(body, c.enc)
	if err != nil {
		return nil, false, e(err, "failed to marshal body")
	}

	r, err := c.send(ctx, ci, HandlerPrefixProposal, b)
	if err != nil {
		return nil, false, e(err, "failed to send request")
	}

	h, enc, err := c.loadOKHeader(ctx, r)

	switch {
	case err != nil:
		return nil, false, e(err, "failed to read stream")
	case h.Err() != nil:
		return nil, false, e(h.Err(), "")
	case !h.OK():
		return nil, false, nil
	default:
		switch i, err := c.loadProposal(r, enc); {
		case err != nil:
			return nil, false, e(err, "")
		default:
			return i, true, nil
		}
	}
}

func (c *baseNetworkClient) LastSuffrageProof(
	ctx context.Context, ci quictransport.ConnInfo, manifest util.Hash,
) (isaac.SuffrageProof, bool, error) {
	e := util.StringErrorFunc("failed to get last suffrage proof")

	m, updated, err := c.LastBlockMap(ctx, ci, manifest)

	switch {
	case err != nil:
		return nil, false, e(err, "")
	case !updated:
		return nil, false, nil
	}

	proof, found, err := c.SuffrageProof(ctx, ci, m.Manifest().Suffrage())

	switch {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, e(err, "")
	default:
		return proof, true, nil
	}
}

func (c *baseNetworkClient) SuffrageProof(
	ctx context.Context, ci quictransport.ConnInfo, state util.Hash,
) (isaac.SuffrageProof, bool, error) {
	e := util.StringErrorFunc("failed to get suffrage proof")

	body := NewSuffrageProofBody(state)

	if err := body.IsValid(nil); err != nil {
		return nil, false, e(err, "")
	}

	b, err := c.marshal(body, c.enc)
	if err != nil {
		return nil, false, e(err, "failed to marshal body")
	}

	r, err := c.send(ctx, ci, HandlerPrefixSuffrageProof, b)
	if err != nil {
		return nil, false, e(err, "failed to send request")
	}

	h, enc, err := c.loadOKHeader(ctx, r)

	switch {
	case err != nil:
		return nil, false, e(err, "failed to read stream")
	case h.Err() != nil:
		return nil, false, e(h.Err(), "")
	case !h.OK():
		return nil, false, nil
	default:
		switch i, err := c.loadSuffrageProof(r, enc); {
		case err != nil:
			return nil, false, e(err, "")
		default:
			return i, true, nil
		}
	}
}

func (c *baseNetworkClient) LastBlockMap(
	ctx context.Context, ci quictransport.ConnInfo, manifest util.Hash,
) (base.BlockMap, bool, error) {
	e := util.StringErrorFunc("failed to get last BlockMap")

	body := NewLastBlockMapBody(manifest)

	if err := body.IsValid(nil); err != nil {
		return nil, false, e(err, "")
	}

	b, err := c.marshal(body, c.enc)
	if err != nil {
		return nil, false, e(err, "failed to marshal body")
	}

	r, err := c.send(ctx, ci, HandlerPrefixLastBlockMap, b)
	if err != nil {
		return nil, false, e(err, "failed to send request")
	}

	h, enc, err := c.loadOKHeader(ctx, r)

	switch {
	case err != nil:
		return nil, false, e(err, "failed to read stream")
	case h.Err() != nil:
		return nil, false, e(h.Err(), "")
	case !h.OK():
		return nil, false, nil
	default:
		switch i, err := c.loadBlockMap(r, enc); {
		case err != nil:
			return nil, false, e(err, "")
		default:
			return i, true, nil
		}
	}
}

func (c *baseNetworkClient) BlockMap(
	ctx context.Context, ci quictransport.ConnInfo, height base.Height,
) (base.BlockMap, bool, error) {
	e := util.StringErrorFunc("failed to get BlockMap")

	body := NewBlockMapBody(height)

	if err := body.IsValid(nil); err != nil {
		return nil, false, e(err, "")
	}

	b, err := c.marshal(body, c.enc)
	if err != nil {
		return nil, false, e(err, "failed to marshal body")
	}

	r, err := c.send(ctx, ci, HandlerPrefixBlockMap, b)
	if err != nil {
		return nil, false, e(err, "failed to send request")
	}

	h, enc, err := c.loadOKHeader(ctx, r)

	switch {
	case err != nil:
		return nil, false, e(err, "failed to read stream")
	case h.Err() != nil:
		return nil, false, e(h.Err(), "")
	case !h.OK():
		return nil, false, nil
	default:
		switch i, err := c.loadBlockMap(r, enc); {
		case err != nil:
			return nil, false, e(err, "")
		default:
			return i, true, nil
		}
	}
}

func (c *baseNetworkClient) BlockMapItem(
	ctx context.Context, ci quictransport.ConnInfo, height base.Height, item base.BlockMapItemType,
) (io.ReadCloser, error) {
	e := util.StringErrorFunc("failed to get BlockMap")

	body := NewBlockMapItemBody(height, item)

	if err := body.IsValid(nil); err != nil {
		return nil, e(err, "")
	}

	b, err := c.marshal(body, c.enc)
	if err != nil {
		return nil, e(err, "failed to marshal body")
	}

	r, err := c.send(ctx, ci, HandlerPrefixBlockMapItem, b)
	if err != nil {
		return nil, e(err, "failed to send request")
	}

	// BLOCK apply new quicstream header system

	return r, nil
}

func (c *baseNetworkClient) loadOKHeader(ctx context.Context, r io.ReadCloser) (h OKResponseHeader, enc encoder.Encoder, _ error) {
	e := util.StringErrorFunc("failed to load ok header")

	enc, err := c.readEncoder(r)
	if err != nil {
		return h, nil, e(err, "")
	}

	if err := c.readHeader(r, enc, &h); err != nil {
		return h, nil, e(err, "failed to read stream")
	}

	return h, enc, nil
}

func (c *baseNetworkClient) loadProposal(r io.Reader, enc encoder.Encoder) (base.ProposalSignedFact, error) {
	// BLOCK use readHinter()
	e := util.StringErrorFunc("failed to load proposal")

	b, err := io.ReadAll(r)
	if err != nil {
		return nil, e(err, "")
	}

	hinter, err := enc.Decode(b)

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

func (c *baseNetworkClient) loadSuffrageProof(r io.Reader, enc encoder.Encoder) (isaac.SuffrageProof, error) {
	e := util.StringErrorFunc("failed to load SuffrageProof")

	b, err := io.ReadAll(r)
	if err != nil {
		return nil, e(err, "")
	}

	hinter, err := enc.Decode(b)

	switch {
	case err != nil:
		return nil, err
	case hinter == nil:
		return nil, errors.Errorf("empty proposal")
	}

	switch i, ok := hinter.(isaac.SuffrageProof); {
	case !ok:
		return nil, errors.Errorf("not SuffrageProof: %T", hinter)
	default:
		return i, nil
	}
}

func (c *baseNetworkClient) loadBlockMap(r io.Reader, enc encoder.Encoder) (base.BlockMap, error) {
	e := util.StringErrorFunc("failed to load BlockMap")

	b, err := io.ReadAll(r)
	if err != nil {
		return nil, e(err, "")
	}

	hinter, err := enc.Decode(b)

	switch {
	case err != nil:
		return nil, err
	case hinter == nil:
		return nil, errors.Errorf("empty BlockMap")
	}

	switch i, ok := hinter.(base.BlockMap); {
	case !ok:
		return nil, errors.Errorf("not BlockMap: %T", hinter)
	default:
		return i, nil
	}
}

func (c *baseNetworkClient) marshal(body interface{}, enc encoder.Encoder) ([]byte, error) {
	if body == nil {
		return nil, nil
	}

	e := util.StringErrorFunc("failed to marshal body")

	buf := bytes.NewBuffer(nil)
	if err := writeHint(buf, enc.Hint()); err != nil {
		return nil, e(err, "")
	}

	b, err := enc.Marshal(body)
	if err != nil {
		return nil, e(err, "")
	}

	_, _ = buf.Write(b)

	return buf.Bytes(), nil
}

func (c *baseNetworkClient) readHeader(r io.Reader, enc encoder.Encoder, header Header) error {
	e := util.StringErrorFunc("failed to read header")

	b, err := readHeader(r)
	if err != nil {
		return e(err, "")
	}

	hinter, err := enc.Decode(b)
	if err != nil {
		return e(err, "")
	}

	if err := util.InterfaceSetValue(hinter, header); err != nil {
		return e(err, "")
	}

	return nil
}
