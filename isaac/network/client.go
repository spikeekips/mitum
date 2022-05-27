package isaacnetwork

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

type baseNetworkClientWriteFunc func(
	ctx context.Context,
	conninfo quictransport.ConnInfo,
	writef quicstream.ClientWriteFunc,
) (io.ReadCloser, error)

type baseNetworkClient struct {
	*baseNetwork
	writef baseNetworkClientWriteFunc
}

func newBaseNetworkClient(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	writef baseNetworkClientWriteFunc,
) *baseNetworkClient {
	return &baseNetworkClient{
		baseNetwork: newBaseNetwork(encs, enc),
		writef:      writef,
	}
}

func (c *baseNetworkClient) RequestProposal(
	ctx context.Context,
	ci quictransport.ConnInfo,
	point base.Point,
	proposer base.Address,
) (base.ProposalSignedFact, bool, error) {
	e := util.StringErrorFunc("failed to request proposal")

	header := NewRequestProposalRequestHeader(point, proposer)

	if err := header.IsValid(nil); err != nil {
		return nil, false, e(err, "")
	}

	r, err := c.write(ctx, ci, c.enc, HandlerPrefixRequestProposal, header, nil)
	if err != nil {
		return nil, false, e(err, "failed to send request")
	}

	defer func() {
		_ = r.Close()
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
		var u base.ProposalSignedFact

		if err := encoder.DecodeReader(enc, r, &u); err != nil {
			return nil, false, e(err, "")
		}

		return u, true, nil
	}
}

func (c *baseNetworkClient) Proposal( //nolint:dupl //...
	ctx context.Context,
	ci quictransport.ConnInfo,
	pr util.Hash,
) (base.ProposalSignedFact, bool, error) {
	e := util.StringErrorFunc("failed to get proposal")

	header := NewProposalRequestHeader(pr)

	if err := header.IsValid(nil); err != nil {
		return nil, false, e(err, "")
	}

	r, err := c.write(ctx, ci, c.enc, HandlerPrefixProposal, header, nil)
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
		var u base.ProposalSignedFact

		if err := encoder.DecodeReader(enc, r, &u); err != nil {
			return nil, false, e(err, "")
		}

		return u, true, nil
	}
}

func (c *baseNetworkClient) LastSuffrageProof(
	ctx context.Context, ci quictransport.ConnInfo, manifest util.Hash,
) (base.SuffrageProof, bool, error) {
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

func (c *baseNetworkClient) SuffrageProof( //nolint:dupl //...
	ctx context.Context, ci quictransport.ConnInfo, state util.Hash,
) (base.SuffrageProof, bool, error) {
	e := util.StringErrorFunc("failed to get suffrage proof")

	header := NewSuffrageProofRequestHeader(state)

	if err := header.IsValid(nil); err != nil {
		return nil, false, e(err, "")
	}

	r, err := c.write(ctx, ci, c.enc, HandlerPrefixSuffrageProof, header, nil)
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
		var u base.SuffrageProof

		if err := encoder.DecodeReader(enc, r, &u); err != nil {
			return nil, false, e(err, "")
		}

		return u, true, nil
	}
}

func (c *baseNetworkClient) LastBlockMap( //nolint:dupl //...
	ctx context.Context, ci quictransport.ConnInfo, manifest util.Hash,
) (base.BlockMap, bool, error) {
	e := util.StringErrorFunc("failed to get last BlockMap")

	header := NewLastBlockMapHeader(manifest)

	if err := header.IsValid(nil); err != nil {
		return nil, false, e(err, "")
	}

	r, err := c.write(ctx, ci, c.enc, HandlerPrefixLastBlockMap, header, nil)
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
		var u base.BlockMap

		if err := encoder.DecodeReader(enc, r, &u); err != nil {
			return nil, false, e(err, "")
		}

		return u, true, nil
	}
}

func (c *baseNetworkClient) BlockMap( //nolint:dupl //...
	ctx context.Context, ci quictransport.ConnInfo, height base.Height,
) (base.BlockMap, bool, error) {
	e := util.StringErrorFunc("failed to get BlockMap")

	header := NewBlockMapHeader(height)

	if err := header.IsValid(nil); err != nil {
		return nil, false, e(err, "")
	}

	r, err := c.write(ctx, ci, c.enc, HandlerPrefixBlockMap, header, nil)
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
		var u base.BlockMap

		if err := encoder.DecodeReader(enc, r, &u); err != nil {
			return nil, false, e(err, "")
		}

		return u, true, nil
	}
}

func (c *baseNetworkClient) BlockMapItem(
	ctx context.Context, ci quictransport.ConnInfo, height base.Height, item base.BlockMapItemType,
) (io.ReadCloser, bool, error) {
	// NOTE the io.ReadCloser should be closed.

	e := util.StringErrorFunc("failed to get BlockMap")

	header := NewBlockMapItemRequestHeader(height, item)

	if err := header.IsValid(nil); err != nil {
		return nil, false, e(err, "")
	}

	r, err := c.write(ctx, ci, c.enc, HandlerPrefixBlockMapItem, header, nil)
	if err != nil {
		return nil, false, e(err, "failed to send request")
	}

	h, _, err := c.loadOKHeader(ctx, r)

	switch {
	case err != nil:
		return nil, false, e(err, "failed to read stream")
	case h.Err() != nil:
		return nil, false, e(h.Err(), "")
	case !h.OK():
		return nil, false, nil
	default:
		return r, true, nil
	}
}

func (c *baseNetworkClient) loadOKHeader(
	_ context.Context,
	r io.ReadCloser,
) (h OKResponseHeader, enc encoder.Encoder, _ error) {
	e := util.StringErrorFunc("failed to load ok header")

	enc, err := c.readEncoder(r)
	if err != nil {
		return h, nil, e(err, "")
	}

	switch b, err := readHeader(r); {
	case err != nil:
		return h, nil, e(err, "")
	default:
		if err := encoder.Decode(enc, b, &h); err != nil {
			return h, nil, e(err, "failed to read stream")
		}

		return h, enc, nil
	}
}

func (c *baseNetworkClient) write(
	ctx context.Context,
	ci quictransport.ConnInfo,
	enc encoder.Encoder,
	handlerprefix string,
	header Header,
	body io.Reader,
) (io.ReadCloser, error) {
	b, err := enc.Marshal(header)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	r, err := c.writef(ctx, ci, func(w io.Writer) error {
		if err = quicstream.WritePrefix(w, handlerprefix); err != nil {
			return errors.Wrap(err, "")
		}

		if err = writeHint(w, enc.Hint()); err != nil {
			return errors.Wrap(err, "")
		}

		if _, err = w.Write(b); err != nil {
			return errors.Wrap(err, "")
		}

		if body != nil {
			if _, err = io.Copy(w, body); err != nil {
				return errors.Wrap(err, "")
			}
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to write")
	}

	return r, nil
}
