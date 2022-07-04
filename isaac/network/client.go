package isaacnetwork

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type baseNetworkClientWriteFunc func(
	ctx context.Context,
	conninfo quicstream.UDPConnInfo,
	writef quicstream.ClientWriteFunc,
) (_ io.ReadCloser, cancel func() error, _ error)

type baseNetworkClient struct {
	*baseNetwork
	writef baseNetworkClientWriteFunc
}

func newBaseNetworkClient(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	idleTimeout time.Duration,
	writef baseNetworkClientWriteFunc,
) *baseNetworkClient {
	return &baseNetworkClient{
		baseNetwork: newBaseNetwork(encs, enc, idleTimeout),
		writef:      writef,
	}
}

func (c *baseNetworkClient) Request(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header isaac.NetworkHeader,
) (
	isaac.NetworkResponseHeader,
	interface{},
	error,
) {
	e := util.StringErrorFunc("failed to request")

	r, cancel, err := c.write(ctx, ci, c.enc, header, nil)
	if err != nil {
		return nil, false, e(err, "failed to send request")
	}

	defer func() {
		_ = cancel()
	}()

	h, enc, err := c.loadResponseHeader(ctx, r)

	switch {
	case err != nil:
		return h, nil, e(err, "failed to read stream")
	case h.Err() != nil, !h.OK():
		return h, nil, nil
	default:
		var u interface{}

		if err := encoder.DecodeReader(enc, r, &u); err != nil {
			return h, nil, e(err, "")
		}

		return h, u, nil
	}
}

func (c *baseNetworkClient) RequestProposal(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	point base.Point,
	proposer base.Address,
) (base.ProposalSignedFact, bool, error) {
	e := util.StringErrorFunc("failed to request proposal")

	header := NewRequestProposalRequestHeader(point, proposer)

	if err := header.IsValid(nil); err != nil {
		return nil, false, e(err, "")
	}

	r, cancel, err := c.write(ctx, ci, c.enc, header, nil)
	if err != nil {
		return nil, false, e(err, "failed to send request")
	}

	defer func() {
		_ = cancel()
	}()

	h, enc, err := c.loadResponseHeader(ctx, r)

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
	ci quicstream.UDPConnInfo,
	pr util.Hash,
) (base.ProposalSignedFact, bool, error) {
	e := util.StringErrorFunc("failed to get proposal")

	header := NewProposalRequestHeader(pr)

	if err := header.IsValid(nil); err != nil {
		return nil, false, e(err, "")
	}

	r, cancel, err := c.write(ctx, ci, c.enc, header, nil)
	if err != nil {
		return nil, false, e(err, "failed to send request")
	}

	defer func() {
		_ = cancel()
	}()

	h, enc, err := c.loadResponseHeader(ctx, r)

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
	ctx context.Context, ci quicstream.UDPConnInfo, state util.Hash,
) (base.SuffrageProof, bool, error) {
	header := NewLastSuffrageProofRequestHeader(state)

	var u base.SuffrageProof

	found, err := c.requestOK(ctx, ci, header, nil, &u)

	return u, found, errors.Wrap(err, "failed to get last SuffrageProof")
}

func (c *baseNetworkClient) SuffrageProof( //nolint:dupl //...
	ctx context.Context, ci quicstream.UDPConnInfo, suffrageheight base.Height,
) (base.SuffrageProof, bool, error) {
	header := NewSuffrageProofRequestHeader(suffrageheight)

	var u base.SuffrageProof

	found, err := c.requestOK(ctx, ci, header, nil, &u)

	return u, found, errors.Wrap(err, "failed to get SuffrageProof")
}

func (c *baseNetworkClient) LastBlockMap( //nolint:dupl //...
	ctx context.Context, ci quicstream.UDPConnInfo, manifest util.Hash,
) (base.BlockMap, bool, error) {
	header := NewLastBlockMapRequestHeader(manifest)

	var u base.BlockMap

	found, err := c.requestOK(ctx, ci, header, nil, &u)

	return u, found, errors.Wrap(err, "failed to get last BlockMap")
}

func (c *baseNetworkClient) BlockMap( //nolint:dupl //...
	ctx context.Context, ci quicstream.UDPConnInfo, height base.Height,
) (base.BlockMap, bool, error) {
	header := NewBlockMapRequestHeader(height)

	var u base.BlockMap

	found, err := c.requestOK(ctx, ci, header, nil, &u)

	return u, found, errors.Wrap(err, "failed to get BlockMap")
}

func (c *baseNetworkClient) BlockMapItem(
	ctx context.Context, ci quicstream.UDPConnInfo, height base.Height, item base.BlockMapItemType,
) (_ io.ReadCloser, cancel func() error, found bool, _ error) {
	// NOTE the io.ReadCloser should be closed.

	e := util.StringErrorFunc("failed to get BlockMap")

	header := NewBlockMapItemRequestHeader(height, item)

	if err := header.IsValid(nil); err != nil {
		return nil, nil, false, e(err, "")
	}

	r, cancel, err := c.write(ctx, ci, c.enc, header, nil)
	if err != nil {
		return nil, nil, false, e(err, "failed to send request")
	}

	h, _, err := c.loadResponseHeader(ctx, r)

	switch {
	case err != nil:
		_ = cancel()

		return nil, nil, false, e(err, "failed to read stream")
	case h.Err() != nil:
		_ = cancel()

		return nil, nil, false, e(h.Err(), "")
	case !h.OK():
		_ = cancel()

		return nil, nil, false, nil
	default:
		return r, cancel, true, nil
	}
}

func (c *baseNetworkClient) NodeChallenge(
	ctx context.Context, ci quicstream.UDPConnInfo,
	networkID base.NetworkID,
	node base.Address, pub base.Publickey, input []byte,
) (base.Signature, error) {
	e := util.StringErrorFunc("failed NodeChallenge")

	header := NewNodeChallengeRequestHeader(input)

	if err := header.IsValid(nil); err != nil {
		return nil, e(err, "")
	}

	r, cancel, err := c.write(ctx, ci, c.enc, header, nil)
	if err != nil {
		return nil, e(err, "failed to send request")
	}

	defer func() {
		_ = cancel()
	}()

	h, _, err := c.loadResponseHeader(ctx, r)

	switch {
	case err != nil:
		return nil, e(err, "failed to read stream")
	case h.Err() != nil:
		return nil, e(h.Err(), "")
	case !h.OK():
		return nil, nil
	default:
		b, err := io.ReadAll(r)
		if err != nil {
			return nil, e(err, "")
		}

		var sig base.Signature
		_ = c.enc.Unmarshal(b, &sig)

		if err := pub.Verify(util.ConcatBytesSlice(
			node.Bytes(),
			networkID,
			input,
		), sig); err != nil {
			return nil, e(err, "")
		}

		return sig, nil
	}
}

func (c *baseNetworkClient) SuffrageNodeConnInfo(
	ctx context.Context, ci quicstream.UDPConnInfo,
) ([]isaac.NodeConnInfo, error) {
	e := util.StringErrorFunc("failed SuffrageNodeConnInfo")

	ncis, err := c.requestNodeConnInfos(ctx, ci, NewSuffrageNodeConnInfoRequestHeader())
	if err != nil {
		return nil, e(err, "")
	}

	return ncis, nil
}

func (c *baseNetworkClient) SyncSourceConnInfo(
	ctx context.Context, ci quicstream.UDPConnInfo,
) ([]isaac.NodeConnInfo, error) {
	e := util.StringErrorFunc("failed SyncSourceConnInfo")

	ncis, err := c.requestNodeConnInfos(ctx, ci, NewSyncSourceConnInfoRequestHeader())
	if err != nil {
		return nil, e(err, "")
	}

	return ncis, nil
}

func (c *baseNetworkClient) requestOK(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header isaac.NetworkHeader,
	body io.Reader,
	u interface{},
) (bool, error) {
	if err := header.IsValid(nil); err != nil {
		return false, err
	}

	r, cancel, err := c.write(ctx, ci, c.enc, header, body)
	if err != nil {
		return false, errors.Wrap(err, "failed to send request")
	}

	defer func() {
		_ = cancel()
	}()

	h, enc, err := c.loadResponseHeader(ctx, r)

	switch {
	case err != nil:
		return false, errors.Wrap(err, "failed to read stream")
	case h.Err() != nil:
		return false, h.Err()
	case !h.OK():
		return false, nil
	default:
		if err := encoder.DecodeReader(enc, r, u); err != nil {
			return false, err
		}

		return true, nil
	}
}

func (c *baseNetworkClient) loadResponseHeader(
	ctx context.Context,
	r io.ReadCloser,
) (h isaac.NetworkResponseHeader, enc encoder.Encoder, _ error) {
	e := util.StringErrorFunc("failed to load response header")

	tctx, cancel := context.WithTimeout(ctx, c.idleTimeout)
	defer cancel()

	enc, b, err := HandlerReadHead(tctx, c.encs, r)
	if err != nil {
		return nil, nil, err
	}

	if err := encoder.Decode(enc, b, &h); err != nil {
		return h, nil, e(err, "failed to read stream")
	}

	return h, enc, nil
}

func (c *baseNetworkClient) write(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	enc encoder.Encoder,
	header isaac.NetworkHeader,
	body io.Reader,
) (io.ReadCloser, func() error, error) {
	b, err := enc.Marshal(header)
	if err != nil {
		return nil, nil, err
	}

	r, cancel, err := c.writef(ctx, ci, func(w io.Writer) error {
		return ClientWrite(w, header.HandlerPrefix(), enc.Hint(), b, body)
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to write")
	}

	return r, cancel, nil
}

func (c *baseNetworkClient) requestNodeConnInfos(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header isaac.NetworkHeader,
) ([]isaac.NodeConnInfo, error) {
	if err := header.IsValid(nil); err != nil {
		return nil, err
	}

	r, cancel, err := c.write(ctx, ci, c.enc, header, nil)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to send request")
	}

	defer func() {
		_ = cancel()
	}()

	h, _, err := c.loadResponseHeader(ctx, r)

	switch {
	case err != nil:
		return nil, errors.WithMessage(err, "failed to read stream")
	case h.Err() != nil:
		return nil, errors.WithStack(h.Err())
	case !h.OK():
		return nil, nil
	default:
		b, err := io.ReadAll(r)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		var u []json.RawMessage
		if err := c.enc.Unmarshal(b, &u); err != nil {
			return nil, err
		}

		cis := make([]isaac.NodeConnInfo, len(u))

		for i := range u {
			if err := encoder.Decode(c.enc, u[i], &cis[i]); err != nil {
				return nil, err
			}
		}

		return cis, nil
	}
}
