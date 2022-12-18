package isaacnetwork

import (
	"bytes"
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

type BaseNetworkClientWriteFunc func(
	ctx context.Context,
	conninfo quicstream.UDPConnInfo,
	writef quicstream.ClientWriteFunc,
) (_ io.ReadCloser, cancel func() error, _ error)

type BaseNetworkClient struct {
	*baseNetwork
	writef BaseNetworkClientWriteFunc
}

func NewBaseNetworkClient(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	idleTimeout time.Duration,
	writef BaseNetworkClientWriteFunc,
) *BaseNetworkClient {
	return &BaseNetworkClient{
		baseNetwork: newBaseNetwork(encs, enc, idleTimeout),
		writef:      writef,
	}
}

func (c *BaseNetworkClient) NewClient() *BaseNetworkClient {
	return &BaseNetworkClient{
		baseNetwork: newBaseNetwork(c.encs, c.enc, c.idleTimeout),
		writef:      c.writef,
	}
}

func (c *BaseNetworkClient) Request(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header isaac.NetworkHeader,
	body io.Reader,
) (
	_ isaac.NetworkResponseHeader,
	_ interface{},
	cancel func() error,
	_ error,
) {
	e := util.StringErrorFunc("failed to request")

	cancel = func() error { return nil }

	r, cancelf, err := c.write(ctx, ci, c.enc, header, body)
	if err != nil {
		return nil, nil, cancel, e(err, "failed to send request")
	}

	h, enc, err := c.loadResponseHeader(ctx, r)

	switch {
	case err != nil:
		return h, nil, cancelf, e(err, "failed to read stream")
	case h.Err() != nil, !h.OK():
		return h, nil, cancelf, nil
	}

	switch h.Type() {
	case isaac.NetworkResponseHinterContentType:
		defer func() {
			_ = cancelf()
		}()

		var u interface{}

		if err := encoder.DecodeReader(enc, r, &u); err != nil {
			return h, nil, cancel, e(err, "")
		}

		return h, u, cancel, nil
	case isaac.NetworkResponseRawContentType:
		return h, r, cancelf, nil
	default:
		defer func() {
			_ = cancelf()
		}()

		return nil, nil, cancel, errors.Errorf("unknown content type, %q", h.Type())
	}
}

func (c *BaseNetworkClient) Operation(
	ctx context.Context, ci quicstream.UDPConnInfo, operationhash util.Hash,
) (base.Operation, bool, error) {
	header := NewOperationRequestHeader(operationhash)

	var u base.Operation

	found, err := c.requestOK(ctx, ci, header, nil, &u)

	return u, found, errors.WithMessage(err, "failed to get Operation")
}

func (c *BaseNetworkClient) SendOperation(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	op base.Operation,
) (bool, error) {
	e := util.StringErrorFunc("failed to NewOperation")

	b, err := c.enc.Marshal(op)
	if err != nil {
		return false, e(err, "")
	}

	header := NewSendOperationRequestHeader()

	if err = header.IsValid(nil); err != nil {
		return false, e(err, "")
	}

	buf := bytes.NewBuffer(b)
	defer buf.Reset()

	r, cancel, err := c.write(ctx, ci, c.enc, header, buf)
	if err != nil {
		return false, e(err, "failed to send request")
	}

	defer func() {
		_ = cancel()
	}()

	h, _, err := c.loadResponseHeader(ctx, r)

	switch {
	case err != nil:
		return false, e(err, "failed to read stream")
	case h.Err() != nil:
		return false, e(h.Err(), "")
	default:
		return h.OK(), nil
	}
}

func (c *BaseNetworkClient) RequestProposal(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	point base.Point,
	proposer base.Address,
) (base.ProposalSignFact, bool, error) {
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
		var u base.ProposalSignFact

		if err := encoder.DecodeReader(enc, r, &u); err != nil {
			return nil, false, e(err, "")
		}

		return u, true, nil
	}
}

func (c *BaseNetworkClient) Proposal( //nolint:dupl //...
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	pr util.Hash,
) (base.ProposalSignFact, bool, error) {
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
		var u base.ProposalSignFact

		if err := encoder.DecodeReader(enc, r, &u); err != nil {
			return nil, false, e(err, "")
		}

		return u, true, nil
	}
}

func (c *BaseNetworkClient) LastSuffrageProof(
	ctx context.Context, ci quicstream.UDPConnInfo, state util.Hash,
) (base.Height, base.SuffrageProof, bool, error) {
	header := NewLastSuffrageProofRequestHeader(state)

	var lastheight base.Height
	var u base.SuffrageProof

	found, err := c.requestWithCallback(ctx, ci, header, nil, func(enc encoder.Encoder, r io.Reader, ok bool) error {
		b, err := io.ReadAll(r)
		if err != nil {
			return errors.WithMessage(err, "failed to read response")
		}

		switch _, m, _, err := util.ReadLengthedBytesSlice(b); {
		case err != nil:
			return errors.WithMessage(err, "failed to read response")
		case len(m) != 2: //nolint:gomnd //...
			return errors.Errorf("invalid response message")
		default:
			lastheight, err = base.ParseHeightBytes(m[0])
			if err != nil {
				return errors.WithMessage(err, "failed to load last height")
			}

			if ok {
				return encoder.Decode(enc, m[1], &u)
			}

			return nil
		}
	})

	return lastheight, u, found, errors.WithMessage(err, "failed to get last SuffrageProof")
}

func (c *BaseNetworkClient) SuffrageProof( //nolint:dupl //...
	ctx context.Context, ci quicstream.UDPConnInfo, suffrageheight base.Height,
) (base.SuffrageProof, bool, error) {
	header := NewSuffrageProofRequestHeader(suffrageheight)

	var u base.SuffrageProof

	found, err := c.requestOK(ctx, ci, header, nil, &u)

	return u, found, errors.WithMessage(err, "failed to get SuffrageProof")
}

func (c *BaseNetworkClient) LastBlockMap( //nolint:dupl //...
	ctx context.Context, ci quicstream.UDPConnInfo, manifest util.Hash,
) (base.BlockMap, bool, error) {
	header := NewLastBlockMapRequestHeader(manifest)

	var u base.BlockMap

	found, err := c.requestOK(ctx, ci, header, nil, &u)

	return u, found, errors.WithMessage(err, "failed to get last BlockMap")
}

func (c *BaseNetworkClient) BlockMap( //nolint:dupl //...
	ctx context.Context, ci quicstream.UDPConnInfo, height base.Height,
) (base.BlockMap, bool, error) {
	header := NewBlockMapRequestHeader(height)

	var u base.BlockMap

	found, err := c.requestOK(ctx, ci, header, nil, &u)

	return u, found, errors.WithMessage(err, "failed to get BlockMap")
}

func (c *BaseNetworkClient) BlockMapItem(
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

func (c *BaseNetworkClient) NodeChallenge(
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

func (c *BaseNetworkClient) SuffrageNodeConnInfo(
	ctx context.Context, ci quicstream.UDPConnInfo,
) ([]isaac.NodeConnInfo, error) {
	e := util.StringErrorFunc("failed SuffrageNodeConnInfo")

	ncis, err := c.requestNodeConnInfos(ctx, ci, NewSuffrageNodeConnInfoRequestHeader())
	if err != nil {
		return nil, e(err, "")
	}

	return ncis, nil
}

func (c *BaseNetworkClient) SyncSourceConnInfo(
	ctx context.Context, ci quicstream.UDPConnInfo,
) ([]isaac.NodeConnInfo, error) {
	e := util.StringErrorFunc("failed SyncSourceConnInfo")

	ncis, err := c.requestNodeConnInfos(ctx, ci, NewSyncSourceConnInfoRequestHeader())
	if err != nil {
		return nil, e(err, "")
	}

	return ncis, nil
}

func (c *BaseNetworkClient) State(
	ctx context.Context, ci quicstream.UDPConnInfo, key string, h util.Hash,
) (base.State, bool, error) {
	header := NewStateRequestHeader(key, h)

	var st base.State

	found, err := c.requestOK(ctx, ci, header, nil, &st)

	return st, found, errors.WithMessage(err, "failed State")
}

func (c *BaseNetworkClient) ExistsInStateOperation(
	ctx context.Context, ci quicstream.UDPConnInfo, facthash util.Hash,
) (bool, error) {
	header := NewExistsInStateOperationRequestHeader(facthash)

	found, err := c.requestOK(ctx, ci, header, nil, nil)

	return found, errors.WithMessage(err, "failed ExistsInStateOperation")
}

func (c *BaseNetworkClient) requestOK(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header isaac.NetworkHeader,
	body io.Reader,
	u interface{},
) (bool, error) {
	return c.requestWithCallback(ctx, ci, header, body, func(enc encoder.Encoder, r io.Reader, ok bool) error {
		if !ok || u == nil {
			return nil
		}

		return encoder.DecodeReader(enc, r, u)
	})
}

func (c *BaseNetworkClient) requestWithCallback(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header isaac.NetworkHeader,
	body io.Reader,
	callback func(encoder.Encoder, io.Reader, bool) error,
) (bool, error) {
	if err := header.IsValid(nil); err != nil {
		return false, err
	}

	r, cancel, err := c.write(ctx, ci, c.enc, header, body)
	if err != nil {
		return false, errors.WithMessage(err, "failed to send request")
	}

	defer func() {
		_ = cancel()
	}()

	h, enc, err := c.loadResponseHeader(ctx, r)

	switch {
	case err != nil:
		return false, errors.WithMessage(err, "failed to read stream")
	case h.Err() != nil:
		return false, h.Err()
	default:
		if err := callback(enc, r, h.OK()); err != nil {
			return false, err
		}

		return h.OK(), nil
	}
}

func (c *BaseNetworkClient) loadResponseHeader(
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

func (c *BaseNetworkClient) write(
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
		return nil, nil, errors.WithMessage(err, "failed to write")
	}

	return r, cancel, nil
}

func (c *BaseNetworkClient) requestNodeConnInfos(
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
