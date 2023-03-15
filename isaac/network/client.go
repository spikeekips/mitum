package isaacnetwork

import (
	"bytes"
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type BaseClient struct {
	*quicstream.HeaderClient
}

func NewBaseClient(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	writef quicstream.HeaderClientWriteFunc,
) *BaseClient {
	return &BaseClient{
		HeaderClient: quicstream.NewHeaderClient(encs, enc, writef),
	}
}

func (c *BaseClient) Request(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header quicstream.Header,
	body io.Reader,
) (
	quicstream.ResponseHeader,
	io.ReadCloser,
	func() error,
	encoder.Encoder,
	error,
) {
	e := util.StringErrorFunc("request")

	h, r, cancel, enc, err := c.RequestBody(ctx, ci, header, body)

	switch {
	case err != nil:
		return h, nil, cancel, enc, e(err, "read stream")
	case h.Err() != nil:
		return h, nil, cancel, enc, nil
	case !h.OK():
		return h, nil, cancel, enc, nil
	}

	switch h.ContentType() {
	case quicstream.HinterContentType, quicstream.RawContentType:
		return h, r, cancel, enc, nil
	default:
		defer func() {
			_ = cancel()
		}()

		return nil, nil, util.EmptyCancelFunc, enc, errors.Errorf("unknown content type, %q", h.ContentType())
	}
}

func (c *BaseClient) Operation(
	ctx context.Context, ci quicstream.UDPConnInfo, op util.Hash,
) (base.Operation, bool, error) {
	header := NewOperationRequestHeader(op)

	var u base.Operation

	switch h, _, err := c.RequestDecode(ctx, ci, header, nil, &u); {
	case err != nil:
		return nil, false, err
	default:
		return u, h.OK(), h.Err()
	}
}

func (c *BaseClient) SendOperation(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	op base.Operation,
) (bool, error) {
	buf := bytes.NewBuffer(nil)
	defer buf.Reset()

	if err := c.Encoder.StreamEncoder(buf).Encode(op); err != nil {
		return false, err
	}

	header := NewSendOperationRequestHeader()

	h, _, cancel, _, err := c.RequestBody(ctx, ci, header, buf)
	_ = cancel()

	switch {
	case err != nil:
		return false, err
	default:
		return h.OK(), h.Err()
	}
}

func (c *BaseClient) RequestProposal(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	point base.Point,
	proposer base.Address,
) (base.ProposalSignFact, bool, error) {
	header := NewRequestProposalRequestHeader(point, proposer)

	var u base.ProposalSignFact

	switch h, _, err := c.RequestBodyDecode(ctx, ci, header, nil, &u); {
	case err != nil:
		return nil, false, err
	default:
		return u, h.OK(), h.Err()
	}
}

func (c *BaseClient) Proposal( //nolint:dupl //...
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	pr util.Hash,
) (base.ProposalSignFact, bool, error) {
	header := NewProposalRequestHeader(pr)

	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	var u base.ProposalSignFact

	switch h, _, err := c.RequestBodyDecode(ctx, ci, header, nil, &u); {
	case err != nil:
		return nil, false, err
	default:
		return u, h.OK(), h.Err()
	}
}

func (c *BaseClient) LastSuffrageProof(
	ctx context.Context, ci quicstream.UDPConnInfo, state util.Hash,
) (base.Height, base.SuffrageProof, bool, error) {
	header := NewLastSuffrageProofRequestHeader(state)

	var lastheight base.Height

	if err := header.IsValid(nil); err != nil {
		return lastheight, nil, false, err
	}

	h, r, cancel, enc, err := c.RequestBody(ctx, ci, header, nil)
	defer func() {
		_ = cancel()
	}()

	switch {
	case err != nil:
		return lastheight, nil, false, err
	case h.Err() != nil:
		return lastheight, nil, h.OK(), h.Err()
	default:
		b, err := io.ReadAll(r)
		if err != nil {
			return lastheight, nil, true, errors.WithMessage(err, "read response")
		}

		switch _, m, _, err := util.ReadLengthedBytesSlice(b); {
		case err != nil:
			return lastheight, nil, true, errors.WithMessage(err, "read response")
		case len(m) != 2: //nolint:gomnd //...
			return lastheight, nil, true, errors.Errorf("invalid response message")
		default:
			i, err := base.ParseHeightBytes(m[0])
			if err != nil {
				return lastheight, nil, true, errors.WithMessage(err, "load last height")
			}

			lastheight = i

			if !h.OK() {
				return lastheight, nil, false, nil
			}

			var u base.SuffrageProof

			if err := encoder.Decode(enc, m[1], &u); err != nil {
				return lastheight, nil, true, err
			}

			return lastheight, u, true, nil
		}
	}
}

func (c *BaseClient) SuffrageProof( //nolint:dupl //...
	ctx context.Context, ci quicstream.UDPConnInfo, suffrageheight base.Height,
) (base.SuffrageProof, bool, error) {
	header := NewSuffrageProofRequestHeader(suffrageheight)
	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	var u base.SuffrageProof

	switch h, _, err := c.RequestDecode(ctx, ci, header, nil, &u); {
	case err != nil:
		return nil, false, errors.WithMessage(err, "get SuffrageProof")
	case h.Err() != nil:
		return nil, false, errors.WithMessage(h.Err(), "geProof")
	default:
		return u, h.OK(), nil
	}
}

func (c *BaseClient) LastBlockMap( //nolint:dupl //...
	ctx context.Context, ci quicstream.UDPConnInfo, manifest util.Hash,
) (base.BlockMap, bool, error) {
	header := NewLastBlockMapRequestHeader(manifest)
	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	var u base.BlockMap

	switch h, _, err := c.RequestDecode(ctx, ci, header, nil, &u); {
	case err != nil:
		return nil, false, errors.WithMessage(err, "get last BlockMap")
	case h.Err() != nil:
		return nil, false, errors.WithMessage(h.Err(), "get last BlockMap")
	default:
		return u, h.OK(), nil
	}
}

func (c *BaseClient) BlockMap( //nolint:dupl //...
	ctx context.Context, ci quicstream.UDPConnInfo, height base.Height,
) (base.BlockMap, bool, error) {
	header := NewBlockMapRequestHeader(height)
	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	var u base.BlockMap

	switch h, _, err := c.RequestDecode(ctx, ci, header, nil, &u); {
	case err != nil:
		return nil, false, errors.WithMessage(err, "get BlockMap")
	case h.Err() != nil:
		return nil, false, errors.WithMessage(h.Err(), "get BlockMap")
	default:
		return u, h.OK(), nil
	}
}

func (c *BaseClient) BlockMapItem(
	ctx context.Context, ci quicstream.UDPConnInfo, height base.Height, item base.BlockMapItemType,
) (
	_ io.ReadCloser,
	_ func() error,
	_ bool,
	_ error,
) {
	// NOTE the io.ReadCloser should be closed.

	header := NewBlockMapItemRequestHeader(height, item)

	if err := header.IsValid(nil); err != nil {
		return nil, nil, false, err
	}

	h, r, cancel, _, err := c.RequestBody(ctx, ci, header, nil)

	switch {
	case err != nil:
		return nil, nil, false, err
	case h.Err() != nil, !h.OK():
		_ = cancel()

		return nil, util.EmptyCancelFunc, h.OK(), h.Err()
	default:
		return r, cancel, h.OK(), h.Err()
	}
}

func (c *BaseClient) NodeChallenge(
	ctx context.Context, ci quicstream.UDPConnInfo,
	networkID base.NetworkID,
	node base.Address, pub base.Publickey, input []byte,
) (base.Signature, error) {
	e := util.StringErrorFunc("NodeChallenge")

	header := NewNodeChallengeRequestHeader(input)

	if err := header.IsValid(nil); err != nil {
		return nil, e(err, "")
	}

	h, r, cancel, enc, err := c.RequestBody(ctx, ci, header, nil)

	switch {
	case err != nil:
		return nil, e(err, "")
	case h.Err() != nil, !h.OK():
		_ = cancel()

		return nil, e(h.Err(), "")
	default:
		b, err := io.ReadAll(r)
		if err != nil {
			return nil, e(err, "")
		}

		var sig base.Signature

		if err := enc.Unmarshal(b, &sig); err != nil {
			return nil, e(err, "")
		}

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

func (c *BaseClient) SuffrageNodeConnInfo(
	ctx context.Context, ci quicstream.UDPConnInfo,
) ([]isaac.NodeConnInfo, error) {
	ncis, err := c.requestNodeConnInfos(ctx, ci, NewSuffrageNodeConnInfoRequestHeader())

	return ncis, errors.WithMessage(err, "request; SuffrageNodeConnInfo")
}

func (c *BaseClient) SyncSourceConnInfo(
	ctx context.Context, ci quicstream.UDPConnInfo,
) ([]isaac.NodeConnInfo, error) {
	e := util.StringErrorFunc("SyncSourceConnInfo")

	ncis, err := c.requestNodeConnInfos(ctx, ci, NewSyncSourceConnInfoRequestHeader())
	if err != nil {
		return nil, e(err, "")
	}

	return ncis, nil
}

func (c *BaseClient) requestNodeConnInfos(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header quicstream.Header,
) ([]isaac.NodeConnInfo, error) {
	h, r, cancel, enc, err := c.RequestBody(ctx, ci, header, nil)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = cancel()
	}()

	switch {
	case h.Err() != nil:
		return nil, errors.WithStack(h.Err())
	case !h.OK():
		return nil, nil
	default:
		b, err := io.ReadAll(r)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		u, err := enc.DecodeSlice(b)
		if err != nil {
			return nil, err
		}

		cis := make([]isaac.NodeConnInfo, len(u))

		for i := range u {
			rci, ok := u[i].(isaac.NodeConnInfo)
			if !ok {
				return nil, errors.Errorf("expected NodeConnInfo, but %T", u[i])
			}

			cis[i] = rci
		}

		return cis, nil
	}
}

func (c *BaseClient) State(
	ctx context.Context, ci quicstream.UDPConnInfo, key string, st util.Hash,
) (base.State, bool, error) {
	header := NewStateRequestHeader(key, st)
	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	var u base.State

	switch h, _, err := c.RequestDecode(ctx, ci, header, nil, &u); {
	case err != nil:
		return nil, false, errors.WithMessage(err, "get State")
	case h.Err() != nil:
		return nil, false, errors.WithMessage(h.Err(), "get State")
	default:
		return u, h.OK(), nil
	}
}

func (c *BaseClient) ExistsInStateOperation(
	ctx context.Context, ci quicstream.UDPConnInfo, facthash util.Hash,
) (bool, error) {
	header := NewExistsInStateOperationRequestHeader(facthash)
	if err := header.IsValid(nil); err != nil {
		return false, err
	}

	h, _, cancel, _, err := c.RequestBody(ctx, ci, header, nil)
	defer func() {
		_ = cancel()
	}()

	switch {
	case err != nil:
		return false, errors.WithMessage(err, "request; ExistsInStateOperation")
	case h.Err() != nil:
		return false, errors.WithMessage(h.Err(), "response; ExistsInStateOperation")
	default:
		return h.OK(), nil
	}
}

func (c *BaseClient) SendBallots(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	ballots []base.BallotSignFact,
) error {
	e := util.StringErrorFunc("SendBallots")

	if len(ballots) < 1 {
		return e(nil, "empty ballots")
	}

	header := NewSendBallotsHeader()

	h, _, cancel, _, err := c.RequestEncode(ctx, ci, header, ballots)
	if err != nil {
		return e(err, "send request")
	}

	defer func() {
		_ = cancel()
	}()

	if h.Err() != nil {
		return e(h.Err(), "")
	}

	return nil
}
