package isaacnetwork

import (
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
	openstreamf quicstream.OpenStreamFunc,
) *BaseClient {
	return &BaseClient{
		HeaderClient: quicstream.NewHeaderClient(encs, enc, openstreamf),
	}
}

func (c *BaseClient) OpenStream(ctx context.Context, ci quicstream.UDPConnInfo) (
	io.Reader,
	io.WriteCloser,
	error,
) {
	return c.HeaderClient.OpenStream(ctx, ci)
}

func (c *BaseClient) Broker(ctx context.Context, ci quicstream.UDPConnInfo) (*quicstream.HeaderClientBroker, error) {
	return c.HeaderClient.Broker(ctx, ci)
}

func (c *BaseClient) Operation(
	ctx context.Context, ci quicstream.UDPConnInfo, op util.Hash,
) (base.Operation, bool, error) {
	var u base.Operation

	switch h, err := HCReqResDec(
		ctx,
		ci,
		NewOperationRequestHeader(op),
		&u,
		c.HeaderClient.Broker,
		func(enc encoder.Encoder, r io.Reader, v interface{}) error {
			return encoder.DecodeReader(enc, r, v)
		},
	); {
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
	switch h, err := HCBodyReqRes(
		ctx,
		ci,
		NewSendOperationRequestHeader(),
		op,
		c.HeaderClient.Broker,
	); {
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
	previousBlock util.Hash,
) (base.ProposalSignFact, bool, error) {
	header := NewRequestProposalRequestHeader(point, proposer, previousBlock)
	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	return c.requestProposal(ctx, ci, header)
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

	return c.requestProposal(ctx, ci, header)
}

func (c *BaseClient) LastSuffrageProof(
	ctx context.Context, ci quicstream.UDPConnInfo, state util.Hash,
) (base.Height, base.SuffrageProof, bool, error) {
	header := NewLastSuffrageProofRequestHeader(state)

	lastheight := base.NilHeight

	if err := header.IsValid(nil); err != nil {
		return lastheight, nil, false, err
	}

	var proof base.SuffrageProof

	switch rh, err := HCReqResDec(
		ctx,
		ci,
		header,
		nil,
		c.HeaderClient.Broker,
		func(enc encoder.Encoder, r io.Reader, _ interface{}) error {
			if r == nil {
				return nil
			}

			b, err := io.ReadAll(r)
			if err != nil {
				return errors.WithMessage(err, "read response")
			}

			switch _, m, _, err := util.ReadLengthedBytesSlice(b); {
			case err != nil:
				return errors.WithMessage(err, "read response")
			case len(m) != 2: //nolint:gomnd //...
				return errors.Errorf("invalid response message")
			case len(m[0]) < 1:
				return nil
			default:
				i, err := base.ParseHeightBytes(m[0])
				if err != nil {
					return errors.WithMessage(err, "load last height")
				}

				lastheight = i

				if len(m[1]) > 0 {
					if err := encoder.Decode(enc, m[1], &proof); err != nil {
						return err
					}
				}

				return nil
			}
		},
	); {
	case err != nil:
		return lastheight, nil, false, err
	case rh.Err() != nil:
		return lastheight, nil, false, rh.Err()
	case proof == nil:
		return lastheight, nil, false, nil
	default:
		return lastheight, proof, rh.OK(), nil
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

	switch h, err := HCReqResDec(
		ctx,
		ci,
		header,
		&u,
		c.HeaderClient.Broker,
		func(enc encoder.Encoder, r io.Reader, v interface{}) error {
			return encoder.DecodeReader(enc, r, v)
		},
	); {
	case err != nil:
		return nil, false, err
	default:
		return u, h.OK(), h.Err()
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

	switch h, err := HCReqResDec(
		ctx,
		ci,
		header,
		&u,
		c.HeaderClient.Broker,
		func(enc encoder.Encoder, r io.Reader, v interface{}) error {
			return encoder.DecodeReader(enc, r, v)
		},
	); {
	case err != nil:
		return nil, false, err
	default:
		return u, h.OK(), h.Err()
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

	switch h, err := HCReqResDec(
		ctx,
		ci,
		header,
		&u,
		c.HeaderClient.Broker,
		func(enc encoder.Encoder, r io.Reader, v interface{}) error {
			return encoder.DecodeReader(enc, r, v)
		},
	); {
	case err != nil:
		return nil, false, err
	default:
		return u, h.OK(), h.Err()
	}
}

func (c *BaseClient) BlockMapItem(
	ctx context.Context, ci quicstream.UDPConnInfo, height base.Height, item base.BlockMapItemType,
) (
	_ io.Reader,
	_ func() error,
	_ bool,
	_ error,
) {
	// NOTE the io.ReadCloser should be closed.

	header := NewBlockMapItemRequestHeader(height, item)
	if err := header.IsValid(nil); err != nil {
		return nil, nil, false, err
	}

	switch h, _, body, cancel, err := HCReqBodyRes(ctx, ci, header, c.HeaderClient.Broker); {
	case err != nil:
		return nil, nil, false, err
	case h.Err() != nil, !h.OK():
		_ = cancel()

		return nil, util.EmptyCancelFunc, h.OK(), h.Err()
	default:
		return body, cancel, h.OK(), h.Err()
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

	var sig base.Signature

	switch h, err := HCReqResDec(
		ctx,
		ci,
		header,
		&sig,
		c.HeaderClient.Broker,
		func(enc encoder.Encoder, r io.Reader, v interface{}) error {
			return enc.StreamDecoder(r).Decode(v)
		},
	); {
	case err != nil:
		return nil, e(err, "")
	case !h.OK():
		return nil, e(nil, "empty")
	case h.Err() != nil:
		return nil, e(h.Err(), "")
	default:
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
	header quicstream.RequestHeader,
) ([]isaac.NodeConnInfo, error) {
	var cis []isaac.NodeConnInfo

	switch h, err := HCReqResDec(
		ctx,
		ci,
		header,
		nil,
		c.HeaderClient.Broker,
		func(enc encoder.Encoder, r io.Reader, _ interface{}) error {
			b, err := io.ReadAll(r)
			if err != nil {
				return errors.WithStack(err)
			}

			u, err := enc.DecodeSlice(b)
			if err != nil {
				return err
			}

			cis = make([]isaac.NodeConnInfo, len(u))

			for i := range u {
				rci, ok := u[i].(isaac.NodeConnInfo)
				if !ok {
					return errors.Errorf("expected NodeConnInfo, but %T", u[i])
				}

				cis[i] = rci
			}

			return nil
		},
	); {
	case err != nil:
		return nil, err
	case !h.OK():
		return nil, errors.Errorf("empty")
	case h.Err() != nil:
		return nil, h.Err()
	default:
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

	switch h, err := HCReqResDec(
		ctx,
		ci,
		header,
		&u,
		c.HeaderClient.Broker,
		func(enc encoder.Encoder, r io.Reader, v interface{}) error {
			return encoder.DecodeReader(enc, r, v)
		},
	); {
	case err != nil:
		return nil, false, err
	default:
		return u, h.OK(), h.Err()
	}
}

func (c *BaseClient) ExistsInStateOperation(
	ctx context.Context, ci quicstream.UDPConnInfo, facthash util.Hash,
) (bool, error) {
	header := NewExistsInStateOperationRequestHeader(facthash)
	if err := header.IsValid(nil); err != nil {
		return false, err
	}

	switch h, err := HCReqRes(
		ctx,
		ci,
		header,
		c.HeaderClient.Broker,
	); {
	case err != nil:
		return false, err
	default:
		return h.OK(), h.Err()
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

	switch h, err := HCBodyReqRes(
		ctx,
		ci,
		header,
		ballots,
		c.HeaderClient.Broker,
	); {
	case err != nil:
		return err
	default:
		return h.Err()
	}
}

func (c *BaseClient) SetAllowConsensus(
	ctx context.Context, ci quicstream.UDPConnInfo,
	priv base.Privatekey, networkID base.NetworkID, allow bool,
) (bool, error) {
	header := NewSetAllowConsensusHeader(allow)
	if err := header.IsValid(nil); err != nil {
		return false, err
	}

	broker, err := c.HeaderClient.Broker(ctx, ci)
	if err != nil {
		return false, err
	}

	if err := broker.WriteRequestHead(ctx, header); err != nil {
		return false, err
	}

	switch input, err := util.ReadLengthed(broker.Reader); {
	case err != nil:
		return false, errors.WithMessage(err, "signature input")
	case len(input) < 1:
		return false, errors.Errorf("empty signature input")
	default:
		switch sig, err := priv.Sign(util.ConcatBytesSlice(networkID, input)); {
		case err != nil:
			return false, errors.WithMessage(err, "sign input")
		default:
			if err := util.WriteLengthed(broker.Writer, sig); err != nil {
				return false, errors.WithMessage(err, "write signature")
			}
		}
	}

	switch _, rh, err := broker.ReadResponseHead(ctx); {
	case err != nil:
		return false, errors.WithMessage(err, "response")
	case rh.Err() != nil:
		return false, errors.WithMessage(rh.Err(), "response")
	default:
		return rh.OK(), nil
	}
}

func (c *BaseClient) requestProposal(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header quicstream.RequestHeader,
) (base.ProposalSignFact, bool, error) {
	var u base.ProposalSignFact

	switch h, err := HCReqResDec(
		ctx,
		ci,
		header,
		&u,
		c.HeaderClient.Broker,
		func(enc encoder.Encoder, r io.Reader, v interface{}) error {
			return encoder.DecodeReader(enc, r, v)
		},
	); {
	case err != nil:
		return nil, false, err
	default:
		return u, h.OK(), h.Err()
	}
}

func HCReqRes(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header quicstream.RequestHeader,
	brokerf quicstream.HeaderBrokerFunc,
) (
	quicstream.ResponseHeader,
	error,
) {
	broker, err := brokerf(ctx, ci)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = broker.Close()
	}()

	_, rh, err := hcReqRes(ctx, broker, header)
	if err != nil {
		return nil, err
	}

	return rh, err
}

func HCReqBodyRes(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header quicstream.RequestHeader,
	brokerf quicstream.HeaderBrokerFunc,
) (
	_ quicstream.ResponseHeader,
	_ encoder.Encoder,
	body io.Reader,
	cancel func() error,
	_ error,
) {
	broker, err := brokerf(ctx, ci)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	cancel = func() error {
		return broker.Close()
	}

	switch rh, renc, rbody, err := hcReqBodyRes(ctx, broker, header); {
	case err != nil:
		_ = cancel()

		return nil, nil, nil, nil, err
	default:
		return rh, renc, rbody, cancel, nil
	}
}

func HCReqResDec(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header quicstream.RequestHeader,
	u interface{},
	brokerf quicstream.HeaderBrokerFunc,
	decodef func(encoder.Encoder, io.Reader, interface{}) error,
) (
	quicstream.ResponseHeader,
	error,
) {
	rh, renc, rbody, cancel, err := HCReqBodyRes(ctx, ci, header, brokerf)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = cancel()
	}()

	if rh.Err() != nil {
		return rh, nil
	}

	if rbody == nil {
		return rh, nil
	}

	return rh, decodef(renc, rbody, u)
}

func HCBodyReqRes(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header quicstream.RequestHeader,
	body interface{},
	brokerf quicstream.HeaderBrokerFunc,
) (
	quicstream.ResponseHeader,
	error,
) {
	broker, err := brokerf(ctx, ci)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = broker.Close()
	}()

	_, rh, err := hcBodyReqRes(ctx, broker, header, body)
	if err != nil {
		return nil, err
	}

	return rh, err
}

func HCBodyReqBodyRes(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header quicstream.RequestHeader,
	i interface{},
	brokerf quicstream.HeaderBrokerFunc,
) (
	_ quicstream.ResponseHeader,
	_ encoder.Encoder,
	body io.Reader,
	cancel func() error,
	_ error,
) {
	broker, err := brokerf(ctx, ci)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	cancel = func() error {
		return broker.Close()
	}

	switch rh, renc, rbody, err := hcBodyReqBodyRes(ctx, broker, header, i); {
	case err != nil:
		_ = cancel()

		return nil, nil, nil, nil, err
	default:
		return rh, renc, rbody, cancel, nil
	}
}

func HCBodyReqResDec(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header quicstream.RequestHeader,
	body, u interface{},
	brokerf quicstream.HeaderBrokerFunc,
	decodef func(encoder.Encoder, io.Reader, interface{}) error,
) (
	quicstream.ResponseHeader,
	error,
) {
	rh, renc, rbody, cancel, err := HCBodyReqBodyRes(ctx, ci, header, body, brokerf)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = cancel()
	}()

	if rh.Err() != nil {
		return rh, nil
	}

	if rbody == nil {
		return rh, nil
	}

	return rh, decodef(renc, rbody, u)
}

func hcReqRes(
	ctx context.Context,
	broker *quicstream.HeaderClientBroker,
	header quicstream.RequestHeader,
) (
	encoder.Encoder,
	quicstream.ResponseHeader,
	error,
) {
	if err := broker.WriteRequestHead(ctx, header); err != nil {
		return nil, nil, err
	}

	renc, rh, err := broker.ReadResponseHead(ctx)

	return renc, rh, err
}

func hcBodyReqRes(
	ctx context.Context,
	broker *quicstream.HeaderClientBroker,
	header quicstream.RequestHeader,
	body interface{},
) (
	encoder.Encoder,
	quicstream.ResponseHeader,
	error,
) {
	if err := broker.WriteRequestHead(ctx, header); err != nil {
		return nil, nil, err
	}

	if body != nil {
		if err := util.PipeReadWrite(
			ctx,
			func(ctx context.Context, pr io.Reader) error {
				return broker.WriteBody(ctx, quicstream.StreamDataFormat, 0, pr)
			},
			func(ctx context.Context, pw io.Writer) error {
				return broker.Encoder.StreamEncoder(pw).Encode(body)
			},
		); err != nil {
			return nil, nil, err
		}
	}

	renc, rh, err := broker.ReadResponseHead(ctx)

	return renc, rh, err
}

func hcReqBodyRes(
	ctx context.Context,
	broker *quicstream.HeaderClientBroker,
	header quicstream.RequestHeader,
) (
	_ quicstream.ResponseHeader,
	_ encoder.Encoder,
	responseBody io.Reader,
	_ error,
) {
	var renc encoder.Encoder
	var rh quicstream.ResponseHeader

	switch enc, h, err := hcReqRes(ctx, broker, header); {
	case err != nil:
		return nil, nil, nil, err
	case h.Err() != nil:
		return h, nil, nil, nil
	default:
		renc = enc
		rh = h
	}

	switch dataFormat, bodyLength, r, err := broker.ReadBody(ctx); {
	case err != nil:
		return rh, renc, nil, err
	case dataFormat == quicstream.EmptyDataFormat:
		return rh, renc, nil, nil
	case dataFormat == quicstream.LengthedDataFormat:
		if bodyLength < 1 {
			return rh, renc, nil, nil
		}

		return rh, renc, r, nil
	case dataFormat == quicstream.StreamDataFormat:
		return rh, renc, r, nil
	default:
		return rh, renc, nil, errors.Errorf("unknown DataFormat, %d", dataFormat)
	}
}

func hcBodyReqBodyRes(
	ctx context.Context,
	broker *quicstream.HeaderClientBroker,
	header quicstream.RequestHeader,
	body interface{},
) (
	_ quicstream.ResponseHeader,
	_ encoder.Encoder,
	responseBody io.Reader,
	_ error,
) {
	var renc encoder.Encoder
	var rh quicstream.ResponseHeader

	switch enc, h, err := hcBodyReqRes(ctx, broker, header, body); {
	case err != nil:
		return nil, nil, nil, err
	case h.Err() != nil:
		return h, nil, nil, nil
	default:
		renc = enc
		rh = h
	}

	switch dataFormat, bodyLength, r, err := broker.ReadBody(ctx); {
	case err != nil:
		return rh, renc, nil, err
	case dataFormat == quicstream.LengthedDataFormat:
		if bodyLength < 1 {
			return rh, renc, nil, nil
		}

		return rh, renc, r, nil
	case dataFormat == quicstream.StreamDataFormat:
		return rh, renc, r, nil
	default:
		return rh, renc, nil, errors.Errorf("unknown DataFormat, %d", dataFormat)
	}
}
