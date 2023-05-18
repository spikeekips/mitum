package isaacnetwork

import (
	"bytes"
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type BaseClient struct {
	*quicstreamheader.Client
}

func NewBaseClient(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	openstreamf quicstreamheader.OpenStreamFunc,
) *BaseClient {
	return &BaseClient{
		Client: quicstreamheader.NewClient(encs, enc, openstreamf),
	}
}

func (c *BaseClient) OpenStream(ctx context.Context, ci quicstream.UDPConnInfo) (
	io.Reader,
	io.WriteCloser,
	error,
) {
	return c.Client.OpenStream(ctx, ci)
}

func (c *BaseClient) Broker(ctx context.Context, ci quicstream.UDPConnInfo) (*quicstreamheader.ClientBroker, error) {
	return c.Client.Broker(ctx, ci)
}

func (c *BaseClient) Operation(
	ctx context.Context, ci quicstream.UDPConnInfo, op util.Hash,
) (base.Operation, bool, error) {
	var u base.Operation

	ok, err := hcReqResBodyDecOK(
		ctx,
		ci,
		NewOperationRequestHeader(op),
		c.Client.Broker,
		func(enc encoder.Encoder, r io.Reader) error {
			return encoder.DecodeReader(enc, r, &u)
		},
	)

	return u, ok, err
}

func (c *BaseClient) SendOperation(ctx context.Context, ci quicstream.UDPConnInfo, op base.Operation) (bool, error) {
	return hcBodyReqResOK(
		ctx,
		ci,
		NewSendOperationRequestHeader(),
		op,
		c.Client.Broker,
	)
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

	switch ok, err := hcReqResBodyDecOK(
		ctx,
		ci,
		header,
		c.Client.Broker,
		func(enc encoder.Encoder, r io.Reader) error {
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
	case proof == nil:
		return lastheight, nil, false, nil
	default:
		return lastheight, proof, ok, nil
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

	ok, err := hcReqResBodyDecOK(
		ctx,
		ci,
		header,
		c.Client.Broker,
		func(enc encoder.Encoder, r io.Reader) error {
			return encoder.DecodeReader(enc, r, &u)
		},
	)

	return u, ok, err
}

func (c *BaseClient) LastBlockMap( //nolint:dupl //...
	ctx context.Context, ci quicstream.UDPConnInfo, manifest util.Hash,
) (base.BlockMap, bool, error) {
	header := NewLastBlockMapRequestHeader(manifest)
	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	var u base.BlockMap

	ok, err := hcReqResBodyDecOK(
		ctx,
		ci,
		header,
		c.Client.Broker,
		func(enc encoder.Encoder, r io.Reader) error {
			return encoder.DecodeReader(enc, r, &u)
		},
	)

	return u, ok, err
}

func (c *BaseClient) BlockMap( //nolint:dupl //...
	ctx context.Context, ci quicstream.UDPConnInfo, height base.Height,
) (base.BlockMap, bool, error) {
	header := NewBlockMapRequestHeader(height)
	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	var u base.BlockMap

	ok, err := hcReqResBodyDecOK(
		ctx,
		ci,
		header,
		c.Client.Broker,
		func(enc encoder.Encoder, r io.Reader) error {
			return encoder.DecodeReader(enc, r, &u)
		},
	)

	return u, ok, err
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

	switch h, _, body, cancel, err := hcReqResBody(ctx, ci, header, c.Client.Broker); {
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
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	networkID base.NetworkID,
	node base.Address,
	pub base.Publickey,
	input []byte,
) (base.Signature, error) {
	e := util.StringError("NodeChallenge")

	header := NewNodeChallengeRequestHeader(input)
	if err := header.IsValid(nil); err != nil {
		return nil, e.Wrap(err)
	}

	var sig base.Signature

	switch ok, err := hcReqResBodyDecOK(
		ctx,
		ci,
		header,
		c.Client.Broker,
		func(enc encoder.Encoder, r io.Reader) error {
			return enc.StreamDecoder(r).Decode(&sig)
		},
	); {
	case err != nil:
		return nil, e.Wrap(err)
	case !ok:
		return nil, e.Errorf("empty")
	default:
		if err := pub.Verify(util.ConcatBytesSlice(
			node.Bytes(),
			networkID,
			input,
		), sig); err != nil {
			return nil, e.Wrap(err)
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

func (c *BaseClient) SyncSourceConnInfo(ctx context.Context, ci quicstream.UDPConnInfo) ([]isaac.NodeConnInfo, error) {
	e := util.StringError("SyncSourceConnInfo")

	ncis, err := c.requestNodeConnInfos(ctx, ci, NewSyncSourceConnInfoRequestHeader())
	if err != nil {
		return nil, e.Wrap(err)
	}

	return ncis, nil
}

func (c *BaseClient) State(
	ctx context.Context, ci quicstream.UDPConnInfo, key string, st util.Hash,
) (base.State, bool, error) {
	header := NewStateRequestHeader(key, st)
	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	var u base.State

	ok, err := hcReqResBodyDecOK(
		ctx,
		ci,
		header,
		c.Client.Broker,
		func(enc encoder.Encoder, r io.Reader) error {
			return encoder.DecodeReader(enc, r, &u)
		},
	)

	return u, ok, err
}

func (c *BaseClient) ExistsInStateOperation(
	ctx context.Context, ci quicstream.UDPConnInfo, facthash util.Hash,
) (bool, error) {
	header := NewExistsInStateOperationRequestHeader(facthash)
	if err := header.IsValid(nil); err != nil {
		return false, err
	}

	return hcReqResOK(
		ctx,
		ci,
		header,
		c.Client.Broker,
	)
}

func (c *BaseClient) SendBallots(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	ballots []base.BallotSignFact,
) error {
	e := util.StringError("SendBallots")

	if len(ballots) < 1 {
		return e.Errorf("empty ballots")
	}

	header := NewSendBallotsHeader()

	_, err := hcBodyReqResOK(
		ctx,
		ci,
		header,
		ballots,
		c.Client.Broker,
	)

	return err
}

func (c *BaseClient) SetAllowConsensus(
	ctx context.Context, ci quicstream.UDPConnInfo,
	priv base.Privatekey, networkID base.NetworkID, allow bool,
) (bool, error) {
	switch _, rh, err := c.verifyNodeWithResponse(ctx, ci, priv, networkID, NewSetAllowConsensusHeader(allow)); {
	case err != nil:
		return false, errors.WithMessage(err, "response")
	case rh.Err() != nil:
		return false, errors.WithMessage(rh.Err(), "response")
	default:
		return rh.OK(), nil
	}
}

func (c *BaseClient) StartHandover(
	ctx context.Context,
	yci quicstream.UDPConnInfo, // NOTE broker y
	priv base.Privatekey,
	networkID base.NetworkID,
	address base.Address,
	xci quicstream.UDPConnInfo, // NOTE broker x
) (bool, error) {
	switch _, rh, err := c.verifyNodeWithResponse(ctx, yci, priv, networkID, NewStartHandoverHeader(xci, address)); {
	case err != nil:
		return false, errors.WithMessage(err, "response")
	case rh.Err() != nil:
		return false, errors.WithMessage(rh.Err(), "response")
	default:
		return rh.OK(), nil
	}
}

func (c *BaseClient) CancelHandover(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	priv base.Privatekey,
	networkID base.NetworkID,
) (bool, error) {
	switch _, rh, err := c.verifyNodeWithResponse(ctx, ci, priv, networkID, NewCancelHandoverHeader()); {
	case err != nil:
		return false, errors.WithMessage(err, "response")
	case rh.Err() != nil:
		return false, errors.WithMessage(rh.Err(), "response")
	default:
		return rh.OK(), nil
	}
}

func (c *BaseClient) HandoverMessage(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	message isaacstates.HandoverMessage,
) error {
	broker, err := c.Client.Broker(ctx, ci)
	if err != nil {
		return err
	}

	if err := broker.WriteRequestHead(ctx, NewHandoverMessageHeader()); err != nil {
		return err
	}

	if err := brokerPipeEncode(ctx, broker, message); err != nil {
		return err
	}

	switch _, h, err := broker.ReadResponseHead(ctx); {
	case err != nil:
		return err
	case h.Err() != nil:
		return h.Err()
	default:
		return nil
	}
}

func (c *BaseClient) CheckHandover(
	ctx context.Context,
	xci quicstream.UDPConnInfo, // NOTE broker x
	priv base.Privatekey,
	networkID base.NetworkID,
	address base.Address,
	yci quicstream.UDPConnInfo, // NOTE broker y
) (bool, error) {
	switch _, rh, err := c.verifyNodeWithResponse(ctx, xci, priv, networkID, NewCheckHandoverHeader(yci, address)); {
	case err != nil:
		return false, errors.WithMessage(err, "response")
	case rh.Err() != nil:
		return false, errors.WithMessage(rh.Err(), "response")
	default:
		return rh.OK(), nil
	}
}

func (c *BaseClient) CheckHandoverX(
	ctx context.Context,
	xci quicstream.UDPConnInfo, // NOTE broker x
	priv base.Privatekey,
	networkID base.NetworkID,
	address base.Address,
) (bool, error) {
	switch _, rh, err := c.verifyNodeWithResponse(ctx, xci, priv, networkID, NewCheckHandoverXHeader(address)); {
	case err != nil:
		return false, errors.WithMessage(err, "response")
	case rh.Err() != nil:
		return false, errors.WithMessage(rh.Err(), "response")
	default:
		return rh.OK(), nil
	}
}

func (c *BaseClient) AskHandover(
	ctx context.Context,
	xci quicstream.UDPConnInfo, // NOTE broker x
	priv base.Privatekey,
	networkID base.NetworkID,
	address base.Address,
	yci quicstream.UDPConnInfo, // NOTE broker y
) (handoverid string, canMoveConsensus bool, _ error) {
	switch _, rh, err := c.verifyNodeWithResponse(ctx, xci, priv, networkID, NewAskHandoverHeader(yci, address)); {
	case err != nil:
		return "", false, errors.WithMessage(err, "response")
	case rh.Err() != nil:
		return "", false, errors.WithMessage(rh.Err(), "response")
	default:
		header, ok := rh.(AskHandoverResponseHeader)
		if !ok {
			return "", false, errors.Errorf("expect AskHandoverResponseHeader, but %T", rh)
		}

		if err := header.IsValid(nil); err != nil {
			return "", false, err
		}

		return header.ID(), header.OK(), nil
	}
}

func (*BaseClient) verifyNode(
	ctx context.Context,
	broker *quicstreamheader.ClientBroker,
	priv base.Privatekey,
	networkID base.NetworkID,
	header quicstreamheader.RequestHeader,
) error {
	if err := header.IsValid(nil); err != nil {
		return err
	}

	if err := broker.WriteRequestHead(ctx, header); err != nil {
		return err
	}

	var input []byte

	if err := func() error {
		switch _, _, body, _, res, err := broker.ReadBody(ctx); {
		case err != nil:
			return err
		case res != nil:
			if res.Err() != nil {
				return res.Err()
			}

			return errors.Errorf("error response")
		case body == nil:
			return errors.Errorf("empty signature input")
		default:
			b, err := io.ReadAll(body)
			if err != nil && !errors.Is(err, io.EOF) {
				return errors.WithStack(err)
			}

			if len(b) < 1 {
				return errors.Errorf("empty signature input")
			}

			input = b

			return nil
		}
	}(); err != nil {
		return errors.WithMessage(err, "read signature input")
	}

	switch sig, err := priv.Sign(util.ConcatBytesSlice(networkID, input)); {
	case err != nil:
		return errors.WithMessage(err, "sign input")
	default:
		buf := bytes.NewBuffer(sig)

		if err := broker.WriteBody(ctx, quicstreamheader.FixedLengthBodyType, uint64(buf.Len()), buf); err != nil {
			return errors.WithMessage(err, "write signature")
		}

		return nil
	}
}

func (c *BaseClient) verifyNodeWithResponse(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	priv base.Privatekey,
	networkID base.NetworkID,
	header quicstreamheader.RequestHeader,
) (encoder.Encoder, quicstreamheader.ResponseHeader, error) {
	broker, err := c.Client.Broker(ctx, ci)
	if err != nil {
		return nil, nil, err
	}

	if err := c.verifyNode(ctx, broker, priv, networkID, header); err != nil {
		return nil, nil, err
	}

	return broker.ReadResponseHead(ctx)
}

func (c *BaseClient) requestNodeConnInfos(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header quicstreamheader.RequestHeader,
) ([]isaac.NodeConnInfo, error) {
	var cis []isaac.NodeConnInfo

	_, err := hcReqResBodyDecOK(
		ctx,
		ci,
		header,
		c.Client.Broker,
		func(enc encoder.Encoder, r io.Reader) error {
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
	)

	return cis, err
}

func (c *BaseClient) requestProposal(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header quicstreamheader.RequestHeader,
) (base.ProposalSignFact, bool, error) {
	var u base.ProposalSignFact

	ok, err := hcReqResBodyDecOK(
		ctx,
		ci,
		header,
		c.Client.Broker,
		func(enc encoder.Encoder, r io.Reader) error {
			return encoder.DecodeReader(enc, r, &u)
		},
	)

	return u, ok, err
}

func hcReqResOK(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header quicstreamheader.RequestHeader,
	brokerf quicstreamheader.ClientBrokerFunc,
) (bool, error) {
	broker, err := brokerf(ctx, ci)
	if err != nil {
		return false, err
	}

	defer func() {
		_ = broker.Close()
	}()

	switch _, rh, err := hcReqRes(ctx, broker, header); {
	case err != nil:
		return false, err
	default:
		return rh.OK(), rh.Err()
	}
}

func hcReqResBodyDecOK(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header quicstreamheader.RequestHeader,
	brokerf quicstreamheader.ClientBrokerFunc,
	decodef func(encoder.Encoder, io.Reader) error,
) (bool, error) {
	h, renc, rbody, cancel, err := hcReqResBody(ctx, ci, header, brokerf)
	if err != nil {
		return false, err
	}

	defer func() {
		_ = cancel()
	}()

	if h.Err() != nil {
		return h.OK(), h.Err()
	}

	if rbody == nil {
		return h.OK(), h.Err()
	}

	if err := decodef(renc, rbody); err != nil {
		return false, err
	}

	return h.OK(), h.Err()
}

func hcReqResBody(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header quicstreamheader.RequestHeader,
	brokerf quicstreamheader.ClientBrokerFunc,
) (
	_ quicstreamheader.ResponseHeader,
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

	switch rh, renc, rbody, err := hcReqResBodyWithBroker(ctx, broker, header); {
	case err != nil:
		_ = cancel()

		return nil, nil, nil, nil, err
	default:
		return rh, renc, rbody, cancel, nil
	}
}

func hcBodyReqResOK(
	ctx context.Context,
	ci quicstream.UDPConnInfo,
	header quicstreamheader.RequestHeader,
	i interface{},
	brokerf quicstreamheader.ClientBrokerFunc,
) (bool, error) {
	var broker *quicstreamheader.ClientBroker

	switch j, err := brokerf(ctx, ci); {
	case err != nil:
		return false, err
	default:
		broker = j
	}

	defer func() {
		_ = broker.Close()
	}()

	if err := broker.WriteRequestHead(ctx, header); err != nil {
		return false, err
	}

	if i != nil {
		if err := brokerPipeEncode(ctx, broker, i); err != nil {
			return false, err
		}
	}

	_, h, err := broker.ReadResponseHead(ctx)
	if err != nil {
		return false, err
	}

	return h.OK(), h.Err()
}

func hcReqResBodyWithBroker(
	ctx context.Context,
	broker *quicstreamheader.ClientBroker,
	header quicstreamheader.RequestHeader,
) (
	_ quicstreamheader.ResponseHeader,
	_ encoder.Encoder,
	responseBody io.Reader,
	_ error,
) {
	var renc encoder.Encoder
	var rh quicstreamheader.ResponseHeader

	switch enc, h, err := hcReqRes(ctx, broker, header); {
	case err != nil:
		return nil, nil, nil, err
	case h.Err() != nil:
		return h, nil, nil, nil
	default:
		renc = enc
		rh = h
	}

	switch bodyType, bodyLength, body, _, res, err := broker.ReadBody(ctx); {
	case err != nil:
		return rh, renc, nil, err
	case res != nil:
		return res, renc, nil, nil
	case bodyType == quicstreamheader.EmptyBodyType:
		return rh, renc, nil, nil
	case bodyType == quicstreamheader.FixedLengthBodyType:
		if bodyLength < 1 {
			return rh, renc, nil, nil
		}

		return rh, renc, body, nil
	case bodyType == quicstreamheader.StreamBodyType:
		return rh, renc, body, nil
	default:
		return rh, renc, nil, errors.Errorf("unknown body type, %d", bodyType)
	}
}

func hcReqRes(
	ctx context.Context,
	broker *quicstreamheader.ClientBroker,
	header quicstreamheader.RequestHeader,
) (
	encoder.Encoder,
	quicstreamheader.ResponseHeader,
	error,
) {
	if err := broker.WriteRequestHead(ctx, header); err != nil {
		return nil, nil, err
	}

	enc, res, err := broker.ReadResponseHead(ctx)

	return enc, res, err
}

func brokerPipeEncode(
	ctx context.Context,
	broker *quicstreamheader.ClientBroker,
	i interface{},
) error {
	return util.PipeReadWrite(
		ctx,
		func(ctx context.Context, pr io.Reader) error {
			return broker.WriteBody(ctx, quicstreamheader.StreamBodyType, 0, pr)
		},
		func(ctx context.Context, pw io.Writer) error {
			return broker.Encoder.StreamEncoder(pw).Encode(i)
		},
	)
}
