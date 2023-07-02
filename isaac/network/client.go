package isaacnetwork

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type BaseClient struct {
	Encoders *encoder.Encoders
	Encoder  encoder.Encoder
	dialf    quicstreamheader.DialFunc
	closef   func() error
}

func NewBaseClient(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	dialf quicstream.ConnInfoDialFunc,
	closef func() error,
) *BaseClient {
	return &BaseClient{
		Encoders: encs,
		Encoder:  enc,
		dialf:    quicstreamheader.NewDialFunc(dialf, encs, enc),
		closef:   closef,
	}
}

func (c *BaseClient) Dial(
	ctx context.Context, ci quicstream.ConnInfo,
) (quicstreamheader.StreamFunc, func() error, error) {
	return c.dialf(ctx, ci)
}

func (c *BaseClient) Close() error {
	return c.closef()
}

func (c *BaseClient) dial(ctx context.Context, ci quicstream.ConnInfo) (quicstreamheader.StreamFunc, error) {
	f, _, err := c.dialf(ctx, ci)

	return f, err
}

func (c *BaseClient) Operation(
	ctx context.Context, ci quicstream.ConnInfo, oph util.Hash,
) (op base.Operation, found bool, _ error) {
	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return nil, false, err
	}

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		rfound, rerr := HCReqResBodyDecOK(
			ctx,
			broker,
			NewOperationRequestHeader(oph),
			func(enc encoder.Encoder, r io.Reader) error {
				return encoder.DecodeReader(enc, r, &op)
			},
		)
		if rerr != nil {
			return rerr
		}

		found = rfound

		return nil
	})

	return op, found, err
}

func (c *BaseClient) StreamOperations(
	ctx context.Context, ci quicstream.ConnInfo,
	priv base.Privatekey, networkID base.NetworkID, offset []byte,
	f func(_ base.Operation, offset []byte) error,
) error {
	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return err
	}

	return streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		return c.streamOperationsBytes(ctx, broker, priv, networkID, offset,
			func(enchint hint.Hint, body, roffset []byte) error {
				enc := c.Encoders.Find(enchint)
				if enc == nil {
					return errors.Errorf("unknown encoder, %q", enchint)
				}

				var op base.Operation

				if err := encoder.Decode(enc, body, &op); err != nil {
					return err
				}

				return f(op, roffset)
			},
		)
	})
}

func (c *BaseClient) StreamOperationsBytes(
	ctx context.Context, ci quicstream.ConnInfo,
	priv base.Privatekey, networkID base.NetworkID, offset []byte,
	f func(enchint hint.Hint, body, offset []byte) error,
) error {
	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return err
	}

	return streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		return c.streamOperationsBytes(ctx, broker, priv, networkID, offset, f)
	})
}

func (*BaseClient) streamOperationsBytes(
	ctx context.Context, broker *quicstreamheader.ClientBroker,
	priv base.Privatekey, networkID base.NetworkID, offset []byte,
	f func(enchint hint.Hint, body, offset []byte) error,
) error {
	if err := VerifyNode(ctx, broker, priv, networkID, NewStreamOperationsHeader(offset)); err != nil {
		return err
	}

	for {
		switch _, _, body, _, res, err := broker.ReadBody(ctx); {
		case err != nil:
			return err
		case res != nil:
			if res.Err() != nil {
				return res.Err()
			}

			return nil
		case body == nil:
			return errors.Errorf("empty body")
		default:
			var enchint hint.Hint

			switch b, err := util.ReadLengthed(body); {
			case err != nil:
				return errors.WithMessage(err, "enc hint")
			default:
				ht, err := hint.ParseHint(string(b))
				if err != nil {
					return errors.WithMessage(err, "enc hint")
				}

				enchint = ht
			}

			var op, of []byte

			switch b, err := util.ReadLengthed(body); {
			case err != nil:
				return errors.WithMessage(err, "body")
			default:
				op = b
			}

			switch b, err := util.ReadLengthed(body); {
			case err != nil:
				return errors.WithMessage(err, "offset")
			default:
				of = b
			}

			if err := f(enchint, op, of); err != nil {
				return err
			}
		}
	}
}

func (c *BaseClient) SendOperation(ctx context.Context, ci quicstream.ConnInfo, op base.Operation) (bool, error) {
	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return false, err
	}

	var sent bool

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		i, rerr := hcBodyReqResOK(
			ctx,
			broker,
			NewSendOperationRequestHeader(),
			op,
		)
		if rerr != nil {
			return rerr
		}

		sent = i

		return nil
	})

	return sent, err
}

func (c *BaseClient) RequestProposal(
	ctx context.Context,
	ci quicstream.ConnInfo,
	point base.Point,
	proposer base.Address,
	previousBlock util.Hash,
) (pr base.ProposalSignFact, found bool, _ error) {
	header := NewRequestProposalRequestHeader(point, proposer, previousBlock)
	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	return c.requestProposal(ctx, ci, header)
}

func (c *BaseClient) Proposal( //nolint:dupl //...
	ctx context.Context,
	ci quicstream.ConnInfo,
	pr util.Hash,
) (base.ProposalSignFact, bool, error) {
	header := NewProposalRequestHeader(pr)
	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	return c.requestProposal(ctx, ci, header)
}

func (c *BaseClient) LastSuffrageProof(
	ctx context.Context, ci quicstream.ConnInfo, state util.Hash,
) (lastheight base.Height, proof base.SuffrageProof, ok bool, _ error) {
	header := NewLastSuffrageProofRequestHeader(state)

	lastheight = base.NilHeight

	if err := header.IsValid(nil); err != nil {
		return lastheight, nil, false, err
	}

	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return lastheight, nil, false, err
	}

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		switch rok, rerr := HCReqResBodyDecOK(
			ctx,
			broker,
			header,
			func(enc encoder.Encoder, r io.Reader) error {
				if r == nil {
					return nil
				}

				b, rerr := io.ReadAll(r)
				if rerr != nil {
					return errors.WithMessage(rerr, "read response")
				}

				switch _, m, _, rerr := util.ReadLengthedBytesSlice(b); {
				case rerr != nil:
					return errors.WithMessage(rerr, "read response")
				case len(m) != 2:
					return errors.Errorf("invalid response message")
				case len(m[0]) < 1:
					return nil
				default:
					i, rerr := base.ParseHeightBytes(m[0])
					if rerr != nil {
						return errors.WithMessage(rerr, "load last height")
					}

					lastheight = i

					if len(m[1]) > 0 {
						if rerr := encoder.Decode(enc, m[1], &proof); rerr != nil {
							return rerr
						}
					}

					return nil
				}
			},
		); {
		case rerr != nil:
			return rerr
		case proof == nil:
			return nil
		default:
			ok = rok

			return nil
		}
	})

	return lastheight, proof, ok, err
}

func (c *BaseClient) SuffrageProof( //nolint:dupl //...
	ctx context.Context, ci quicstream.ConnInfo, suffrageheight base.Height,
) (proof base.SuffrageProof, ok bool, _ error) {
	header := NewSuffrageProofRequestHeader(suffrageheight)
	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return nil, false, err
	}

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		rok, rerr := HCReqResBodyDecOK(
			ctx,
			broker,
			header,
			func(enc encoder.Encoder, r io.Reader) error {
				return encoder.DecodeReader(enc, r, &proof)
			},
		)
		if rerr != nil {
			return rerr
		}

		ok = rok

		return nil
	})

	return proof, ok, err
}

func (c *BaseClient) LastBlockMap( //nolint:dupl //...
	ctx context.Context, ci quicstream.ConnInfo, manifest util.Hash,
) (bm base.BlockMap, ok bool, _ error) {
	header := NewLastBlockMapRequestHeader(manifest)
	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return nil, false, err
	}

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		rok, rerr := HCReqResBodyDecOK(
			ctx,
			broker,
			header,
			func(enc encoder.Encoder, r io.Reader) error {
				return encoder.DecodeReader(enc, r, &bm)
			},
		)
		if rerr != nil {
			return rerr
		}

		ok = rok

		return nil
	})

	return bm, ok, err
}

func (c *BaseClient) BlockMap( //nolint:dupl //...
	ctx context.Context, ci quicstream.ConnInfo, height base.Height,
) (bm base.BlockMap, ok bool, _ error) {
	header := NewBlockMapRequestHeader(height)
	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return nil, false, err
	}

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		rok, rerr := HCReqResBodyDecOK(
			ctx,
			broker,
			header,
			func(enc encoder.Encoder, r io.Reader) error {
				return encoder.DecodeReader(enc, r, &bm)
			},
		)
		if rerr != nil {
			return rerr
		}

		ok = rok

		return nil
	})

	return bm, ok, err
}

func (c *BaseClient) BlockMapItem(
	ctx context.Context, ci quicstream.ConnInfo, height base.Height, item base.BlockMapItemType,
	f func(io.Reader, bool) error,
) error {
	header := NewBlockMapItemRequestHeader(height, item)
	if err := header.IsValid(nil); err != nil {
		return err
	}

	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return err
	}

	return streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		switch h, _, body, err := hcReqResBody(ctx, broker, header); {
		case err != nil:
			return err
		case h.Err() != nil:
			return h.Err()
		default:
			return f(body, h.OK())
		}
	})
}

func (c *BaseClient) NodeChallenge(
	ctx context.Context,
	ci quicstream.ConnInfo,
	networkID base.NetworkID,
	node base.Address,
	pub base.Publickey,
	input []byte,
) (sig base.Signature, _ error) {
	e := util.StringError("NodeChallenge")

	header := NewNodeChallengeRequestHeader(input)
	if err := header.IsValid(nil); err != nil {
		return nil, e.Wrap(err)
	}

	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return nil, e.Wrap(err)
	}

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		switch ok, rerr := HCReqResBodyDecOK(
			ctx,
			broker,
			header,
			func(enc encoder.Encoder, r io.Reader) error {
				return enc.StreamDecoder(r).Decode(&sig)
			},
		); {
		case rerr != nil:
			return rerr
		case !ok:
			return e.Errorf("empty")
		default:
			if rerr := pub.Verify(util.ConcatBytesSlice(
				node.Bytes(),
				networkID,
				input,
			), sig); rerr != nil {
				return rerr
			}

			return nil
		}
	})

	return sig, e.Wrap(err)
}

func (c *BaseClient) SuffrageNodeConnInfo(
	ctx context.Context, ci quicstream.ConnInfo,
) ([]isaac.NodeConnInfo, error) {
	ncis, err := c.requestNodeConnInfos(ctx, ci, NewSuffrageNodeConnInfoRequestHeader())

	return ncis, errors.WithMessage(err, "request; SuffrageNodeConnInfo")
}

func (c *BaseClient) SyncSourceConnInfo(ctx context.Context, ci quicstream.ConnInfo) ([]isaac.NodeConnInfo, error) {
	e := util.StringError("SyncSourceConnInfo")

	ncis, err := c.requestNodeConnInfos(ctx, ci, NewSyncSourceConnInfoRequestHeader())
	if err != nil {
		return nil, e.Wrap(err)
	}

	return ncis, nil
}

func (c *BaseClient) State(
	ctx context.Context, ci quicstream.ConnInfo, key string, sth util.Hash,
) (st base.State, found bool, _ error) {
	header := NewStateRequestHeader(key, sth)
	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return nil, false, err
	}

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		rfound, rerr := HCReqResBodyDecOK(
			ctx,
			broker,
			header,
			func(enc encoder.Encoder, r io.Reader) error {
				return encoder.DecodeReader(enc, r, &st)
			},
		)
		if rerr != nil {
			return rerr
		}

		found = rfound

		return nil
	})

	return st, found, err
}

func (c *BaseClient) ExistsInStateOperation(
	ctx context.Context, ci quicstream.ConnInfo, facthash util.Hash,
) (found bool, _ error) {
	header := NewExistsInStateOperationRequestHeader(facthash)
	if err := header.IsValid(nil); err != nil {
		return false, err
	}

	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return false, err
	}

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		rfound, rerr := hcReqResOK(
			ctx,
			broker,
			header,
		)
		if rerr != nil {
			return rerr
		}

		found = rfound

		return nil
	})

	return found, err
}

func (c *BaseClient) SendBallots(
	ctx context.Context,
	ci quicstream.ConnInfo,
	ballots []base.BallotSignFact,
) error {
	e := util.StringError("SendBallots")

	if len(ballots) < 1 {
		return e.Errorf("empty ballots")
	}

	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return e.Wrap(err)
	}

	return e.Wrap(streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		header := NewSendBallotsHeader()

		_, err := hcBodyReqResOK(
			ctx,
			broker,
			header,
			ballots,
		)

		return err
	}))
}

func (c *BaseClient) SetAllowConsensus(
	ctx context.Context, ci quicstream.ConnInfo,
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
	yci quicstream.ConnInfo, // NOTE broker y
	priv base.Privatekey,
	networkID base.NetworkID,
	address base.Address,
	xci quicstream.ConnInfo, // NOTE broker x
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
	ci quicstream.ConnInfo,
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
	ci quicstream.ConnInfo,
	message isaacstates.HandoverMessage,
) error {
	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return err
	}

	return streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
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
	})
}

func (c *BaseClient) CheckHandover(
	ctx context.Context,
	xci quicstream.ConnInfo, // NOTE broker x
	priv base.Privatekey,
	networkID base.NetworkID,
	address base.Address,
	yci quicstream.ConnInfo, // NOTE broker y
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
	xci quicstream.ConnInfo, // NOTE broker x
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
	xci quicstream.ConnInfo, // NOTE broker x
	priv base.Privatekey,
	networkID base.NetworkID,
	address base.Address,
	yci quicstream.ConnInfo, // NOTE broker y
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

func (c *BaseClient) LastHandoverYLogs(
	ctx context.Context,
	yci quicstream.ConnInfo, // NOTE broker y
	priv base.Privatekey,
	networkID base.NetworkID,
	f func(json.RawMessage) bool,
) error {
	streamer, err := c.dial(ctx, yci)
	if err != nil {
		return err
	}

	return streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		switch _, rh, err := VerifyNodeWithResponse(ctx, broker, priv, networkID, NewLastHandoverYLogsHeader()); {
		case err != nil:
			return errors.WithMessage(err, "response")
		case rh.Err() != nil:
			return errors.WithMessage(rh.Err(), "response")
		}

		var br *bufio.Reader

		switch bodyType, _, body, _, rh, err := broker.ReadBody(ctx); {
		case err != nil:
			return err
		case rh != nil:
			return errors.WithMessage(rh.Err(), "response")
		case bodyType == quicstreamheader.EmptyBodyType:
			return nil
		case bodyType != quicstreamheader.StreamBodyType:
			return errors.Errorf("unknown body type, %d", bodyType)
		default:
			br = bufio.NewReader(body)
		}

	end:
		for {
			b, err := br.ReadBytes('\n')

			if bytes.HasSuffix(b, []byte{'\n'}) {
				b = b[:len(b)-1]
			}

			switch {
			case err == nil:
			case errors.Is(err, io.EOF):
			default:
				return errors.WithStack(err)
			}

			if len(b) > 0 {
				if !f(json.RawMessage(b)) {
					break end
				}
			}

			switch {
			case err == nil:
			case errors.Is(err, io.EOF):
				break end
			}
		}

		return nil
	})
}

func (c *BaseClient) verifyNodeWithResponse(
	ctx context.Context,
	ci quicstream.ConnInfo,
	priv base.Privatekey,
	networkID base.NetworkID,
	header quicstreamheader.RequestHeader,
) (enc encoder.Encoder, h quicstreamheader.ResponseHeader, _ error) {
	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return nil, nil, err
	}

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		renc, rh, rerr := VerifyNodeWithResponse(ctx, broker, priv, networkID, header)
		if rerr != nil {
			return rerr
		}

		enc = renc
		h = rh

		return nil
	})

	return enc, h, err
}

func (c *BaseClient) requestNodeConnInfos(
	ctx context.Context,
	ci quicstream.ConnInfo,
	header quicstreamheader.RequestHeader,
) (cis []isaac.NodeConnInfo, _ error) {
	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return nil, err
	}

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		_, rerr := HCReqResBodyDecOK(
			ctx,
			broker,
			header,
			func(enc encoder.Encoder, r io.Reader) error {
				b, rerr := io.ReadAll(r)
				if rerr != nil {
					return errors.WithStack(rerr)
				}

				u, rerr := enc.DecodeSlice(b)
				if err != nil {
					return rerr
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

		return rerr
	})

	return cis, err
}

func (c *BaseClient) requestProposal(
	ctx context.Context,
	ci quicstream.ConnInfo,
	header quicstreamheader.RequestHeader,
) (pr base.ProposalSignFact, found bool, _ error) {
	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return nil, false, err
	}

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		rfound, rerr := HCReqResBodyDecOK(
			ctx,
			broker,
			header,
			func(enc encoder.Encoder, r io.Reader) error {
				return encoder.DecodeReader(enc, r, &pr)
			},
		)
		if rerr != nil {
			return rerr
		}

		found = rfound

		return nil
	})

	return pr, found, err
}

func hcReqResOK(
	ctx context.Context,
	broker *quicstreamheader.ClientBroker,
	header quicstreamheader.RequestHeader,
) (bool, error) {
	switch _, rh, err := hcReqRes(ctx, broker, header); {
	case err != nil:
		return false, err
	default:
		return rh.OK(), rh.Err()
	}
}

func HCReqResBodyDecOK(
	ctx context.Context,
	broker *quicstreamheader.ClientBroker,
	header quicstreamheader.RequestHeader,
	decodef func(encoder.Encoder, io.Reader) error,
) (bool, error) {
	h, renc, rbody, err := hcReqResBody(ctx, broker, header)

	switch {
	case err != nil:
		return false, err
	case rbody != nil && h.Err() == nil:
		if err := decodef(renc, rbody); err != nil {
			return false, err
		}
	}

	return h.OK(), h.Err()
}

func hcBodyReqResOK(
	ctx context.Context,
	broker *quicstreamheader.ClientBroker,
	header quicstreamheader.RequestHeader,
	i interface{},
) (bool, error) {
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

func hcReqResBody(
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

func VerifyNode(
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

func VerifyNodeWithResponse(
	ctx context.Context,
	broker *quicstreamheader.ClientBroker,
	priv base.Privatekey,
	networkID base.NetworkID,
	header quicstreamheader.RequestHeader,
) (encoder.Encoder, quicstreamheader.ResponseHeader, error) {
	if err := VerifyNode(ctx, broker, priv, networkID, header); err != nil {
		return nil, nil, err
	}

	return broker.ReadResponseHead(ctx)
}
