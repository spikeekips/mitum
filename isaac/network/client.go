package isaacnetwork

import (
	"bytes"
	"context"
	"io"
	"net/url"

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
	Encoders *encoder.Encoders
	Encoder  encoder.Encoder
	dialf    quicstreamheader.DialFunc
	closef   func() error
	clientid string
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

func (c *BaseClient) ClientID() string {
	return c.clientid
}

func (c *BaseClient) SetClientID(id string) *BaseClient {
	c.clientid = id

	return c
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

	h := NewOperationRequestHeader(oph)
	h.SetClientID(c.ClientID())

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		rfound, rerr := HCReqResBodyDecOK(
			ctx,
			broker,
			h,
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
			func(enchint string, body, roffset []byte) error {
				_, enc, found, err := c.Encoders.FindByString(enchint)
				switch {
				case err != nil:
					return errors.Errorf("unknown encoder, %q", enchint)
				case !found:
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
	f func(enchint string, body, offset []byte) error,
) error {
	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return err
	}

	return streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		return c.streamOperationsBytes(ctx, broker, priv, networkID, offset, f)
	})
}

func (c *BaseClient) streamOperationsBytes(
	ctx context.Context, broker *quicstreamheader.ClientBroker,
	priv base.Privatekey, networkID base.NetworkID, offset []byte,
	f func(enchint string, body, offset []byte) error,
) error {
	header := NewStreamOperationsHeader(offset)
	header.SetClientID(c.ClientID())

	if err := header.IsValid(nil); err != nil {
		return err
	}

	if err := broker.WriteRequestHead(ctx, header); err != nil {
		return err
	}

	if err := VerifyNode(ctx, broker, priv, networkID); err != nil {
		return err
	}

	for {
		switch _, _, body, _, res, err := broker.ReadBody(ctx); {
		case errors.Is(err, io.EOF):
			return nil
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
			var enchint string

			switch _, b, err := util.ReadLengthed(body); {
			case err != nil:
				return errors.WithMessage(err, "enc hint")
			default:
				enchint = string(b)
			}

			var op, of []byte

			switch _, b, err := util.ReadLengthed(body); {
			case err != nil:
				return errors.WithMessage(err, "body")
			default:
				op = b
			}

			var iseof bool

			switch _, b, err := util.ReadLengthed(body); {
			case err == nil, errors.Is(err, io.EOF):
				of = b

				iseof = errors.Is(err, io.EOF)
			default:
				return errors.WithMessage(err, "offset")
			}

			if err := f(enchint, op, of); err != nil {
				return err
			}

			if iseof {
				return nil
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

	h := NewSendOperationRequestHeader()
	h.SetClientID(c.ClientID())

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		i, rerr := hcBodyReqResOK(ctx, broker, h, op)
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
	header.SetClientID(c.ClientID())

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
	header.SetClientID(c.ClientID())

	if err := header.IsValid(nil); err != nil {
		return nil, false, err
	}

	return c.requestProposal(ctx, ci, header)
}

func (c *BaseClient) LastSuffrageProof(
	ctx context.Context, ci quicstream.ConnInfo, state util.Hash,
) (lastheight base.Height, proof base.SuffrageProof, ok bool, _ error) {
	header := NewLastSuffrageProofRequestHeader(state)
	header.SetClientID(c.ClientID())

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

				switch m, _, rerr := util.ReadLengthedBytesSlice(b); {
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
	header.SetClientID(c.ClientID())

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
	header.SetClientID(c.ClientID())

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
	header.SetClientID(c.ClientID())

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

func (c *BaseClient) BlockItem(
	ctx context.Context, ci quicstream.ConnInfo, height base.Height, item base.BlockItemType,
	f func(_ io.Reader, uri url.URL, compressFormat string) error,
) (found bool, _ error) {
	header := NewBlockItemRequestHeader(height, item)
	header.SetClientID(c.ClientID())

	if err := header.IsValid(nil); err != nil {
		return false, err
	}

	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return false, err
	}

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		switch h, _, body, rerr := hcReqResBody(ctx, broker, header); {
		case rerr != nil:
			return rerr
		case h.Err() != nil:
			return h.Err()
		case !h.OK():
			return nil
		default:
			found = true

			switch i, rerr := util.AssertInterfaceValue[BlockItemResponseHeader](h); {
			case rerr != nil:
				return rerr
			default:
				return f(body, i.URI(), i.CompressFormat())
			}
		}
	})

	return found, err
}

func (c *BaseClient) BlockItemFiles(
	ctx context.Context, ci quicstream.ConnInfo,
	height base.Height,
	priv base.Privatekey,
	networkID base.NetworkID,
	f func(io.Reader) error,
) (bool, error) {
	header := NewBlockItemFilesRequestHeader(height, priv.Publickey())
	header.SetClientID(c.ClientID())

	if err := header.IsValid(nil); err != nil {
		return false, err
	}

	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return false, err
	}

	var found bool

	if err := streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		if err := broker.WriteRequestHead(ctx, header); err != nil {
			return err
		}

		if err := VerifyNode(ctx, broker, priv, networkID); err != nil {
			return err
		}

		switch h, _, body, err := hcResBody(ctx, broker); {
		case err != nil:
			return err
		case h.Err() != nil:
			return h.Err()
		case !h.OK(), body == nil:
			found = false

			return nil
		default:
			found = true

			return f(body)
		}
	}); err != nil {
		return found, err
	}

	return found, nil
}

func (c *BaseClient) NodeChallenge(
	ctx context.Context,
	ci quicstream.ConnInfo,
	networkID base.NetworkID,
	node base.Address,
	nodePublickey base.Publickey,
	input []byte,
	me base.LocalNode,
) (sig base.Signature, _ error) {
	return c.nodeChallenge(
		ctx, ci, networkID, node, nodePublickey, input, me,
		func(input []byte) (base.Signature, error) {
			return me.Privatekey().Sign(util.ConcatBytesSlice(
				me.Address().Bytes(),
				networkID,
				input,
			))
		},
	)
}

func (c *BaseClient) SuffrageNodeConnInfo(
	ctx context.Context, ci quicstream.ConnInfo,
) ([]isaac.NodeConnInfo, error) {
	header := NewSuffrageNodeConnInfoRequestHeader()
	header.SetClientID(c.ClientID())

	ncis, err := c.requestNodeConnInfos(ctx, ci, header)

	return ncis, errors.WithMessage(err, "request; SuffrageNodeConnInfo")
}

func (c *BaseClient) SyncSourceConnInfo(ctx context.Context, ci quicstream.ConnInfo) ([]isaac.NodeConnInfo, error) {
	e := util.StringError("SyncSourceConnInfo")

	header := NewSyncSourceConnInfoRequestHeader()
	header.SetClientID(c.ClientID())

	ncis, err := c.requestNodeConnInfos(ctx, ci, header)
	if err != nil {
		return nil, e.Wrap(err)
	}

	return ncis, nil
}

func (c *BaseClient) State(
	ctx context.Context, ci quicstream.ConnInfo, key string, sth util.Hash,
) (st base.State, found bool, _ error) {
	header := NewStateRequestHeader(key, sth)
	header.SetClientID(c.ClientID())

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
	header.SetClientID(c.ClientID())

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
		header.SetClientID(c.ClientID())

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
	header := NewSetAllowConsensusHeader(allow)
	header.SetClientID(c.ClientID())

	switch _, rh, err := c.verifyNodeWithResponse(ctx, ci, priv, networkID, header); {
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
	header := NewStartHandoverHeader(xci, address, priv.Publickey())
	header.SetClientID(c.ClientID())

	switch _, rh, err := c.verifyNodeWithResponse(ctx, yci, priv, networkID, header); {
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
	header := NewCancelHandoverHeader(priv.Publickey())
	header.SetClientID(c.ClientID())

	switch _, rh, err := c.verifyNodeWithResponse(ctx, ci, priv, networkID, header); {
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

	header := NewHandoverMessageHeader()
	header.SetClientID(c.ClientID())

	return streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		if err := broker.WriteRequestHead(ctx, header); err != nil {
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
	header := NewCheckHandoverHeader(yci, address, priv.Publickey())
	header.SetClientID(c.ClientID())

	switch _, rh, err := c.verifyNodeWithResponse(ctx, xci, priv, networkID, header); {
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
	header := NewCheckHandoverXHeader(address)
	header.SetClientID(c.ClientID())

	switch _, rh, err := c.verifyNodeWithResponse(ctx, xci, priv, networkID, header); {
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
	header := NewAskHandoverHeader(yci, address)
	header.SetClientID(c.ClientID())

	switch _, rh, err := c.verifyNodeWithResponse(ctx, xci, priv, networkID, header); {
	case err != nil:
		return "", false, errors.WithMessage(err, "response")
	case rh.Err() != nil:
		return "", false, errors.WithMessage(rh.Err(), "response")
	default:
		switch header, err := util.AssertInterfaceValue[AskHandoverResponseHeader](rh); {
		case err != nil:
			return "", false, err
		default:
			if err := header.IsValid(nil); err != nil {
				return "", false, err
			}

			return header.ID(), header.OK(), nil
		}
	}
}

func (c *BaseClient) verifyNodeWithResponse(
	ctx context.Context,
	ci quicstream.ConnInfo,
	priv base.Privatekey,
	networkID base.NetworkID,
	header quicstreamheader.RequestHeader,
) (enc encoder.Encoder, h quicstreamheader.ResponseHeader, _ error) {
	if err := header.IsValid(nil); err != nil {
		return nil, nil, err
	}

	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return nil, nil, err
	}

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		if rerr := broker.WriteRequestHead(ctx, header); rerr != nil {
			return rerr
		}

		renc, rh, rerr := VerifyNodeWithResponse(ctx, broker, priv, networkID)
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
				if rerr != nil {
					return rerr
				}

				cis = make([]isaac.NodeConnInfo, len(u))

				for i := range u {
					switch rci, rerr := util.AssertInterfaceValue[isaac.NodeConnInfo](u[i]); {
					case rerr != nil:
						return rerr
					default:
						cis[i] = rci
					}
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

func (c *BaseClient) nodeChallenge(
	ctx context.Context,
	ci quicstream.ConnInfo,
	networkID base.NetworkID,
	node base.Address,
	nodePublickey base.Publickey,
	input []byte,
	me base.LocalNode,
	signf func([]byte) (base.Signature, error),
) (sig base.Signature, _ error) {
	e := util.StringError("NodeChallenge")

	var header NodeChallengeRequestHeader

	switch {
	case me == nil:
		header = NewNodeChallengeRequestHeader(input, nil, nil)
	default:
		header = NewNodeChallengeRequestHeader(input, me.Address(), me.Publickey())
	}

	header.SetClientID(c.ClientID())

	if err := header.IsValid(nil); err != nil {
		return nil, e.Wrap(err)
	}

	streamer, err := c.dial(ctx, ci)
	if err != nil {
		return nil, e.Wrap(err)
	}

	err = streamer(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		switch i, nerr := c.nodeChallengeVerifyNode(ctx, broker, networkID, node, nodePublickey, header); {
		case nerr != nil:
			return e.WithMessage(nerr, "node")
		default:
			sig = i
		}

		if header.Me() == nil {
			return nil
		}

		if merr := c.nodeChallengeVerifyMe(ctx, broker, networkID, me, signf); merr != nil {
			return e.WithMessage(merr, "me")
		}

		return nil
	})

	return sig, e.Wrap(err)
}

func (*BaseClient) nodeChallengeVerifyNode(
	ctx context.Context,
	broker *quicstreamheader.ClientBroker,
	networkID base.NetworkID,
	node base.Address,
	nodePublickey base.Publickey,
	header NodeChallengeRequestHeader,
) (sig base.Signature, _ error) {
	// verify node
	if err := broker.WriteRequestHead(ctx, header); err != nil {
		return sig, errors.Wrap(err, "write header")
	}

	switch bodyType, _, body, _, res, err := ReadBodyNotEmpty(ctx, broker); {
	case err != nil:
		return sig, errors.WithStack(err)
	case res != nil:
		return sig, errors.WithStack(res.Err())
	case
		bodyType == quicstreamheader.FixedLengthBodyType,
		bodyType == quicstreamheader.StreamBodyType:
		switch i, err := io.ReadAll(body); {
		case err != nil:
			return sig, errors.WithStack(err)
		default:
			sig = base.Signature(i)
			if err := nodePublickey.Verify(util.ConcatBytesSlice(
				node.Bytes(),
				networkID,
				header.Input(),
			), sig); err != nil {
				return sig, errors.WithStack(err)
			}

			return sig, nil
		}
	default:
		return sig, errors.Errorf("node signature; unknown body type, %d", bodyType)
	}
}

func (*BaseClient) nodeChallengeVerifyMe(
	ctx context.Context,
	broker *quicstreamheader.ClientBroker,
	networkID base.NetworkID,
	me base.LocalNode,
	signf func([]byte) (base.Signature, error),
) error {
	// verify me
	var inputbody io.Reader

	switch bodyType, _, i, _, res, err := ReadBodyNotEmpty(ctx, broker); {
	case err != nil:
		return errors.WithStack(err)
	case res != nil:
		return errors.WithStack(res.Err())
	case bodyType == quicstreamheader.FixedLengthBodyType,
		bodyType == quicstreamheader.StreamBodyType:
		inputbody = i
	default:
		return errors.Errorf("input; unknown body type, %d", bodyType)
	}

	switch input, err := io.ReadAll(inputbody); {
	case err != nil:
		return errors.WithStack(err)
	default:
		if signf == nil {
			signf = func(input []byte) (base.Signature, error) { //revive:disable-line:modifies-parameter
				return me.Privatekey().Sign(util.ConcatBytesSlice(
					me.Address().Bytes(),
					networkID,
					input,
				))
			}
		}

		sig, err := signf(input)
		if err != nil {
			return errors.WithStack(err)
		}

		if err := writeBytes(ctx, broker, quicstreamheader.StreamBodyType, sig); err != nil {
			return errors.WithStack(err)
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

func hcResBody(ctx context.Context, broker *quicstreamheader.ClientBroker) (
	_ quicstreamheader.ResponseHeader,
	_ encoder.Encoder,
	responseBody io.Reader,
	_ error,
) {
	var renc encoder.Encoder
	var rh quicstreamheader.ResponseHeader

	switch enc, h, err := broker.ReadResponseHead(ctx); {
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

func writeBytes(
	ctx context.Context,
	broker quicstreamheader.WriteBodyBroker,
	bodyType quicstreamheader.BodyType,
	b []byte,
) error {
	buf := bytes.NewBuffer(b)
	defer buf.Reset()

	return broker.WriteBody(ctx, bodyType, uint64(len(b)), buf)
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
) error {
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
		if err := writeBytes(ctx, broker, quicstreamheader.FixedLengthBodyType, sig); err != nil {
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
) (encoder.Encoder, quicstreamheader.ResponseHeader, error) {
	if err := VerifyNode(ctx, broker, priv, networkID); err != nil {
		return nil, nil, err
	}

	return broker.ReadResponseHead(ctx)
}

func ReadBodyNotEmpty(ctx context.Context, broker quicstreamheader.ReadBodyBroker) (
	bodyType quicstreamheader.BodyType,
	bodyLength uint64,
	body io.Reader, // NOTE response data body
	enc encoder.Encoder,
	res quicstreamheader.ResponseHeader,
	_ error,
) {
	bodyType, bodyLength, body, enc, res, err := broker.ReadBody(ctx)

	var empty bool

	switch {
	case err != nil:
		return bodyType, bodyLength, body, enc, res, err
	case res != nil:
	case bodyType == quicstreamheader.EmptyBodyType:
		empty = true
	case bodyType == quicstreamheader.FixedLengthBodyType && bodyLength < 1:
		empty = true
	}

	if empty {
		return bodyType, bodyLength, nil, enc, res, errors.Errorf("empty body")
	}

	return bodyType, bodyLength, body, enc, res, nil
}
