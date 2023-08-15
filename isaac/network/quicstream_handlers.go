package isaacnetwork

import (
	"bytes"
	"context"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"golang.org/x/sync/singleflight"
)

var ErrNoMoreNext = util.NewIDError("no more next")

var ContextKeyNodeChallengedNode = util.ContextKey("node-challenge-node")

func QuicstreamHandlerOperation(
	oppool isaac.NewOperationPool,
	getFromHandoverX func(context.Context, OperationRequestHeader) (
		enchint hint.Hint, body []byte, found bool, _ error,
	),
) quicstreamheader.Handler[OperationRequestHeader] {
	return boolBytesQUICstreamHandler(
		func(header OperationRequestHeader) string {
			return HandlerPrefixOperationString + header.Operation().String()
		},
		func(ctx context.Context, header OperationRequestHeader, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
			enchint, _, body, found, err := oppool.OperationBytes(context.Background(), header.Operation())
			if getFromHandoverX != nil && (err == nil || !found) {
				enchint, body, found, err = getFromHandoverX(ctx, header)
			}

			return enchint, body, found, err
		},
	)
}

func QuicstreamHandlerSendOperation(
	networkID base.NetworkID,
	oppool isaac.NewOperationPool,
	existsInStateOperationf func(util.Hash) (bool, error),
	filterSendOperationf func(base.Operation) (bool, error),
	svvote isaac.SuffrageVoteFunc,
	broadcast func(context.Context, string, base.Operation, []byte) error,
	maxMessageSize func() uint64,
) quicstreamheader.Handler[SendOperationRequestHeader] {
	filterNewOperation := quicstreamHandlerFilterOperation(
		existsInStateOperationf,
		filterSendOperationf,
	)

	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, _ SendOperationRequestHeader,
	) (context.Context, error) {
		e := util.StringError("handle new operation")

		var rbody io.Reader

		switch _, _, body, err := broker.ReadBodyErr(ctx); {
		case err != nil:
			return ctx, e.Wrap(err)
		case body == nil:
			return ctx, e.Errorf("empty body")
		default:
			rbody = body
		}

		var op base.Operation
		var body []byte

		max := maxMessageSize()

		switch i, err := io.ReadAll(rbody); {
		case err != nil:
			return ctx, e.Wrap(err)
		case uint64(len(i)) > max:
			return ctx, e.Errorf("too big size; >= %d", max)
		default:
			if err = encoder.Decode(broker.Encoder, i, &op); err != nil {
				return ctx, e.Wrap(err)
			}

			if op == nil {
				return ctx, e.Errorf("empty operation found")
			}

			if err = op.IsValid(networkID); err != nil {
				return ctx, e.Wrap(err)
			}

			body = i
		}

		if err := filterNewOperation(op); err != nil {
			return ctx, e.Wrap(err)
		}

		var added bool

		switch i, err := quicstreamHandlerSetOperation(ctx, oppool, svvote, op); {
		case err != nil:
			return ctx, e.Wrap(err)
		case i && broadcast != nil:
			go func() {
				_ = broadcast(ctx, op.Hash().String(), op, body)
			}()

			added = i
		default:
			added = i
		}

		if err := broker.WriteResponseHeadOK(ctx, added, nil); err != nil {
			return ctx, e.Wrap(err)
		}

		return ctx, nil
	}
}

func QuicstreamHandlerRequestProposal(
	local base.LocalNode,
	pool isaac.ProposalPool,
	proposalMaker *isaac.ProposalMaker,
	lastBlockMapf func() (base.BlockMap, bool, error),
	getFromHandoverX func(context.Context, RequestProposalRequestHeader) (base.ProposalSignFact, error),
) quicstreamheader.Handler[RequestProposalRequestHeader] {
	getOrCreateProposal := func(ctx context.Context, header RequestProposalRequestHeader) (
		base.ProposalSignFact, error,
	) {
		point := header.point
		proposer := header.Proposer()
		previousBlock := header.PreviousBlock()

		switch pr, found, err := pool.ProposalByPoint(point, proposer, previousBlock); {
		case err != nil:
			return nil, err
		case found:
			return pr, nil
		}

		if proposer.Equal(local.Address()) {
			switch pr, err := getFromHandoverX(ctx, header); {
			case err != nil:
				return nil, errors.WithMessage(err, "handover x")
			case pr != nil:
				if _, err := pool.SetProposal(pr); err != nil {
					return nil, errors.WithMessage(err, "handover x; set proposal")
				}

				return pr, nil
			}
		}

		if lastBlockMapf != nil {
			switch m, found, err := lastBlockMapf(); {
			case err != nil:
				return nil, err
			case !found:
			case point.Height() < m.Manifest().Height()-1:
				return nil, errors.Errorf("too old; ignored")
			case point.Height() > m.Manifest().Height(): // NOTE empty proposal for unreachable point
				return proposalMaker.Empty(context.Background(), point, previousBlock)
			}
		}

		if proposer.Equal(local.Address()) {
			return proposalMaker.New(context.Background(), point, previousBlock)
		}

		return nil, nil
	}

	return boolEncodeQUICstreamHandler[RequestProposalRequestHeader](
		func(header RequestProposalRequestHeader) string {
			return HandlerPrefixRequestProposalString + header.Point().String() + header.Proposer().String()
		},
		func(ctx context.Context, header RequestProposalRequestHeader, _ encoder.Encoder) (interface{}, bool, error) {
			pr, err := getOrCreateProposal(ctx, header)

			return pr, pr != nil, err
		},
	)
}

func QuicstreamHandlerProposal(
	pool isaac.ProposalPool,
	getFromHandoverX func(context.Context, ProposalRequestHeader) (hint.Hint, []byte, bool, error),
) quicstreamheader.Handler[ProposalRequestHeader] {
	return boolBytesQUICstreamHandler(
		func(header ProposalRequestHeader) string {
			return HandlerPrefixProposalString + header.Proposal().String()
		},
		func(ctx context.Context, header ProposalRequestHeader, _ encoder.Encoder) (
			hint.Hint, []byte, bool, error,
		) {
			enchint, _, body, found, err := pool.ProposalBytes(header.Proposal())
			if err == nil && found {
				return enchint, body, found, err
			}

			switch enchint, body, found, err := getFromHandoverX(ctx, header); {
			case err != nil:
				return enchint, nil, false, errors.WithMessage(err, "handover x")
			case !found:
				return enchint, nil, false, nil
			default:
				return enchint, body, found, nil
			}
		},
	)
}

func QuicstreamHandlerLastSuffrageProof(
	lastSuffrageProoff func(suffragestate util.Hash) (hint.Hint, []byte, []byte, bool, error),
) quicstreamheader.Handler[LastSuffrageProofRequestHeader] {
	return boolBytesQUICstreamHandler(
		func(header LastSuffrageProofRequestHeader) string {
			sgkey := HandlerPrefixLastSuffrageProofString
			if header.State() != nil {
				sgkey += header.State().String()
			}

			return sgkey
		},
		func(_ context.Context, header LastSuffrageProofRequestHeader, _ encoder.Encoder) (
			hint.Hint, []byte, bool, error,
		) {
			enchint, _, body, found, err := lastSuffrageProoff(header.State())

			return enchint, body, found, err
		},
	)
}

func QuicstreamHandlerSuffrageProof(
	suffrageProoff func(base.Height) (hint.Hint, []byte, []byte, bool, error),
) quicstreamheader.Handler[SuffrageProofRequestHeader] {
	return boolBytesQUICstreamHandler(
		func(header SuffrageProofRequestHeader) string {
			return HandlerPrefixSuffrageProofString + header.Height().String()
		},
		func(_ context.Context, header SuffrageProofRequestHeader, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
			enchint, _, body, found, err := suffrageProoff(header.Height())

			return enchint, body, found, err
		},
	)
}

// LastBlockMap responds the last BlockMap to client; if there is no BlockMap,
// it returns nil BlockMap and not updated without error.
func QuicstreamHandlerLastBlockMap(
	lastBlockMapf func(util.Hash) (hint.Hint, []byte, []byte, bool, error),
) quicstreamheader.Handler[LastBlockMapRequestHeader] {
	return boolBytesQUICstreamHandler(
		func(header LastBlockMapRequestHeader) string {
			sgkey := HandlerPrefixLastBlockMapString

			if header.Manifest() != nil {
				sgkey += header.Manifest().String()
			}

			return sgkey
		},
		func(_ context.Context, header LastBlockMapRequestHeader, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
			enchint, _, body, found, err := lastBlockMapf(header.Manifest())

			return enchint, body, found, err
		},
	)
}

func QuicstreamHandlerBlockMap(
	blockMapf func(base.Height) (hint.Hint, []byte, []byte, bool, error),
) quicstreamheader.Handler[BlockMapRequestHeader] {
	return boolBytesQUICstreamHandler(
		func(header BlockMapRequestHeader) string {
			return HandlerPrefixBlockMapString + header.Height().String()
		},
		func(_ context.Context, header BlockMapRequestHeader, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
			enchint, _, body, found, err := blockMapf(header.Height())

			return enchint, body, found, err
		},
	)
}

func QuicstreamHandlerBlockMapItem(
	blockMapItemf func(base.Height, base.BlockMapItemType) (io.ReadCloser, bool, error),
) quicstreamheader.Handler[BlockMapItemRequestHeader] {
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, header BlockMapItemRequestHeader,
	) (context.Context, error) {
		switch itemr, found, err := blockMapItemf(header.Height(), header.Item()); {
		case err != nil:
			return ctx, err
		case !found:
			return ctx, writeResponseStream(ctx, broker, found, nil, nil)
		case itemr == nil:
			return ctx, writeResponseStream(ctx, broker, false, nil, nil)
		default:
			defer func() {
				_ = itemr.Close()
			}()

			return ctx, writeResponseStream(ctx, broker, true, nil, itemr)
		}
	}
}

func QuicstreamHandlerState(
	statef func(string) (enchint hint.Hint, meta, body []byte, found bool, err error),
) quicstreamheader.Handler[StateRequestHeader] {
	return boolBytesQUICstreamHandler(
		func(header StateRequestHeader) string {
			return HandlerPrefixStateString + header.Key()
		},
		func(_ context.Context, header StateRequestHeader, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
			enchint, meta, body, found, err := statef(header.Key())
			if err != nil || !found {
				return enchint, nil, false, err
			}

			if found && header.Hash() != nil {
				mh, err := isaacdatabase.ReadHashRecordMeta(meta)
				if err != nil {
					return enchint, nil, found, err
				}

				if mh.Equal(header.Hash()) {
					body = nil
				}
			}

			return enchint, body, found, nil
		},
	)
}

func QuicstreamHandlerExistsInStateOperation(
	existsInStateOperationf func(util.Hash) (bool, error),
) quicstreamheader.Handler[ExistsInStateOperationRequestHeader] {
	var sg singleflight.Group

	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, header ExistsInStateOperationRequestHeader,
	) (context.Context, error) {
		e := util.StringError("handle exists instate operation")

		found, err, _ := util.SingleflightDo[bool](&sg,
			HandlerPrefixExistsInStateOperationString+header.FactHash().String(),
			func() (bool, error) {
				return existsInStateOperationf(header.FactHash())
			},
		)

		if err != nil {
			return ctx, e.Wrap(err)
		}

		if err := broker.WriteResponseHeadOK(ctx, found, nil); err != nil {
			return ctx, e.Wrap(err)
		}

		return ctx, nil
	}
}

func QuicstreamHandlerSuffrageNodeConnInfo(
	suffrageNodeConnInfof func() ([]isaac.NodeConnInfo, error),
) quicstreamheader.Handler[SuffrageNodeConnInfoRequestHeader] {
	handler := quicstreamHandlerNodeConnInfos(suffrageNodeConnInfof)

	return func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header SuffrageNodeConnInfoRequestHeader,
	) (context.Context, error) {
		nctx, err := handler(ctx, addr, broker, header)
		if err != nil {
			return nctx, errors.WithMessage(err, "handle SuffrageNodeConnInfo")
		}

		return nctx, nil
	}
}

func QuicstreamHandlerSyncSourceConnInfo(
	syncSourceConnInfof func() ([]isaac.NodeConnInfo, error),
) quicstreamheader.Handler[SyncSourceConnInfoRequestHeader] {
	handler := quicstreamHandlerNodeConnInfos(syncSourceConnInfof)

	return func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header SyncSourceConnInfoRequestHeader,
	) (context.Context, error) {
		nctx, err := handler(ctx, addr, broker, header)
		if err != nil {
			return nctx, errors.WithMessage(err, "handle SyncSourceConnInfo")
		}

		return nctx, nil
	}
}

func QuicstreamHandlerNodeChallenge(
	networkID base.NetworkID,
	local base.LocalNode,
) quicstreamheader.Handler[NodeChallengeRequestHeader] {
	return quicstreamHandlerNodeChallenge(
		networkID, local,
		func(input []byte) (base.Signature, error) {
			return local.Privatekey().Sign(util.ConcatBytesSlice(
				local.Address().Bytes(),
				networkID,
				input,
			))
		},
	)
}

func quicstreamHandlerNodeChallenge(
	networkID base.NetworkID,
	local base.LocalNode,
	signf func([]byte) (base.Signature, error),
) quicstreamheader.Handler[NodeChallengeRequestHeader] {
	return func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header NodeChallengeRequestHeader,
	) (context.Context, error) {
		e := util.StringError("handle NodeChallenge")

		// verify node
		if signf == nil {
			signf = func(input []byte) (base.Signature, error) { //revive:disable-line:modifies-parameter
				return local.Privatekey().Sign(util.ConcatBytesSlice(
					local.Address().Bytes(),
					networkID,
					input,
				))
			}
		}

		sig, err := signf(header.Input())
		if err != nil {
			return ctx, e.Wrap(err)
		}

		if err := writeBytes(ctx, broker, quicstreamheader.FixedLengthBodyType, sig); err != nil {
			return ctx, e.Wrap(err)
		}

		// verify client
		if header.Me() == nil {
			return ctx, e.Wrap(broker.WriteResponseHeadOK(ctx, true, nil))
		}

		input := util.UUID().Bytes()
		if err := writeBytes(ctx, broker, quicstreamheader.FixedLengthBodyType, input); err != nil {
			return ctx, e.Wrap(err)
		}

		var body io.Reader

		switch bodyType, bodyLength, i, err := broker.ReadBodyErr(ctx); {
		case err != nil:
			return ctx, e.WithMessage(err, "me signature")
		case bodyType == quicstreamheader.EmptyBodyType:
			return ctx, e.Errorf("empty me signature")
		case bodyType == quicstreamheader.FixedLengthBodyType:
			if bodyLength < 1 {
				return ctx, e.Errorf("empty me signature")
			}

			body = i
		case bodyType == quicstreamheader.StreamBodyType:
			body = i
		default:
			return ctx, e.Errorf("me signature; unknown body type, %d", bodyType)
		}

		switch sig, err := io.ReadAll(body); {
		case err != nil:
			return ctx, e.WithMessage(err, "me signature body")
		default:
			if rerr := header.MePublickey().Verify(util.ConcatBytesSlice(
				header.Me().Bytes(),
				networkID,
				input,
			), sig); rerr != nil {
				return ctx, e.WithMessage(rerr, "me signature")
			}

			ctx = context.WithValue(ctx, ContextKeyNodeChallengedNode, header.Me())
		}

		return ctx, e.Wrap(broker.WriteResponseHeadOK(ctx, true, nil))
	}
}

func QuicstreamHandlerNodeInfo(
	getNodeInfo func() ([]byte, error),
) quicstreamheader.Handler[NodeInfoRequestHeader] {
	var sg singleflight.Group

	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, _ NodeInfoRequestHeader,
	) (context.Context, error) {
		e := util.StringError("handle node info")

		b, err, _ := util.SingleflightDo[[]byte](&sg, HandlerPrefixNodeInfoString, func() ([]byte, error) {
			return getNodeInfo()
		})

		switch {
		case err != nil:
			return ctx, e.Wrap(err)
		case len(b) < 1:
			return ctx, e.Errorf("empty node info")
		}

		body := bytes.NewBuffer(b)
		defer body.Reset()

		if err := writeResponseStream(ctx, broker, true, nil, body); err != nil {
			return ctx, e.Wrap(err)
		}

		return ctx, nil
	}
}

func QuicstreamHandlerSendBallots(
	networkID base.NetworkID,
	votef func(base.BallotSignFact) error,
	maxMessageSize func() uint64,
) quicstreamheader.Handler[SendBallotsHeader] {
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, _ SendBallotsHeader,
	) (context.Context, error) {
		e := util.StringError("handle new ballot")

		var rbody io.Reader

		switch _, _, i, err := broker.ReadBodyErr(ctx); {
		case err != nil:
			return ctx, e.Wrap(err)
		default:
			rbody = i
		}

		var body []byte

		max := maxMessageSize()

		switch i, err := io.ReadAll(rbody); {
		case err != nil:
			return ctx, e.Wrap(err)
		case uint64(len(body)) > max:
			return ctx, e.Errorf("too big size; >= %d", max)
		default:
			body = i
		}

		switch u, err := broker.Encoder.DecodeSlice(body); {
		case err != nil:
			return ctx, e.Wrap(err)
		case len(u) < 1:
			return ctx, e.Errorf("empty body")
		default:
			for i := range u {
				bl, ok := u[i].(base.BallotSignFact)
				if !ok {
					return ctx, e.Errorf("expected BallotSignFact, but %T", u[i])
				}

				if err = bl.IsValid(networkID); err != nil {
					return ctx, e.Wrap(err)
				}

				if err = votef(bl); err != nil {
					return ctx, e.Wrap(err)
				}
			}
		}

		if err := broker.WriteResponseHeadOK(ctx, true, nil); err != nil {
			return ctx, e.Wrap(err)
		}

		return ctx, nil
	}
}

func QuicstreamHandlerSetAllowConsensus(
	pub base.Publickey,
	networkID base.NetworkID,
	setf func(allow bool) (isset bool),
) quicstreamheader.Handler[SetAllowConsensusHeader] {
	return func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header SetAllowConsensusHeader,
	) (context.Context, error) {
		err := QuicstreamHandlerVerifyNode(
			ctx, addr, broker,
			pub, networkID,
		)

		var ok bool

		if err == nil {
			ok = setf(header.Allow())
		}

		return ctx, broker.WriteResponseHeadOK(ctx, ok, err)
	}
}

func QuicstreamHandlerStreamOperations(
	pub base.Publickey,
	networkID base.NetworkID,
	limit uint64,
	traverse func(
		_ context.Context,
		offset []byte,
		callback func(enchint hint.Hint, meta isaacdatabase.PoolOperationRecordMeta, body, offset []byte) (bool, error),
	) error,
) quicstreamheader.Handler[StreamOperationsHeader] {
	return func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header StreamOperationsHeader,
	) (context.Context, error) {
		if err := QuicstreamHandlerVerifyNode(
			ctx, addr, broker,
			pub, networkID,
		); err != nil {
			return ctx, err
		}

		writeBody := func(enchint hint.Hint, body, offset []byte) error {
			buf := bytes.NewBuffer(nil)
			defer buf.Reset()

			if err := util.WriteLengthed(buf, enchint.Bytes()); err != nil {
				return err
			}

			if err := util.WriteLengthed(buf, body); err != nil {
				return err
			}

			if err := util.WriteLengthed(buf, offset); err != nil {
				return err
			}

			return broker.WriteBody(ctx, quicstreamheader.FixedLengthBodyType, uint64(buf.Len()), buf)
		}

		var count uint64

		err := traverse(
			ctx,
			header.Offset(),
			func(enchint hint.Hint, _ isaacdatabase.PoolOperationRecordMeta, body, offset []byte) (bool, error) {
				switch {
				case body == nil || offset == nil:
					return false, errors.Errorf("empty body")
				default:
					count++

					return limit < 1 || count < limit, writeBody(enchint, body, offset)
				}
			},
		)

		return ctx, broker.WriteResponseHeadOK(ctx, false, err)
	}
}

func quicstreamHandlerFilterOperation(
	existsInStateOperationf func(util.Hash) (bool, error),
	filterSendOperationf func(base.Operation) (bool, error),
) func(op base.Operation) error {
	return func(op base.Operation) error {
		switch found, err := existsInStateOperationf(op.Fact().Hash()); {
		case err != nil:
			return err
		case found:
			return util.ErrFound.Errorf("already in state")
		}

		switch passed, err := filterSendOperationf(op); {
		case err != nil:
			var reason base.OperationProcessReasonError

			if errors.As(err, &reason) {
				err = reason
			}

			return err
		case !passed:
			return errors.Errorf("filtered")
		default:
			return nil
		}
	}
}

func quicstreamHandlerSetOperation(
	ctx context.Context,
	oppool isaac.NewOperationPool,
	vote isaac.SuffrageVoteFunc,
	op base.Operation,
) (bool, error) {
	switch t := op.(type) {
	case base.SuffrageExpelOperation:
		return vote(t)
	default:
		return oppool.SetOperation(ctx, op)
	}
}

func quicstreamHandlerNodeConnInfos(
	f func() ([]isaac.NodeConnInfo, error),
) quicstreamheader.Handler[quicstreamheader.RequestHeader] {
	cache := util.NewLRUGCache[string, []isaac.NodeConnInfo](2) //nolint:gomnd //...

	var sg singleflight.Group

	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, _ quicstreamheader.RequestHeader,
	) (context.Context, error) {
		cis, err, _ := util.SingleflightDo[[]isaac.NodeConnInfo](&sg, "node_conn_infos",
			func() ([]isaac.NodeConnInfo, error) {
				var cis []isaac.NodeConnInfo

				switch i, found := cache.Get("node_conn_infos"); {
				case found:
					if i != nil {
						cis = i
					}
				default:
					k, err := f()
					if err != nil {
						return nil, err
					}

					cache.Set("node_conn_infos", k, time.Second*3) //nolint:gomnd //...

					cis = k
				}

				return cis, nil
			},
		)

		if err != nil {
			return ctx, errors.WithStack(err)
		}

		return ctx, writeResponseStreamEncode(ctx, broker, true, nil, cis)
	}
}

func boolEncodeQUICstreamHandler[T quicstreamheader.RequestHeader](
	sgkeyf func(T) string,
	f func(context.Context, T, encoder.Encoder) (interface{}, bool, error),
) quicstreamheader.Handler[T] {
	var sg singleflight.Group

	return func(
		ctx context.Context, _ net.Addr, broker *quicstreamheader.HandlerBroker, req T,
	) (context.Context, error) { //nolint:dupl //...
		sgkey := sgkeyf(req)

		i, err, _ := util.SingleflightDo[[2]interface{}](&sg, sgkey, func() ([2]interface{}, error) {
			j, bo, oerr := f(ctx, req, broker.Encoder)
			if oerr != nil {
				return [2]interface{}{}, oerr
			}

			return [2]interface{}{j, bo}, oerr
		})
		if err != nil {
			return ctx, errors.WithStack(err)
		}

		return ctx, writeResponseStreamEncode(ctx, broker, i[1].(bool), nil, i[0]) //nolint:forcetypeassert //...
	}
}

func boolBytesQUICstreamHandler[T quicstreamheader.RequestHeader](
	sgkeyf func(T) string,
	f func(context.Context, T, encoder.Encoder) (hint.Hint, []byte, bool, error),
) quicstreamheader.Handler[T] {
	var sg singleflight.Group

	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, header T,
	) (context.Context, error) {
		sgkey := sgkeyf(header)

		i, err, _ := util.SingleflightDo(&sg, sgkey, func() ([3]interface{}, error) {
			enchint, b, found, oerr := f(ctx, header, broker.Encoder)
			if oerr != nil {
				return [3]interface{}{}, oerr
			}

			return [3]interface{}{enchint, b, found}, nil
		})
		if err != nil {
			return ctx, errors.WithStack(err)
		}

		enchint := i[0].(hint.Hint) //nolint:forcetypeassert //..
		found := i[2].(bool)        //nolint:forcetypeassert //...

		var body io.Reader

		if i[1] != nil {
			buf := bytes.NewBuffer(i[1].([]byte)) //nolint:forcetypeassert //..
			defer buf.Reset()

			body = buf
		}

		if !enchint.IsEmpty() {
			broker.Encoder = broker.Encoders.Find(enchint) //nolint:forcetypeassert //...
			if broker.Encoder == nil {
				return ctx, errors.Errorf("find encoder, %q", enchint)
			}
		}

		return ctx, writeResponseStream(ctx, broker, found, nil, body)
	}
}

func writeResponseStream(
	ctx context.Context,
	broker *quicstreamheader.HandlerBroker,
	ok bool, err error,
	body io.Reader,
) error {
	if eerr := broker.WriteResponseHeadOK(ctx, ok, err); eerr != nil {
		return eerr
	}

	if body == nil {
		return broker.WriteBody(ctx, quicstreamheader.EmptyBodyType, 0, nil)
	}

	return broker.WriteBody(ctx, quicstreamheader.StreamBodyType, 0, body)
}

func writeResponseStreamEncode(
	ctx context.Context,
	broker *quicstreamheader.HandlerBroker,
	ok bool, err error,
	i interface{},
) error {
	if eerr := broker.WriteResponseHeadOK(ctx, ok, err); eerr != nil {
		return eerr
	}

	if i == nil {
		return broker.WriteBody(ctx, quicstreamheader.EmptyBodyType, 0, nil)
	}

	return util.PipeReadWrite(
		ctx,
		func(ctx context.Context, pr io.Reader) error {
			return broker.WriteBody(ctx, quicstreamheader.StreamBodyType, 0, pr)
		},
		func(_ context.Context, pw io.Writer) error {
			return broker.Encoder.StreamEncoder(pw).Encode(i)
		},
	)
}

func QuicstreamHandlerVerifyNode(
	ctx context.Context,
	_ net.Addr,
	broker *quicstreamheader.HandlerBroker,
	pub base.Publickey,
	networkID base.NetworkID,
) error {
	input := util.UUID().Bytes()

	if err := writeBytes(ctx, broker, quicstreamheader.FixedLengthBodyType, input); err != nil {
		return err
	}

	var sig base.Signature

	if err := func() error {
		switch _, _, body, err := broker.ReadBodyErr(ctx); {
		case err != nil:
			return err
		default:
			b, err := io.ReadAll(body)
			if err != nil && !errors.Is(err, io.EOF) {
				return errors.WithStack(err)
			}

			sig = base.Signature(b)

			return nil
		}
	}(); err != nil {
		return errors.WithMessage(err, "read signature")
	}

	if err := pub.Verify(util.ConcatBytesSlice(networkID, input), sig); err != nil {
		return errors.WithMessage(err, "node verify signature")
	}

	return nil
}
