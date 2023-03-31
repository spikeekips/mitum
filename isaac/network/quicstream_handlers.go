package isaacnetwork

import (
	"bytes"
	"context"
	"io"
	"net"
	"time"

	"github.com/bluele/gcache"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"golang.org/x/sync/singleflight"
)

var (
	HandlerPrefixRequestProposalString        = "request_proposal"
	HandlerPrefixProposalString               = "proposal"
	HandlerPrefixLastSuffrageProofString      = "last_suffrage_proof"
	HandlerPrefixSuffrageProofString          = "suffrage_proof"
	HandlerPrefixLastBlockMapString           = "last_blockmap"
	HandlerPrefixBlockMapString               = "blockmap"
	HandlerPrefixBlockMapItemString           = "blockmap_item"
	HandlerPrefixMemberlistString             = "memberlist"
	HandlerPrefixNodeChallengeString          = "node_challenge"
	HandlerPrefixSuffrageNodeConnInfoString   = "suffrage_node_conninfo"
	HandlerPrefixSyncSourceConnInfoString     = "sync_source_conninfo"
	HandlerPrefixOperationString              = "operation"
	HandlerPrefixSendOperationString          = "send_operation"
	HandlerPrefixStateString                  = "state"
	HandlerPrefixExistsInStateOperationString = "exists_instate_operation"
	HandlerPrefixNodeInfoString               = "node_info"
	HandlerPrefixSendBallotsString            = "send_ballots"

	HandlerPrefixRequestProposal        []byte = quicstream.HashPrefix(HandlerPrefixRequestProposalString)
	HandlerPrefixProposal                      = quicstream.HashPrefix(HandlerPrefixProposalString)
	HandlerPrefixLastSuffrageProof             = quicstream.HashPrefix(HandlerPrefixLastSuffrageProofString)
	HandlerPrefixSuffrageProof                 = quicstream.HashPrefix(HandlerPrefixSuffrageProofString)
	HandlerPrefixLastBlockMap                  = quicstream.HashPrefix(HandlerPrefixLastBlockMapString)
	HandlerPrefixBlockMap                      = quicstream.HashPrefix(HandlerPrefixBlockMapString)
	HandlerPrefixBlockMapItem                  = quicstream.HashPrefix(HandlerPrefixBlockMapItemString)
	HandlerPrefixMemberlist                    = quicstream.HashPrefix(HandlerPrefixMemberlistString)
	HandlerPrefixNodeChallenge                 = quicstream.HashPrefix(HandlerPrefixNodeChallengeString)
	HandlerPrefixSuffrageNodeConnInfo          = quicstream.HashPrefix(HandlerPrefixSuffrageNodeConnInfoString)
	HandlerPrefixSyncSourceConnInfo            = quicstream.HashPrefix(HandlerPrefixSyncSourceConnInfoString)
	HandlerPrefixOperation                     = quicstream.HashPrefix(HandlerPrefixOperationString)
	HandlerPrefixSendOperation                 = quicstream.HashPrefix(HandlerPrefixSendOperationString)
	HandlerPrefixState                         = quicstream.HashPrefix(HandlerPrefixStateString)
	HandlerPrefixExistsInStateOperation        = quicstream.HashPrefix(HandlerPrefixExistsInStateOperationString)
	HandlerPrefixNodeInfo                      = quicstream.HashPrefix(HandlerPrefixNodeInfoString)
	HandlerPrefixSendBallots                   = quicstream.HashPrefix(HandlerPrefixSendBallotsString)
)

func QuicstreamErrorHandler(enc encoder.Encoder, requestTimeoutf func() time.Duration) quicstream.ErrorHandler {
	nrequestTimeoutf := func() time.Duration {
		return time.Second * 2
	}

	if requestTimeoutf != nil {
		nrequestTimeoutf = requestTimeoutf
	}

	return func(_ net.Addr, _ io.Reader, w io.Writer, err error) error {
		ctx, cancel := context.WithTimeout(context.Background(), nrequestTimeoutf()) //nolint:gomnd //...
		defer cancel()

		return quicstream.HeaderWriteHead(ctx, w, enc,
			quicstream.NewDefaultResponseHeader(false, err),
		)
	}
}

func QuicstreamHandlerOperation(
	oppool isaac.NewOperationPool,
) quicstream.HeaderHandler {
	return boolBytesQUICstreamHandler(
		func(header quicstream.RequestHeader) string {
			h := header.(OperationRequestHeader) //nolint:forcetypeassert //...

			return HandlerPrefixOperationString + h.Operation().String()
		},
		func(header quicstream.RequestHeader, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
			h := header.(OperationRequestHeader) //nolint:forcetypeassert //...

			enchint, _, body, found, err := oppool.NewOperationBytes(context.Background(), h.Operation())

			return enchint, body, found, err
		},
	)
}

func QuicstreamHandlerSendOperation(
	params *isaac.LocalParams,
	oppool isaac.NewOperationPool,
	existsInStateOperationf func(util.Hash) (bool, error),
	filterSendOperationf func(base.Operation) (bool, error),
	svvote isaac.SuffrageVoteFunc,
	broadcast func(string, []byte) error,
) quicstream.HeaderHandler {
	filterNewOperation := quicstreamHandlerFilterOperation(
		existsInStateOperationf,
		filterSendOperationf,
	)

	return func(ctx context.Context, _ net.Addr, r io.Reader, w io.Writer, detail quicstream.RequestHeadDetail) error {
		e := util.StringErrorFunc("handle new operation")

		if _, ok := detail.Header.(SendOperationRequestHeader); !ok {
			return e(nil, "expected SendOperationRequestHeader, but %T", detail.Header)
		}

		var op base.Operation
		var body []byte

		var rbody io.Reader

		switch _, _, i, err := quicstream.HeaderReadBody(ctx, r); {
		case err != nil:
			return e(err, "")
		default:
			rbody = i
		}

		switch i, err := io.ReadAll(rbody); {
		case err != nil:
			return e(err, "")
		case uint64(len(body)) > params.MaxMessageSize():
			return e(nil, "too big size; >= %d", params.MaxMessageSize())
		default:
			if err = encoder.Decode(detail.Encoder, i, &op); err != nil {
				return e(err, "")
			}

			if op == nil {
				return e(nil, "empty operation found")
			}

			if err = op.IsValid(params.NetworkID()); err != nil {
				return e(err, "")
			}

			body = i
		}

		if err := filterNewOperation(op); err != nil {
			return e(err, "")
		}

		added, err := quicstreamHandlerSetOperation(ctx, oppool, svvote, op)
		if err == nil && added {
			if broadcast != nil {
				go func() {
					_ = broadcast(op.Hash().String(), body)
				}()
			}
		}

		if err = quicstream.WriteResponse(ctx, w,
			detail.Encoder,
			quicstream.NewDefaultResponseHeader(added, err),
			quicstream.EmptyDataFormat,
			0,
			nil,
		); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func quicstreamHandlerSetOperation(
	ctx context.Context,
	oppool isaac.NewOperationPool,
	vote isaac.SuffrageVoteFunc,
	op base.Operation,
) (bool, error) {
	switch t := op.(type) {
	case base.SuffrageWithdrawOperation:
		return vote(t)
	default:
		return oppool.SetNewOperation(ctx, op)
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

func QuicstreamHandlerRequestProposal(
	local base.LocalNode,
	pool isaac.ProposalPool,
	proposalMaker *isaac.ProposalMaker,
	lastBlockMapf func() (base.BlockMap, bool, error),
) quicstream.HeaderHandler {
	getOrCreateProposal := func(
		point base.Point,
		proposer base.Address,
	) (base.ProposalSignFact, error) {
		switch pr, found, err := pool.ProposalByPoint(point, proposer); {
		case err != nil:
			return nil, err
		case found:
			return pr, nil
		}

		if lastBlockMapf != nil {
			switch m, found, err := lastBlockMapf(); {
			case err != nil:
				return nil, err
			case !found:
			case point.Height() < m.Manifest().Height()-1:
				return nil, errors.Errorf("too old; ignored")
			case point.Height() > m.Manifest().Height(): // NOTE empty proposal for unreachable point
				return proposalMaker.Empty(context.Background(), point)
			}
		}

		if proposer.Equal(local.Address()) {
			return proposalMaker.New(context.Background(), point)
		}

		return nil, nil
	}

	return boolQUICstreamHandler(
		func(header quicstream.RequestHeader) string {
			h := header.(RequestProposalRequestHeader) //nolint:forcetypeassert //...

			return HandlerPrefixRequestProposalString + h.Point().String() + h.Proposer().String()
		},
		func(header quicstream.RequestHeader, _ encoder.Encoder) (interface{}, bool, error) {
			h := header.(RequestProposalRequestHeader) //nolint:forcetypeassert //...

			pr, err := getOrCreateProposal(h.point, h.Proposer())

			return pr, pr != nil, err
		},
	)
}

func QuicstreamHandlerProposal(pool isaac.ProposalPool) quicstream.HeaderHandler {
	return boolBytesQUICstreamHandler(
		func(header quicstream.RequestHeader) string {
			h := header.(ProposalRequestHeader) //nolint:forcetypeassert //...

			return HandlerPrefixProposalString + h.Proposal().String()
		},
		func(header quicstream.RequestHeader, _ encoder.Encoder) (
			enchint hint.Hint, body []byte, found bool, err error,
		) {
			h := header.(ProposalRequestHeader) //nolint:forcetypeassert //...

			enchint, _, body, found, err = pool.ProposalBytes(h.Proposal())

			return enchint, body, found, err
		},
	)
}

func QuicstreamHandlerLastSuffrageProof(
	lastSuffrageProoff func(suffragestate util.Hash) (hint.Hint, []byte, []byte, bool, error),
) quicstream.HeaderHandler {
	return boolBytesQUICstreamHandler(
		func(header quicstream.RequestHeader) string {
			h := header.(LastSuffrageProofRequestHeader) //nolint:forcetypeassert //...
			sgkey := HandlerPrefixLastSuffrageProofString
			if h.State() != nil {
				sgkey += h.State().String()
			}

			return sgkey
		},
		func(header quicstream.RequestHeader, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
			h := header.(LastSuffrageProofRequestHeader) //nolint:forcetypeassert //...

			enchint, _, body, found, err := lastSuffrageProoff(h.State())

			return enchint, body, found, err
		},
	)
}

func QuicstreamHandlerSuffrageProof(
	suffrageProoff func(base.Height) (hint.Hint, []byte, []byte, bool, error),
) quicstream.HeaderHandler {
	return boolBytesQUICstreamHandler(
		func(header quicstream.RequestHeader) string {
			h := header.(SuffrageProofRequestHeader) //nolint:forcetypeassert //...

			return HandlerPrefixSuffrageProofString + h.Height().String()
		},
		func(header quicstream.RequestHeader, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
			h := header.(SuffrageProofRequestHeader) //nolint:forcetypeassert //...

			enchint, _, body, found, err := suffrageProoff(h.Height())

			return enchint, body, found, err
		},
	)
}

// LastBlockMap responds the last BlockMap to client; if there is no BlockMap,
// it returns nil BlockMap and not updated without error.
func QuicstreamHandlerLastBlockMap(
	lastBlockMapf func(util.Hash) (hint.Hint, []byte, []byte, bool, error),
) quicstream.HeaderHandler {
	return boolBytesQUICstreamHandler(
		func(header quicstream.RequestHeader) string {
			sgkey := HandlerPrefixLastBlockMapString

			h := header.(LastBlockMapRequestHeader) //nolint:forcetypeassert //...

			if h.Manifest() != nil {
				sgkey += h.Manifest().String()
			}

			return sgkey
		},
		func(header quicstream.RequestHeader, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
			h := header.(LastBlockMapRequestHeader) //nolint:forcetypeassert //...

			enchint, _, body, found, err := lastBlockMapf(h.Manifest())

			return enchint, body, found, err
		},
	)
}

func QuicstreamHandlerBlockMap(
	blockMapf func(base.Height) (hint.Hint, []byte, []byte, bool, error),
) quicstream.HeaderHandler {
	return boolBytesQUICstreamHandler(
		func(header quicstream.RequestHeader) string {
			h := header.(BlockMapRequestHeader) //nolint:forcetypeassert //...

			return HandlerPrefixBlockMapString + h.Height().String()
		},
		func(header quicstream.RequestHeader, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
			h := header.(BlockMapRequestHeader) //nolint:forcetypeassert //...

			enchint, _, body, found, err := blockMapf(h.Height())

			return enchint, body, found, err
		},
	)
}

func QuicstreamHandlerBlockMapItem(
	blockMapItemf func(base.Height, base.BlockMapItemType) (io.ReadCloser, bool, error),
) quicstream.HeaderHandler {
	return func(ctx context.Context, _ net.Addr, r io.Reader, w io.Writer, detail quicstream.RequestHeadDetail) error {
		header, ok := detail.Header.(BlockMapItemRequestHeader)
		if !ok {
			return errors.Errorf("expected BlockMapItemRequestHeader, but %T", detail.Header)
		}

		var dataFormat quicstream.HeaderDataFormat

		itemr, found, err := blockMapItemf(header.Height(), header.Item())

		switch {
		case err != nil:
			return err
		case !found:
			return quicstream.WriteResponse(ctx, w, detail.Encoder,
				quicstream.NewDefaultResponseHeader(found, err),
				quicstream.EmptyDataFormat,
				0,
				nil,
			)
		case itemr == nil:
			dataFormat = quicstream.EmptyDataFormat
		default:
			defer func() {
				_ = itemr.Close()
			}()

			dataFormat = quicstream.StreamDataFormat
		}

		return quicstream.WriteResponse(ctx, w, detail.Encoder,
			quicstream.NewDefaultResponseHeader(found, err),
			dataFormat,
			0,
			itemr,
		)
	}
}

func QuicstreamHandlerNodeChallenge(
	local base.LocalNode,
	params base.LocalParams,
) quicstream.HeaderHandler {
	var sg singleflight.Group

	return func(
		ctx context.Context, addr net.Addr, r io.Reader, w io.Writer, detail quicstream.RequestHeadDetail,
	) error {
		e := util.StringErrorFunc("handle NodeChallenge")

		header, ok := detail.Header.(NodeChallengeRequestHeader)
		if !ok {
			return e(nil, "expected NodeChallengeRequestHeader, but %T", detail.Header)
		}

		i, err, _ := sg.Do(HandlerPrefixNodeChallengeString+string(header.Input()), func() (interface{}, error) {
			return local.Privatekey().Sign(util.ConcatBytesSlice(
				local.Address().Bytes(),
				params.NetworkID(),
				header.Input(),
			))
		})

		if err != nil {
			return e(err, "")
		}

		sig := i.(base.Signature) //nolint:forcetypeassert //...

		if err := WriteResponseStreamEncode(ctx, detail.Encoder, w,
			quicstream.NewDefaultResponseHeader(true, nil), sig); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerSuffrageNodeConnInfo(
	suffrageNodeConnInfof func() ([]isaac.NodeConnInfo, error),
) quicstream.HeaderHandler {
	handler := quicstreamHandlerNodeConnInfos(suffrageNodeConnInfof)

	return func(ctx context.Context, addr net.Addr, r io.Reader, w io.Writer,
		detail quicstream.RequestHeadDetail,
	) error {
		if err := handler(ctx, addr, r, w, detail); err != nil {
			return errors.WithMessage(err, "handle SuffrageNodeConnInfo")
		}

		return nil
	}
}

func QuicstreamHandlerSyncSourceConnInfo(
	syncSourceConnInfof func() ([]isaac.NodeConnInfo, error),
) quicstream.HeaderHandler {
	handler := quicstreamHandlerNodeConnInfos(syncSourceConnInfof)

	return func(ctx context.Context, addr net.Addr, r io.Reader, w io.Writer,
		detail quicstream.RequestHeadDetail,
	) error {
		if err := handler(ctx, addr, r, w, detail); err != nil {
			return errors.WithMessage(err, "handle SyncSourceConnInfo")
		}

		return nil
	}
}

func QuicstreamHandlerState(
	statef func(string) (enchint hint.Hint, meta, body []byte, found bool, err error),
) quicstream.HeaderHandler {
	return boolBytesQUICstreamHandler(
		func(header quicstream.RequestHeader) string {
			h := header.(StateRequestHeader) //nolint:forcetypeassert //..

			return HandlerPrefixStateString + h.Key()
		},
		func(header quicstream.RequestHeader, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
			h := header.(StateRequestHeader) //nolint:forcetypeassert //..

			enchint, meta, body, found, err := statef(h.Key())
			if err != nil || !found {
				return enchint, nil, false, err
			}

			if found && h.Hash() != nil {
				mh, err := isaacdatabase.ReadHashRecordMeta(meta)
				if err != nil {
					return enchint, nil, found, err
				}

				if mh.Equal(h.Hash()) {
					body = nil
				}
			}

			return enchint, body, found, nil
		},
	)
}

func QuicstreamHandlerExistsInStateOperation(
	existsInStateOperationf func(util.Hash) (bool, error),
) quicstream.HeaderHandler {
	var sg singleflight.Group

	return func(ctx context.Context, _ net.Addr, r io.Reader, w io.Writer, detail quicstream.RequestHeadDetail) error {
		e := util.StringErrorFunc("handle exists instate operation")

		header, ok := detail.Header.(ExistsInStateOperationRequestHeader)
		if !ok {
			return errors.Errorf("expected ExistsInStateOperationRequestHeader, but %T", detail.Header)
		}

		i, err, _ := sg.Do(
			HandlerPrefixExistsInStateOperationString+header.FactHash().String(),
			func() (interface{}, error) {
				return existsInStateOperationf(header.FactHash())
			},
		)

		if err != nil {
			return e(err, "")
		}

		if err := quicstream.HeaderWriteHead(ctx, w, //nolint:forcetypeassert //...
			detail.Encoder,
			quicstream.NewDefaultResponseHeader(i.(bool), nil),
		); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerNodeInfo(
	getNodeInfo func() ([]byte, error),
) quicstream.HeaderHandler {
	var sg singleflight.Group

	return func(ctx context.Context, _ net.Addr, r io.Reader, w io.Writer, detail quicstream.RequestHeadDetail) error {
		e := util.StringErrorFunc("handle node info")

		if _, ok := detail.Header.(NodeInfoRequestHeader); !ok {
			return e(nil, "expected NodeInfoRequestHeader, but %T", detail.Header)
		}

		i, err, _ := sg.Do(HandlerPrefixNodeInfoString, func() (interface{}, error) {
			return getNodeInfo()
		})

		if err != nil {
			return e(err, "")
		}

		var body io.Reader
		dataFormat := quicstream.EmptyDataFormat

		if i != nil {
			j := i.([]byte) //nolint:forcetypeassert //...
			if len(j) > 0 {
				buf := bytes.NewBuffer(j)
				defer buf.Reset()

				body = buf

				dataFormat = quicstream.StreamDataFormat
			}
		}

		if err := quicstream.WriteResponse(ctx, w,
			detail.Encoder,
			quicstream.NewDefaultResponseHeader(true, nil),
			dataFormat,
			0,
			body,
		); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerSendBallots(
	params *isaac.LocalParams,
	votef func(base.BallotSignFact) error,
) quicstream.HeaderHandler {
	return func(ctx context.Context, _ net.Addr, r io.Reader, w io.Writer, detail quicstream.RequestHeadDetail) error {
		e := util.StringErrorFunc("handle new ballot")

		if _, ok := detail.Header.(SendBallotsHeader); !ok {
			return e(nil, "expected SendBallotsHeader, but %T", detail.Header)
		}

		var rbody io.Reader

		switch _, _, i, err := quicstream.HeaderReadBody(ctx, r); {
		case err != nil:
			return e(err, "")
		default:
			rbody = i
		}

		var body []byte

		switch i, err := io.ReadAll(rbody); {
		case err != nil:
			return e(err, "")
		case uint64(len(body)) > params.MaxMessageSize():
			return e(nil, "too big size; >= %d", params.MaxMessageSize())
		default:
			body = i
		}

		switch u, err := detail.Encoder.DecodeSlice(body); {
		case err != nil:
			return e(err, "")
		case len(u) < 1:
			return e(nil, "empty body")
		default:
			for i := range u {
				bl, ok := u[i].(base.BallotSignFact)
				if !ok {
					return e(nil, "expected BallotSignFact, but %T", u[i])
				}

				if err = bl.IsValid(params.NetworkID()); err != nil {
					return e(err, "")
				}

				if err = votef(bl); err != nil {
					return e(err, "")
				}
			}
		}

		if err := quicstream.HeaderWriteHead(ctx, w,
			detail.Encoder,
			quicstream.NewDefaultResponseHeader(true, nil),
		); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func quicstreamHandlerNodeConnInfos(
	f func() ([]isaac.NodeConnInfo, error),
) quicstream.HeaderHandler {
	cache := gcache.New(2).LRU().Build() //nolint:gomnd //...

	var sg singleflight.Group

	return func(ctx context.Context, _ net.Addr, r io.Reader, w io.Writer, detail quicstream.RequestHeadDetail) error {
		i, err, _ := sg.Do("node_conn_infos", func() (interface{}, error) {
			var cis []isaac.NodeConnInfo

			switch j, eerr := cache.Get("node_conn_infos"); {
			case eerr == nil:
				if j != nil {
					cis = j.([]isaac.NodeConnInfo) //nolint:forcetypeassert //...
				}
			default:
				k, eerr := f()
				if eerr != nil {
					return nil, eerr
				}

				_ = cache.SetWithExpire("node_conn_infos", j, time.Second*3) //nolint:gomnd //...

				cis = k
			}

			return cis, nil
		})

		if err != nil {
			return errors.WithStack(err)
		}

		return WriteResponseStreamEncode(
			ctx,
			detail.Encoder,
			w,
			quicstream.NewDefaultResponseHeader(true, nil),
			i,
		)
	}
}

func boolQUICstreamHandler(
	sgkeyf func(quicstream.RequestHeader) string,
	f func(quicstream.RequestHeader, encoder.Encoder) (interface{}, bool, error),
) quicstream.HeaderHandler {
	var sg singleflight.Group

	return func(ctx context.Context, _ net.Addr, r io.Reader, w io.Writer,
		detail quicstream.RequestHeadDetail,
	) error { //nolint:dupl //...
		sgkey := sgkeyf(detail.Header)

		i, err, _ := sg.Do(sgkey, func() (interface{}, error) {
			j, bo, oerr := f(detail.Header, detail.Encoder)
			if oerr != nil {
				return nil, oerr
			}

			return [2]interface{}{j, bo}, oerr
		})
		if err != nil {
			return errors.WithStack(err)
		}

		j := i.([2]interface{}) //nolint:forcetypeassert //...

		return WriteResponseStreamEncode(
			ctx,
			detail.Encoder,
			w,
			quicstream.NewDefaultResponseHeader(j[1].(bool), nil), //nolint:forcetypeassert //...
			j[0],
		)
	}
}

func boolBytesQUICstreamHandler(
	sgkeyf func(quicstream.RequestHeader) string,
	f func(quicstream.RequestHeader, encoder.Encoder) (hint.Hint, []byte, bool, error),
) quicstream.HeaderHandler {
	var sg singleflight.Group

	return func(ctx context.Context, _ net.Addr, r io.Reader, w io.Writer, detail quicstream.RequestHeadDetail) error {
		sgkey := sgkeyf(detail.Header)

		i, err, _ := sg.Do(sgkey, func() (interface{}, error) {
			enchint, b, found, oerr := f(detail.Header, detail.Encoder)
			if oerr != nil {
				return nil, oerr
			}

			return [3]interface{}{enchint, b, found}, nil
		})
		if err != nil {
			return errors.WithStack(err)
		}

		j := i.([3]interface{}) //nolint:forcetypeassert //...

		enchint := j[0].(hint.Hint) //nolint:forcetypeassert //..
		found := j[2].(bool)        //nolint:forcetypeassert //...

		var body io.Reader
		dataFormat := quicstream.EmptyDataFormat

		if j[1] != nil {
			buf := bytes.NewBuffer(j[1].([]byte)) //nolint:forcetypeassert //..
			defer buf.Reset()

			body = buf

			dataFormat = quicstream.StreamDataFormat
		}

		benc := detail.Encoder

		if !enchint.IsEmpty() {
			benc = detail.Encoders.Find(enchint) //nolint:forcetypeassert //...
			if benc == nil {
				return errors.Errorf("find encoder, %q", enchint)
			}
		}

		return quicstream.WriteResponse(ctx, w,
			benc,
			quicstream.NewDefaultResponseHeader(found, nil),
			dataFormat,
			0,
			body,
		)
	}
}

func WriteResponseStreamEncode(
	ctx context.Context,
	enc encoder.Encoder,
	w io.Writer,
	header quicstream.ResponseHeader,
	i interface{},
) error {
	if i == nil {
		return quicstream.WriteResponse(ctx, w,
			enc,
			header,
			quicstream.EmptyDataFormat,
			0,
			nil,
		)
	}

	return util.PipeReadWrite(
		ctx,
		func(ctx context.Context, pr io.Reader) error {
			return quicstream.WriteResponse(ctx, w,
				enc,
				header,
				quicstream.StreamDataFormat,
				0,
				pr,
			)
		},
		func(_ context.Context, pw io.Writer) error {
			return enc.StreamEncoder(pw).Encode(i)
		},
	)
}
