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
	HandlerPrefixRequestProposal        = "request_proposal"
	HandlerPrefixProposal               = "proposal"
	HandlerPrefixLastSuffrageProof      = "last_suffrage_proof"
	HandlerPrefixSuffrageProof          = "suffrage_proof"
	HandlerPrefixLastBlockMap           = "last_blockmap"
	HandlerPrefixBlockMap               = "blockmap"
	HandlerPrefixBlockMapItem           = "blockmap_item"
	HandlerPrefixMemberlist             = "memberlist"
	HandlerPrefixNodeChallenge          = "node_challenge"
	HandlerPrefixSuffrageNodeConnInfo   = "suffrage_node_conninfo"
	HandlerPrefixSyncSourceConnInfo     = "sync_source_conninfo"
	HandlerPrefixOperation              = "operation"
	HandlerPrefixSendOperation          = "send_operation"
	HandlerPrefixState                  = "state"
	HandlerPrefixExistsInStateOperation = "exists_instate_operation"
	HandlerPrefixNodeInfo               = "node_info"
	HandlerPrefixSendBallots            = "send_ballots"
)

func QuicstreamErrorHandler(enc encoder.Encoder) quicstream.ErrorHandler {
	return func(_ net.Addr, _ io.Reader, w io.Writer, err error) error {
		if err = quicstream.WriteResponseBody(w,
			quicstream.NewDefaultResponseHeader(false, err, quicstream.RawContentType), enc, nil); err != nil {
			return errors.WithMessage(err, "failed to response error response")
		}

		return nil
	}
}

func QuicstreamHandlerOperation(
	oppool isaac.NewOperationPool,
) quicstream.HeaderHandler {
	return boolBytesQUICstreamHandler(
		func(header quicstream.Header) string {
			h := header.(OperationRequestHeader) //nolint:forcetypeassert //...

			return HandlerPrefixOperation + h.Operation().String()
		},
		func(header quicstream.Header, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
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

	return func(_ net.Addr, r io.Reader, w io.Writer,
		h quicstream.Header, _ *encoder.Encoders, enc encoder.Encoder,
	) error {
		e := util.StringErrorFunc("failed to handle new operation")

		if _, ok := h.(SendOperationRequestHeader); !ok {
			return e(nil, "expected SendOperationRequestHeader, but %T", h)
		}

		var op base.Operation
		var body []byte

		switch i, err := io.ReadAll(r); {
		case err != nil:
			return e(err, "")
		case uint64(len(body)) > params.MaxMessageSize():
			return e(nil, "too big size; >= %d", params.MaxMessageSize())
		default:
			if err = encoder.Decode(enc, i, &op); err != nil {
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

		added, err := quicstreamHandlerSetOperation(context.Background(), oppool, svvote, op)
		if err == nil && added {
			if broadcast != nil {
				go func() {
					_ = broadcast(op.Hash().String(), body)
				}()
			}
		}

		if err = quicstream.WriteResponseBody(w,
			quicstream.NewDefaultResponseHeader(added, err, quicstream.RawContentType),
			enc,
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
		func(header quicstream.Header) string {
			h := header.(RequestProposalRequestHeader) //nolint:forcetypeassert //...

			return HandlerPrefixRequestProposal + h.Proposer().String()
		},
		func(header quicstream.Header, _ encoder.Encoder) (interface{}, bool, error) {
			h := header.(RequestProposalRequestHeader) //nolint:forcetypeassert //...

			pr, err := getOrCreateProposal(h.point, h.Proposer())

			return pr, pr != nil, err
		},
	)
}

func QuicstreamHandlerProposal(pool isaac.ProposalPool) quicstream.HeaderHandler {
	return boolBytesQUICstreamHandler(
		func(header quicstream.Header) string {
			h := header.(ProposalRequestHeader) //nolint:forcetypeassert //...

			return HandlerPrefixProposal + h.Proposal().String()
		},
		func(header quicstream.Header, _ encoder.Encoder) (enchint hint.Hint, body []byte, found bool, err error) {
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
		func(header quicstream.Header) string {
			h := header.(LastSuffrageProofRequestHeader) //nolint:forcetypeassert //...
			sgkey := HandlerPrefixLastSuffrageProof
			if h.State() != nil {
				sgkey += h.State().String()
			}

			return sgkey
		},
		func(header quicstream.Header, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
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
		func(header quicstream.Header) string {
			h := header.(SuffrageProofRequestHeader) //nolint:forcetypeassert //...

			return HandlerPrefixSuffrageProof + h.Height().String()
		},
		func(header quicstream.Header, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
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
		func(header quicstream.Header) string {
			sgkey := HandlerPrefixLastBlockMap

			h := header.(LastBlockMapRequestHeader) //nolint:forcetypeassert //...

			if h.Manifest() != nil {
				sgkey += h.Manifest().String()
			}

			return sgkey
		},
		func(header quicstream.Header, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
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
		func(header quicstream.Header) string {
			h := header.(BlockMapRequestHeader) //nolint:forcetypeassert //...

			return HandlerPrefixBlockMap + h.Height().String()
		},
		func(header quicstream.Header, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
			h := header.(BlockMapRequestHeader) //nolint:forcetypeassert //...

			enchint, _, body, found, err := blockMapf(h.Height())

			return enchint, body, found, err
		},
	)
}

func QuicstreamHandlerBlockMapItem(
	blockMapItemf func(base.Height, base.BlockMapItemType) (io.ReadCloser, bool, error),
) quicstream.HeaderHandler {
	return func(_ net.Addr, r io.Reader, w io.Writer,
		h quicstream.Header, _ *encoder.Encoders, enc encoder.Encoder,
	) error {
		header, ok := h.(BlockMapItemRequestHeader)
		if !ok {
			return errors.Errorf("expected BlockMapItemRequestHeader, but %T", h)
		}

		itemr, found, err := blockMapItemf(header.Height(), header.Item())
		if err != nil {
			return err
		}

		if itemr != nil {
			defer func() {
				_ = itemr.Close()
			}()
		}

		return quicstream.WriteResponseBody(w,
			quicstream.NewDefaultResponseHeader(found, err, quicstream.RawContentType),
			enc, itemr,
		)
	}
}

func QuicstreamHandlerNodeChallenge(
	local base.LocalNode,
	params base.LocalParams,
) quicstream.HeaderHandler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer,
		h quicstream.Header, _ *encoder.Encoders, enc encoder.Encoder,
	) error {
		e := util.StringErrorFunc("failed to handle NodeChallenge")

		header, ok := h.(NodeChallengeRequestHeader)
		if !ok {
			return errors.Errorf("expected NodeChallengeRequestHeader, but %T", h)
		}

		i, err, _ := sg.Do(HandlerPrefixNodeChallenge+string(header.Input()), func() (interface{}, error) {
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

		if err := quicstream.WriteResponseEncode(w,
			quicstream.NewDefaultResponseHeader(true, nil, quicstream.RawContentType),
			enc, sig,
		); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerSuffrageNodeConnInfo(
	suffrageNodeConnInfof func() ([]isaac.NodeConnInfo, error),
) quicstream.HeaderHandler {
	handler := quicstreamHandlerNodeConnInfos(suffrageNodeConnInfof)

	return func(addr net.Addr, r io.Reader, w io.Writer,
		h quicstream.Header, encs *encoder.Encoders, enc encoder.Encoder,
	) error {
		if err := handler(addr, r, w, h, encs, enc); err != nil {
			return errors.WithMessage(err, "failed to handle SuffrageNodeConnInfo")
		}

		return nil
	}
}

func QuicstreamHandlerSyncSourceConnInfo(
	syncSourceConnInfof func() ([]isaac.NodeConnInfo, error),
) quicstream.HeaderHandler {
	handler := quicstreamHandlerNodeConnInfos(syncSourceConnInfof)

	return func(addr net.Addr, r io.Reader, w io.Writer,
		h quicstream.Header, encs *encoder.Encoders, enc encoder.Encoder,
	) error {
		if err := handler(addr, r, w, h, encs, enc); err != nil {
			return errors.WithMessage(err, "failed to handle SyncSourceConnInfo")
		}

		return nil
	}
}

func QuicstreamHandlerState(
	statef func(string) (enchint hint.Hint, meta, body []byte, found bool, err error),
) quicstream.HeaderHandler {
	return boolBytesQUICstreamHandler(
		func(header quicstream.Header) string {
			h := header.(StateRequestHeader) //nolint:forcetypeassert //..

			return HandlerPrefixState + h.Key()
		},
		func(header quicstream.Header, _ encoder.Encoder) (hint.Hint, []byte, bool, error) {
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

	return func(_ net.Addr, r io.Reader, w io.Writer,
		h quicstream.Header, _ *encoder.Encoders, enc encoder.Encoder,
	) error {
		e := util.StringErrorFunc("failed to handle exists instate operation")

		header, ok := h.(ExistsInStateOperationRequestHeader)
		if !ok {
			return errors.Errorf("expected ExistsInStateOperationRequestHeader, but %T", h)
		}

		i, err, _ := sg.Do(HandlerPrefixExistsInStateOperation+header.FactHash().String(), func() (interface{}, error) {
			return existsInStateOperationf(header.FactHash())
		})

		if err != nil {
			return e(err, "")
		}

		if err := quicstream.WriteResponseBytes(w, //nolint:forcetypeassert //...
			quicstream.NewDefaultResponseHeader(i.(bool), nil, quicstream.RawContentType), enc, nil); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerNodeInfo(
	getNodeInfo func() ([]byte, error),
) quicstream.HeaderHandler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer,
		h quicstream.Header, _ *encoder.Encoders, enc encoder.Encoder,
	) error {
		e := util.StringErrorFunc("failed to handle node info")

		if _, ok := h.(NodeInfoRequestHeader); !ok {
			return e(nil, "expected NodeInfoRequestHeader, but %T", h)
		}

		i, err, _ := sg.Do(HandlerPrefixNodeInfo, func() (interface{}, error) {
			return getNodeInfo()
		})

		if err != nil {
			return e(err, "")
		}

		var b io.Reader

		if i != nil {
			j := i.([]byte) //nolint:forcetypeassert //...
			if len(j) > 0 {
				buf := bytes.NewBuffer(j)
				defer buf.Reset()

				b = buf
			}
		}

		if err := quicstream.WriteResponseBody(w,
			quicstream.NewDefaultResponseHeader(true, nil, quicstream.HinterContentType), enc, b); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerSendBallots(
	params *isaac.LocalParams,
	votef func(base.BallotSignFact) error,
) quicstream.HeaderHandler {
	return func(_ net.Addr, r io.Reader, w io.Writer,
		h quicstream.Header, _ *encoder.Encoders, enc encoder.Encoder,
	) error {
		e := util.StringErrorFunc("failed to handle new ballot")

		if _, ok := h.(SendBallotsHeader); !ok {
			return e(nil, "expected SendBallotsHeader, but %T", h)
		}

		var body []byte

		switch i, err := io.ReadAll(r); {
		case err != nil:
			return e(err, "")
		case uint64(len(body)) > params.MaxMessageSize():
			return e(nil, "too big size; >= %d", params.MaxMessageSize())
		default:
			body = i
		}

		u, err := enc.DecodeSlice(body)

		switch {
		case err != nil:
			return e(err, "")
		case len(u) < 1:
			return e(nil, "empty body")
		}

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

		if err = quicstream.WriteResponseBody(w,
			quicstream.NewDefaultResponseHeader(true, nil, quicstream.RawContentType), enc, nil); err != nil {
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

	return func(_ net.Addr, r io.Reader, w io.Writer,
		h quicstream.Header, _ *encoder.Encoders, enc encoder.Encoder,
	) error {
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

		var cis []isaac.NodeConnInfo

		if i != nil {
			cis = i.([]isaac.NodeConnInfo) //nolint:forcetypeassert //...
		}

		return quicstream.WriteResponseEncode(w,
			quicstream.NewDefaultResponseHeader(true, nil, quicstream.RawContentType), enc, cis)
	}
}

func boolQUICstreamHandler(
	sgkeyf func(quicstream.Header) string,
	f func(quicstream.Header, encoder.Encoder) (interface{}, bool, error),
) quicstream.HeaderHandler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer,
		h quicstream.Header, encs *encoder.Encoders, enc encoder.Encoder,
	) error { //nolint:dupl //...
		sgkey := sgkeyf(h)

		i, err, _ := sg.Do(sgkey, func() (interface{}, error) {
			j, bo, oerr := f(h, enc)
			if oerr != nil {
				return nil, oerr
			}

			return [2]interface{}{j, bo}, oerr
		})
		if err != nil {
			return errors.WithStack(err)
		}

		j := i.([2]interface{}) //nolint:forcetypeassert //...

		return quicstream.WriteResponseEncode(w,
			quicstream.NewDefaultResponseHeader(
				j[1].(bool), nil, quicstream.RawContentType), //nolint:forcetypeassert //...
			enc, j[0])
	}
}

func boolBytesQUICstreamHandler(
	sgkeyf func(quicstream.Header) string,
	f func(quicstream.Header, encoder.Encoder) (hint.Hint, []byte, bool, error),
) quicstream.HeaderHandler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer,
		h quicstream.Header, encs *encoder.Encoders, enc encoder.Encoder,
	) error { //nolint:dupl //...
		sgkey := sgkeyf(h)

		i, err, _ := sg.Do(sgkey, func() (interface{}, error) {
			enchint, b, found, oerr := f(h, enc)
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
		body := j[1].([]byte)       //nolint:forcetypeassert //..
		found := j[2].(bool)        //nolint:forcetypeassert //...

		benc := enc
		contentType := quicstream.RawContentType

		if !enchint.IsEmpty() {
			benc = encs.Find(enchint) //nolint:forcetypeassert //...
			if benc == nil {
				return errors.Errorf("failed to find encoder, %q", enchint)
			}

			contentType = quicstream.HinterContentType
		}

		return quicstream.WriteResponseBytes(w,
			quicstream.NewDefaultResponseHeader(found, nil, contentType),
			benc,
			body,
		)
	}
}
