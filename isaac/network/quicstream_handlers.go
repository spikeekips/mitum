package isaacnetwork

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"reflect"
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

// FIXME rate limit

func QuicstreamErrorHandler(enc encoder.Encoder) quicstream.ErrorHandler {
	return func(_ net.Addr, _ io.Reader, w io.Writer, err error) error {
		if err = WriteResponse(w, NewResponseHeader(false, err), nil, enc); err != nil {
			return errors.WithMessage(err, "failed to response error response")
		}

		return nil
	}
}

func QuicstreamHandlerOperation(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	oppool isaac.NewOperationPool,
) quicstream.Handler {
	return boolBytesQUICstreamHandler(encs, idleTimeout, OperationRequestHeader{},
		func(header isaac.NetworkHeader) string {
			h := header.(OperationRequestHeader) //nolint:forcetypeassert //...

			return HandlerPrefixOperation + h.Operation().String()
		},
		func(header isaac.NetworkHeader) (enchint hint.Hint, body []byte, found bool, err error) {
			h := header.(OperationRequestHeader) //nolint:forcetypeassert //...

			enchint, _, body, found, err = oppool.NewOperationBytes(context.Background(), h.Operation())

			return enchint, body, found, err
		},
	)
}

func QuicstreamHandlerSendOperation(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	params *isaac.LocalParams,
	oppool isaac.NewOperationPool,
	existsInStateOperationf func(util.Hash) (bool, error),
	filterSendOperationf func(base.Operation) (bool, error),
	vote isaac.SuffrageVoteFunc,
) quicstream.Handler {
	filterNewOperation := func(op base.Operation) error {
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

	return func(_ net.Addr, r io.Reader, w io.Writer) error {
		e := util.StringErrorFunc("failed to handle new operation")

		var header SendOperationRequestHeader

		enc, err := quicstreamPreHandle(encs, idleTimeout, r, &header)
		if err != nil {
			return e(err, "")
		}

		var op base.Operation

		switch body, eerr := io.ReadAll(r); {
		case eerr != nil:
			return e(eerr, "")
		case uint64(len(body)) > params.MaxOperationSize():
			return e(nil, "too big size; >= %d", params.MaxOperationSize())
		default:
			if err = encoder.Decode(enc, body, &op); err != nil {
				return e(err, "")
			}

			if op == nil {
				return e(nil, "empty body")
			}

			if err = op.IsValid(params.NetworkID()); err != nil {
				return e(err, "")
			}
		}

		if err = filterNewOperation(op); err != nil {
			return e(err, "")
		}

		added, err := quicstreamHandlerSetOperation(context.Background(), oppool, vote, op)

		if err = WriteResponse(w, NewResponseHeader(added, err), nil, enc); err != nil {
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

func QuicstreamHandlerRequestProposal(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	local base.LocalNode,
	pool isaac.ProposalPool,
	proposalMaker *isaac.ProposalMaker,
	lastBlockMapf func() (base.BlockMap, bool, error),
) quicstream.Handler {
	getOrCreateProposal := func(
		point base.Point,
		proposer base.Address,
	) (base.ProposalSignFact, error) {
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

		switch pr, found, err := pool.ProposalByPoint(point, proposer); {
		case err != nil:
			return nil, err
		case found:
			return pr, nil
		}

		if !proposer.Equal(local.Address()) {
			return proposalMaker.Empty(context.Background(), point)
		}

		return proposalMaker.New(context.Background(), point)
	}

	return boolQUICstreamHandler(encs, idleTimeout, RequestProposalRequestHeader{},
		func(header isaac.NetworkHeader) string {
			h := header.(RequestProposalRequestHeader) //nolint:forcetypeassert //...

			return HandlerPrefixRequestProposal + h.Proposer().String()
		},
		func(header isaac.NetworkHeader) (interface{}, bool, error) {
			h := header.(RequestProposalRequestHeader) //nolint:forcetypeassert //...

			pr, err := getOrCreateProposal(h.point, h.Proposer())

			return pr, pr != nil, err
		},
	)
}

func QuicstreamHandlerProposal(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	pool isaac.ProposalPool,
) quicstream.Handler {
	return boolBytesQUICstreamHandler(encs, idleTimeout, ProposalRequestHeader{},
		func(header isaac.NetworkHeader) string {
			h := header.(ProposalRequestHeader) //nolint:forcetypeassert //...

			return HandlerPrefixProposal + h.Proposal().String()
		},
		func(header isaac.NetworkHeader) (enchint hint.Hint, body []byte, found bool, err error) {
			h := header.(ProposalRequestHeader) //nolint:forcetypeassert //...

			enchint, _, body, found, err = pool.ProposalBytes(h.Proposal())

			return enchint, body, found, err
		},
	)
}

func QuicstreamHandlerLastSuffrageProof(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	lastSuffrageProoff func(suffragestate util.Hash) (hint.Hint, []byte, []byte, bool, error),
) quicstream.Handler {
	return boolBytesQUICstreamHandler(encs, idleTimeout, LastSuffrageProofRequestHeader{},
		func(header isaac.NetworkHeader) string {
			h := header.(LastSuffrageProofRequestHeader) //nolint:forcetypeassert //...
			sgkey := HandlerPrefixLastSuffrageProof
			if h.State() != nil {
				sgkey += h.State().String()
			}

			return sgkey
		},
		func(header isaac.NetworkHeader) (hint.Hint, []byte, bool, error) {
			h := header.(LastSuffrageProofRequestHeader) //nolint:forcetypeassert //...

			enchint, _, body, found, err := lastSuffrageProoff(h.State())

			return enchint, body, found, err
		},
	)
}

func QuicstreamHandlerSuffrageProof(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	suffrageProoff func(base.Height) (hint.Hint, []byte, []byte, bool, error),
) quicstream.Handler {
	return boolBytesQUICstreamHandler(encs, idleTimeout, SuffrageProofRequestHeader{},
		func(header isaac.NetworkHeader) string {
			h := header.(SuffrageProofRequestHeader) //nolint:forcetypeassert //...

			return HandlerPrefixSuffrageProof + h.Height().String()
		},
		func(header isaac.NetworkHeader) (hint.Hint, []byte, bool, error) {
			h := header.(SuffrageProofRequestHeader) //nolint:forcetypeassert //...

			enchint, _, body, found, err := suffrageProoff(h.Height())

			return enchint, body, found, err
		},
	)
}

// LastBlockMap responds the last BlockMap to client; if there is no BlockMap,
// it returns nil BlockMap and not updated without error.
func QuicstreamHandlerLastBlockMap(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	lastBlockMapf func(util.Hash) (hint.Hint, []byte, []byte, bool, error),
) quicstream.Handler {
	return boolBytesQUICstreamHandler(encs, idleTimeout, LastBlockMapRequestHeader{},
		func(header isaac.NetworkHeader) string {
			sgkey := HandlerPrefixLastBlockMap

			h := header.(LastBlockMapRequestHeader) //nolint:forcetypeassert //...

			if h.Manifest() != nil {
				sgkey += h.Manifest().String()
			}

			return sgkey
		},
		func(header isaac.NetworkHeader) (hint.Hint, []byte, bool, error) {
			h := header.(LastBlockMapRequestHeader) //nolint:forcetypeassert //...

			enchint, _, body, found, err := lastBlockMapf(h.Manifest())

			return enchint, body, found, err
		},
	)
}

func QuicstreamHandlerBlockMap(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	blockMapf func(base.Height) (hint.Hint, []byte, []byte, bool, error),
) quicstream.Handler {
	return boolBytesQUICstreamHandler(encs, idleTimeout, BlockMapRequestHeader{},
		func(header isaac.NetworkHeader) string {
			h := header.(BlockMapRequestHeader) //nolint:forcetypeassert //...

			return HandlerPrefixBlockMap + h.Height().String()
		},
		func(header isaac.NetworkHeader) (hint.Hint, []byte, bool, error) {
			h := header.(BlockMapRequestHeader) //nolint:forcetypeassert //...

			enchint, _, body, found, err := blockMapf(h.Height())

			return enchint, body, found, err
		},
	)
}

func QuicstreamHandlerBlockMapItem(
	encs *encoder.Encoders,
	idleTimeout,
	writeTimeout time.Duration,
	blockMapItemf func(base.Height, base.BlockMapItemType) (io.ReadCloser, bool, error),
) quicstream.Handler {
	return func(_ net.Addr, r io.Reader, w io.Writer) error {
		e := util.StringErrorFunc("failed to handle request BlockMapItem")

		var header BlockMapItemRequestHeader

		enc, err := quicstreamPreHandle(encs, idleTimeout, r, &header)
		if err != nil {
			return e(err, "")
		}

		itemr, found, err := blockMapItemf(header.Height(), header.Item())
		if itemr != nil {
			defer func() {
				_ = itemr.Close()
			}()
		}

		if err = WriteResponse(w, NewResponseHeader(found, err), nil, enc); err != nil {
			return e(err, "")
		}

		if itemr == nil {
			return nil
		}

		wch := make(chan error)

		go func() {
			_, err := io.Copy(w, itemr)
			wch <- err
		}()

		select {
		case <-time.After(writeTimeout):
			return errors.Errorf("timeout")
		case err := <-wch:
			return err
		}
	}
}

func QuicstreamHandlerNodeChallenge(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	local base.LocalNode,
	params base.LocalParams,
) quicstream.Handler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error {
		e := util.StringErrorFunc("failed to handle NodeChallenge")

		var header NodeChallengeRequestHeader

		enc, err := quicstreamPreHandle(encs, idleTimeout, r, &header)
		if err != nil {
			return e(err, "")
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

		if err := WriteResponse(w, NewResponseHeader(true, nil), sig, enc); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerSuffrageNodeConnInfo(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	suffrageNodeConnInfof func() ([]isaac.NodeConnInfo, error),
) quicstream.Handler {
	handler := quicstreamHandlerNodeConnInfos(
		encs, idleTimeout, SuffrageNodeConnInfoRequestHeader{}, suffrageNodeConnInfof)

	return func(addr net.Addr, r io.Reader, w io.Writer) error {
		if err := handler(addr, r, w); err != nil {
			return errors.WithMessage(err, "failed to handle SuffrageNodeConnInfo")
		}

		return nil
	}
}

func QuicstreamHandlerSyncSourceConnInfo(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	syncSourceConnInfof func() ([]isaac.NodeConnInfo, error),
) quicstream.Handler {
	handler := quicstreamHandlerNodeConnInfos(encs, idleTimeout, SyncSourceConnInfoRequestHeader{}, syncSourceConnInfof)

	return func(addr net.Addr, r io.Reader, w io.Writer) error {
		if err := handler(addr, r, w); err != nil {
			return errors.WithMessage(err, "failed to handle SyncSourceConnInfo")
		}

		return nil
	}
}

func QuicstreamHandlerState(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	statef func(string) (enchint hint.Hint, meta, body []byte, found bool, err error),
) quicstream.Handler {
	return boolBytesQUICstreamHandler(encs, idleTimeout, StateRequestHeader{},
		func(header isaac.NetworkHeader) string {
			h := header.(StateRequestHeader) //nolint:forcetypeassert //..

			return HandlerPrefixState + h.Key()
		},
		func(header isaac.NetworkHeader) (hint.Hint, []byte, bool, error) {
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
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	existsInStateOperationf func(util.Hash) (bool, error),
) quicstream.Handler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error {
		e := util.StringErrorFunc("failed to handle exists instate operation")

		var header ExistsInStateOperationRequestHeader

		enc, err := quicstreamPreHandle(encs, idleTimeout, r, &header)
		if err != nil {
			return e(err, "")
		}

		i, err, _ := sg.Do(HandlerPrefixExistsInStateOperation+header.FactHash().String(), func() (interface{}, error) {
			return existsInStateOperationf(header.FactHash())
		})

		if err != nil {
			return e(err, "")
		}

		if err := WriteResponse(w, //nolint:forcetypeassert //...
			NewResponseHeader(i.(bool), nil), nil, enc); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerNodeInfo(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	getNodeInfo func() ([]byte, error),
) quicstream.Handler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error {
		e := util.StringErrorFunc("failed to handle node info")

		var header NodeInfoRequestHeader

		enc, err := quicstreamPreHandle(encs, idleTimeout, r, &header)
		if err != nil {
			return e(err, "")
		}

		i, err, _ := sg.Do(HandlerPrefixNodeInfo, func() (interface{}, error) {
			return getNodeInfo()
		})

		if err != nil {
			return e(err, "")
		}

		var b json.RawMessage

		if i != nil {
			b = json.RawMessage(i.([]byte)) //nolint:forcetypeassert //...
		}

		if err := WriteResponse(w, NewResponseHeader(true, nil), b, enc); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func quicstreamPreHandle(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	r io.Reader,
	header interface{},
) (encoder.Encoder, error) {
	ctx, cancel := context.WithTimeout(context.Background(), idleTimeout)
	defer cancel()

	enc, b, err := HandlerReadHead(ctx, encs, r)
	if err != nil {
		return nil, err
	}

	if header != nil {
		if err = encoder.Decode(enc, b, header); err != nil {
			return nil, err
		}

		v := reflect.ValueOf(header).Elem().Interface()
		if i, ok := v.(util.IsValider); ok {
			if err = i.IsValid(nil); err != nil {
				return nil, err
			}
		}
	}

	return enc, nil
}

func quicstreamHandlerNodeConnInfos(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	header isaac.NetworkHeader,
	f func() ([]isaac.NodeConnInfo, error),
) quicstream.Handler {
	cache := gcache.New(2).LRU().Build() //nolint:gomnd //...

	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error {
		h := reflect.Zero(reflect.TypeOf(header)).Interface()

		enc, err := quicstreamPreHandle(encs, idleTimeout, r, &h)
		if err != nil {
			return err
		}

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

		return WriteResponse(w, NewResponseHeader(true, nil), cis, enc)
	}
}

func boolQUICstreamHandler(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	header isaac.NetworkHeader,
	sgkeyf func(isaac.NetworkHeader) string,
	f func(isaac.NetworkHeader) (interface{}, bool, error),
) quicstream.Handler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error { //nolint:dupl //...
		h := reflect.Zero(reflect.TypeOf(header)). //nolint:forcetypeassert //...
								Interface().(isaac.NetworkHeader)

		enc, err := quicstreamPreHandle(encs, idleTimeout, r, &h)
		if err != nil {
			return err
		}

		sgkey := sgkeyf(h)

		i, err, _ := sg.Do(sgkey, func() (interface{}, error) {
			j, bo, oerr := f(h)
			if oerr != nil {
				return nil, oerr
			}

			return [2]interface{}{j, bo}, oerr
		})
		if err != nil {
			return errors.WithStack(err)
		}

		j := i.([2]interface{}) //nolint:forcetypeassert //...

		return WriteResponse(w, NewResponseHeader( //nolint:forcetypeassert //...
			j[1].(bool), nil), j[0], enc) //nolint:forcetypeassert //...
	}
}

func boolBytesQUICstreamHandler(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	header isaac.NetworkHeader,
	sgkeyf func(isaac.NetworkHeader) string,
	f func(isaac.NetworkHeader) (hint.Hint, []byte, bool, error),
) quicstream.Handler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error { //nolint:dupl //...
		h := reflect.Zero(reflect.TypeOf(header)). //nolint:forcetypeassert //...
								Interface().(isaac.NetworkHeader)

		enc, err := quicstreamPreHandle(encs, idleTimeout, r, &h)
		if err != nil {
			return err
		}

		sgkey := sgkeyf(h)

		i, err, _ := sg.Do(sgkey, func() (interface{}, error) {
			ht, b, found, oerr := f(h)
			if oerr != nil {
				return nil, oerr
			}

			return [3]interface{}{ht, b, found}, oerr
		})
		if err != nil {
			return errors.WithStack(err)
		}

		var body []byte

		j := i.([3]interface{}) //nolint:forcetypeassert //...

		found := j[2].(bool) //nolint:forcetypeassert //...
		if found {
			enc = encs.Find(j[0].(hint.Hint)) //nolint:forcetypeassert //...
			if enc == nil {
				return errors.Errorf("failed to find encoder")
			}

			if j[1] != nil {
				body = j[1].([]byte) //nolint:forcetypeassert //..
			}
		}

		return WriteResponseBytes(w, NewResponseHeader(found, nil), body, enc)
	}
}
