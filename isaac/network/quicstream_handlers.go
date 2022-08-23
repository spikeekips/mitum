package isaacnetwork

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"golang.org/x/sync/singleflight"
)

// FIXME rate limit

func QuicstreamErrorHandler(enc encoder.Encoder) quicstream.ErrorHandler {
	return func(_ net.Addr, _ io.Reader, w io.Writer, err error) error {
		if err = Response(w, NewResponseHeader(false, err), nil, enc); err != nil {
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
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error { //nolint:dupl //...
		e := util.StringErrorFunc("failed to handle request last BlockMap")

		enc, hb, err := quicstreamPreHandle(encs, idleTimeout, r)
		if err != nil {
			return e(err, "")
		}

		var header OperationRequestHeader
		if err = encoder.Decode(enc, hb, &header); err != nil {
			return e(err, "")
		}

		if err = header.IsValid(nil); err != nil {
			return e(err, "")
		}

		i, err, _ := sg.Do(HandlerPrefixOperation+header.Operation().String(), func() (interface{}, error) {
			op, found, oerr := oppool.NewOperation(context.Background(), header.Operation())

			return [2]interface{}{op, found}, oerr
		})

		if err != nil {
			return e(err, "")
		}

		j := i.([2]interface{}) //nolint:forcetypeassert //...

		var op base.Operation

		if j[0] != nil {
			op = j[0].(base.Operation) //nolint:forcetypeassert //...
		}

		found := j[1].(bool) //nolint:forcetypeassert //...

		if err := Response(w, NewResponseHeader(found, nil), op, enc); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerSendOperation(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	params *isaac.LocalParams,
	oppool isaac.NewOperationPool,
	existsInStateOperationf func(util.Hash) (bool, error),
	filterSendOperationf func(base.Operation) (bool, error),
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

		var enc encoder.Encoder
		var hb []byte

		switch i, j, err := quicstreamPreHandle(encs, idleTimeout, r); {
		case err != nil:
			return e(err, "")
		default:
			enc = i
			hb = j
		}

		var header SendOperationRequestHeader

		switch err := encoder.Decode(enc, hb, &header); {
		case err != nil:
			return e(err, "")
		default:
			if err := header.IsValid(nil); err != nil {
				return e(err, "")
			}
		}

		var op base.Operation

		switch body, err := io.ReadAll(r); {
		case err != nil:
			return e(err, "")
		case uint64(len(body)) > params.MaxOperationSize():
			return e(nil, "too big size; >= %d", params.MaxOperationSize())
		default:
			if err := encoder.Decode(enc, body, &op); err != nil {
				return e(err, "")
			}

			if op == nil {
				return e(nil, "empty body")
			}

			if err := op.IsValid(params.NetworkID()); err != nil {
				return e(err, "")
			}
		}

		if err := filterNewOperation(op); err != nil {
			return e(err, "")
		}

		added, err := oppool.SetNewOperation(context.Background(), op)

		if err = Response(w, NewResponseHeader(added, err), nil, enc); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerRequestProposal(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	local base.LocalNode,
	pool isaac.ProposalPool,
	proposalMaker *isaac.ProposalMaker,
	lastBlockMapf func(util.Hash) (base.BlockMap, bool, error),
) quicstream.Handler {
	getOrCreateProposal := func(
		point base.Point,
		proposer base.Address,
	) (base.ProposalSignedFact, error) {
		if lastBlockMapf != nil {
			switch m, found, err := lastBlockMapf(nil); {
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

	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error {
		e := util.StringErrorFunc("failed to handle request proposal")

		enc, hb, err := quicstreamPreHandle(encs, idleTimeout, r)
		if err != nil {
			return e(err, "")
		}

		var header RequestProposalRequestHeader
		if err = encoder.Decode(enc, hb, &header); err != nil {
			return e(err, "")
		}

		if err = header.IsValid(nil); err != nil {
			return e(err, "")
		}

		i, err, _ := sg.Do(HandlerPrefixRequestProposal+header.Proposer().String(), func() (interface{}, error) {
			return getOrCreateProposal(header.point, header.Proposer())
		})

		if err != nil {
			return e(err, "")
		}

		pr := i.(base.ProposalSignedFact) //nolint:forcetypeassert //...

		if err := Response(w, NewResponseHeader(pr != nil, nil), pr, enc); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerProposal(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	pool isaac.ProposalPool,
) quicstream.Handler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error {
		e := util.StringErrorFunc("failed to handle get proposal")

		enc, hb, err := quicstreamPreHandle(encs, idleTimeout, r)
		if err != nil {
			return e(err, "")
		}

		var header ProposalRequestHeader
		if err = encoder.Decode(enc, hb, &header); err != nil {
			return e(err, "")
		}

		if err = header.IsValid(nil); err != nil {
			return e(err, "")
		}

		i, err, _ := sg.Do(HandlerPrefixProposal+header.Proposal().String(), func() (interface{}, error) {
			pr, found, oerr := pool.Proposal(header.Proposal())

			return [2]interface{}{pr, found}, oerr
		})

		if err != nil {
			return e(err, "")
		}

		j := i.([2]interface{}) //nolint:forcetypeassert //...

		var pr base.ProposalSignedFact

		if j[0] != nil {
			pr = j[0].(base.ProposalSignedFact) //nolint:forcetypeassert //...
		}

		found := j[1].(bool) //nolint:forcetypeassert //...

		if err := Response(w, NewResponseHeader(found, nil), pr, enc); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerLastSuffrageProof(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	lastSuffrageProoff func(suffragestate util.Hash) (base.SuffrageProof, bool, error),
) quicstream.Handler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error {
		e := util.StringErrorFunc("failed to handle get last suffrage proof")

		enc, hb, err := quicstreamPreHandle(encs, idleTimeout, r)
		if err != nil {
			return e(err, "")
		}

		var header LastSuffrageProofRequestHeader
		if err = encoder.Decode(enc, hb, &header); err != nil {
			return e(err, "")
		}

		sgkey := HandlerPrefixLastSuffrageProof
		if header.State() != nil {
			sgkey += header.State().String()
		}

		i, err, _ := sg.Do(sgkey, func() (interface{}, error) {
			proof, updated, oerr := lastSuffrageProoff(header.State())

			return [2]interface{}{proof, updated}, oerr
		})

		if err != nil {
			return e(err, "")
		}

		j := i.([2]interface{}) //nolint:forcetypeassert //...

		var proof base.SuffrageProof

		if j[0] != nil {
			proof = j[0].(base.SuffrageProof) //nolint:forcetypeassert //...
		}

		updated := j[1].(bool) //nolint:forcetypeassert //...

		if err := Response(w, NewResponseHeader(updated, nil), proof, enc); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerSuffrageProof(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	suffrageProoff func(base.Height) (base.SuffrageProof, bool, error),
) quicstream.Handler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error {
		e := util.StringErrorFunc("failed to handle get suffrage proof")

		enc, hb, err := quicstreamPreHandle(encs, idleTimeout, r)
		if err != nil {
			return e(err, "")
		}

		var header SuffrageProofRequestHeader
		if err = encoder.Decode(enc, hb, &header); err != nil {
			return e(err, "")
		}

		i, err, _ := sg.Do(HandlerPrefixSuffrageProof+header.Height().String(), func() (interface{}, error) {
			proof, found, oerr := suffrageProoff(header.Height())

			return [2]interface{}{proof, found}, oerr
		})

		if err != nil {
			return e(err, "")
		}

		j := i.([2]interface{}) //nolint:forcetypeassert //...

		var proof base.SuffrageProof

		if j[0] != nil {
			proof = j[0].(base.SuffrageProof) //nolint:forcetypeassert //...
		}

		found := j[1].(bool) //nolint:forcetypeassert //...

		if err := Response(w, NewResponseHeader(found, nil), proof, enc); err != nil {
			return e(err, "")
		}

		return nil
	}
}

// LastBlockMap responds the last BlockMap to client; if there is no BlockMap,
// it returns nil BlockMap and not updated without error.
func QuicstreamHandlerLastBlockMap(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	lastBlockMapf func(util.Hash) (base.BlockMap, bool, error),
) quicstream.Handler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error { //nolint:dupl //...
		e := util.StringErrorFunc("failed to handle request last BlockMap")

		enc, hb, err := quicstreamPreHandle(encs, idleTimeout, r)
		if err != nil {
			return e(err, "")
		}

		var header LastBlockMapRequestHeader
		if err = encoder.Decode(enc, hb, &header); err != nil {
			return e(err, "")
		}

		if err = header.IsValid(nil); err != nil {
			return e(err, "")
		}

		sgkey := HandlerPrefixLastBlockMap

		if header.Manifest() != nil {
			sgkey += header.Manifest().String()
		}

		i, err, _ := sg.Do(sgkey, func() (interface{}, error) {
			m, updated, oerr := lastBlockMapf(header.Manifest())

			return [2]interface{}{m, updated}, oerr
		})

		if err != nil {
			return e(err, "")
		}

		j := i.([2]interface{}) //nolint:forcetypeassert //...

		var m base.BlockMap

		if j[0] != nil {
			m = j[0].(base.BlockMap) //nolint:forcetypeassert //...
		}

		updated := j[1].(bool) //nolint:forcetypeassert //...

		if err := Response(w, NewResponseHeader(updated, nil), m, enc); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerBlockMap(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	blockMapf func(base.Height) (base.BlockMap, bool, error),
) quicstream.Handler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error { //nolint:dupl //...
		e := util.StringErrorFunc("failed to handle request BlockMap")

		enc, hb, err := quicstreamPreHandle(encs, idleTimeout, r)
		if err != nil {
			return e(err, "")
		}

		var header BlockMapRequestHeader
		if err = encoder.Decode(enc, hb, &header); err != nil {
			return e(err, "")
		}

		if err = header.IsValid(nil); err != nil {
			return e(err, "")
		}

		i, err, _ := sg.Do(HandlerPrefixBlockMap+header.Height().String(), func() (interface{}, error) {
			m, found, oerr := blockMapf(header.Height())

			return [2]interface{}{m, found}, oerr
		})

		if err != nil {
			return e(err, "")
		}

		j := i.([2]interface{}) //nolint:forcetypeassert //...

		var m base.BlockMap

		if j[0] != nil {
			m = j[0].(base.BlockMap) //nolint:forcetypeassert //...
		}

		found := j[1].(bool) //nolint:forcetypeassert //...

		if err := Response(w, NewResponseHeader(found, nil), m, enc); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerBlockMapItem(
	encs *encoder.Encoders,
	idleTimeout,
	writeTimeout time.Duration,
	blockMapItemf func(base.Height, base.BlockMapItemType) (io.ReadCloser, bool, error),
) quicstream.Handler {
	return func(_ net.Addr, r io.Reader, w io.Writer) error {
		e := util.StringErrorFunc("failed to handle request BlockMapItem")

		enc, hb, err := quicstreamPreHandle(encs, idleTimeout, r)
		if err != nil {
			return e(err, "")
		}

		var header BlockMapItemRequestHeader
		if err = encoder.Decode(enc, hb, &header); err != nil {
			return e(err, "")
		}

		if err = header.IsValid(nil); err != nil {
			return e(err, "")
		}

		itemr, found, err := blockMapItemf(header.Height(), header.Item())
		if itemr != nil {
			defer func() {
				_ = itemr.Close()
			}()
		}

		if err = Response(w, NewResponseHeader(found, err), nil, enc); err != nil {
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

		enc, hb, err := quicstreamPreHandle(encs, idleTimeout, r)
		if err != nil {
			return e(err, "")
		}

		var header NodeChallengeRequestHeader
		if err = encoder.Decode(enc, hb, &header); err != nil {
			return e(err, "")
		}

		if err = header.IsValid(nil); err != nil {
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

		if err := Response(w, NewResponseHeader(true, nil), sig, enc); err != nil {
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
	var header SuffrageNodeConnInfoRequestHeader

	handler := quicstreamHandlerNodeConnInfos(encs, idleTimeout, header, suffrageNodeConnInfof)

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
	var header SyncSourceConnInfoRequestHeader

	handler := quicstreamHandlerNodeConnInfos(encs, idleTimeout, header, syncSourceConnInfof)

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
	statef func(string) (base.State, bool, error),
) quicstream.Handler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error {
		e := util.StringErrorFunc("failed to handle get state by key")

		enc, hb, err := quicstreamPreHandle(encs, idleTimeout, r)
		if err != nil {
			return e(err, "")
		}

		var header StateRequestHeader
		if err = encoder.Decode(enc, hb, &header); err != nil {
			return e(err, "")
		}

		if err = header.IsValid(nil); err != nil {
			return e(err, "")
		}

		i, err, _ := sg.Do(HandlerPrefixState+header.Key(), func() (interface{}, error) {
			st, found, oerr := statef(header.Key())

			return [2]interface{}{st, found}, oerr
		})

		if err != nil {
			return e(err, "")
		}

		j := i.([2]interface{}) //nolint:forcetypeassert //...

		var st base.State

		if j[0] != nil {
			st = j[0].(base.State) //nolint:forcetypeassert //...
		}

		found := j[1].(bool) //nolint:forcetypeassert //...

		res := NewResponseHeader(found, nil)

		if found && header.Hash() != nil && st.Hash().Equal(header.Hash()) {
			if err := Response(w, res, nil, enc); err != nil {
				return e(err, "")
			}

			return nil
		}

		if err := Response(w, res, st, enc); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func QuicstreamHandlerExistsInStateOperation(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	existsInStateOperationf func(util.Hash) (bool, error),
) quicstream.Handler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error {
		e := util.StringErrorFunc("failed to handle exists instate operation")

		enc, hb, err := quicstreamPreHandle(encs, idleTimeout, r)
		if err != nil {
			return e(err, "")
		}

		var header ExistsInStateOperationRequestHeader
		if err = encoder.Decode(enc, hb, &header); err != nil {
			return e(err, "")
		}

		if err = header.IsValid(nil); err != nil {
			return e(err, "")
		}

		i, err, _ := sg.Do(HandlerPrefixExistsInStateOperation+header.FactHash().String(), func() (interface{}, error) {
			return existsInStateOperationf(header.FactHash())
		})

		if err != nil {
			return e(err, "")
		}

		if err := Response(w, //nolint:forcetypeassert //...
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

		enc, hb, err := quicstreamPreHandle(encs, idleTimeout, r)
		if err != nil {
			return e(err, "")
		}

		var header NodeInfoRequestHeader
		if err = encoder.Decode(enc, hb, &header); err != nil {
			return e(err, "")
		}

		if err = header.IsValid(nil); err != nil {
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

		if err := Response(w, NewResponseHeader(true, nil), b, enc); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func quicstreamPreHandle(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	r io.Reader,
) (encoder.Encoder, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), idleTimeout)
	defer cancel()

	return HandlerReadHead(ctx, encs, r)
}

func quicstreamHandlerNodeConnInfos(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	header isaac.NetworkHeader,
	f func() ([]isaac.NodeConnInfo, error),
) quicstream.Handler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer) error {
		enc, hb, err := quicstreamPreHandle(encs, idleTimeout, r)
		if err != nil {
			return err
		}

		if err = encoder.Decode(enc, hb, &header); err != nil {
			return err
		}

		if err = header.IsValid(nil); err != nil {
			return err
		}

		i, err, _ := sg.Do("node_conn_infos", func() (interface{}, error) {
			return f()
		})

		if err != nil {
			return errors.Wrap(err, "")
		}

		var cis []isaac.NodeConnInfo

		if i != nil {
			cis = i.([]isaac.NodeConnInfo) //nolint:forcetypeassert //...
		}

		return Response(w, NewResponseHeader(true, nil), cis, enc)
	}
}
