package isaacnetwork

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type QuicstreamHandlers struct {
	pool   isaac.ProposalPool
	oppool isaac.NewOperationPool
	*baseNetwork
	proposalMaker           *isaac.ProposalMaker
	lastSuffrageProoff      func(suffragestate util.Hash) (base.SuffrageProof, bool, error)
	suffrageProoff          func(base.Height) (base.SuffrageProof, bool, error)
	lastBlockMapf           func(util.Hash) (base.BlockMap, bool, error)
	blockMapf               func(base.Height) (base.BlockMap, bool, error)
	blockMapItemf           func(base.Height, base.BlockMapItemType) (io.ReadCloser, bool, error)
	suffrageNodeConnInfof   func() ([]isaac.NodeConnInfo, error)
	syncSourceConnInfof     func() ([]isaac.NodeConnInfo, error)
	statef                  func(string) (base.State, bool, error)
	existsInStateOperationf func(util.Hash) (bool, error)
	filterSendOperationf    func(base.Operation) (bool, error)
	local                   base.LocalNode
	nodepolicy              isaac.NodePolicy
}

func NewQuicstreamHandlers( // revive:disable-line:argument-limit
	local base.LocalNode,
	nodepolicy isaac.NodePolicy,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	idleTimeout time.Duration,
	pool isaac.ProposalPool,
	oppool isaac.NewOperationPool,
	proposalMaker *isaac.ProposalMaker,
	lastSuffrageProoff func(util.Hash) (base.SuffrageProof, bool, error),
	suffrageProoff func(base.Height) (base.SuffrageProof, bool, error),
	lastBlockMapf func(util.Hash) (base.BlockMap, bool, error),
	blockMapf func(base.Height) (base.BlockMap, bool, error),
	blockMapItemf func(base.Height, base.BlockMapItemType) (io.ReadCloser, bool, error),
	suffrageNodeConnInfof func() ([]isaac.NodeConnInfo, error),
	syncSourceConnInfof func() ([]isaac.NodeConnInfo, error),
	statef func(string) (base.State, bool, error),
	existsInStateOperationf func(util.Hash) (bool, error),
	filterSendOperationf func(base.Operation) (bool, error),
) *QuicstreamHandlers {
	return &QuicstreamHandlers{
		baseNetwork:             newBaseNetwork(encs, enc, idleTimeout),
		local:                   local,
		nodepolicy:              nodepolicy,
		pool:                    pool,
		oppool:                  oppool,
		proposalMaker:           proposalMaker,
		lastSuffrageProoff:      lastSuffrageProoff,
		suffrageProoff:          suffrageProoff,
		lastBlockMapf:           lastBlockMapf,
		blockMapf:               blockMapf,
		blockMapItemf:           blockMapItemf,
		suffrageNodeConnInfof:   suffrageNodeConnInfof,
		syncSourceConnInfof:     syncSourceConnInfof,
		statef:                  statef,
		existsInStateOperationf: existsInStateOperationf,
		filterSendOperationf:    filterSendOperationf,
	}
}

func (c *QuicstreamHandlers) ErrorHandler(_ net.Addr, _ io.Reader, w io.Writer, err error) error {
	if e := c.response(w, NewResponseHeader(false, err), nil); e != nil {
		return errors.Wrap(e, "failed to response error response")
	}

	return nil
}

func (c *QuicstreamHandlers) Operation(_ net.Addr, r io.Reader, w io.Writer) error { //nolint:dupl //...
	e := util.StringErrorFunc("failed to handle request last BlockMap")

	enc, hb, err := c.prehandle(r)
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

	op, found, err := c.oppool.NewOperation(context.Background(), header.Operation())
	res := NewResponseHeader(found, err)

	if err := c.response(w, res, op); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) SendOperation(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle new operation")

	var enc encoder.Encoder
	var hb []byte

	switch i, j, err := c.prehandle(r); {
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
	default:
		if err := encoder.Decode(enc, body, &op); err != nil {
			return e(err, "")
		}

		if op == nil {
			return e(nil, "empty body")
		}

		if err := op.IsValid(c.nodepolicy.NetworkID()); err != nil {
			return e(err, "")
		}
	}

	switch passed, err := c.filterSendOperationf(op); {
	case err != nil:
		var reason base.OperationProcessReasonError

		if errors.As(err, &reason) {
			err = reason
		}

		res := NewResponseHeader(false, err)

		if err := c.response(w, res, nil); err != nil {
			return e(err, "")
		}

		return nil
	case !passed:
		return e(nil, "filtered")
	}

	added, err := c.oppool.SetNewOperation(context.Background(), op)

	res := NewResponseHeader(added, err)

	if err := c.response(w, res, nil); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) RequestProposal(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle request proposal")

	enc, hb, err := c.prehandle(r)
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

	pr, err := c.getOrCreateProposal(header.point, header.proposer)

	res := NewResponseHeader(pr != nil, err)

	if err := c.response(w, res, pr); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) Proposal(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle get proposal")

	enc, hb, err := c.prehandle(r)
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

	pr, found, err := c.pool.Proposal(header.proposal)

	res := NewResponseHeader(found, err)

	if err := c.response(w, res, pr); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) LastSuffrageProof(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle get last suffrage proof")

	enc, hb, err := c.prehandle(r)
	if err != nil {
		return e(err, "")
	}

	var header LastSuffrageProofRequestHeader
	if err = encoder.Decode(enc, hb, &header); err != nil {
		return e(err, "")
	}

	proof, updated, err := c.lastSuffrageProoff(header.State())
	res := NewResponseHeader(updated, err)

	if err := c.response(w, res, proof); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) SuffrageProof(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle get suffrage proof")

	enc, hb, err := c.prehandle(r)
	if err != nil {
		return e(err, "")
	}

	var header SuffrageProofRequestHeader
	if err = encoder.Decode(enc, hb, &header); err != nil {
		return e(err, "")
	}

	proof, found, err := c.suffrageProoff(header.Height())
	res := NewResponseHeader(found, err)

	if err := c.response(w, res, proof); err != nil {
		return e(err, "")
	}

	return nil
}

// LastBlockMap responds the last BlockMap to client; if there is no BlockMap,
// it returns nil BlockMap and not updated without error.
func (c *QuicstreamHandlers) LastBlockMap(_ net.Addr, r io.Reader, w io.Writer) error { //nolint:dupl //...
	e := util.StringErrorFunc("failed to handle request last BlockMap")

	enc, hb, err := c.prehandle(r)
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

	m, updated, err := c.lastBlockMapf(header.Manifest())
	res := NewResponseHeader(updated, err)

	if err := c.response(w, res, m); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) BlockMap(_ net.Addr, r io.Reader, w io.Writer) error { //nolint:dupl //...
	e := util.StringErrorFunc("failed to handle request BlockMap")

	enc, hb, err := c.prehandle(r)
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

	m, found, err := c.blockMapf(header.Height())
	res := NewResponseHeader(found, err)

	if err := c.response(w, res, m); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) BlockMapItem(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle request BlockMapItem")

	enc, hb, err := c.prehandle(r)
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

	itemr, found, err := c.blockMapItemf(header.Height(), header.Item())
	if itemr != nil {
		defer func() {
			_ = itemr.Close()
		}()
	}

	res := NewResponseHeader(found, err)

	if err := c.response(w, res, nil); err != nil {
		return e(err, "")
	}

	if itemr == nil {
		return nil
	}

	if _, err := io.Copy(w, itemr); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) NodeChallenge(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle NodeChallenge")

	enc, hb, err := c.prehandle(r)
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

	sig, err := c.local.Privatekey().Sign(util.ConcatBytesSlice(
		c.local.Address().Bytes(),
		c.nodepolicy.NetworkID(),
		header.Input(),
	))
	if err != nil {
		return e(err, "")
	}

	res := NewResponseHeader(true, nil)

	if err := c.response(w, res, sig); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) SuffrageNodeConnInfo(addr net.Addr, r io.Reader, w io.Writer) error {
	var header SuffrageNodeConnInfoRequestHeader

	if err := c.nodeConnInfos(addr, r, w, header, c.suffrageNodeConnInfof); err != nil {
		return errors.WithMessage(err, "failed to handle SuffrageNodeConnInfo")
	}

	return nil
}

func (c *QuicstreamHandlers) SyncSourceConnInfo(addr net.Addr, r io.Reader, w io.Writer) error {
	var header SyncSourceConnInfoRequestHeader

	if err := c.nodeConnInfos(addr, r, w, header, c.syncSourceConnInfof); err != nil {
		return errors.WithMessage(err, "failed to handle SyncSourceConnInfo")
	}

	return nil
}

func (c *QuicstreamHandlers) State(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle get state by key")

	enc, hb, err := c.prehandle(r)
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

	st, found, err := c.statef(header.Key())
	res := NewResponseHeader(found, err)

	if found && header.Hash() != nil && st.Hash().Equal(header.Hash()) {
		if err := c.response(w, res, nil); err != nil {
			return e(err, "")
		}

		return nil
	}

	if err := c.response(w, res, st); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) ExistsInStateOperation(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle exists instate operation")

	enc, hb, err := c.prehandle(r)
	if err != nil {
		return e(err, "")
	}

	var body ExistsInStateOperationRequestHeader
	if err = encoder.Decode(enc, hb, &body); err != nil {
		return e(err, "")
	}

	if err = body.IsValid(nil); err != nil {
		return e(err, "")
	}

	found, err := c.existsInStateOperationf(body.facthash)
	header := NewResponseHeader(found, err)

	if err := c.response(w, header, nil); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) prehandle(r io.Reader) (encoder.Encoder, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.idleTimeout)
	defer cancel()

	return HandlerReadHead(ctx, c.encs, r)
}

func (c *QuicstreamHandlers) getOrCreateProposal(
	point base.Point,
	proposer base.Address,
) (base.ProposalSignedFact, error) {
	if c.lastBlockMapf != nil {
		switch m, found, err := c.lastBlockMapf(nil); {
		case err != nil:
			return nil, err
		case !found:
		case point.Height() < m.Manifest().Height()-1:
			return nil, errors.Errorf("too old; ignored")
		case point.Height() > m.Manifest().Height(): // NOTE empty proposal for unreachable point
			return c.proposalMaker.Empty(context.Background(), point)
		}
	}

	switch pr, found, err := c.pool.ProposalByPoint(point, proposer); {
	case err != nil:
		return nil, err
	case found:
		return pr, nil
	}

	if !proposer.Equal(c.local.Address()) {
		return c.proposalMaker.Empty(context.Background(), point)
	}

	return c.proposalMaker.New(context.Background(), point)
}

func (c *QuicstreamHandlers) nodeConnInfos(
	_ net.Addr, r io.Reader, w io.Writer,
	header isaac.NetworkHeader,
	f func() ([]isaac.NodeConnInfo, error),
) error {
	enc, hb, err := c.prehandle(r)
	if err != nil {
		return err
	}

	if err = encoder.Decode(enc, hb, &header); err != nil {
		return err
	}

	if err = header.IsValid(nil); err != nil {
		return err
	}

	cis, err := f()
	if err != nil {
		return err
	}

	res := NewResponseHeader(true, nil)

	return c.response(w, res, cis)
}

func (c *QuicstreamHandlers) response(w io.Writer, header isaac.NetworkHeader, body interface{}) error {
	return Response(w, header, body, c.enc)
}
