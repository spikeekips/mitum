package isaacnetwork

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/localtime"
	"golang.org/x/sync/singleflight"
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
	nodepolicy              *isaac.NodePolicy
	nodeinfo                *NodeInfoUpdater
	getNodeInfo             func() ([]byte, error)
	sg                      singleflight.Group
}

func NewQuicstreamHandlers( // revive:disable-line:argument-limit
	local base.LocalNode,
	nodepolicy *isaac.NodePolicy,
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
	nodeinfo *NodeInfoUpdater,
) *QuicstreamHandlers {
	handlers := &QuicstreamHandlers{
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
		nodeinfo:                nodeinfo,
	}

	handlers.getNodeInfo = handlers.getNodeInfoFunc()

	return handlers
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

	i, err, _ := c.sg.Do(HandlerPrefixOperation+header.Operation().String(), func() (interface{}, error) {
		op, found, oerr := c.oppool.NewOperation(context.Background(), header.Operation())

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

	res := NewResponseHeader(found, nil)

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

	if err := c.filterNewOperation(op); err != nil {
		return e(err, "")
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

	i, err, _ := c.sg.Do(HandlerPrefixRequestProposal+header.Proposer().String(), func() (interface{}, error) {
		return c.getOrCreateProposal(header.point, header.Proposer())
	})

	if err != nil {
		return e(err, "")
	}

	pr := i.(base.ProposalSignedFact) //nolint:forcetypeassert //...

	res := NewResponseHeader(pr != nil, nil)

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

	i, err, _ := c.sg.Do(HandlerPrefixProposal+header.Proposal().String(), func() (interface{}, error) {
		pr, found, oerr := c.pool.Proposal(header.Proposal())

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

	res := NewResponseHeader(found, nil)

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

	sgkey := HandlerPrefixLastSuffrageProof
	if header.State() != nil {
		sgkey += header.State().String()
	}

	i, err, _ := c.sg.Do(sgkey, func() (interface{}, error) {
		proof, updated, oerr := c.lastSuffrageProoff(header.State())

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

	res := NewResponseHeader(updated, nil)

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

	i, err, _ := c.sg.Do(HandlerPrefixSuffrageProof+header.Height().String(), func() (interface{}, error) {
		proof, found, oerr := c.suffrageProoff(header.Height())

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

	res := NewResponseHeader(found, nil)

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

	sgkey := HandlerPrefixLastBlockMap

	if header.Manifest() != nil {
		sgkey += header.Manifest().String()
	}

	i, err, _ := c.sg.Do(sgkey, func() (interface{}, error) {
		m, updated, oerr := c.lastBlockMapf(header.Manifest())

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

	res := NewResponseHeader(updated, nil)

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

	i, err, _ := c.sg.Do(HandlerPrefixBlockMap+header.Height().String(), func() (interface{}, error) {
		m, found, oerr := c.blockMapf(header.Height())

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

	res := NewResponseHeader(found, nil)

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

	i, err, _ := c.sg.Do(HandlerPrefixNodeChallenge+string(header.Input()), func() (interface{}, error) {
		return c.local.Privatekey().Sign(util.ConcatBytesSlice(
			c.local.Address().Bytes(),
			c.nodepolicy.NetworkID(),
			header.Input(),
		))
	})

	if err != nil {
		return e(err, "")
	}

	sig := i.(base.Signature) //nolint:forcetypeassert //...

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

	i, err, _ := c.sg.Do(HandlerPrefixState+header.Key(), func() (interface{}, error) {
		st, found, oerr := c.statef(header.Key())

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

	var header ExistsInStateOperationRequestHeader
	if err = encoder.Decode(enc, hb, &header); err != nil {
		return e(err, "")
	}

	if err = header.IsValid(nil); err != nil {
		return e(err, "")
	}

	i, err, _ := c.sg.Do(HandlerPrefixExistsInStateOperation+header.FactHash().String(), func() (interface{}, error) {
		return c.existsInStateOperationf(header.FactHash())
	})

	if err != nil {
		return e(err, "")
	}

	res := NewResponseHeader(i.(bool), nil) //nolint:forcetypeassert //...

	if err := c.response(w, res, nil); err != nil {
		return e(err, "")
	}

	return nil
}

func (c *QuicstreamHandlers) NodeInfo(_ net.Addr, r io.Reader, w io.Writer) error {
	e := util.StringErrorFunc("failed to handle node info")

	enc, hb, err := c.prehandle(r)
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

	i, err, _ := c.sg.Do(HandlerPrefixNodeInfo, func() (interface{}, error) {
		return c.getNodeInfo()
	})

	if err != nil {
		return e(err, "")
	}

	var b json.RawMessage

	if i != nil {
		b = json.RawMessage(i.([]byte)) //nolint:forcetypeassert //...
	}

	res := NewResponseHeader(true, nil)

	if err := c.response(w, res, b); err != nil {
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

	i, err, _ := c.sg.Do("node_conn_infos", func() (interface{}, error) {
		return f()
	})

	if err != nil {
		return errors.Wrap(err, "")
	}

	var cis []isaac.NodeConnInfo

	if i != nil {
		cis = i.([]isaac.NodeConnInfo) //nolint:forcetypeassert //...
	}

	res := NewResponseHeader(true, nil)

	return c.response(w, res, cis)
}

func (c *QuicstreamHandlers) response(w io.Writer, header isaac.NetworkHeader, body interface{}) error {
	return Response(w, header, body, c.enc)
}

func (c *QuicstreamHandlers) filterNewOperation(op base.Operation) error {
	switch found, err := c.existsInStateOperationf(op.Fact().Hash()); {
	case err != nil:
		return err
	case found:
		return util.ErrFound.Errorf("already in state")
	}

	switch passed, err := c.filterSendOperationf(op); {
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

func (c *QuicstreamHandlers) getNodeInfoFunc() func() ([]byte, error) {
	var lastid string
	var lastb []byte

	startedAt := c.nodeinfo.StartedAt()

	uptimet := []byte("<uptime>")
	updateUptime := func(b []byte) []byte {
		return bytes.Replace(
			b,
			uptimet,
			[]byte(fmt.Sprintf("%0.3f", localtime.UTCNow().Sub(startedAt).Seconds())),
			1,
		)
	}

	return func() ([]byte, error) {
		if c.nodeinfo.ID() == lastid {
			return updateUptime(lastb), nil
		}

		jm := c.nodeinfo.NodeInfo().JSONMarshaler()
		jm.Local.Uptime = string(uptimet)

		b, err := c.enc.Marshal(jm)
		if err != nil {
			return nil, err
		}

		lastid = c.nodeinfo.ID()
		lastb = b

		return updateUptime(b), nil
	}
}
