package isaacnetwork

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

func newQuicstreamHandlers( // revive:disable-line:argument-limit
	local base.LocalNode,
	nodepolicy isaac.NodePolicy,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	idleTimeout time.Duration,
) *QuicstreamHandlers {
	return &QuicstreamHandlers{
		baseNetwork: newBaseNetwork(encs, enc, idleTimeout),
		local:       local,
		nodepolicy:  nodepolicy,
	}
}

type testQuicstreamHandlers struct {
	isaacdatabase.BaseTestDatabase
	isaac.BaseTestBallots
}

func (t *testQuicstreamHandlers) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testQuicstreamHandlers) SetupSuite() {
	t.BaseTestDatabase.SetupSuite()

	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: BlockMapItemRequestHeaderHint, Instance: BlockMapItemRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: BlockMapRequestHeaderHint, Instance: BlockMapRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: LastBlockMapRequestHeaderHint, Instance: LastBlockMapRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: LastSuffrageProofRequestHeaderHint, Instance: LastSuffrageProofRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: NodeChallengeRequestHeaderHint, Instance: NodeChallengeRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: NodeConnInfoHint, Instance: NodeConnInfo{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: OperationRequestHeaderHint, Instance: OperationRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: ProposalRequestHeaderHint, Instance: ProposalRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: RequestProposalRequestHeaderHint, Instance: RequestProposalRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: SendOperationRequestHeaderHint, Instance: SendOperationRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: SuffrageNodeConnInfoRequestHeaderHint, Instance: SuffrageNodeConnInfoRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: SuffrageProofRequestHeaderHint, Instance: SuffrageProofRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: SyncSourceConnInfoRequestHeaderHint, Instance: SyncSourceConnInfoRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: StateRequestHeaderHint, Instance: StateRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: ExistsInStateOperationRequestHeaderHint, Instance: ExistsInStateOperationRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: ResponseHeaderHint, Instance: ResponseHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: base.DummySuffrageProofHint, Instance: base.DummySuffrageProof{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationFactHint, Instance: isaac.DummyOperationFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationHint, Instance: isaac.DummyOperation{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.SuffrageCandidateHint, Instance: isaac.SuffrageCandidate{}}))
}

func (t *testQuicstreamHandlers) TestClient() {
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, nil)

	_ = (interface{})(c).(isaac.NetworkClient)
}

func (t *testQuicstreamHandlers) writef(prefix string, handler quicstream.Handler) BaseNetworkClientWriteFunc {
	return func(ctx context.Context, ci quicstream.UDPConnInfo, f quicstream.ClientWriteFunc) (io.ReadCloser, func() error, error) {
		r := bytes.NewBuffer(nil)
		if err := f(r); err != nil {
			return nil, nil, errors.WithStack(err)
		}

		uprefix, err := quicstream.ReadPrefix(r)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}

		if !bytes.Equal(uprefix, quicstream.HashPrefix(prefix)) {
			return nil, nil, errors.Errorf("unknown request, %q", prefix)
		}

		w := bytes.NewBuffer(nil)
		if err := handler(nil, r, w); err != nil {
			return nil, nil, errors.Wrap(err, "failed to handle request")
		}

		return io.NopCloser(w), func() error { return nil }, nil
	}
}

func (t *testQuicstreamHandlers) TestRequest() {
	handlers := newQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second)

	ci := quicstream.NewUDPConnInfo(nil, true)
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(HandlerPrefixLastBlockMap, handlers.LastBlockMap))

	t.Run("ok", func() {
		m := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
		mp := base.NewDummyBlockMap(m)

		handlers.lastBlockMapf = func(manifest util.Hash) (base.BlockMap, bool, error) {
			if manifest != nil && manifest.Equal(m.Hash()) {
				return nil, false, nil
			}

			return mp, true, nil
		}

		header := NewLastBlockMapRequestHeader(nil)
		response, v, _, err := c.Request(context.Background(), ci, header, nil)
		t.NoError(err)

		t.NoError(response.Err())
		t.True(response.OK())

		rmp, ok := v.(base.BlockMap)
		t.True(ok)

		base.EqualBlockMap(t.Assert(), mp, rmp)
	})

	t.Run("error", func() {
		handlers.lastBlockMapf = func(manifest util.Hash) (base.BlockMap, bool, error) {
			return nil, false, errors.Errorf("hehehe")
		}

		header := NewLastBlockMapRequestHeader(nil)
		response, _, _, err := c.Request(context.Background(), ci, header, nil)
		t.NoError(err)

		t.Error(response.Err())
		t.ErrorContains(response.Err(), "hehehe")
		t.False(response.OK())
	})
}

func (t *testQuicstreamHandlers) TestOperation() {
	fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
	op, err := isaac.NewDummyOperation(fact, t.Local.Privatekey(), t.NodePolicy.NetworkID())
	t.NoError(err)

	pool := t.NewPool()
	defer pool.DeepClose()

	inserted, err := pool.SetNewOperation(context.Background(), op)
	t.NoError(err)
	t.True(inserted)

	handlers := newQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second)
	handlers.oppool = pool

	ci := quicstream.NewUDPConnInfo(nil, true)
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(HandlerPrefixOperation, handlers.Operation))

	t.Run("found", func() {
		uop, found, err := c.Operation(context.Background(), ci, op.Hash())
		t.NoError(err)
		t.True(found)
		t.NotNil(op)

		base.EqualOperation(t.Assert(), op, uop)
	})

	t.Run("not found", func() {
		op, found, err := c.Operation(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
		t.Nil(op)
	})
}

func (t *testQuicstreamHandlers) TestSendOperation() {
	fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
	op, err := isaac.NewDummyOperation(fact, t.Local.Privatekey(), t.NodePolicy.NetworkID())
	t.NoError(err)

	pool := t.NewPool()
	defer pool.DeepClose()

	handlers := newQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second)
	handlers.filterSendOperationf = func(base.Operation) (bool, error) { return true, nil }
	handlers.oppool = pool

	ci := quicstream.NewUDPConnInfo(nil, true)
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(HandlerPrefixSendOperation, handlers.SendOperation))

	t.Run("ok", func() {
		updated, err := c.SendOperation(context.Background(), ci, op)
		t.NoError(err)
		t.True(updated)
	})

	t.Run("already exists", func() {
		updated, err := c.SendOperation(context.Background(), ci, op)
		t.NoError(err)
		t.False(updated)
	})

	t.Run("filtered", func() {
		handlers.filterSendOperationf = func(base.Operation) (bool, error) { return false, nil }

		updated, err := c.SendOperation(context.Background(), ci, op)
		t.Error(err)
		t.False(updated)
		t.ErrorContains(err, "filtered")
	})
}

func (t *testQuicstreamHandlers) TestRequestProposal() {
	pool := t.NewPool()
	defer pool.DeepClose()

	proposalMaker := isaac.NewProposalMaker(
		t.Local,
		t.NodePolicy,
		func(context.Context, base.Height) ([]util.Hash, error) {
			return []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}, nil
		},
		pool,
	)

	handlers := newQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second)
	handlers.pool = pool
	handlers.proposalMaker = proposalMaker

	ci := quicstream.NewUDPConnInfo(nil, true)
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(HandlerPrefixRequestProposal, handlers.RequestProposal))

	t.Run("local is proposer", func() {
		point := base.RawPoint(33, 1)
		pr, found, err := c.RequestProposal(context.Background(), ci, point, t.Local.Address())
		t.NoError(err)
		t.True(found)

		t.Equal(point, pr.Point())
		t.True(t.Local.Address().Equal(pr.ProposalFact().Proposer()))
		t.NoError(base.IsValidProposalSignedFact(pr, t.NodePolicy.NetworkID()))
		t.NotEmpty(pr.ProposalFact().Operations())
	})

	t.Run("local is not proposer", func() {
		point := base.RawPoint(33, 2)
		proposer := base.RandomAddress("")
		pr, found, err := c.RequestProposal(context.Background(), ci, point, proposer)
		t.NoError(err)
		t.True(found)
		t.NotNil(pr)
		t.Empty(pr.ProposalFact().Operations())
	})

	t.Run("too high height", func() {
		handlers.lastBlockMapf = func(util.Hash) (base.BlockMap, bool, error) {
			m := base.NewDummyManifest(base.Height(22), valuehash.RandomSHA256())
			mp := base.NewDummyBlockMap(m)

			return mp, true, nil
		}

		point := base.RawPoint(33, 3)
		proposer := base.RandomAddress("")
		pr, found, err := c.RequestProposal(context.Background(), ci, point, proposer)
		t.NoError(err)
		t.True(found)
		t.NotNil(pr)
		t.Empty(pr.ProposalFact().Operations())
	})

	t.Run("too low height", func() {
		handlers.lastBlockMapf = func(util.Hash) (base.BlockMap, bool, error) {
			m := base.NewDummyManifest(base.Height(44), valuehash.RandomSHA256())
			mp := base.NewDummyBlockMap(m)

			return mp, true, nil
		}

		point := base.RawPoint(33, 4)
		proposer := base.RandomAddress("")
		pr, found, err := c.RequestProposal(context.Background(), ci, point, proposer)
		t.Error(err)
		t.False(found)
		t.Nil(pr)
		t.ErrorContains(err, "too old")
	})
}

func (t *testQuicstreamHandlers) TestProposal() {
	pool := t.NewPool()
	defer pool.DeepClose()

	proposalMaker := isaac.NewProposalMaker(
		t.Local,
		t.NodePolicy,
		func(context.Context, base.Height) ([]util.Hash, error) {
			return []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}, nil
		},
		pool,
	)

	point := base.RawPoint(33, 1)
	pr, err := proposalMaker.New(context.Background(), point)
	t.NoError(err)
	_, err = pool.SetProposal(pr)
	t.NoError(err)

	handlers := newQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second)
	handlers.pool = pool
	handlers.proposalMaker = proposalMaker

	ci := quicstream.NewUDPConnInfo(nil, true)
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(HandlerPrefixProposal, handlers.Proposal))

	t.Run("found", func() {
		pr, found, err := c.Proposal(context.Background(), ci, pr.Fact().Hash())
		t.NoError(err)
		t.True(found)

		t.Equal(point, pr.Point())
		t.True(t.Local.Address().Equal(pr.ProposalFact().Proposer()))
		t.NoError(base.IsValidProposalSignedFact(pr, t.NodePolicy.NetworkID()))
	})

	t.Run("unknown", func() {
		pr, found, err := c.Proposal(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
		t.Nil(pr)
	})

	t.Run("nil proposal fact hash", func() {
		pr, found, err := c.Proposal(context.Background(), ci, nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid ProposalHeader")
		t.False(found)
		t.Nil(pr)
	})
}

func (t *testQuicstreamHandlers) TestLastSuffrageProof() {
	handlers := newQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second)

	ci := quicstream.NewUDPConnInfo(nil, true)
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(HandlerPrefixLastSuffrageProof, handlers.LastSuffrageProof))

	st, _ := t.SuffrageState(base.Height(33), base.Height(11), nil)
	proof := base.NewDummySuffrageProof()
	proof = proof.SetState(st)

	handlers.lastSuffrageProoff = func(h util.Hash) (base.SuffrageProof, bool, error) {
		if h != nil && h.Equal(st.Hash()) {
			return nil, false, nil
		}

		return proof, true, nil
	}

	t.Run("not updated", func() {
		rproof, updated, err := c.LastSuffrageProof(context.Background(), ci, st.Hash())
		t.NoError(err)
		t.False(updated)
		t.Nil(rproof)
	})

	t.Run("nil state", func() {
		rproof, updated, err := c.LastSuffrageProof(context.Background(), ci, nil)
		t.NoError(err)
		t.True(updated)
		t.NotNil(rproof)
	})

	t.Run("updated", func() {
		rproof, updated, err := c.LastSuffrageProof(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.True(updated)
		t.NotNil(proof)

		t.True(base.IsEqualState(proof.State(), rproof.State()))
	})
}

func (t *testQuicstreamHandlers) TestSuffrageProof() {
	handlers := newQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second)

	ci := quicstream.NewUDPConnInfo(nil, true)
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(HandlerPrefixSuffrageProof, handlers.SuffrageProof))

	suffrageheight := base.Height(11)

	t.Run("found", func() {
		st, _ := t.SuffrageState(base.Height(33), suffrageheight, nil)
		proof := base.NewDummySuffrageProof()
		proof = proof.SetState(st)

		handlers.suffrageProoff = func(h base.Height) (base.SuffrageProof, bool, error) {
			if h != suffrageheight {
				return nil, false, nil
			}

			return proof, true, nil
		}

		rproof, found, err := c.SuffrageProof(context.Background(), ci, suffrageheight)
		t.NoError(err)
		t.True(found)
		t.NotNil(rproof)

		t.True(base.IsEqualState(proof.State(), rproof.State()))
	})

	t.Run("not found", func() {
		handlers.lastSuffrageProoff = func(state util.Hash) (base.SuffrageProof, bool, error) {
			return nil, false, nil
		}

		proof, found, err := c.SuffrageProof(context.Background(), ci, suffrageheight+1)
		t.NoError(err)
		t.False(found)
		t.Nil(proof)
	})
}

func (t *testQuicstreamHandlers) TestLastBlockMap() {
	handlers := newQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second)

	ci := quicstream.NewUDPConnInfo(nil, true)
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(HandlerPrefixLastBlockMap, handlers.LastBlockMap))

	t.Run("nil and updated", func() {
		m := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
		mp := base.NewDummyBlockMap(m)

		handlers.lastBlockMapf = func(manifest util.Hash) (base.BlockMap, bool, error) {
			if manifest != nil && manifest.Equal(m.Hash()) {
				return nil, false, nil
			}

			return mp, true, nil
		}

		rmp, updated, err := c.LastBlockMap(context.Background(), ci, nil)
		t.NoError(err)
		t.True(updated)
		t.NotNil(rmp)

		base.EqualBlockMap(t.Assert(), mp, rmp)
	})

	t.Run("not nil and not updated", func() {
		m := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
		mp := base.NewDummyBlockMap(m)

		handlers.lastBlockMapf = func(manifest util.Hash) (base.BlockMap, bool, error) {
			if manifest != nil && manifest.Equal(m.Hash()) {
				return nil, false, nil
			}

			return mp, true, nil
		}

		rmp, updated, err := c.LastBlockMap(context.Background(), ci, m.Hash())
		t.NoError(err)
		t.False(updated)
		t.Nil(rmp)
	})

	t.Run("not found", func() {
		handlers.lastBlockMapf = func(manifest util.Hash) (base.BlockMap, bool, error) {
			return nil, false, nil
		}

		rmp, updated, err := c.LastBlockMap(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.False(updated)
		t.Nil(rmp)
	})
}

func (t *testQuicstreamHandlers) TestBlockMap() {
	handlers := newQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second)

	ci := quicstream.NewUDPConnInfo(nil, true)
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(HandlerPrefixBlockMap, handlers.BlockMap))

	t.Run("found", func() {
		m := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
		mp := base.NewDummyBlockMap(m)

		handlers.blockMapf = func(height base.Height) (base.BlockMap, bool, error) {
			if height != m.Height() {
				return nil, false, nil
			}

			return mp, true, nil
		}

		rmp, found, err := c.BlockMap(context.Background(), ci, m.Height())
		t.NoError(err)
		t.True(found)
		t.NotNil(rmp)

		base.EqualBlockMap(t.Assert(), mp, rmp)
	})

	t.Run("not found", func() {
		handlers.blockMapf = func(height base.Height) (base.BlockMap, bool, error) {
			return nil, false, nil
		}

		rmp, found, err := c.BlockMap(context.Background(), ci, base.Height(33))
		t.NoError(err)
		t.False(found)
		t.Nil(rmp)
	})

	t.Run("error", func() {
		handlers.blockMapf = func(height base.Height) (base.BlockMap, bool, error) {
			return nil, false, errors.Errorf("hehehe")
		}

		_, found, err := c.BlockMap(context.Background(), ci, base.Height(33))
		t.Error(err)
		t.False(found)

		t.ErrorContains(err, "hehehe")
	})
}

func (t *testQuicstreamHandlers) TestBlockMapItem() {
	handlers := newQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second)

	ci := quicstream.NewUDPConnInfo(nil, true)
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(HandlerPrefixBlockMapItem, handlers.BlockMapItem))

	t.Run("known item", func() {
		height := base.Height(33)
		item := base.BlockMapItemTypeVoteproofs

		body := util.UUID().Bytes()
		r := bytes.NewBuffer(body)

		handlers.blockMapItemf = func(h base.Height, i base.BlockMapItemType) (io.ReadCloser, bool, error) {
			if h != height {
				return nil, false, nil
			}

			if i != item {
				return nil, false, nil
			}

			return io.NopCloser(r), true, nil
		}

		rr, cancel, found, err := c.BlockMapItem(context.Background(), ci, height, item)
		t.NoError(err)
		t.True(found)
		t.NotNil(rr)

		rb, err := io.ReadAll(rr)
		t.NoError(err)
		cancel()

		t.Equal(body, rb)
	})

	t.Run("unknown item", func() {
		handlers.blockMapItemf = func(h base.Height, i base.BlockMapItemType) (io.ReadCloser, bool, error) {
			return nil, false, nil
		}

		rr, _, found, err := c.BlockMapItem(context.Background(), ci, base.Height(33), base.BlockMapItemTypeVoteproofs)
		t.NoError(err)
		t.False(found)
		t.Nil(rr)
	})
}

func (t *testQuicstreamHandlers) TestNodeChallenge() {
	handlers := newQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second)

	ci := quicstream.NewUDPConnInfo(nil, true)
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(HandlerPrefixNodeChallenge, handlers.NodeChallenge))

	t.Run("ok", func() {
		input := util.UUID().Bytes()

		sig, err := c.NodeChallenge(context.Background(), ci, t.NodePolicy.NetworkID(), t.Local.Address(), t.Local.Publickey(), input)
		t.NoError(err)
		t.NotNil(sig)

		t.NoError(t.Local.Publickey().Verify(util.ConcatBytesSlice(t.Local.Address().Bytes(), t.NodePolicy.NetworkID(), input), sig))
	})

	t.Run("empty input", func() {
		sig, err := c.NodeChallenge(context.Background(), ci, t.NodePolicy.NetworkID(), t.Local.Address(), t.Local.Publickey(), nil)
		t.Error(err)
		t.Nil(sig)

		t.ErrorContains(err, "empty input")
	})
}

func (t *testQuicstreamHandlers) TestSuffrageNodeConnInfo() {
	handlers := newQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second)

	ci := quicstream.NewUDPConnInfo(nil, true)
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(HandlerPrefixSuffrageNodeConnInfo, handlers.SuffrageNodeConnInfo))

	t.Run("empty", func() {
		handlers.suffrageNodeConnInfof = func() ([]isaac.NodeConnInfo, error) {
			return nil, nil
		}

		cis, err := c.SuffrageNodeConnInfo(context.Background(), ci)
		t.NoError(err)
		t.Equal(0, len(cis))
	})

	t.Run("ok", func() {
		ncis := make([]isaac.NodeConnInfo, 3)
		for i := range ncis {
			ci := quicstream.RandomConnInfo()
			ncis[i] = NewNodeConnInfo(base.RandomNode(), ci.UDPAddr().String(), true)
		}

		handlers.suffrageNodeConnInfof = func() ([]isaac.NodeConnInfo, error) {
			return ncis, nil
		}

		uncis, err := c.SuffrageNodeConnInfo(context.Background(), ci)
		t.NoError(err)
		t.Equal(len(ncis), len(uncis))

		for i := range ncis {
			a := ncis[i]
			b := uncis[i]

			t.True(base.IsEqualNode(a, b))
		}
	})
}

func (t *testQuicstreamHandlers) TestSyncSourceConnInfo() {
	handlers := newQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second)

	ci := quicstream.NewUDPConnInfo(nil, true)
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(HandlerPrefixSyncSourceConnInfo, handlers.SyncSourceConnInfo))

	t.Run("empty", func() {
		handlers.syncSourceConnInfof = func() ([]isaac.NodeConnInfo, error) {
			return nil, nil
		}

		cis, err := c.SyncSourceConnInfo(context.Background(), ci)
		t.NoError(err)
		t.Equal(0, len(cis))
	})

	t.Run("ok", func() {
		ncis := make([]isaac.NodeConnInfo, 3)
		for i := range ncis {
			ci := quicstream.RandomConnInfo()
			ncis[i] = NewNodeConnInfo(base.RandomNode(), ci.UDPAddr().String(), true)
		}

		handlers.syncSourceConnInfof = func() ([]isaac.NodeConnInfo, error) {
			return ncis, nil
		}

		uncis, err := c.SyncSourceConnInfo(context.Background(), ci)
		t.NoError(err)
		t.Equal(len(ncis), len(uncis))

		for i := range ncis {
			a := ncis[i]
			b := uncis[i]

			t.True(base.IsEqualNode(a, b))
		}
	})
}

func (t *testQuicstreamHandlers) TestState() {
	v := base.NewDummyStateValue(util.UUID().String())
	st := base.NewBaseState(
		base.Height(33),
		util.UUID().String(),
		v,
		valuehash.RandomSHA256(),
		[]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
	)

	handlers := newQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second)

	ci := quicstream.NewUDPConnInfo(nil, true)
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(HandlerPrefixState, handlers.State))

	t.Run("ok", func() {
		handlers.statef = func(key string) (base.State, bool, error) {
			if key == st.Key() {
				return st, true, nil
			}

			return nil, false, nil
		}

		ust, found, err := c.State(context.Background(), ci, st.Key(), nil)
		t.NoError(err)
		t.True(found)
		t.True(base.IsEqualState(st, ust))
	})

	t.Run("ok with hash", func() {
		handlers.statef = func(key string) (base.State, bool, error) {
			if key == st.Key() {
				return st, true, nil
			}

			return nil, false, nil
		}

		ust, found, err := c.State(context.Background(), ci, st.Key(), st.Hash())
		t.NoError(err)
		t.True(found)
		t.Nil(ust)
	})

	t.Run("not found", func() {
		handlers.statef = func(key string) (base.State, bool, error) {
			return nil, false, nil
		}

		ust, found, err := c.State(context.Background(), ci, st.Key(), nil)
		t.NoError(err)
		t.False(found)
		t.Nil(ust)
	})

	t.Run("error", func() {
		handlers.statef = func(key string) (base.State, bool, error) {
			return nil, false, errors.Errorf("hehehe")
		}

		ust, found, err := c.State(context.Background(), ci, st.Key(), nil)
		t.Error(err)
		t.False(found)
		t.Nil(ust)
		t.ErrorContains(err, "hehehe")
	})
}

func (t *testQuicstreamHandlers) TestExistsInStateOperation() {
	handlers := newQuicstreamHandlers(t.Local, t.NodePolicy, t.Encs, t.Enc, time.Second)

	ci := quicstream.NewUDPConnInfo(nil, true)
	c := NewBaseNetworkClient(t.Encs, t.Enc, time.Second, t.writef(HandlerPrefixExistsInStateOperation, handlers.ExistsInStateOperation))

	t.Run("found", func() {
		handlers.existsInStateOperationf = func(util.Hash) (bool, error) {
			return true, nil
		}

		found, err := c.ExistsInStateOperation(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.True(found)
	})

	t.Run("nil facthash", func() {
		handlers.existsInStateOperationf = func(util.Hash) (bool, error) {
			return true, nil
		}

		_, err := c.ExistsInStateOperation(context.Background(), ci, nil)
		t.Error(err)
		t.ErrorContains(err, "empty operation fact hash")
	})

	t.Run("found", func() {
		handlers.existsInStateOperationf = func(util.Hash) (bool, error) {
			return false, nil
		}

		found, err := c.ExistsInStateOperation(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
	})
}

func TestQuicstreamHandlers(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testQuicstreamHandlers))
}
