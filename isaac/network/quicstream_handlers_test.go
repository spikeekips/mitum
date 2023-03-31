package isaacnetwork

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

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
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: SendBallotsHeaderHint, Instance: SendBallotsHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: quicstream.DefaultResponseHeaderHint, Instance: quicstream.DefaultResponseHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: base.DummySuffrageProofHint, Instance: base.DummySuffrageProof{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationFactHint, Instance: isaac.DummyOperationFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationHint, Instance: isaac.DummyOperation{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.SuffrageCandidateStateValueHint, Instance: isaac.SuffrageCandidateStateValue{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.SuffrageWithdrawOperationHint, Instance: isaac.SuffrageWithdrawOperation{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.SuffrageWithdrawFactHint, Instance: isaac.SuffrageWithdrawFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.INITBallotSignFactHint, Instance: isaac.INITBallotSignFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.INITBallotFactHint, Instance: isaac.INITBallotFact{}}))
}

func (t *testQuicstreamHandlers) TestClient() {
	c := NewBaseClient(t.Encs, t.Enc, nil)

	_ = (interface{})(c).(isaac.NetworkClient)
}

func (t *testQuicstreamHandlers) openstreamf(prefix []byte, handler quicstream.HeaderHandler) (quicstream.OpenStreamFunc, func()) {
	hr, cw := io.Pipe()
	cr, hw := io.Pipe()

	ph := quicstream.NewPrefixHandler(nil)
	ph.Add(prefix, quicstream.NewHeaderHandler(t.Encs, 0, handler))

	handlerf := func() error {
		defer hw.Close()

		if err := ph.Handler(nil, hr, hw); err != nil {
			if errors.Is(err, quicstream.ErrHandlerNotFound) {

				go io.ReadAll(cr)
				go io.ReadAll(hr)
			}

			defer func() {
				hw.Close()
			}()

			return quicstream.HeaderWriteHead(context.Background(), hw, t.Enc,
				quicstream.NewDefaultResponseHeader(false, err))
		}

		return nil
	}

	donech := make(chan error, 1)
	go func() {
		donech <- handlerf()
	}()

	return func(context.Context, quicstream.UDPConnInfo) (io.Reader, io.WriteCloser, error) {
			return cr, cw, nil
		}, func() {
			hr.Close()
			hw.Close()
			cr.Close()
			cw.Close()

			<-donech
		}
}

func (t *testQuicstreamHandlers) openstreamfs(prefix []byte, handlers map[string]quicstream.HeaderHandler) (quicstream.OpenStreamFunc, func()) {
	ops := map[string]quicstream.OpenStreamFunc{}
	cancels := make([]func(), len(handlers))

	var n int

	for i := range handlers {
		ops[i], cancels[n] = t.openstreamf(prefix, handlers[i])

		n++
	}

	return func(ctx context.Context, ci quicstream.UDPConnInfo) (io.Reader, io.WriteCloser, error) {
			op, found := ops[ci.String()]
			if !found {
				return nil, nil, errors.Errorf("unknown conn, %q", ci.String())
			}

			return op(ctx, ci)
		}, func() {
			for i := range cancels {
				cancels[i]()
			}
		}
}

func (t *testQuicstreamHandlers) TestRequest() {
	ci := quicstream.NewUDPConnInfo(nil, true)

	t.Run("ok", func() {
		handler := QuicstreamHandlerExistsInStateOperation(func(util.Hash) (bool, error) {
			return true, nil
		})
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixExistsInStateOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		broker, err := c.Broker(ctx, ci)
		t.NoError(err)

		header := NewExistsInStateOperationRequestHeader(valuehash.RandomSHA256())
		t.NoError(broker.WriteRequestHead(ctx, header))

		_, rh, err := broker.ReadResponseHead(ctx)
		t.NoError(err)

		t.NoError(rh.Err())
		t.True(rh.OK())
	})

	t.Run("error", func() {
		handler := QuicstreamHandlerExistsInStateOperation(func(util.Hash) (bool, error) {
			return false, errors.Errorf("hehehe")
		})

		openstreamf, handlercancel := t.openstreamf(HandlerPrefixExistsInStateOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		broker, err := c.Broker(ctx, ci)
		t.NoError(err)

		header := NewExistsInStateOperationRequestHeader(valuehash.RandomSHA256())
		t.NoError(broker.WriteRequestHead(ctx, header))

		_, rh, err := broker.ReadResponseHead(ctx)
		t.NoError(err)

		t.False(rh.OK())
		t.Error(rh.Err())
		t.ErrorContains(rh.Err(), "hehehe")
	})
}

func (t *testQuicstreamHandlers) TestOperation() {
	fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
	op, err := isaac.NewDummyOperation(fact, t.Local.Privatekey(), t.LocalParams.NetworkID())
	t.NoError(err)

	pool := t.NewPool()
	defer pool.DeepClose()

	inserted, err := pool.SetNewOperation(context.Background(), op)
	t.NoError(err)
	t.True(inserted)

	ci := quicstream.NewUDPConnInfo(nil, true)
	handler := QuicstreamHandlerOperation(pool)

	t.Run("found", func() {
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		uop, found, err := c.Operation(context.Background(), ci, op.Hash())
		t.NoError(err)
		t.True(found)
		t.NotNil(op)

		base.EqualOperation(t.Assert(), op, uop)
	})

	t.Run("not found", func() {
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		op, found, err := c.Operation(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
		t.Nil(op)
	})
}

func (t *testQuicstreamHandlers) TestSendOperation() {
	fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
	op, err := isaac.NewDummyOperation(fact, t.Local.Privatekey(), t.LocalParams.NetworkID())
	t.NoError(err)

	pool := t.NewPool()
	defer pool.DeepClose()

	ci := quicstream.NewUDPConnInfo(nil, true)

	t.Run("ok", func() {
		handler := QuicstreamHandlerSendOperation(t.LocalParams, pool,
			func(util.Hash) (bool, error) { return false, nil },
			func(base.Operation) (bool, error) { return true, nil },
			nil,
			nil,
		)

		openstreamf, handlercancel := t.openstreamf(HandlerPrefixSendOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		updated, err := c.SendOperation(context.Background(), ci, op)
		t.NoError(err)
		t.True(updated)

		t.Run("already exists", func() {
			openstreamf, handlercancel := t.openstreamf(HandlerPrefixSendOperation, handler)
			defer handlercancel()

			c := NewBaseClient(t.Encs, t.Enc, openstreamf)

			updated, err := c.SendOperation(context.Background(), ci, op)
			t.NoError(err)
			t.False(updated)
		})
	})

	t.Run("broadcast", func() {
		_ = pool.Clean()

		ch := make(chan []byte, 1)
		handler := QuicstreamHandlerSendOperation(t.LocalParams, pool,
			func(util.Hash) (bool, error) { return false, nil },
			func(base.Operation) (bool, error) { return true, nil },
			nil,
			func(_ string, b []byte) error {
				ch <- b

				return nil
			},
		)

		openstreamf, handlercancel := t.openstreamf(HandlerPrefixSendOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		updated, err := c.SendOperation(context.Background(), ci, op)
		t.NoError(err)
		t.True(updated)

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("wait broadcast operation, but failed"))
		case b := <-ch:
			var rop isaac.DummyOperation

			t.NoError(encoder.Decode(t.Enc, b, &rop))
			t.True(op.Hash().Equal(rop.Hash()))
		}
	})

	t.Run("filtered", func() {
		handler := QuicstreamHandlerSendOperation(t.LocalParams, pool,
			func(util.Hash) (bool, error) { return false, nil },
			func(base.Operation) (bool, error) { return false, nil },
			nil,
			nil,
		)

		openstreamf, handlercancel := t.openstreamf(HandlerPrefixSendOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		updated, err := c.SendOperation(context.Background(), ci, op)
		t.Error(err)
		t.False(updated)
		t.ErrorContains(err, "filtered")
	})
}

func (t *testQuicstreamHandlers) TestSendOperationWithdraw() {
	fact := isaac.NewSuffrageWithdrawFact(base.RandomAddress(""), base.Height(33), base.Height(34), util.UUID().String())
	op := isaac.NewSuffrageWithdrawOperation(fact)
	t.NoError(op.NodeSign(t.Local.Privatekey(), t.LocalParams.NetworkID(), t.Local.Address()))

	var votedop base.SuffrageWithdrawOperation

	handler := QuicstreamHandlerSendOperation(t.LocalParams, nil,
		func(util.Hash) (bool, error) { return false, nil },
		func(base.Operation) (bool, error) { return true, nil },
		func(op base.SuffrageWithdrawOperation) (bool, error) {
			var voted bool

			switch {
			case votedop == nil:
				voted = true
			case votedop.Hash().Equal(op.Hash()):
				voted = false
			}

			votedop = op

			return voted, nil
		},
		nil,
	)

	ci := quicstream.NewUDPConnInfo(nil, true)

	t.Run("ok", func() {
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixSendOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		voted, err := c.SendOperation(context.Background(), ci, op)
		t.NoError(err)
		t.True(voted)
	})

	t.Run("already voted", func() {
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixSendOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		voted, err := c.SendOperation(context.Background(), ci, op)
		t.NoError(err)
		t.False(voted)
	})

	t.Run("filtered", func() {
		handler := QuicstreamHandlerSendOperation(t.LocalParams, nil,
			func(util.Hash) (bool, error) { return false, nil },
			func(base.Operation) (bool, error) { return false, nil },
			func(op base.SuffrageWithdrawOperation) (bool, error) { return true, nil },
			nil,
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixSendOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		voted, err := c.SendOperation(context.Background(), ci, op)
		t.Error(err)
		t.False(voted)
		t.ErrorContains(err, "filtered")
	})
}

func (t *testQuicstreamHandlers) TestRequestProposal() {
	pool := t.NewPool()
	defer pool.DeepClose()

	proposalMaker := isaac.NewProposalMaker(
		t.Local,
		t.LocalParams,
		func(context.Context, base.Height) ([]util.Hash, error) {
			return []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}, nil
		},
		pool,
	)

	handler := QuicstreamHandlerRequestProposal(t.Local, pool, proposalMaker,
		func() (base.BlockMap, bool, error) { return nil, false, nil },
	)

	ci := quicstream.NewUDPConnInfo(nil, true)

	t.Run("local is proposer", func() {
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixRequestProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		point := base.RawPoint(33, 1)
		pr, found, err := c.RequestProposal(context.Background(), ci, point, t.Local.Address())
		t.NoError(err)
		t.True(found)

		t.Equal(point, pr.Point())
		t.True(t.Local.Address().Equal(pr.ProposalFact().Proposer()))
		t.NoError(base.IsValidProposalSignFact(pr, t.LocalParams.NetworkID()))
		t.NotEmpty(pr.ProposalFact().Operations())
	})

	t.Run("local is not proposer", func() {
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixRequestProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		point := base.RawPoint(33, 2)
		proposer := base.RandomAddress("")
		pr, found, err := c.RequestProposal(context.Background(), ci, point, proposer)
		t.NoError(err)
		t.False(found)
		t.Nil(pr)
	})

	t.Run("too high height", func() {
		handler := QuicstreamHandlerRequestProposal(t.Local, pool, proposalMaker,
			func() (base.BlockMap, bool, error) {
				m := base.NewDummyManifest(base.Height(22), valuehash.RandomSHA256())
				mp := base.NewDummyBlockMap(m)

				return mp, true, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixRequestProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		point := base.RawPoint(33, 3)
		proposer := base.RandomAddress("")
		pr, found, err := c.RequestProposal(context.Background(), ci, point, proposer)
		t.NoError(err)
		t.True(found)
		t.NotNil(pr)
		t.Empty(pr.ProposalFact().Operations())
	})

	t.Run("too low height", func() {
		handler := QuicstreamHandlerRequestProposal(t.Local, pool, proposalMaker,
			func() (base.BlockMap, bool, error) {
				m := base.NewDummyManifest(base.Height(44), valuehash.RandomSHA256())
				mp := base.NewDummyBlockMap(m)

				return mp, true, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixRequestProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

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
		t.LocalParams,
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

	handler := QuicstreamHandlerProposal(pool)

	ci := quicstream.NewUDPConnInfo(nil, true)

	t.Run("found", func() {
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		pr, found, err := c.Proposal(context.Background(), ci, pr.Fact().Hash())
		t.NoError(err)
		t.True(found)

		t.Equal(point, pr.Point())
		t.True(t.Local.Address().Equal(pr.ProposalFact().Proposer()))
		t.NoError(base.IsValidProposalSignFact(pr, t.LocalParams.NetworkID()))
	})

	t.Run("unknown", func() {
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		pr, found, err := c.Proposal(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
		t.Nil(pr)
	})

	t.Run("nil proposal fact hash", func() {
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		pr, found, err := c.Proposal(context.Background(), ci, nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid ProposalHeader")
		t.False(found)
		t.Nil(pr)
	})
}

func (t *testQuicstreamHandlers) TestLastSuffrageProof() {
	lastheight := base.Height(44)
	st, _ := t.SuffrageState(base.Height(33), base.Height(11), nil)
	proof := base.NewDummySuffrageProof()
	proof = proof.SetState(st)

	handler := QuicstreamHandlerLastSuffrageProof(
		func(h util.Hash) (hint.Hint, []byte, []byte, bool, error) {
			if h != nil && h.Equal(st.Hash()) {
				nbody, _ := util.NewLengthedBytesSlice(0x01, [][]byte{lastheight.Bytes(), nil})

				return t.Enc.Hint(), nil, nbody, false, nil
			}

			b, err := t.Enc.Marshal(proof)
			if err != nil {
				return hint.Hint{}, nil, nil, false, err
			}

			nbody, _ := util.NewLengthedBytesSlice(0x01, [][]byte{lastheight.Bytes(), b})

			return t.Enc.Hint(), nil, nbody, true, nil
		},
	)

	ci := quicstream.NewUDPConnInfo(nil, true)

	t.Run("not updated", func() {
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixLastSuffrageProof, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rlastheight, rproof, updated, err := c.LastSuffrageProof(context.Background(), ci, st.Hash())
		t.NoError(err)
		t.False(updated)
		t.Nil(rproof)
		t.Equal(lastheight, rlastheight)
	})

	t.Run("nil state", func() {
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixLastSuffrageProof, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		_, rproof, updated, err := c.LastSuffrageProof(context.Background(), ci, nil)
		t.NoError(err)
		t.True(updated)
		t.NotNil(rproof)
	})

	t.Run("updated", func() {
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixLastSuffrageProof, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		_, rproof, updated, err := c.LastSuffrageProof(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.True(updated)
		t.NotNil(proof)

		t.True(base.IsEqualState(proof.State(), rproof.State()))
	})
}

func (t *testQuicstreamHandlers) TestSuffrageProof() {
	ci := quicstream.NewUDPConnInfo(nil, true)

	suffrageheight := base.Height(11)

	t.Run("found", func() {
		st, _ := t.SuffrageState(base.Height(33), suffrageheight, nil)
		proof := base.NewDummySuffrageProof()
		proof = proof.SetState(st)

		proofb, err := t.Enc.Marshal(proof)
		t.NoError(err)

		handler := QuicstreamHandlerSuffrageProof(
			func(h base.Height) (hint.Hint, []byte, []byte, bool, error) {
				if h != suffrageheight {
					return hint.Hint{}, nil, nil, false, nil
				}

				return t.Enc.Hint(), nil, proofb, true, nil
			},
		)

		openstreamf, handlercancel := t.openstreamf(HandlerPrefixSuffrageProof, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rproof, found, err := c.SuffrageProof(context.Background(), ci, suffrageheight)
		t.NoError(err)
		t.True(found)
		t.NotNil(rproof)

		t.True(base.IsEqualState(proof.State(), rproof.State()))
	})

	t.Run("not found", func() {
		handler := QuicstreamHandlerSuffrageProof(
			func(h base.Height) (hint.Hint, []byte, []byte, bool, error) {
				return hint.Hint{}, nil, nil, false, nil
			},
		)

		openstreamf, handlercancel := t.openstreamf(HandlerPrefixSuffrageProof, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		proof, found, err := c.SuffrageProof(context.Background(), ci, suffrageheight+1)
		t.NoError(err)
		t.False(found)
		t.Nil(proof)
	})
}

func (t *testQuicstreamHandlers) TestLastBlockMap() {
	ci := quicstream.NewUDPConnInfo(nil, true)

	t.Run("nil and updated", func() {
		m := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
		mp := base.NewDummyBlockMap(m)
		mpb, err := t.Enc.Marshal(mp)
		t.NoError(err)

		handler := QuicstreamHandlerLastBlockMap(
			func(manifest util.Hash) (hint.Hint, []byte, []byte, bool, error) {
				if manifest != nil && manifest.Equal(m.Hash()) {
					return hint.Hint{}, nil, nil, false, nil
				}

				return t.Enc.Hint(), nil, mpb, true, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixLastBlockMap, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rmp, updated, err := c.LastBlockMap(context.Background(), ci, nil)
		t.NoError(err)
		t.True(updated)
		t.NotNil(rmp)

		base.EqualBlockMap(t.Assert(), mp, rmp)
	})

	t.Run("not nil and not updated", func() {
		m := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
		mp := base.NewDummyBlockMap(m)
		mpb, err := t.Enc.Marshal(mp)
		t.NoError(err)

		handler := QuicstreamHandlerLastBlockMap(
			func(manifest util.Hash) (hint.Hint, []byte, []byte, bool, error) {
				if manifest != nil && manifest.Equal(m.Hash()) {
					return hint.Hint{}, nil, nil, false, nil
				}

				return t.Enc.Hint(), nil, mpb, true, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixLastBlockMap, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rmp, updated, err := c.LastBlockMap(context.Background(), ci, m.Hash())
		t.NoError(err)
		t.False(updated)
		t.Nil(rmp)
	})

	t.Run("not found", func() {
		handler := QuicstreamHandlerLastBlockMap(
			func(manifest util.Hash) (hint.Hint, []byte, []byte, bool, error) {
				return hint.Hint{}, nil, nil, false, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixLastBlockMap, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rmp, updated, err := c.LastBlockMap(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.False(updated)
		t.Nil(rmp)
	})
}

func (t *testQuicstreamHandlers) TestBlockMap() {
	ci := quicstream.NewUDPConnInfo(nil, true)

	t.Run("found", func() {
		m := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
		mp := base.NewDummyBlockMap(m)
		mpb, err := t.Enc.Marshal(mp)
		t.NoError(err)

		handler := QuicstreamHandlerBlockMap(
			func(height base.Height) (hint.Hint, []byte, []byte, bool, error) {
				if height != m.Height() {
					return hint.Hint{}, nil, nil, false, nil
				}

				return t.Enc.Hint(), nil, mpb, true, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixBlockMap, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rmp, found, err := c.BlockMap(context.Background(), ci, m.Height())
		t.NoError(err)
		t.True(found)
		t.NotNil(rmp)

		base.EqualBlockMap(t.Assert(), mp, rmp)
	})

	t.Run("not found", func() {
		handler := QuicstreamHandlerBlockMap(
			func(height base.Height) (hint.Hint, []byte, []byte, bool, error) {
				return hint.Hint{}, nil, nil, false, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixBlockMap, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rmp, found, err := c.BlockMap(context.Background(), ci, base.Height(33))
		t.NoError(err)
		t.False(found)
		t.Nil(rmp)
	})

	t.Run("error", func() {
		handler := QuicstreamHandlerBlockMap(
			func(height base.Height) (hint.Hint, []byte, []byte, bool, error) {
				return hint.Hint{}, nil, nil, false, errors.Errorf("hehehe")
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixBlockMap, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		_, found, err := c.BlockMap(context.Background(), ci, base.Height(33))
		t.Error(err)
		t.False(found)

		t.ErrorContains(err, "hehehe")
	})
}

func (t *testQuicstreamHandlers) TestBlockMapItem() {
	ci := quicstream.NewUDPConnInfo(nil, true)

	t.Run("known item", func() {
		height := base.Height(33)
		item := base.BlockMapItemTypeVoteproofs

		body := util.UUID().Bytes()
		r := bytes.NewBuffer(body)

		handler := QuicstreamHandlerBlockMapItem(
			func(h base.Height, i base.BlockMapItemType) (io.ReadCloser, bool, error) {
				if h != height {
					return nil, false, nil
				}

				if i != item {
					return nil, false, nil
				}

				return io.NopCloser(r), true, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixBlockMapItem, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rr, cancel, found, err := c.BlockMapItem(context.Background(), ci, height, item)
		t.NoError(err)
		t.True(found)
		t.NotNil(rr)

		rb, err := io.ReadAll(rr)
		t.NoError(err)
		cancel()

		t.Equal(body, rb, "%q != %q", string(body), string(rb))
	})

	t.Run("unknown item", func() {
		handler := QuicstreamHandlerBlockMapItem(
			func(h base.Height, i base.BlockMapItemType) (io.ReadCloser, bool, error) {
				return nil, false, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixBlockMapItem, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rr, _, found, err := c.BlockMapItem(context.Background(), ci, base.Height(33), base.BlockMapItemTypeVoteproofs)
		t.NoError(err)
		t.False(found)
		t.Nil(rr)
	})
}

func (t *testQuicstreamHandlers) TestNodeChallenge() {
	handler := QuicstreamHandlerNodeChallenge(t.Local, t.LocalParams)

	ci := quicstream.NewUDPConnInfo(nil, true)

	t.Run("ok", func() {
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixNodeChallenge, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		input := util.UUID().Bytes()

		sig, err := c.NodeChallenge(context.Background(), ci, t.LocalParams.NetworkID(), t.Local.Address(), t.Local.Publickey(), input)
		t.NoError(err)
		t.NotNil(sig)

		t.NoError(t.Local.Publickey().Verify(util.ConcatBytesSlice(t.Local.Address().Bytes(), t.LocalParams.NetworkID(), input), sig))
	})

	t.Run("empty input", func() {
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixNodeChallenge, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		sig, err := c.NodeChallenge(context.Background(), ci, t.LocalParams.NetworkID(), t.Local.Address(), t.Local.Publickey(), nil)
		t.Error(err)
		t.Nil(sig)

		t.ErrorContains(err, "empty input")
	})
}

func (t *testQuicstreamHandlers) TestSuffrageNodeConnInfo() {
	ci := quicstream.NewUDPConnInfo(nil, true)

	t.Run("empty", func() {
		handler := QuicstreamHandlerSuffrageNodeConnInfo(
			func() ([]isaac.NodeConnInfo, error) {
				return nil, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixSuffrageNodeConnInfo, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

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

		handler := QuicstreamHandlerSuffrageNodeConnInfo(
			func() ([]isaac.NodeConnInfo, error) {
				return ncis, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixSuffrageNodeConnInfo, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

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
	ci := quicstream.NewUDPConnInfo(nil, true)

	t.Run("empty", func() {
		handler := QuicstreamHandlerSyncSourceConnInfo(
			func() ([]isaac.NodeConnInfo, error) {
				return nil, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixSyncSourceConnInfo, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

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

		handler := QuicstreamHandlerSyncSourceConnInfo(
			func() ([]isaac.NodeConnInfo, error) {
				return ncis, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixSyncSourceConnInfo, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

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

	stb, err := t.Enc.Marshal(st)
	t.NoError(err)
	meta := isaacdatabase.NewHashRecordMeta(st.Hash())

	ci := quicstream.NewUDPConnInfo(nil, true)

	t.Run("ok", func() {
		handler := QuicstreamHandlerState(
			func(key string) (hint.Hint, []byte, []byte, bool, error) {
				return t.Enc.Hint(), meta.Bytes(), stb, true, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixState, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		ust, found, err := c.State(context.Background(), ci, st.Key(), nil)
		t.NoError(err)
		t.True(found)
		t.True(base.IsEqualState(st, ust))
	})

	t.Run("ok with hash", func() {
		handler := QuicstreamHandlerState(
			func(key string) (hint.Hint, []byte, []byte, bool, error) {
				if key == st.Key() {
					return t.Enc.Hint(), meta.Bytes(), stb, true, nil
				}

				return hint.Hint{}, nil, nil, false, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixState, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		ust, found, err := c.State(context.Background(), ci, st.Key(), st.Hash())
		t.NoError(err)
		t.True(found)
		t.Nil(ust)
	})

	t.Run("not found", func() {
		handler := QuicstreamHandlerState(
			func(key string) (hint.Hint, []byte, []byte, bool, error) {
				return hint.Hint{}, nil, nil, false, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixState, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		ust, found, err := c.State(context.Background(), ci, st.Key(), nil)
		t.NoError(err)
		t.False(found)
		t.Nil(ust)
	})

	t.Run("error", func() {
		handler := QuicstreamHandlerState(
			func(key string) (hint.Hint, []byte, []byte, bool, error) {
				return hint.Hint{}, nil, nil, false, errors.Errorf("hehehe")
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixState, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		ust, found, err := c.State(context.Background(), ci, st.Key(), nil)
		t.Error(err)
		t.False(found)
		t.Nil(ust)
		t.ErrorContains(err, "hehehe")
	})
}

func (t *testQuicstreamHandlers) TestExistsInStateOperation() {
	ci := quicstream.NewUDPConnInfo(nil, true)

	t.Run("found", func() {
		handler := QuicstreamHandlerExistsInStateOperation(
			func(util.Hash) (bool, error) {
				return true, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixExistsInStateOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		found, err := c.ExistsInStateOperation(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.True(found)
	})

	t.Run("nil facthash", func() {
		handler := QuicstreamHandlerExistsInStateOperation(
			func(util.Hash) (bool, error) {
				return true, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixExistsInStateOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		_, err := c.ExistsInStateOperation(context.Background(), ci, nil)
		t.Error(err)
		t.ErrorContains(err, "empty operation fact hash")
	})

	t.Run("found", func() {
		handler := QuicstreamHandlerExistsInStateOperation(
			func(util.Hash) (bool, error) {
				return false, nil
			},
		)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixExistsInStateOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		found, err := c.ExistsInStateOperation(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
	})
}

func (t *testQuicstreamHandlers) TestSendBallots() {
	newballot := func(point base.Point, node base.LocalNode) base.BallotSignFact {
		fact := isaac.NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

		signfact := isaac.NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(node.Privatekey(), t.LocalParams.NetworkID(), base.RandomAddress("")))

		return signfact
	}

	t.Run("ok", func() {
		votedch := make(chan base.BallotSignFact, 1)
		handler := QuicstreamHandlerSendBallots(t.LocalParams, func(bl base.BallotSignFact) error {
			go func() {
				votedch <- bl
			}()

			return nil
		})

		ci := quicstream.NewUDPConnInfo(nil, true)
		openstreamf, handlercancel := t.openstreamf(HandlerPrefixSendBallots, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		var ballots []base.BallotSignFact

		point := base.RawPoint(33, 44)
		for _, i := range []base.LocalNode{base.RandomLocalNode(), base.RandomLocalNode()} {
			ballots = append(ballots, newballot(point, i))
		}

		t.NoError(c.SendBallots(context.Background(), ci, ballots))

		select {
		case <-time.After(time.Second):
			t.NoError(errors.Errorf("wait ballot, but failed"))
		case bl := <-votedch:
			base.EqualBallotSignFact(t.Assert(), ballots[0], bl)

			bl = <-votedch
			base.EqualBallotSignFact(t.Assert(), ballots[1], bl)
		}
	})
}

func (t *testQuicstreamHandlers) TestConcurrentRequestProposal() {
	var localci quicstream.UDPConnInfo
	var localmaker *isaac.ProposalMaker

	handlers := map[string]quicstream.HeaderHandler{}
	pools := map[string]isaac.ProposalPool{}
	cis := make([]quicstream.UDPConnInfo, 3)

	for i := range cis {
		ci := quicstream.MustNewUDPConnInfoFromString(fmt.Sprintf("0.0.0.0:%d", i))

		if i == 0 {
			localci = ci
		}

		var local base.LocalNode
		if i == 0 {
			local = t.Local
		} else {
			local = base.RandomLocalNode()
		}

		pool := t.NewPool()
		defer pool.DeepClose()

		pools[ci.String()] = pool

		proposalMaker := isaac.NewProposalMaker(
			local,
			t.LocalParams,
			func(context.Context, base.Height) ([]util.Hash, error) {
				return []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}, nil
			},
			pool,
		)

		if i == 0 {
			localmaker = proposalMaker
		}

		handlers[ci.String()] = QuicstreamHandlerRequestProposal(local, pool, proposalMaker, nil)

		cis[i] = ci
	}

	point := base.RawPoint(33, 1)

	t.Run("local is proposer", func() {
		openstreamf, handlercancel := t.openstreamfs(HandlerPrefixRequestProposal, handlers)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		pr, found, err := isaac.ConcurrentRequestProposal(context.Background(), point, t.Local, c, cis, t.LocalParams.NetworkID())
		t.NoError(err)
		t.True(found)

		t.Equal(point, pr.Point())
		t.True(t.Local.Address().Equal(pr.ProposalFact().Proposer()))
		t.NoError(base.IsValidProposalSignFact(pr, t.LocalParams.NetworkID()))
		t.NotEmpty(pr.ProposalFact().Operations())
	})

	t.Run("local not respond", func() {
		newhandlers := map[string]quicstream.HeaderHandler{}
		for i := range handlers {
			if i == localci.String() {
				continue
			}

			newhandlers[i] = handlers[i]
		}

		openstreamf, handlercancel := t.openstreamfs(HandlerPrefixRequestProposal, newhandlers)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		pr, found, err := isaac.ConcurrentRequestProposal(context.Background(), point, t.Local, c, cis, t.LocalParams.NetworkID())
		t.NoError(err)
		t.False(found)
		t.Nil(pr)
	})

	t.Run("local not respond, other node has proposal", func() {
		localpr, err := localmaker.New(context.Background(), point)
		t.NoError(err)

		newhandlers := map[string]quicstream.HeaderHandler{}
		for i := range handlers {
			if i == localci.String() {
				continue
			}

			newhandlers[i] = handlers[i]
		}

		pools[cis[1].String()].SetProposal(localpr)

		openstreamf, handlercancel := t.openstreamfs(HandlerPrefixRequestProposal, newhandlers)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		pr, found, err := isaac.ConcurrentRequestProposal(context.Background(), point, t.Local, c, cis, t.LocalParams.NetworkID())

		t.NoError(err)
		t.True(found)

		base.EqualProposalSignFact(t.Assert(), localpr, pr)
	})

	t.Run("timout", func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		newhandlers := map[string]quicstream.HeaderHandler{}
		for i := range handlers {
			newhandlers[i] = func(context.Context, net.Addr, io.Reader, io.Writer, quicstream.RequestHeadDetail) error {
				select {
				case <-time.After(time.Minute):
				case <-ctx.Done():
					return ctx.Err()
				}

				return nil
			}
		}

		openstreamf, handlercancel := t.openstreamfs(HandlerPrefixRequestProposal, newhandlers)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		pr, found, err := isaac.ConcurrentRequestProposal(ctx, point, t.Local, c, cis, t.LocalParams.NetworkID())

		t.Error(err)
		t.False(found)
		t.Nil(pr)
		t.True(errors.Is(err, context.DeadlineExceeded))
	})

	t.Run("client timout", func() {
		newhandlers := map[string]quicstream.HeaderHandler{}
		for i := range handlers {
			newhandlers[i] = func(context.Context, net.Addr, io.Reader, io.Writer, quicstream.RequestHeadDetail) error {
				return context.DeadlineExceeded
			}
		}

		openstreamf, handlercancel := t.openstreamfs(HandlerPrefixRequestProposal, newhandlers)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		pr, found, err := isaac.ConcurrentRequestProposal(context.Background(), point, t.Local, c, cis, t.LocalParams.NetworkID())

		t.NoError(err)
		t.False(found)
		t.Nil(pr)
	})
}

func TestQuicstreamHandlers(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testQuicstreamHandlers))
}
