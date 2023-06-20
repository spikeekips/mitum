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
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
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
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: StreamOperationsHeaderHint, Instance: StreamOperationsHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: SuffrageNodeConnInfoRequestHeaderHint, Instance: SuffrageNodeConnInfoRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: SuffrageProofRequestHeaderHint, Instance: SuffrageProofRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: SyncSourceConnInfoRequestHeaderHint, Instance: SyncSourceConnInfoRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: StateRequestHeaderHint, Instance: StateRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: ExistsInStateOperationRequestHeaderHint, Instance: ExistsInStateOperationRequestHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: SendBallotsHeaderHint, Instance: SendBallotsHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: SetAllowConsensusHeaderHint, Instance: SetAllowConsensusHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: StartHandoverHeaderHint, Instance: StartHandoverHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: CancelHandoverHeaderHint, Instance: CancelHandoverHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: CheckHandoverHeaderHint, Instance: CheckHandoverHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: AskHandoverHeaderHint, Instance: AskHandoverHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: AskHandoverResponseHeaderHint, Instance: AskHandoverResponseHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: HandoverMessageHeaderHint, Instance: HandoverMessageHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: CheckHandoverXHeaderHint, Instance: CheckHandoverXHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: LastHandoverYLogsHeaderHint, Instance: LastHandoverYLogsHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaacstates.HandoverMessageCancelHint, Instance: isaacstates.HandoverMessageCancel{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: quicstreamheader.DefaultResponseHeaderHint, Instance: quicstreamheader.DefaultResponseHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: base.DummySuffrageProofHint, Instance: base.DummySuffrageProof{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationFactHint, Instance: isaac.DummyOperationFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationHint, Instance: isaac.DummyOperation{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.SuffrageCandidateStateValueHint, Instance: isaac.SuffrageCandidateStateValue{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.SuffrageExpelOperationHint, Instance: isaac.SuffrageExpelOperation{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.SuffrageExpelFactHint, Instance: isaac.SuffrageExpelFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.INITBallotSignFactHint, Instance: isaac.INITBallotSignFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.INITBallotFactHint, Instance: isaac.INITBallotFact{}}))
}

func (t *testQuicstreamHandlers) TestClient() {
	c := NewBaseClient(t.Encs, t.Enc, nil)

	_ = (interface{})(c).(isaac.NetworkClient)
}

func testOpenstreamf[T quicstreamheader.RequestHeader](encs *encoder.Encoders, prefix [32]byte, handler quicstreamheader.Handler[T]) (quicstreamheader.OpenStreamFunc, func()) {
	hr, cw := io.Pipe()
	cr, hw := io.Pipe()

	ph := quicstream.NewPrefixHandler(nil)
	ph.Add(prefix, quicstreamheader.NewHandler[T](encs, nil, handler, nil))

	handlerf := func() error {
		defer hw.Close()

		if err := ph.Handler(nil, hr, hw); err != nil {
			if errors.Is(err, quicstream.ErrHandlerNotFound) {
				go io.ReadAll(cr)
				go io.ReadAll(hr)
			}

			return err
		}

		return nil
	}

	donech := make(chan error, 1)
	go func() {
		donech <- handlerf()
	}()

	return func(context.Context, quicstream.ConnInfo) (io.Reader, io.WriteCloser, error) {
			return cr, cw, nil
		}, func() {
			hr.Close()
			hw.Close()
			cr.Close()
			cw.Close()

			<-donech
		}
}

func (t *testQuicstreamHandlers) TestRequest() {
	ci := quicstream.UnsafeConnInfo(nil, true)

	t.Run("ok", func() {
		handler := QuicstreamHandlerExistsInStateOperation(func(util.Hash) (bool, error) {
			return true, nil
		})
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixExistsInStateOperation, handler)
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

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixExistsInStateOperation, handler)
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

	inserted, err := pool.SetOperation(context.Background(), op)
	t.NoError(err)
	t.True(inserted)

	ci := quicstream.UnsafeConnInfo(nil, true)
	handler := QuicstreamHandlerOperation(pool, nil)

	t.Run("found", func() {
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		uop, found, err := c.Operation(context.Background(), ci, op.Hash())
		t.NoError(err)
		t.True(found)
		t.NotNil(uop)

		base.EqualOperation(t.Assert(), op, uop)
	})

	t.Run("not found", func() {
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		uop, found, err := c.Operation(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
		t.Nil(uop)
	})

	t.Run("from handover x", func() {
		npool := t.NewPool()
		defer npool.DeepClose()

		handler := QuicstreamHandlerOperation(npool,
			func(_ context.Context, header OperationRequestHeader) (hint.Hint, []byte, bool, error) {
				b, err := t.Enc.Marshal(op)
				if err != nil {
					return hint.Hint{}, nil, false, err
				}

				return t.Enc.Hint(), b, true, nil
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		uop, found, err := c.Operation(context.Background(), ci, op.Hash())
		t.NoError(err)
		t.True(found)
		t.NotNil(uop)

		base.EqualOperation(t.Assert(), op, uop)
	})

	t.Run("from handover x; not found", func() {
		npool := t.NewPool()
		defer npool.DeepClose()

		handler := QuicstreamHandlerOperation(npool,
			func(_ context.Context, header OperationRequestHeader) (hint.Hint, []byte, bool, error) {
				return hint.Hint{}, nil, false, nil
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		uop, found, err := c.Operation(context.Background(), ci, op.Hash())
		t.NoError(err)
		t.False(found)
		t.Nil(uop)
	})
}

func (t *testQuicstreamHandlers) TestSendOperation() {
	fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
	op, err := isaac.NewDummyOperation(fact, t.Local.Privatekey(), t.LocalParams.NetworkID())
	t.NoError(err)

	pool := t.NewPool()
	defer pool.DeepClose()

	ci := quicstream.UnsafeConnInfo(nil, true)

	t.Run("ok", func() {
		handler := QuicstreamHandlerSendOperation(t.LocalParams.NetworkID(), pool,
			func(util.Hash) (bool, error) { return false, nil },
			func(base.Operation) (bool, error) { return true, nil },
			nil,
			nil,
			func() uint64 { return 1 << 18 },
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixSendOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		updated, err := c.SendOperation(context.Background(), ci, op)
		t.NoError(err)
		t.True(updated)

		t.Run("already exists", func() {
			openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixSendOperation, handler)
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
		handler := QuicstreamHandlerSendOperation(t.LocalParams.NetworkID(), pool,
			func(util.Hash) (bool, error) { return false, nil },
			func(base.Operation) (bool, error) { return true, nil },
			nil,
			func(_ context.Context, _ string, _ base.Operation, b []byte) error {
				ch <- b

				return nil
			},
			func() uint64 { return 1 << 18 },
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixSendOperation, handler)
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
		handler := QuicstreamHandlerSendOperation(t.LocalParams.NetworkID(), pool,
			func(util.Hash) (bool, error) { return false, nil },
			func(base.Operation) (bool, error) { return false, nil },
			nil,
			nil,
			func() uint64 { return 1 << 18 },
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixSendOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		updated, err := c.SendOperation(context.Background(), ci, op)
		t.Error(err)
		t.False(updated)
		t.ErrorContains(err, "filtered")
	})
}

func (t *testQuicstreamHandlers) TestStreamOperations() {
	pool := t.NewPool()
	defer pool.DeepClose()

	ops := make([]base.Operation, 33)
	opbs := make([][]byte, 33)

	for i := range ops {
		fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := isaac.NewDummyOperation(fact, t.Local.Privatekey(), t.LocalParams.NetworkID())
		t.NoError(err)

		inserted, err := pool.SetOperation(context.Background(), op)
		t.NoError(err)
		t.True(inserted)

		ops[i] = op

		b, err := t.Enc.Marshal(op)
		t.NoError(err)
		opbs[i] = b
	}

	ci := quicstream.UnsafeConnInfo(nil, true)

	newoffset := func(b []byte) []byte {
		return valuehash.NewSHA256(b).Bytes()
	}

	var limit uint64

	handler := func(opch chan []byte) quicstreamheader.Handler[StreamOperationsHeader] {
		return QuicstreamHandlerStreamOperations(t.Local.Publickey(), t.LocalParams.NetworkID(), limit,
			func(
				_ context.Context,
				offset []byte,
				callback func(enchint hint.Hint, meta isaacdatabase.PoolOperationRecordMeta, body, offset []byte) (bool, error),
			) error {
				var found bool
				if offset == nil {
					found = true
				}

				var meta isaacdatabase.PoolOperationRecordMeta

				var count uint64
			end:
				for {
					switch b := <-opch; {
					case b == nil:
						return nil
					case found:
						keep, err := callback(t.Enc.Hint(), meta, b, b[:8])
						if err != nil {
							return err
						}

						if !keep {
							return nil
						}

						count++

						if limit > 0 && count == limit {
							return nil
						}
					case bytes.Equal(offset, newoffset(b)):
						found = true
					}

					continue end
				}
			},
		)
	}

	t.Run("nil offset", func() {
		opch := make(chan []byte, len(ops))

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixStreamOperations, handler(opch))
		defer handlercancel()

		go func() {
			for i := range opbs {
				opch <- opbs[i]
			}

			opch <- nil
		}()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rops := make([]base.Operation, 33)

		var i int
		t.NoError(c.StreamOperationsBytes(context.Background(), ci,
			t.Local.Privatekey(), t.LocalParams.NetworkID(), nil,
			func(enchint hint.Hint, body, offset []byte) error {
				t.NoError(enchint.IsValid(nil))
				t.NotEmpty(body)
				t.NotEmpty(offset)

				enc := t.Encs.Find(enchint)
				t.NotNil(enc)

				var op base.Operation
				t.NoError(encoder.Decode(enc, body, &op))

				base.EqualOperation(t.Assert(), ops[i], op)

				rops[i] = op
				i++

				return nil
			},
		))

		t.Equal(len(ops), i)
	})

	t.Run("with offset", func() {
		opch := make(chan []byte, len(ops))

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixStreamOperations, handler(opch))
		defer handlercancel()

		go func() {
			for i := range opbs {
				opch <- opbs[i]
			}

			opch <- nil
		}()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		offset := newoffset(opbs[len(ops)/2])
		var ropbs [][]byte

		start := (len(ops) / 2) + 1

		var i int

		t.NoError(c.StreamOperationsBytes(context.Background(), ci,
			t.Local.Privatekey(), t.LocalParams.NetworkID(), offset,
			func(enchint hint.Hint, body, offset []byte) error {
				t.NoError(enchint.IsValid(nil))
				t.NotEmpty(body)
				t.NotEmpty(offset)

				t.Equal(opbs[start+i], body)

				enc := t.Encs.Find(enchint)
				t.NotNil(enc)

				var op base.Operation
				t.NoError(encoder.Decode(enc, body, &op))

				base.EqualOperation(t.Assert(), ops[start+i], op)

				ropbs = append(ropbs, body)

				i++

				return nil
			},
		))

		t.Equal(33-start, len(ropbs))
	})

	t.Run("limit", func() {
		opch := make(chan []byte, len(ops))

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixStreamOperations, handler(opch))
		defer handlercancel()

		go func() {
			for i := range opbs {
				opch <- opbs[i]
			}

			opch <- nil
		}()

		limit = uint64(len(ops) / 2)
		defer func() {
			limit = 0
		}()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		var ropbs [][]byte
		var i int

		t.NoError(c.StreamOperationsBytes(context.Background(), ci,
			t.Local.Privatekey(), t.LocalParams.NetworkID(), nil,
			func(enchint hint.Hint, body, offset []byte) error {
				t.NoError(enchint.IsValid(nil))
				t.NotEmpty(body)
				t.NotEmpty(offset)

				t.Equal(opbs[i], body)

				enc := t.Encs.Find(enchint)
				t.NotNil(enc)

				var op base.Operation
				t.NoError(encoder.Decode(enc, body, &op))

				base.EqualOperation(t.Assert(), ops[i], op)

				ropbs = append(ropbs, body)

				i++

				return nil
			},
		))

		t.Equal(limit, uint64(len(ropbs)))
	})

	t.Run("callback error", func() {
		opch := make(chan []byte, len(ops))

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixStreamOperations, handler(opch))
		defer handlercancel()

		donech := make(chan struct{})
		go func() {
			for i := range opbs {
				opch <- opbs[i]
			}

			opch <- nil

			donech <- struct{}{}
		}()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		var i int

		err := c.StreamOperationsBytes(context.Background(), ci,
			t.Local.Privatekey(), t.LocalParams.NetworkID(), nil,
			func(enchint hint.Hint, body, offset []byte) error {
				if i > 3 {
					return errors.Errorf("hohoho")
				}

				i++

				return nil
			},
		)
		t.Error(err)
		t.ErrorContains(err, "hohoho")

		<-donech
	})
}

func (t *testQuicstreamHandlers) TestSendOperationExpel() {
	fact := isaac.NewSuffrageExpelFact(base.RandomAddress(""), base.Height(33), base.Height(34), util.UUID().String())
	op := isaac.NewSuffrageExpelOperation(fact)
	t.NoError(op.NodeSign(t.Local.Privatekey(), t.LocalParams.NetworkID(), t.Local.Address()))

	var votedop base.SuffrageExpelOperation

	handler := QuicstreamHandlerSendOperation(t.LocalParams.NetworkID(), nil,
		func(util.Hash) (bool, error) { return false, nil },
		func(base.Operation) (bool, error) { return true, nil },
		func(op base.SuffrageExpelOperation) (bool, error) {
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
		func() uint64 { return 1 << 18 },
	)

	ci := quicstream.UnsafeConnInfo(nil, true)

	t.Run("ok", func() {
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixSendOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		voted, err := c.SendOperation(context.Background(), ci, op)
		t.NoError(err)
		t.True(voted)
	})

	t.Run("already voted", func() {
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixSendOperation, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		voted, err := c.SendOperation(context.Background(), ci, op)
		t.NoError(err)
		t.False(voted)
	})

	t.Run("filtered", func() {
		handler := QuicstreamHandlerSendOperation(t.LocalParams.NetworkID(), nil,
			func(util.Hash) (bool, error) { return false, nil },
			func(base.Operation) (bool, error) { return false, nil },
			func(op base.SuffrageExpelOperation) (bool, error) { return true, nil },
			nil,
			func() uint64 { return 1 << 18 },
		)
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixSendOperation, handler)
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
		t.LocalParams.NetworkID(),
		func(context.Context, base.Height) ([]util.Hash, error) {
			return []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}, nil
		},
		pool,
	)

	handler := QuicstreamHandlerRequestProposal(t.Local, pool, proposalMaker,
		func() (base.BlockMap, bool, error) { return nil, false, nil },
		func(context.Context, RequestProposalRequestHeader) (base.ProposalSignFact, error) { return nil, nil },
	)

	ci := quicstream.UnsafeConnInfo(nil, true)

	t.Run("local is proposer", func() {
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixRequestProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		prev := valuehash.RandomSHA256()
		point := base.RawPoint(33, 1)
		pr, found, err := c.RequestProposal(context.Background(), ci, point, t.Local.Address(), prev)
		t.NoError(err)
		t.True(found)

		t.Equal(point, pr.Point())
		t.True(t.Local.Address().Equal(pr.ProposalFact().Proposer()))
		t.NoError(base.IsValidProposalSignFact(pr, t.LocalParams.NetworkID()))
		t.NotEmpty(pr.ProposalFact().Operations())
		t.True(prev.Equal(pr.ProposalFact().PreviousBlock()))
	})

	t.Run("local is not proposer", func() {
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixRequestProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		prev := valuehash.RandomSHA256()
		point := base.RawPoint(33, 2)
		proposer := base.RandomAddress("")
		pr, found, err := c.RequestProposal(context.Background(), ci, point, proposer, prev)
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
			func(context.Context, RequestProposalRequestHeader) (base.ProposalSignFact, error) { return nil, nil },
		)
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixRequestProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		point := base.RawPoint(33, 3)
		proposer := base.RandomAddress("")
		pr, found, err := c.RequestProposal(context.Background(), ci, point, proposer, valuehash.RandomSHA256())
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
			func(context.Context, RequestProposalRequestHeader) (base.ProposalSignFact, error) { return nil, nil },
		)
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixRequestProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		point := base.RawPoint(33, 4)
		proposer := base.RandomAddress("")
		pr, found, err := c.RequestProposal(context.Background(), ci, point, proposer, valuehash.RandomSHA256())
		t.Error(err)
		t.False(found)
		t.Nil(pr)
		t.ErrorContains(err, "too old")
	})

	t.Run("handover x; ok", func() {
		prev := valuehash.RandomSHA256()
		point := base.RawPoint(33, 1)
		xpr, err := proposalMaker.New(context.Background(), point, prev)
		t.NoError(err)

		t.T().Log("clear proposals from pool")
		npool := t.NewPool()
		defer npool.DeepClose()

		proposalMaker := isaac.NewProposalMaker(
			t.Local,
			t.LocalParams.NetworkID(),
			func(context.Context, base.Height) ([]util.Hash, error) {
				return []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}, nil
			},
			npool,
		)

		handler := QuicstreamHandlerRequestProposal(t.Local, pool, proposalMaker,
			nil,
			func(context.Context, RequestProposalRequestHeader) (base.ProposalSignFact, error) { return xpr, nil },
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixRequestProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		pr, found, err := c.RequestProposal(context.Background(), ci, point, t.Local.Address(), prev)
		t.NoError(err)
		t.True(found)

		t.Equal(xpr.HashBytes(), pr.HashBytes())

		t.Equal(point, pr.Point())
		t.True(t.Local.Address().Equal(pr.ProposalFact().Proposer()))
		t.NoError(base.IsValidProposalSignFact(pr, t.LocalParams.NetworkID()))
		t.NotEmpty(pr.ProposalFact().Operations())
		t.True(prev.Equal(pr.ProposalFact().PreviousBlock()))
	})

	t.Run("handover x; error", func() {
		prev := valuehash.RandomSHA256()
		point := base.RawPoint(33, 1)
		xpr, err := proposalMaker.New(context.Background(), point, prev)
		t.NoError(err)

		t.T().Log("clear proposals from pool")
		npool := t.NewPool()
		defer npool.DeepClose()

		proposalMaker := isaac.NewProposalMaker(
			t.Local,
			t.LocalParams.NetworkID(),
			func(context.Context, base.Height) ([]util.Hash, error) {
				return []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}, nil
			},
			npool,
		)

		handler := QuicstreamHandlerRequestProposal(t.Local, npool, proposalMaker,
			nil,
			func(context.Context, RequestProposalRequestHeader) (base.ProposalSignFact, error) {
				return xpr, errors.Errorf("hehehe")
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixRequestProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		_, _, err = c.RequestProposal(context.Background(), ci, point, t.Local.Address(), prev)
		t.Error(err)
		t.ErrorContains(err, "hehehe")
	})
}

func (t *testQuicstreamHandlers) TestProposal() {
	pool := t.NewPool()
	defer pool.DeepClose()

	proposalMaker := isaac.NewProposalMaker(
		t.Local,
		t.LocalParams.NetworkID(),
		func(context.Context, base.Height) ([]util.Hash, error) {
			return []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}, nil
		},
		pool,
	)

	point := base.RawPoint(33, 1)
	pr, err := proposalMaker.New(context.Background(), point, valuehash.RandomSHA256())
	t.NoError(err)
	_, err = pool.SetProposal(pr)
	t.NoError(err)

	handler := QuicstreamHandlerProposal(
		pool,
		func(context.Context, ProposalRequestHeader) (hint.Hint, []byte, bool, error) {
			return hint.Hint{}, nil, false, nil
		},
	)

	ci := quicstream.UnsafeConnInfo(nil, true)

	t.Run("found", func() {
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixProposal, handler)
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
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		pr, found, err := c.Proposal(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
		t.Nil(pr)
	})

	t.Run("nil proposal fact hash", func() {
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		pr, found, err := c.Proposal(context.Background(), ci, nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid ProposalHeader")
		t.False(found)
		t.Nil(pr)
	})

	t.Run("handover x", func() {
		t.T().Log("clean proposals from pool")
		npool := t.NewPool()
		defer npool.DeepClose()

		handler := QuicstreamHandlerProposal(
			npool,
			func(context.Context, ProposalRequestHeader) (hint.Hint, []byte, bool, error) {
				b, _ := t.Enc.Marshal(pr)

				return t.Enc.Hint(), b, true, nil
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		pr, found, err := c.Proposal(context.Background(), ci, pr.Fact().Hash())
		t.NoError(err)
		t.True(found)

		t.Equal(point, pr.Point())
		t.True(t.Local.Address().Equal(pr.ProposalFact().Proposer()))
		t.NoError(base.IsValidProposalSignFact(pr, t.LocalParams.NetworkID()))
	})

	t.Run("handover x; error", func() {
		t.T().Log("clean proposals from pool")

		npool := t.NewPool()
		defer npool.DeepClose()

		handler := QuicstreamHandlerProposal(
			npool,
			func(context.Context, ProposalRequestHeader) (hint.Hint, []byte, bool, error) {
				return hint.Hint{}, nil, false, errors.Errorf("hehehe")
			},
		)

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixProposal, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		_, _, err = c.Proposal(context.Background(), ci, pr.Fact().Hash())
		t.Error(err)
		t.ErrorContains(err, "hehehe")
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

	ci := quicstream.UnsafeConnInfo(nil, true)

	t.Run("not updated", func() {
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixLastSuffrageProof, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rlastheight, rproof, updated, err := c.LastSuffrageProof(context.Background(), ci, st.Hash())
		t.NoError(err)
		t.False(updated)
		t.Nil(rproof)
		t.Equal(lastheight, rlastheight)
	})

	t.Run("nil state", func() {
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixLastSuffrageProof, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		_, rproof, updated, err := c.LastSuffrageProof(context.Background(), ci, nil)
		t.NoError(err)
		t.True(updated)
		t.NotNil(rproof)
	})

	t.Run("updated", func() {
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixLastSuffrageProof, handler)
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
	ci := quicstream.UnsafeConnInfo(nil, true)

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

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixSuffrageProof, handler)
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

		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixSuffrageProof, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		proof, found, err := c.SuffrageProof(context.Background(), ci, suffrageheight+1)
		t.NoError(err)
		t.False(found)
		t.Nil(proof)
	})
}

func (t *testQuicstreamHandlers) TestLastBlockMap() {
	ci := quicstream.UnsafeConnInfo(nil, true)

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
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixLastBlockMap, handler)
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
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixLastBlockMap, handler)
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
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixLastBlockMap, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rmp, updated, err := c.LastBlockMap(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.False(updated)
		t.Nil(rmp)
	})
}

func (t *testQuicstreamHandlers) TestBlockMap() {
	ci := quicstream.UnsafeConnInfo(nil, true)

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
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixBlockMap, handler)
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
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixBlockMap, handler)
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
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixBlockMap, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		_, found, err := c.BlockMap(context.Background(), ci, base.Height(33))
		t.Error(err)
		t.False(found)

		t.ErrorContains(err, "hehehe")
	})
}

func (t *testQuicstreamHandlers) TestBlockMapItem() {
	ci := quicstream.UnsafeConnInfo(nil, true)

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
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixBlockMapItem, handler)
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
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixBlockMapItem, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		rr, _, found, err := c.BlockMapItem(context.Background(), ci, base.Height(33), base.BlockMapItemTypeVoteproofs)
		t.NoError(err)
		t.False(found)
		t.Nil(rr)
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

	ci := quicstream.UnsafeConnInfo(nil, true)

	t.Run("ok", func() {
		handler := QuicstreamHandlerState(
			func(key string) (hint.Hint, []byte, []byte, bool, error) {
				return t.Enc.Hint(), meta.Bytes(), stb, true, nil
			},
		)
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixState, handler)
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
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixState, handler)
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
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixState, handler)
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
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixState, handler)
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
	ci := quicstream.UnsafeConnInfo(nil, true)

	t.Run("found", func() {
		handler := QuicstreamHandlerExistsInStateOperation(
			func(util.Hash) (bool, error) {
				return true, nil
			},
		)
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixExistsInStateOperation, handler)
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
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixExistsInStateOperation, handler)
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
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixExistsInStateOperation, handler)
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
		handler := QuicstreamHandlerSendBallots(t.LocalParams.NetworkID(), func(bl base.BallotSignFact) error {
			go func() {
				votedch <- bl
			}()

			return nil
		},
			func() uint64 { return 1 << 18 },
		)

		ci := quicstream.UnsafeConnInfo(nil, true)
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixSendBallots, handler)
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

func (t *testQuicstreamHandlers) TestSetAllowConsensus() {
	t.Run("set", func() {
		setch := make(chan bool, 1)

		handler := QuicstreamHandlerSetAllowConsensus(
			t.Local.Publickey(),
			t.LocalParams.NetworkID(),
			func(allow bool) bool {
				setch <- allow

				return true
			},
		)

		ci := quicstream.UnsafeConnInfo(nil, true)
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixSetAllowConsensus, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		isset, err := c.SetAllowConsensus(context.Background(), ci,
			t.Local.Privatekey(),
			t.LocalParams.NetworkID(),
			true,
		)
		t.NoError(err)
		t.True(isset)

		select {
		case <-time.After(time.Second):
			t.NoError(errors.Errorf("not set"))
		case allow := <-setch:
			t.True(allow)
		}
	})

	t.Run("not set", func() {
		setch := make(chan bool, 1)

		handler := QuicstreamHandlerSetAllowConsensus(
			t.Local.Publickey(),
			t.LocalParams.NetworkID(),
			func(allow bool) bool {
				setch <- allow

				return false
			},
		)

		ci := quicstream.UnsafeConnInfo(nil, true)
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixSetAllowConsensus, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		isset, err := c.SetAllowConsensus(context.Background(), ci,
			t.Local.Privatekey(),
			t.LocalParams.NetworkID(),
			true,
		)
		t.NoError(err)
		t.False(isset)

		select {
		case <-time.After(time.Second):
			t.NoError(errors.Errorf("not set"))
		case allow := <-setch:
			t.True(allow)
		}
	})

	t.Run("wrong key", func() {
		handler := QuicstreamHandlerSetAllowConsensus(
			t.Local.Publickey(),
			t.LocalParams.NetworkID(),
			func(allow bool) bool {
				return true
			},
		)

		ci := quicstream.UnsafeConnInfo(nil, true)
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixSetAllowConsensus, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		_, err := c.SetAllowConsensus(context.Background(), ci,
			base.NewMPrivatekey(),
			t.LocalParams.NetworkID(),
			true,
		)
		t.Error(err)
		t.ErrorContains(err, "signature verification failed")
	})

	t.Run("wrong networkID", func() {
		handler := QuicstreamHandlerSetAllowConsensus(
			t.Local.Publickey(),
			t.LocalParams.NetworkID(),
			func(allow bool) bool {
				return true
			},
		)

		ci := quicstream.UnsafeConnInfo(nil, true)
		openstreamf, handlercancel := testOpenstreamf(t.Encs, HandlerPrefixSetAllowConsensus, handler)
		defer handlercancel()

		c := NewBaseClient(t.Encs, t.Enc, openstreamf)

		_, err := c.SetAllowConsensus(context.Background(), ci,
			t.Local.Privatekey(),
			util.UUID().Bytes(),
			true,
		)
		t.Error(err)
		t.ErrorContains(err, "signature verification failed")
	})
}

func TestQuicstreamHandlers(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testQuicstreamHandlers))
}
