package isaacnetwork

import (
	"bytes"
	"context"
	"io"
	"net"
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
	c := NewBaseClient(t.Encs, t.Enc, nil, func() error { return nil })

	_ = (interface{})(c).(isaac.NetworkClient)
}

func (t *testQuicstreamHandlers) TestRequest() {
	ci := quicstream.UnsafeConnInfo(nil, true)

	t.Run("ok", func() {
		handler := QuicstreamHandlerExistsInStateOperation(func(util.Hash) (bool, error) {
			return true, nil
		})
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixExistsInStateOperation, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stream, closef, err := c.Dial(ctx, ci)
		t.NoError(err)
		defer closef()

		t.NoError(stream(ctx, func(_ context.Context, broker *quicstreamheader.ClientBroker) error {
			header := NewExistsInStateOperationRequestHeader(valuehash.RandomSHA256())
			t.NoError(broker.WriteRequestHead(ctx, header))

			_, rh, err := broker.ReadResponseHead(ctx)
			t.NoError(err)

			t.NoError(rh.Err())
			t.True(rh.OK())

			return nil
		}))
	})

	t.Run("error", func() {
		handler := QuicstreamHandlerExistsInStateOperation(func(util.Hash) (bool, error) {
			return false, errors.Errorf("hehehe")
		})

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixExistsInStateOperation, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stream, closef, err := c.Dial(ctx, ci)
		t.NoError(err)
		defer closef()

		t.NoError(stream(ctx, func(_ context.Context, broker *quicstreamheader.ClientBroker) error {
			header := NewExistsInStateOperationRequestHeader(valuehash.RandomSHA256())
			t.NoError(broker.WriteRequestHead(ctx, header))

			_, rh, err := broker.ReadResponseHead(ctx)
			t.NoError(err)

			t.False(rh.OK())
			t.Error(rh.Err())
			t.ErrorContains(rh.Err(), "hehehe")

			return nil
		}))
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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixOperation, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		uop, found, err := c.Operation(context.Background(), ci, op.Hash())
		t.NoError(err)
		t.True(found)
		t.NotNil(uop)

		base.EqualOperation(t.Assert(), op, uop)
	})

	t.Run("not found", func() {
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixOperation, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		uop, found, err := c.Operation(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
		t.Nil(uop)
	})

	t.Run("from handover x", func() {
		npool := t.NewPool()
		defer npool.DeepClose()

		handler := QuicstreamHandlerOperation(npool,
			func(_ context.Context, header OperationRequestHeader) (string, []byte, bool, error) {
				b, err := t.Enc.Marshal(op)
				if err != nil {
					return "", nil, false, err
				}

				return t.Enc.Hint().String(), b, true, nil
			},
		)

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixOperation, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			func(_ context.Context, header OperationRequestHeader) (string, []byte, bool, error) {
				return "", nil, false, nil
			},
		)

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixOperation, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixSendOperation, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		updated, err := c.SendOperation(context.Background(), ci, op)
		t.NoError(err)
		t.True(updated)

		t.Run("already exists", func() {
			_, dialf := TestingDialFunc(t.Encs, HandlerPrefixSendOperation, handler)

			c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixSendOperation, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixSendOperation, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
				callback func(enchint string, meta isaacdatabase.FrameHeaderPoolOperation, body, offset []byte) (bool, error),
			) error {
				var found bool
				if offset == nil {
					found = true
				}

				var meta isaacdatabase.FrameHeaderPoolOperation

				var count uint64
			end:
				for {
					switch b := <-opch; {
					case b == nil:
						return nil
					case found:
						keep, err := callback(t.Enc.Hint().String(), meta, b, b[:8])
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

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixStreamOperations, handler(opch))

		go func() {
			for i := range opbs {
				opch <- opbs[i]
			}

			opch <- nil
		}()

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		rops := make([]base.Operation, 33)

		var i int
		t.NoError(c.StreamOperationsBytes(context.Background(), ci,
			t.Local.Privatekey(), t.LocalParams.NetworkID(), nil,
			func(enchint string, body, offset []byte) error {
				t.NotEmpty(enchint)
				t.NotEmpty(body)
				t.NotEmpty(offset)

				_, enc, found, err := t.Encs.FindByString(enchint)
				t.NoError(err)
				t.True(found)
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

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixStreamOperations, handler(opch))

		go func() {
			for i := range opbs {
				opch <- opbs[i]
			}

			opch <- nil
		}()

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		offset := newoffset(opbs[len(ops)/2])
		var ropbs [][]byte

		start := (len(ops) / 2) + 1

		var i int

		t.NoError(c.StreamOperationsBytes(context.Background(), ci,
			t.Local.Privatekey(), t.LocalParams.NetworkID(), offset,
			func(enchint string, body, offset []byte) error {
				t.NotEmpty(enchint)
				t.NotEmpty(body)
				t.NotEmpty(offset)

				t.Equal(opbs[start+i], body)

				_, enc, found, err := t.Encs.FindByString(enchint)
				t.NoError(err)
				t.True(found)
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

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixStreamOperations, handler(opch))

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

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		var ropbs [][]byte
		var i int

		t.NoError(c.StreamOperationsBytes(context.Background(), ci,
			t.Local.Privatekey(), t.LocalParams.NetworkID(), nil,
			func(enchint string, body, offset []byte) error {
				t.NotEmpty(enchint)
				t.NotEmpty(body)
				t.NotEmpty(offset)

				t.Equal(opbs[i], body)

				_, enc, found, err := t.Encs.FindByString(enchint)
				t.NoError(err)
				t.True(found)
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

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixStreamOperations, handler(opch))

		donech := make(chan struct{})
		go func() {
			for i := range opbs {
				opch <- opbs[i]
			}

			opch <- nil

			donech <- struct{}{}
		}()

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		var i int

		err := c.StreamOperationsBytes(context.Background(), ci,
			t.Local.Privatekey(), t.LocalParams.NetworkID(), nil,
			func(enchint string, body, offset []byte) error {
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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixSendOperation, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		voted, err := c.SendOperation(context.Background(), ci, op)
		t.NoError(err)
		t.True(voted)
	})

	t.Run("already voted", func() {
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixSendOperation, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixSendOperation, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixRequestProposal, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixRequestProposal, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixRequestProposal, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixRequestProposal, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixRequestProposal, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixRequestProposal, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		func(context.Context, ProposalRequestHeader) (string, []byte, bool, error) {
			return "", nil, false, nil
		},
	)

	ci := quicstream.UnsafeConnInfo(nil, true)

	t.Run("found", func() {
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixProposal, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		pr, found, err := c.Proposal(context.Background(), ci, pr.Fact().Hash())
		t.NoError(err)
		t.True(found)

		t.Equal(point, pr.Point())
		t.True(t.Local.Address().Equal(pr.ProposalFact().Proposer()))
		t.NoError(base.IsValidProposalSignFact(pr, t.LocalParams.NetworkID()))
	})

	t.Run("unknown", func() {
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixProposal, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		pr, found, err := c.Proposal(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
		t.Nil(pr)
	})

	t.Run("nil proposal fact hash", func() {
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixProposal, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			func(context.Context, ProposalRequestHeader) (string, []byte, bool, error) {
				b, _ := t.Enc.Marshal(pr)

				return t.Enc.Hint().String(), b, true, nil
			},
		)

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixProposal, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			func(context.Context, ProposalRequestHeader) (string, []byte, bool, error) {
				return "", nil, false, errors.Errorf("hehehe")
			},
		)

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixProposal, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		func(h util.Hash) (string, []byte, []byte, bool, error) {
			if h != nil && h.Equal(st.Hash()) {
				nbody, _ := util.NewLengthedBytesSlice([][]byte{lastheight.Bytes(), nil})

				return t.Enc.Hint().String(), nil, nbody, false, nil
			}

			b, err := t.Enc.Marshal(proof)
			if err != nil {
				return "", nil, nil, false, err
			}

			nbody, _ := util.NewLengthedBytesSlice([][]byte{lastheight.Bytes(), b})

			return t.Enc.Hint().String(), nil, nbody, true, nil
		},
	)

	ci := quicstream.UnsafeConnInfo(nil, true)

	t.Run("not updated", func() {
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixLastSuffrageProof, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		rlastheight, rproof, updated, err := c.LastSuffrageProof(context.Background(), ci, st.Hash())
		t.NoError(err)
		t.False(updated)
		t.Nil(rproof)
		t.Equal(lastheight, rlastheight)
	})

	t.Run("nil state", func() {
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixLastSuffrageProof, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		_, rproof, updated, err := c.LastSuffrageProof(context.Background(), ci, nil)
		t.NoError(err)
		t.True(updated)
		t.NotNil(rproof)
	})

	t.Run("updated", func() {
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixLastSuffrageProof, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			func(h base.Height) (string, []byte, []byte, bool, error) {
				if h != suffrageheight {
					return "", nil, nil, false, nil
				}

				return t.Enc.Hint().String(), nil, proofb, true, nil
			},
		)

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixSuffrageProof, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		rproof, found, err := c.SuffrageProof(context.Background(), ci, suffrageheight)
		t.NoError(err)
		t.True(found)
		t.NotNil(rproof)

		t.True(base.IsEqualState(proof.State(), rproof.State()))
	})

	t.Run("not found", func() {
		handler := QuicstreamHandlerSuffrageProof(
			func(h base.Height) (string, []byte, []byte, bool, error) {
				return "", nil, nil, false, nil
			},
		)

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixSuffrageProof, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			func(manifest util.Hash) (string, []byte, []byte, bool, error) {
				if manifest != nil && manifest.Equal(m.Hash()) {
					return "", nil, nil, false, nil
				}

				return t.Enc.Hint().String(), nil, mpb, true, nil
			},
		)
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixLastBlockMap, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			func(manifest util.Hash) (string, []byte, []byte, bool, error) {
				if manifest != nil && manifest.Equal(m.Hash()) {
					return "", nil, nil, false, nil
				}

				return t.Enc.Hint().String(), nil, mpb, true, nil
			},
		)
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixLastBlockMap, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		rmp, updated, err := c.LastBlockMap(context.Background(), ci, m.Hash())
		t.NoError(err)
		t.False(updated)
		t.Nil(rmp)
	})

	t.Run("not found", func() {
		handler := QuicstreamHandlerLastBlockMap(
			func(manifest util.Hash) (string, []byte, []byte, bool, error) {
				return "", nil, nil, false, nil
			},
		)
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixLastBlockMap, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
			func(height base.Height) (string, []byte, []byte, bool, error) {
				if height != m.Height() {
					return "", nil, nil, false, nil
				}

				return t.Enc.Hint().String(), nil, mpb, true, nil
			},
		)
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixBlockMap, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		rmp, found, err := c.BlockMap(context.Background(), ci, m.Height())
		t.NoError(err)
		t.True(found)
		t.NotNil(rmp)

		base.EqualBlockMap(t.Assert(), mp, rmp)
	})

	t.Run("not found", func() {
		handler := QuicstreamHandlerBlockMap(
			func(height base.Height) (string, []byte, []byte, bool, error) {
				return "", nil, nil, false, nil
			},
		)
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixBlockMap, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		rmp, found, err := c.BlockMap(context.Background(), ci, base.Height(33))
		t.NoError(err)
		t.False(found)
		t.Nil(rmp)
	})

	t.Run("error", func() {
		handler := QuicstreamHandlerBlockMap(
			func(height base.Height) (string, []byte, []byte, bool, error) {
				return "", nil, nil, false, errors.Errorf("hehehe")
			},
		)
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixBlockMap, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixBlockMapItem, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		var rb []byte
		var found bool

		t.NoError(c.BlockMapItem(context.Background(), ci, height, item, func(r io.Reader, rfound bool) error {
			if rfound {
				b, err := io.ReadAll(r)
				if err != nil {
					return err
				}

				rb = b
			}

			found = rfound

			return nil
		}))

		t.True(found)
		t.Equal(body, rb, "%q != %q", string(body), string(rb))
	})

	t.Run("unknown item", func() {
		handler := QuicstreamHandlerBlockMapItem(
			func(h base.Height, i base.BlockMapItemType) (io.ReadCloser, bool, error) {
				return nil, false, nil
			},
		)
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixBlockMapItem, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		var rb []byte
		var found bool

		err := c.BlockMapItem(context.Background(), ci, base.Height(33), base.BlockMapItemTypeVoteproofs, func(r io.Reader, rfound bool) error {
			if rfound {
				b, err := io.ReadAll(r)
				if err != nil {
					return err
				}

				rb = b
			}

			found = rfound

			return nil
		})
		t.NoError(err)

		t.False(found)
		t.Nil(rb)
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
	meta := st.Hash().Bytes()

	ci := quicstream.UnsafeConnInfo(nil, true)

	t.Run("ok", func() {
		handler := QuicstreamHandlerState(
			func(key string) (string, []byte, []byte, bool, error) {
				return t.Enc.Hint().String(), meta, stb, true, nil
			},
		)
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixState, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		ust, found, err := c.State(context.Background(), ci, st.Key(), nil)
		t.NoError(err)
		t.True(found)
		t.True(base.IsEqualState(st, ust))
	})

	t.Run("ok with hash", func() {
		handler := QuicstreamHandlerState(
			func(key string) (string, []byte, []byte, bool, error) {
				if key == st.Key() {
					return t.Enc.Hint().String(), meta, stb, true, nil
				}

				return "", nil, nil, false, nil
			},
		)
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixState, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		ust, found, err := c.State(context.Background(), ci, st.Key(), st.Hash())
		t.NoError(err)
		t.True(found)
		t.Nil(ust)
	})

	t.Run("not found", func() {
		handler := QuicstreamHandlerState(
			func(key string) (string, []byte, []byte, bool, error) {
				return "", nil, nil, false, nil
			},
		)
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixState, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		ust, found, err := c.State(context.Background(), ci, st.Key(), nil)
		t.NoError(err)
		t.False(found)
		t.Nil(ust)
	})

	t.Run("error", func() {
		handler := QuicstreamHandlerState(
			func(key string) (string, []byte, []byte, bool, error) {
				return "", nil, nil, false, errors.Errorf("hehehe")
			},
		)
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixState, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixExistsInStateOperation, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixExistsInStateOperation, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixExistsInStateOperation, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixSendBallots, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixSetAllowConsensus, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixSetAllowConsensus, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixSetAllowConsensus, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

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
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixSetAllowConsensus, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		_, err := c.SetAllowConsensus(context.Background(), ci,
			t.Local.Privatekey(),
			util.UUID().Bytes(),
			true,
		)
		t.Error(err)
		t.ErrorContains(err, "signature verification failed")
	})
}

func (t *testQuicstreamHandlers) TestNodeChallenge() {
	networkID := base.NetworkID(util.UUID().Bytes())
	local := base.RandomLocalNode()
	remote := base.RandomLocalNode()
	ci := quicstream.UnsafeConnInfo(nil, true)

	chandler := QuicstreamHandlerNodeChallenge(networkID, local)

	ctxch := make(chan context.Context, 1)
	handler := func(
		ctx context.Context,
		addr net.Addr,
		broker *quicstreamheader.HandlerBroker,
		header NodeChallengeRequestHeader,
	) (context.Context, error) {
		nctx, err := chandler(ctx, addr, broker, header)

		ctxch <- nctx

		return nctx, err
	}

	t.Run("me", func() {
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixNodeChallenge, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		sig, err := c.NodeChallenge(
			context.Background(), ci,
			networkID,
			local.Address(),
			local.Publickey(),
			util.UUID().Bytes(),
			remote,
		)
		t.NoError(err)
		t.NotNil(sig)

		nctx := <-ctxch
		t.NotNil(nctx)
		t.True(remote.Address().Equal(nctx.Value(ContextKeyNodeChallengedNode).(base.Address)))
	})

	t.Run("without me", func() {
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixNodeChallenge, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		sig, err := c.NodeChallenge(
			context.Background(), ci,
			networkID,
			local.Address(),
			local.Publickey(),
			util.UUID().Bytes(),
			nil,
		)
		t.NoError(err)
		t.NotNil(sig)

		nctx := <-ctxch
		t.NotNil(nctx)
		t.Nil(nctx.Value(ContextKeyNodeChallengedNode))
	})

	t.Run("me sign failed", func() {
		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixNodeChallenge, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		sig, err := c.nodeChallenge(
			context.Background(), ci,
			networkID,
			local.Address(),
			local.Publickey(),
			util.UUID().Bytes(),
			remote,
			func([]byte) (base.Signature, error) {
				return base.Signature(util.UUID().Bytes()), nil
			},
		)
		t.Error(err)
		t.NotNil(sig)
		t.ErrorContains(err, "me signature; verify signature by publickey")

		nctx := <-ctxch
		t.NotNil(nctx)
		t.Nil(nctx.Value(ContextKeyNodeChallengedNode))
	})

	t.Run("local sign failed", func() {
		chandler := quicstreamHandlerNodeChallenge(networkID, local, func([]byte) (base.Signature, error) {
			return base.Signature(util.UUID().Bytes()), nil
		})
		handler := func(
			ctx context.Context,
			addr net.Addr,
			broker *quicstreamheader.HandlerBroker,
			header NodeChallengeRequestHeader,
		) (context.Context, error) {
			nctx, err := chandler(ctx, addr, broker, header)

			ctxch <- nctx

			return nctx, err
		}

		_, dialf := TestingDialFunc(t.Encs, HandlerPrefixNodeChallenge, handler)

		c := NewBaseClient(t.Encs, t.Enc, dialf, func() error { return nil })

		sig, err := c.NodeChallenge(
			context.Background(), ci,
			networkID,
			local.Address(),
			local.Publickey(),
			util.UUID().Bytes(),
			remote,
		)
		t.Error(err)
		t.Nil(sig)
		t.ErrorContains(err, "node; verify signature by publickey")

		nctx := <-ctxch
		t.NotNil(nctx)
		t.Nil(nctx.Value(ContextKeyNodeChallengedNode))
	})
}

func TestQuicstreamHandlers(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testQuicstreamHandlers))
}
