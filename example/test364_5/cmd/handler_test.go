package main

import (
	"context"
	"iter"
	"slices"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	"github.com/spikeekips/mitum/example/test364_5/quicstream"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testHandlerStartWorker struct {
	dutil.BaseSuite
}

func (t *testHandlerStartWorker) TestOK() {
	doHandlers := func(
		workID string,
		workers, opsWorkers []string,
		opsShare, statesShare, maxOpsSize uint64,
	) {
		ctx := context.Background()

		request := requestHandlerStartWorker(workID, workers, opsWorkers, opsShare, statesShare, maxOpsSize)

		var handlerCalled bool
		handler := acceptHandlerStartWorker(func(
			rworkID string,
			rworkers, ropsWorkers []string,
			ropsShare, rstatesShare, rmaxOpsSize uint64,
		) error {
			t.IfTruePanic(workID != rworkID, "work id")
			t.IfTruePanic(!slices.Equal(workers, rworkers), "workers")
			t.IfTruePanic(!slices.Equal(opsWorkers, ropsWorkers), "ops workers")
			t.IfTruePanic(opsShare != ropsShare, "ops share")
			t.IfTruePanic(statesShare != rstatesShare, "states share")
			t.IfTruePanic(maxOpsSize != rmaxOpsSize, "max ops size")

			handlerCalled = true

			return nil
		})

		rstream, hstream := quicstream.NewDummyStreams()

		requestdone := make(chan error)
		go func() {
			_, err := doHandler(ctx, rstream, request)

			requestdone <- err
		}()

		_, err := doHandler(ctx, hstream, handler)
		t.NoError(err)

		t.True(handlerCalled)

		select {
		case <-time.After(time.Second):
			t.Fail("wait request done")
		case err := <-requestdone:
			t.NoError(err)
		}
	}

	t.RunWithGoLeak("ok", func() {
		doHandlers(
			util.UUID().String(),
			[]string{"w0", "w1", "w2"},
			[]string{"w3", "w4"},
			3, 4, 5,
		)
	})

	t.RunWithGoLeak("empty workers", func() {
		doHandlers(
			util.UUID().String(),
			nil,
			[]string{"w3", "w4"},
			3, 4, 5,
		)
	})

	t.RunWithGoLeak("empty ops workers", func() {
		doHandlers(
			util.UUID().String(),
			[]string{"w3", "w4"},
			nil,
			3, 4, 5,
		)
	})
}

func (t *testHandlerStartWorker) TestAccept() {
	t.Run("eof", func() {
		handler := acceptHandlerStartWorker(func(string, []string, []string, uint64, uint64, uint64) error { return nil })

		rstream, hstream := quicstream.NewDummyStreams()
		rstream.Close()

		_, err := handler(context.Background(), hstream)
		t.ErrorContains(err, "insufficient read")
	})

	t.Run("missing work id", func() {
		rstream, hstream := quicstream.NewDummyStreams()

		requestdone := make(chan struct{})
		go func() {
			err := frame.WriteFixedSlice(rstream, 1, slices.Values([][]byte{util.UUID().Bytes()}))
			t.PrintErr(err, "write workers")
			rstream.Close()

			close(requestdone)
		}()

		handler := acceptHandlerStartWorker(func(string, []string, []string, uint64, uint64, uint64) error { return nil })

		_, err := doHandler(context.Background(), hstream, handler)
		t.ErrorContains(err, "insufficient read")

		select {
		case <-time.After(time.Second):
			t.Fail("wait request done")
		case <-requestdone:
		}
	})
}

func TestHandlerStartWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testHandlerStartWorker))
}

type testHandlerSendOps struct {
	quicstream.BaseTest
}

func (t *testHandlerSendOps) doHandler(
	maxItems, subStreamItems uint64,
	maxSubStreams uint16,
) {
	ops := make([][2]util.Hash, maxItems)
	for i := range ops {
		ops[i] = [2]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}
	}

	var addOp dutil.AppendSliceAddFunc[[3]interface{}]
	doneOp := func() [][3]interface{} { return nil }

	if maxItems > 0 {
		add, done, err := dutil.NewLockAppendSlice[[3]interface{}](maxItems, 0)
		t.NoError(err)
		addOp, doneOp = add, done
	}

	handler := acceptHandlerSendOps(
		func(seq iter.Seq2[uint64, [2]util.Hash]) error {
			for opindex, hs := range seq {
				if added, _ := addOp([3]interface{}{opindex, hs[0], hs[1]}); added < 1 {
					return errors.Errorf("not added")
				}
			}

			return nil
		},
	)

	_, _, rConn, hConn, rstream, hstream, closef := t.OpenStreams(context.Background())
	defer closef()

	rctx := context.WithValue(context.Background(), quicstream.QuicConnectionContextKey, rConn)
	hctx := context.WithValue(context.Background(), quicstream.QuicConnectionContextKey, hConn)

	handlerdone := make(chan error)

	go func() {
		_, err := handler.StreamHandler(quicstream.NewFuncAcceptSubStreams(1<<9))(hctx, hstream)
		handlerdone <- err
	}()

	sendOp, sendDone, requestHandler := requestHandlerSendOps(context.Background(),
		maxItems, subStreamItems, maxSubStreams,
	)

	requestDone, requestWait := dutil.NewContextDone(context.Background())

	go func() {
		openConnSubStreams, openConnSubStreamsClose, _ := quicstream.NewConnsAcceptSubStreams(context.Background(), time.Minute)
		defer openConnSubStreamsClose()
		openSubStreams := quicstream.NewFuncOpenSubStreams(openConnSubStreams)

		_, err := requestHandler.StreamHandler(openSubStreams)(rctx, rstream)
		_ = requestDone(err)
	}()

	for i, v := range slices.All(ops) {
		t.NoError(sendOp(uint64(i), v[0], v[1]))
	}

	t.NoError(sendDone(nil))
	t.NoError(requestWait())

	select {
	case <-time.After(time.Second):
		t.Fail("wait handler done")
	case err := <-handlerdone:
		t.NoError(err)

		rops := make([][2]util.Hash, len(ops))

		for v := range slices.Values(doneOp()) {
			rops[v[0].(uint64)] = [2]util.Hash{
				v[1].(util.Hash),
				v[2].(util.Hash),
			}
		}

		t.Equal(len(ops), len(rops))

		for i := range ops {
			a := ops[i]
			b := rops[i]

			t.True(a[0].Equal(b[0]))
			t.True(a[1].Equal(b[1]))
		}
	}
}

func (t *testHandlerSendOps) TestOK() {
	t.Run("ok", func() {
		t.doHandler(
			33, 2,
			4,
		)
	})

	t.Run("zero max items", func() {
		t.doHandler(
			0, 2,
			4,
		)
	})
}

func TestHandlerSendOps(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testHandlerSendOps))
}

type testHandlerWorkerOwners struct {
	dutil.BaseSuite
}

func (t *testHandlerWorkerOwners) TestOK() {
	worker := util.UUID().String()
	owners := []string{
		util.UUID().String(),
		util.UUID().String(),
		util.UUID().String(),
	}

	request := requestHandlerWorkerOwners(worker, owners)

	var called bool
	handler := acceptHandlerWorkerOwners(func(rworker string, rowners []string) error {
		t.IfTruePanic(worker != rworker, "worker")
		t.IfTruePanic(!slices.Equal(owners, rowners), "owners")

		called = true

		return nil
	})

	ctx := context.Background()
	rstream, hstream := quicstream.NewDummyStreams()

	handlerdone := make(chan error)
	go func() {
		_, err := handler(ctx, hstream)
		t.PrintIfErrPanic(err, "handler")

		handlerdone <- err
	}()

	_, err := request(ctx, rstream)
	t.NoError(err)

	select {
	case <-time.After(time.Second):
		t.Fail("wait request done")
	case err := <-handlerdone:
		t.NoError(err)
		t.True(called)
	}
}

func TestHandlerWorkerOwners(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testHandlerWorkerOwners))
}

func doHandler(ctx context.Context, stream quic.Stream, h quicstream.StreamHandler) (context.Context, error) {
	ctx, err := h(ctx, stream)

	return ctx, quicstream.CloseStreamWithError(stream, err)
}
