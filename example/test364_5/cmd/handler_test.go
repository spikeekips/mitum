package main

import (
	"context"
	"iter"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
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
		height base.Height,
		workers, opsWorkers []string,
		opsShare, statesShare, maxOpsSize uint64,
	) {
		ctx := context.Background()

		request := requestHandlerStartWorker(workID, height, workers, opsWorkers, opsShare, statesShare, maxOpsSize)

		var handlerCalled bool
		handler := acceptHandlerStartWorker(func(
			rworkID string,
			rheight base.Height,
			rworkers, ropsWorkers []string,
			ropsShare, rstatesShare, rmaxOpsSize uint64,
		) error {
			t.IfTruePanic(workID != rworkID, "work id")
			t.IfTruePanic(height != rheight, "height")
			t.IfTruePanic(!slices.Equal(workers, rworkers), "workers")
			t.IfTruePanic(!slices.Equal(opsWorkers, ropsWorkers), "ops workers")
			t.IfTruePanic(opsShare != ropsShare, "ops share")
			t.IfTruePanic(statesShare != rstatesShare, "states share")
			t.IfTruePanic(maxOpsSize != rmaxOpsSize, "max ops size")

			handlerCalled = true

			return nil
		})

		rstream, hstream := quicstream.NewDummyStreams()

		requestDone := make(chan error)
		go func() {
			_, err := doHandler(ctx, rstream, request)

			requestDone <- err
		}()

		_, err := doHandler(ctx, hstream, handler)
		t.NoError(err)

		t.True(handlerCalled)

		select {
		case <-time.After(time.Second):
			t.Fail("wait request done")
		case err := <-requestDone:
			t.NoError(err)
		}
	}

	t.RunWithGoLeak("ok", func() {
		doHandlers(
			util.UUID().String(),
			33,
			[]string{"w0", "w1", "w2"},
			[]string{"w3", "w4"},
			3, 4, 5,
		)
	})

	t.RunWithGoLeak("empty workers", func() {
		doHandlers(
			util.UUID().String(),
			33,
			nil,
			[]string{"w3", "w4"},
			3, 4, 5,
		)
	})

	t.RunWithGoLeak("empty ops workers", func() {
		doHandlers(
			util.UUID().String(),
			33,
			[]string{"w3", "w4"},
			nil,
			3, 4, 5,
		)
	})
}

func (t *testHandlerStartWorker) TestAccept() {
	t.Run("eof", func() {
		handler := acceptHandlerStartWorker(func(string, base.Height, []string, []string, uint64, uint64, uint64) error { return nil })

		rstream, hstream := quicstream.NewDummyStreams()
		rstream.Close()

		_, err := handler(context.Background(), hstream)
		t.ErrorIs(err, frame.ErrInsufficientRead)
	})

	t.Run("missing work id", func() {
		rstream, hstream := quicstream.NewDummyStreams()

		requestDone := make(chan struct{})
		go func() {
			err := frame.WriteFixedSlice(rstream, 1, frame.SimpleWriteFixedSliceSeq([][]byte{util.UUID().Bytes()}))
			t.PrintErr(err, "write workers")
			rstream.Close()

			close(requestDone)
		}()

		handler := acceptHandlerStartWorker(func(string, base.Height, []string, []string, uint64, uint64, uint64) error { return nil })

		_, err := doHandler(context.Background(), hstream, handler)
		t.ErrorContains(err, "insufficient read")

		select {
		case <-time.After(time.Second):
			t.Fail("wait request done")
		case <-requestDone:
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
		func(rmaxItems uint64, seq iter.Seq2[uint64, [2]util.Hash]) error {
			t.PrintIfTruePanic(maxItems != rmaxItems, "max items")

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

type testHandlerWorkerResult struct {
	quicstream.BaseTest
}

func (t *testHandlerWorkerResult) newState() base.State {
	sv := dop5.NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 1)
	return dop5.NewBaseState(base.Height(33), util.UUID().String(), sv, valuehash.RandomSHA256(), nil)
}

func (t *testHandlerWorkerResult) doHandlers(
	ctx context.Context,
	reasons map[uint64]string,
	states []base.State,
	f dop5.ManagerInsertWorkerResultFunc,
) {
	worker := util.UUID().String()

	request := requestHandlerWorkerResult(
		worker,
		reasons,
		uint64(len(states)),
		func(f dutil.InErrorFunc[base.State]) error {
			for st := range slices.Values(states) {
				if err := f(st); err != nil {
					return err
				}
			}

			return nil
		},
		3,
		1<<9,
	)

	var rreasons map[uint64]string
	rstates := util.NewSingleLockedMap[string, util.Hash]()

	if f == nil {
		f = func(rworker string) (
			dutil.InErrorFunc[map[uint64]string],
			dutil.In2ErrorFunc[string, util.Hash],
			dutil.InErrorFunc[error],
			error,
		) {
			t.PrintIfTruePanic(worker != rworker, "worker")

			return func(m map[uint64]string) error {
					t.Print("reasons: %v", m)

					rreasons = maps.Clone(m)

					return nil
				},
				func(stateKey string, h util.Hash) error {
					t.Print("state key=%q state hash=%q", stateKey, h)

					t.PrintIfTruePanic(!rstates.SetValue(stateKey, h), "set state")

					return nil
				},
				func(err error) error {
					t.Print("err: %v", err)

					return err
				},
				nil
		}
	}

	handler := acceptHandlerWorkerResult(f)

	_, _, rConn, hConn, rstream, hstream, closef := t.OpenStreams(context.Background())
	defer closef()

	rctx := context.WithValue(context.Background(), quicstream.QuicConnectionContextKey, rConn)
	hctx := context.WithValue(context.Background(), quicstream.QuicConnectionContextKey, hConn)

	requestDone := make(chan struct{})

	go func() {
		openConnSubStreams, openConnSubStreamsClose, _ := quicstream.NewConnsAcceptSubStreams(context.Background(), time.Minute)
		defer openConnSubStreamsClose()
		openSubStreams := quicstream.NewFuncOpenSubStreams(openConnSubStreams)

		_, err := request.StreamHandler(openSubStreams)(rctx, rstream)
		t.PrintIfErrPanic(err, "request")

		close(requestDone)
	}()

	_, err := handler.StreamHandler(quicstream.NewFuncAcceptSubStreams(1<<9))(hctx, hstream)
	t.PrintIfErrPanic(err, "handler")

	if rreasons != nil {
		t.PrintIfTruePanic(!maps.Equal(reasons, rreasons), "reasons: %v != %v", reasons, rreasons)

		t.Equal(len(states), rstates.Len())
		for st := range slices.Values(states) {
			h, found := rstates.Value(st.Key())
			t.True(found)
			t.True(st.Hash().Equal(h))
		}
	}

	select {
	case <-time.After(time.Second):
		t.Fail("wait request done")
	case <-requestDone:
	}
}

func (t *testHandlerWorkerResult) TestOK() {
	t.Run("ok", func() {
		reasons := map[uint64]string{
			1: util.UUID().String(),
			2: util.UUID().String(),
			3: util.UUID().String(),
		}
		states := make([]base.State, 33)
		for i := range states {
			states[i] = t.newState()
		}

		t.doHandlers(context.Background(), reasons, states, nil)
	})

	t.Run("nil f", func() {
		reasons := map[uint64]string{
			1: util.UUID().String(),
			2: util.UUID().String(),
			3: util.UUID().String(),
		}
		states := make([]base.State, 33)
		for i := range states {
			states[i] = t.newState()
		}

		t.doHandlers(context.Background(), reasons, states,
			func(rworker string) (
				dutil.InErrorFunc[map[uint64]string],
				dutil.In2ErrorFunc[string, util.Hash],
				dutil.InErrorFunc[error],
				error,
			) {
				return nil, nil, nil, nil
			},
		)
	})

	t.Run("empty reasons", func() {
		states := make([]base.State, 33)
		for i := range states {
			states[i] = t.newState()
		}

		t.doHandlers(context.Background(), nil, states, nil)
	})

	t.Run("empty states", func() {
		reasons := map[uint64]string{
			1: util.UUID().String(),
			2: util.UUID().String(),
			3: util.UUID().String(),
		}

		t.doHandlers(context.Background(), reasons, nil, nil)
	})

	t.Run("empty", func() {
		t.doHandlers(context.Background(), nil, nil, nil)
	})
}

func TestHandlerWorkerResult(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testHandlerWorkerResult))
}

type testHandlerWorkerErr struct {
	quicstream.BaseTest
}

func (t *testHandlerWorkerErr) doHandlers(
	worker, werrstring string,
) {
	var werr error
	if werrstring != "" {
		werr = errors.Errorf(werrstring)
	}

	request := requestHandlerWorkerErr(worker, werr)

	handler := acceptHandlerWorkerErr(func(rworker string, rerr error) error {
		t.PrintIfTruePanic(worker != rworker, "worker")

		var berr string

		if rerr != nil {
			berr = rerr.Error()
		}

		t.PrintIfTruePanic(werrstring != berr, "err")

		return nil
	})

	rstream, hstream := quicstream.NewDummyStreams()

	requestDone := make(chan error)
	go func() {
		_, err := request(context.Background(), rstream)

		requestDone <- err
	}()

	_, err := handler(context.Background(), hstream)
	t.NoError(err)

	select {
	case <-time.After(time.Second):
		t.Fail("wait request done")
	case err := <-requestDone:
		t.NoError(err)
	}
}

func (t *testHandlerWorkerErr) TestOK() {
	t.Run("ok", func() {
		t.doHandlers(util.UUID().String(), "showme")
	})

	t.Run("nil error", func() {
		t.doHandlers(util.UUID().String(), "")
	})

	t.Run("empty worker", func() {
		t.doHandlers("", "showme")
	})
}

func TestHandlerWorkerErr(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testHandlerWorkerErr))
}

type testHandlerWorkerDistributeSMVs struct {
	quicstream.BaseTest
	enc *jsonenc.Encoder
}

func (t *testHandlerWorkerDistributeSMVs) SetupSuite() {
	t.BaseTest.SetupSuite()

	t.enc = jsonenc.NewEncoder(context.Background(), 1<<9)
	t.NoError(jsonenc.AddDecode(t.enc, base.DummyPublickeyHint, base.DummyPublickey{}, dop5.DecodeJSONDummyPublickey))
	t.NoError(jsonenc.AddDecode(t.enc, dop5.AccountSVHint, &dop5.AccountSV{}, dop5.AccountSVDecodeJSON))
	t.NoError(jsonenc.AddDecode(t.enc, dop5.AccountSMVHint, &dop5.AccountSMV{}, dop5.AccountSMVDecodeJSON))
	t.NoError(jsonenc.AddDecode(t.enc, smvsBundleHint, smvsBundle{}, smvsBundleDecodeJSON))
}

func (t *testHandlerWorkerDistributeSMVs) TearDownSuite() {
	t.enc.Close()
}

func (t *testHandlerWorkerDistributeSMVs) randomMergeValues(n int) []dop5.SMV {
	if n < 1 {
		return nil
	}

	s := make([]dop5.SMV, n)

	for i := range n {
		v, err := dop5.NewAccountSMV(base.NewDummyPublickey(util.UUID().String()), 33)
		t.IfErrPanic(err)

		s[i] = v
	}

	return s
}

func (t *testHandlerWorkerDistributeSMVs) randomOwners(n int) []string {
	if n < 1 {
		return nil
	}

	s := make([]string, n)
	for i := range n {
		s[i] = util.UUID().String()
	}

	return s
}

func (t *testHandlerWorkerDistributeSMVs) doHandler(
	nops, nsubs, naffected int,
	maxItems, subStreamItems uint64,
	maxSubStreams uint16,
) {
	ops := make([][2]interface{}, nops)
	for i := range ops {
		ops[i] = [2]interface{}{
			valuehash.RandomSHA256(),
			t.randomMergeValues(3),
		}
	}

	worker := util.UUID().String()

	subs, affected := t.randomOwners(nsubs), t.randomOwners(naffected)

	addOp, doneOp, err := dutil.NewLockAppendSlice[[3]any](1<<9, 1<<9)
	t.NoError(err)

	distribute := func(rworker string) (
		dop5.WorkerProcessInsertSMVsFunc,
		dutil.In2ErrorFunc[[]string, []string],
		error,
	) {
		t.PrintIfTruePanic(worker != rworker, "worker")

		return func(opindex uint64, facth util.Hash, values []dop5.SMV) error {
				if added, _ := addOp([3]interface{}{opindex, facth, values}); added < 1 {
					return errors.Errorf("not added")
				}

				return nil
			},
			func(subs, affected []string) error {
				if added, _ := addOp([3]interface{}{subs, affected, nil}); added < 1 {
					return errors.Errorf("not added; owners")
				}

				return nil
			},
			nil
	}

	handler := acceptHandlerWorkerDistributeSMVs(t.enc, distribute)

	_, _, rConn, hConn, rstream, hstream, closef := t.OpenStreams(context.Background())
	defer closef()

	rctx := context.WithValue(context.Background(), quicstream.QuicConnectionContextKey, rConn)
	hctx := context.WithValue(context.Background(), quicstream.QuicConnectionContextKey, hConn)

	handlerdone := make(chan error)

	go func() {
		_, err := handler.StreamHandler(quicstream.NewFuncAcceptSubStreams(1<<9))(hctx, hstream)
		handlerdone <- err
	}()

	sendOp, sendOwners, requestHandler := requestHandlerWorkerDistributeSMVs(
		context.Background(),
		t.enc,
		worker,
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

	var wg sync.WaitGroup

	for i, v := range slices.All(ops) {
		wg.Add(1)

		go func() {
			defer wg.Done()

			err := sendOp(uint64(i), v[0].(util.Hash), v[1].([]dop5.SMV))
			t.PrintIfErrPanic(err, "send op")
		}()
	}

	wg.Wait()

	t.NoError(sendOwners(subs, affected))
	t.NoError(requestWait())

	select {
	case <-time.After(time.Second * 9):
		t.Fail("wait handler done")
	case err := <-handlerdone:
		t.NoError(err)
	}

	r := doneOp()
	t.Equal(len(ops)+1, len(r))

	var rsubs, raffected []string
	rops := make([][2]interface{}, len(r)-1)
	for i, v := range slices.All(r) {
		if i == len(r)-1 {
			rsubs, raffected = v[0].([]string), v[1].([]string)
		} else {
			rops[v[0].(uint64)] = [2]interface{}{
				v[1],
				v[2],
			}
		}
	}

	for i, a := range slices.All(ops) {
		b := rops[i]

		t.True(a[0].(util.Hash).Equal(b[0].(util.Hash)))
		avs := a[1].([]dop5.SMV)
		bvs := b[1].([]dop5.SMV)

		t.Equal(len(avs), len(bvs))

		for j, av := range slices.All(avs) {
			bv := bvs[j]

			t.True(base.IsEqualStateValue(av.Value(), bv.Value()))
			t.Equal(av.Key(), bv.Key())
		}
	}

	t.EqualValues(subs, rsubs)
	t.EqualValues(affected, raffected)
}

func (t *testHandlerWorkerDistributeSMVs) TestOK() {
	run := func(name string, f func()) {
		t.RunWithGoLeak(name, f,
			goleak.IgnoreTopFunction("github.com/spikeekips/mitum/example/test364_5/util.NewUnlimitedChans[...].func2"),
			goleak.IgnoreTopFunction("github.com/spikeekips/mitum/example/test364_5/util.NewUnlimitedChans[...].func4"),
		)
	}

	run("ok", func() {
		t.doHandler(
			3333, 3, 3,
			3333, 1<<7, 1<<9,
		)
	})

	run("empty ops", func() {
		t.doHandler(
			0, 3, 3,
			2, 3, 3,
		)
	})

	run("empty subs", func() {
		t.doHandler(
			3, 0, 3,
			2, 3, 3,
		)
	})

	run("empty affected", func() {
		t.doHandler(
			3, 3, 0,
			2, 3, 3,
		)
	})

	run("empty all", func() {
		t.doHandler(
			0, 0, 0,
			2, 3, 3,
		)
	})

	run("zero max items", func() {
		t.doHandler(
			3333, 3, 3,
			0, 1<<9, 1<<9,
		)
	})

	run("zero max sub streams items", func() {
		t.doHandler(
			3333, 3, 3,
			333, 0, 1<<9,
		)
	})

	run("zero max sub streams size", func() {
		t.doHandler(
			3333, 3, 3,
			333, 1<<9, 0,
		)
	})
}

func (t *testHandlerWorkerDistributeSMVs) TestNilFuncs() {
	defer goleak.VerifyNone(t.T(),
		goleak.IgnoreTopFunction("github.com/spikeekips/mitum/example/test364_5/util.NewUnlimitedChans[...].func2"),
		goleak.IgnoreTopFunction("github.com/spikeekips/mitum/example/test364_5/util.NewUnlimitedChans[...].func4"),
	)

	ops := make([][2]interface{}, 3333)
	for i := range ops {
		ops[i] = [2]interface{}{
			valuehash.RandomSHA256(),
			t.randomMergeValues(3),
		}
	}

	worker := util.UUID().String()

	subs, affected := t.randomOwners(3), t.randomOwners(3)

	distribute := func(rworker string) (
		dop5.WorkerProcessInsertSMVsFunc,
		dutil.In2ErrorFunc[[]string, []string],
		error,
	) {
		return nil, nil, nil
	}

	handler := acceptHandlerWorkerDistributeSMVs(t.enc, distribute)

	_, _, rConn, hConn, rstream, hstream, closef := t.OpenStreams(context.Background())
	defer closef()

	rctx := context.WithValue(context.Background(), quicstream.QuicConnectionContextKey, rConn)
	hctx := context.WithValue(context.Background(), quicstream.QuicConnectionContextKey, hConn)

	handlerdone := make(chan error)

	go func() {
		_, err := handler.StreamHandler(quicstream.NewFuncAcceptSubStreams(1<<9))(hctx, hstream)
		handlerdone <- err
	}()

	sendOp, sendOwners, requestHandler := requestHandlerWorkerDistributeSMVs(
		context.Background(),
		t.enc,
		worker,
		3333, 1<<9, 1<<9,
	)

	requestDone, requestWait := dutil.NewContextDone(context.Background())

	go func() {
		openConnSubStreams, openConnSubStreamsClose, _ := quicstream.NewConnsAcceptSubStreams(context.Background(), time.Minute)
		defer openConnSubStreamsClose()
		openSubStreams := quicstream.NewFuncOpenSubStreams(openConnSubStreams)

		_, err := requestHandler.StreamHandler(openSubStreams)(rctx, rstream)
		_ = requestDone(err)
	}()

	var wg sync.WaitGroup

	for i, v := range slices.All(ops) {
		wg.Add(1)

		go func() {
			defer wg.Done()

			err := sendOp(uint64(i), v[0].(util.Hash), v[1].([]dop5.SMV))
			t.PrintIfErrPanic(err, "send op")
		}()
	}

	wg.Wait()

	t.NoError(sendOwners(subs, affected))
	t.NoError(requestWait())

	select {
	case <-time.After(time.Second * 9):
		t.Fail("wait handler done")
	case err := <-handlerdone:
		t.NoError(err)
	}
}

func TestHandlerWorkerDistributeSMVs(t *testing.T) {
	suite.Run(t, new(testHandlerWorkerDistributeSMVs))
}

type testHandlerWorkerUnmerged struct {
	dutil.BaseSuite
}

func (t *testHandlerWorkerUnmerged) TestOK() {
	worker := util.UUID().String()
	unmerged := []uint64{3, 4, 5}

	request := requestHandlerWorkerUnmerged(worker, unmerged)

	var called bool
	handler := acceptHandlerWorkerUnmerged(func(rworker string, runmerged []uint64) error {
		t.IfTruePanic(worker != rworker, "worker")
		t.IfTruePanic(!slices.Equal(unmerged, runmerged), "unmerged")

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

func TestHandlerWorkerUnmerged(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testHandlerWorkerUnmerged))
}

func doHandler(ctx context.Context, stream quic.Stream, h quicstream.StreamHandler) (context.Context, error) {
	ctx, err := h(ctx, stream)

	_ = quicstream.CloseStreamWithError(stream, err)

	return ctx, err
}

type testHandlerPreloadOps struct {
	quicstream.BaseTest
	enc *jsonenc.Encoder
}

func (t *testHandlerPreloadOps) SetupSuite() {
	t.BaseTest.SetupSuite()

	t.enc = jsonenc.NewEncoder(context.Background(), 1<<9)
	t.NoError(jsonenc.AddDecode(t.enc, base.DummyPublickeyHint, base.DummyPublickey{}, dop5.DecodeJSONDummyPublickey))
	t.NoError(jsonenc.AddDecode(t.enc, dop5.AccountOpFactHint, &dop5.AccountOpFact{}, dop5.AccountOpFactDecodeJSON))
}

func (t *testHandlerPreloadOps) TearDownSuite() {
	t.enc.Close()
}

func (t *testHandlerPreloadOps) randomFacts(n int) []dop5.OperationFact {
	f := make([]dop5.OperationFact, n)

	for i := range n {
		f[i] = dop5.NewAccountOpFact(
			util.UUID().Bytes(),
			int64(i),
			base.NewDummyPublickey(util.UUID().String()),
			base.NewDummyPublickey(util.UUID().String()),
		)
	}

	return f
}

func (t *testHandlerPreloadOps) run(name string, n int) {
	t.RunWithGoLeak(
		name,
		func() {
			ctx, cancel := t.Ctx()
			defer cancel(nil)

			ops := t.randomFacts(n)

			_, _, rConn, hConn, rstream, hstream, closef := t.OpenStreams(ctx)
			defer closef()

			rctx := context.WithValue(ctx, quicstream.QuicConnectionContextKey, rConn)
			hctx := context.WithValue(ctx, quicstream.QuicConnectionContextKey, hConn)

			add, done, err := dutil.NewLockAppendSlice[dop5.OperationFact](1<<9, 1<<9)
			t.NoError(err)

			handler := acceptHandlerPreloadOps(t.enc, func(fact dop5.OperationFact) error {
				add(fact)

				return nil
			})

			handlerdone := make(chan error)

			go func() {
				_, err := handler.StreamHandler(quicstream.NewFuncAcceptSubStreams(1<<9))(hctx, hstream)
				handlerdone <- err
			}()

			requestHandler := requestHandlerPreloadOps(
				t.enc,
				uint64(len(ops)),
				1<<9,
				1<<9,
				func(yield func(fact dop5.OperationFact) bool) {
					for fact := range slices.Values(ops) {
						if !yield(fact) {
							return
						}
					}
				},
			)

			openConnSubStreams, openConnSubStreamsClose, _ := quicstream.NewConnsAcceptSubStreams(context.Background(), time.Minute)
			defer openConnSubStreamsClose()
			openSubStreams := quicstream.NewFuncOpenSubStreams(openConnSubStreams)

			_, err = requestHandler.StreamHandler(openSubStreams)(rctx, rstream)
			t.NoError(err)

			select {
			case <-time.After(time.Second):
				t.Fail("wait accept handler done")
			case err := <-handlerdone:
				t.NoError(err)
			}

			rops := done()
			t.Equal(len(ops), len(rops))

			slices.SortFunc(ops, func(a, b dop5.OperationFact) int { return strings.Compare(a.Hash().String(), b.Hash().String()) })
			slices.SortFunc(rops, func(a, b dop5.OperationFact) int { return strings.Compare(a.Hash().String(), b.Hash().String()) })

			for i, fact := range slices.All(ops) {
				base.EqualFact(t.Assert(), fact, rops[i])
			}
		},
		goleak.IgnoreTopFunction("github.com/spikeekips/mitum/example/test364_5/util.NewUnlimitedChans[...].func2"),
		goleak.IgnoreTopFunction("github.com/spikeekips/mitum/example/test364_5/util.NewUnlimitedChans[...].func4"),
	)
}

func (t *testHandlerPreloadOps) TestOK() {
	t.run("empty", 0)
	t.run("1", 1)
	t.run("33", 33)
}

func TestHandlerPreloadOps(t *testing.T) {
	suite.Run(t, new(testHandlerPreloadOps))
}
