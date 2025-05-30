package main

import (
	"context"
	"io"
	"iter"
	"maps"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	"github.com/spikeekips/mitum/example/test364_5/quicstream"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

var handlerPrefixesString = map[string]struct{}{
	"manager_start_worker": {},
	"_manager_stop_worker": {},
	"_worker_ping_manager": {},
	"_worker_preload_ops":  {},
}

var workHandlerPrefixesString = map[string]struct{}{
	"manager_start_worker":        {},
	"manager_send_ops":            {},
	"manager_worker_owners":       {},
	"manager_worker_result":       {},
	"manager_worker_error":        {},
	"manager_worker_insert_done":  {},
	"manager_close_work":          {},
	"manager_cancel_work":         {},
	"worker_distribute_smvs":      {},
	"worker_unmerged":             {},
	"_manager_wait_worker_closed": {},
}

func requestHandlerStartWorker(
	workID string, height base.Height, workers, opsWorkers []string,
	opsShare, statesShare, maxOpsSize uint64,
) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		if err := frame.WriteFixed(stream, []byte(workID)); err != nil {
			return ctx, err
		}

		if err := frame.WriteFixed(stream, height.Bytes()); err != nil {
			return ctx, err
		}

		if err := writeFixedFrameStringSlice(stream, workers); err != nil {
			return ctx, err
		}

		if err := writeFixedFrameStringSlice(stream, opsWorkers); err != nil {
			return ctx, err
		}

		if err := dutil.WriteInts(stream, opsShare, statesShare, maxOpsSize); err != nil {
			return ctx, err
		}

		var rerr error
		switch _, err := readError(stream, &rerr); {
		case err != nil:
			return ctx, err
		case rerr != nil:
			return ctx, rerr
		default:
			return ctx, nil
		}
	}
}

type startWorkerFunc = func(
	workID string,
	height base.Height,
	workers, opsWorkers []string,
	opsShare, statesShare, maxOpsSize uint64,
) error

func acceptHandlerStartWorker(f startWorkerFunc) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var workID string
		if _, err := ensureReadFixedFrameString(stream, &workID); err != nil {
			return ctx, err
		}

		var height base.Height
		if _, err := frame.EnsureReadFixedValue(stream, &height, base.ParseHeightBytes); err != nil {
			return ctx, err
		}

		var workers []string
		if err := readFixedFrameStringSlice(stream, &workers); err != nil {
			return ctx, err
		}

		var opsWorkers []string
		if err := readFixedFrameStringSlice(stream, &opsWorkers); err != nil {
			return ctx, err
		}

		var opsShare, statesShare, maxOpsSize uint64

		if err := dutil.ReadInts(stream, &opsShare, &statesShare, &maxOpsSize); err != nil && !errors.Is(err, io.EOF) {
			return ctx, err
		}

		return ctx, writeError(stream, f(workID, height, workers, opsWorkers, opsShare, statesShare, maxOpsSize))
	}
}

func requestHandlerSendOps(
	ctx context.Context,
	maxItems, maxSubStreamItems uint64,
	maxSubStreamsSize uint16,
) (
	dop5.OpindexOpHashesFunc,
	dutil.InErrorFunc[error],
	quicstream.OpenSubStreamsHandler,
) {
	var add dutil.InErrorFunc[[3]interface{}]
	addDone := func() {}
	seq := func(func([3]interface{}) bool) {}

	if maxItems > 0 {
		add, addDone, seq = dutil.NewUnlimitedChan[[3]interface{}](ctx, maxItems, maxItems)
	}

	done, wait := dutil.NewContextDone(ctx)

	handler := func(
		ctx context.Context,
		stream quic.Stream,
		openSubStreams quicstream.OpenSubStreamsHandlerOpenSubStreamsFunc,
	) (context.Context, error) {
		switch err := dutil.WriteInt(stream, maxItems); {
		case err != nil:
			return ctx, err
		case maxItems < 1:
			return ctx, nil
		}

		nsub := subStreamSize(maxItems, maxSubStreamItems, maxSubStreamsSize)
		nsub--

		var subStreams dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]]

		switch f, err := openSubStreams(ctx, nsub); {
		case err != nil:
			return ctx, err
		case nsub < 1:
			subStreams = func(f dutil.InErrorFunc[[]quic.Stream]) error { return f(nil) }
		default:
			subStreams = f
		}

		return ctx, subStreams(func(streams []quic.Stream) error {
			defer func() { _ = stream.Close() }()

			nstreams := make([]quic.Stream, len(streams)+1)
			copy(nstreams, streams)
			nstreams[len(streams)] = stream

			for i, v := range dutil.SeqAll(seq) {
				sub := nstreams[i%len(nstreams)]

				if err := dutil.WriteInt(sub, v[0].(uint64)); err != nil { //nolint:forcetypeassert
					return err
				}

				if err := writeFixedFrameHashes(sub, v[1].(util.Hash), v[2].(util.Hash)); err != nil { //nolint:forcetypeassert
					return err
				}
			}

			return nil
		})
	}

	return func(opindex uint64, oph, fact util.Hash) error {
			return add([3]interface{}{opindex, oph, fact})
		},
		func(err error) error {
			addDone()

			if err != nil {
				return done(err)
			}

			return wait()
		},
		func(
			ctx context.Context,
			stream quic.Stream,
			openSubStreams quicstream.OpenSubStreamsHandlerOpenSubStreamsFunc,
		) (context.Context, error) {
			ctx, err := handler(ctx, stream, openSubStreams)

			return ctx, done(err)
		}
}

func acceptHandlerSendOps(
	w func(uint64, iter.Seq2[uint64, [2]util.Hash]) error,
) quicstream.AcceptSubStreamHandler {
	return func(
		ctx context.Context,
		stream quic.Stream,
		accept quicstream.AcceptSubStreamsHandlerOpenSubStreamsFunc,
	) (context.Context, error) {
		var maxItems uint64

		switch err := dutil.ReadInt(stream, &maxItems); {
		case err != nil:
			return ctx, err
		case maxItems < 1:
			return ctx, w(maxItems, func(yield func(uint64, [2]util.Hash) bool) {})
		}

		var subStreams dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]]

		switch s, err := accept(ctx); {
		case err != nil:
			return ctx, err
		default:
			subStreams = s
		}

		add, addDone, seq := dutil.NewUnlimitedChan[[3]interface{}](ctx, maxItems, maxItems)

		done, wait := dutil.NewContextDone(ctx)

		go func() {
			_ = done(subStreams(func(streams []quic.Stream) error {
				nstreams := make([]quic.Stream, len(streams)+1)
				copy(nstreams, streams)
				nstreams[len(streams)] = stream

				return jobsSubStreams(ctx, nstreams, func(_ context.Context, sub quic.Stream) error {
					return readSendOpStream(sub, func(opindex uint64, oph, fact util.Hash) error {
						_ = add([3]interface{}{opindex, oph, fact})

						return nil
					})
				})
			}))

			addDone()
		}()

		if err := w(maxItems, func(yield func(uint64, [2]util.Hash) bool) {
			for v := range seq {
				if !yield(v[0].(uint64), [2]util.Hash{v[1].(util.Hash), v[2].(util.Hash)}) { //nolint:forcetypeassert
					return
				}
			}
		}); err != nil {
			return ctx, err
		}

		return ctx, wait()
	}
}

func readSendOpStream(r io.Reader, f dop5.OpindexOpHashesFunc) error {
	for {
		var opindex uint64

		switch err := dutil.ReadInt(r, &opindex); {
		case err != nil && !errors.Is(err, io.EOF):
			return err
		case errors.Is(err, io.EOF):
			return nil
		}

		var oph util.Hash

		if _, err := ensureReadFixedFrameHash(r, &oph); err != nil {
			return err
		}

		var fact util.Hash

		switch _, err := ensureReadFixedFrameHash(r, &fact); {
		case err != nil && !errors.Is(err, io.EOF):
			return err
		default:
			if ferr := f(opindex, oph, fact); ferr != nil {
				return ferr
			}

			if errors.Is(err, io.EOF) {
				return nil
			}
		}
	}
}

func requestHandlerWorkerOwners(local string, owners []string) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		if err := frame.WriteFixed(stream, []byte(local)); err != nil {
			return ctx, err
		}

		return ctx, writeFixedFrameStringSlice(stream, owners)
	}
}

func acceptHandlerWorkerOwners(f func(string, []string) error) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var worker string
		if _, err := ensureReadFixedFrameString(stream, &worker); err != nil {
			return ctx, err
		}

		var owners []string
		if err := readFixedFrameStringSlice(stream, &owners); err != nil && !errors.Is(err, io.EOF) {
			return ctx, err
		}

		return ctx, f(worker, owners)
	}
}

func requestHandlerWorkerResult(
	local string,
	reasons map[uint64]string,
	statesSize uint64,
	statesf dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
	maxSubStreamItems uint64,
	maxSubStreamsSize uint16,
) quicstream.OpenSubStreamsHandler {
	return func(
		ctx context.Context,
		stream quic.Stream,
		openSubStreams quicstream.OpenSubStreamsHandlerOpenSubStreamsFunc,
	) (context.Context, error) {
		if err := frame.WriteFixed(stream, []byte(local)); err != nil {
			return ctx, err
		}

		if ok, _, err := readOk(stream); err != nil || !ok {
			return ctx, err
		}

		if err := dutil.WriteInt(stream, statesSize); err != nil {
			return ctx, err
		}

		nsub := subStreamSize(
			max(statesSize, uint64(len(reasons))),
			maxSubStreamItems,
			maxSubStreamsSize,
		)
		nsub--

		var subStreams dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]]

		switch f, err := openSubStreams(ctx, nsub); {
		case err != nil:
			return ctx, err
		case nsub < 1:
			subStreams = func(f dutil.InErrorFunc[[]quic.Stream]) error { return f(nil) }
		default:
			subStreams = f
		}

		return ctx, subStreams(func(streams []quic.Stream) error {
			defer func() { _ = stream.Close() }()

			nstreams := make([]quic.Stream, len(streams)+1)
			copy(nstreams, streams)
			nstreams[len(streams)] = stream

			seq := maps.All(reasons)

			rn := int(math.Ceil(float64(len(reasons)) / float64(len(nstreams))))

			iterNext, iterStop := iter.Pull2(seq)
			defer iterStop()

		end:
			for sub := range slices.Values(nstreams) {
				writehead := sync.OnceValue(func() error {
					return frame.WriteFixed(sub, []byte{workResultDelmReason}) // start reason
				})

				for range rn {
					opindex, reason, ok := iterNext()
					if !ok {
						break end
					}

					if err := writehead(); err != nil {
						return err
					}

					if err := frame.WriteFixed(sub, []byte(reason)); err != nil {
						return err
					}

					if err := dutil.WriteInt(sub, opindex); err != nil {
						return err
					}
				}
			}

			if statesSize < 1 {
				return nil
			}

			var sn uint64

			headWritten := dutil.NewSingleLockedMap[uint64, struct{}]()
			defer headWritten.Close()

			return statesf(func(st base.State) error {
				i := atomic.AddUint64(&sn, 1)
				p := i % uint64(len(nstreams))
				sub := nstreams[p]

				if _, err := headWritten.Set(p, func(_ struct{}, found bool) (struct{}, error) {
					if found {
						return struct{}{}, dutil.ErrLockedSetIgnore
					}

					return struct{}{}, frame.WriteFixed(sub, []byte{workResultDelmStates}) // start states
				}); err != nil {
					return err
				}

				if err := frame.WriteFixeds(sub, []byte(st.Key()), st.Hash().Bytes()); err != nil {
					return err
				}

				return nil
			})
		})
	}
}

func acceptHandlerWorkerResult(f dop5.ManagerInsertWorkerResultFunc) quicstream.AcceptSubStreamHandler {
	return func(
		ctx context.Context,
		stream quic.Stream,
		accept quicstream.AcceptSubStreamsHandlerOpenSubStreamsFunc,
	) (context.Context, error) {
		var worker string
		if _, err := ensureReadFixedFrameString(stream, &worker); err != nil {
			return ctx, err
		}

		var setReasons dutil.InErrorFunc[map[uint64]string]
		var setState dutil.In2ErrorFunc[string, util.Hash]
		var setDone dutil.InErrorFunc[error]

		switch r, s, d, err := f(worker); {
		case err != nil:
			return ctx, err
		case r == nil:
			return ctx, writeOk(stream, false)
		default:
			if err := writeOk(stream, true); err != nil {
				return ctx, err
			}

			setReasons, setState, setDone = r, s, d
		}

		var statesSize uint64
		if err := dutil.ReadInt(stream, &statesSize); err != nil {
			return ctx, err
		}

		var subStreams dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]]

		switch s, err := accept(ctx); {
		case err != nil:
			return ctx, err
		default:
			subStreams = s
		}

		return ctx, subStreams(func(streams []quic.Stream) error {
			defer func() { _ = setDone(nil) }()

			nstreams := make([]quic.Stream, len(streams)+1)
			copy(nstreams, streams)
			nstreams[len(streams)] = stream

			reasons := dutil.NewSingleLockedMap[uint64, string]()
			defer reasons.Close()

			if err := jobsSubStreams(ctx, nstreams, func(_ context.Context, sub quic.Stream) error {
				return readSubStreamWorkResult(sub, reasons.SetValue, setState)
			}); err != nil {
				return err
			}

			return setReasons(maps.Clone(reasons.Map()))
		})
	}
}

const (
	workResultDelmReason = 0x01
	workResultDelmStates = 0x02
)

func readSubStreamWorkResult(
	stream quic.Stream,
	setReason func(uint64, string) bool,
	setState dutil.In2ErrorFunc[string, util.Hash],
) error {
	var delm byte

	switch b, n, err := frame.ReadFixed(stream); {
	case err != nil && !errors.Is(err, io.EOF):
		return err
	case n < 1 && errors.Is(err, io.EOF):
		return nil
	case len(b) < 1:
		return errors.New("empty delm")
	default:
		delm = b[0]

		if delm != workResultDelmReason && delm != workResultDelmStates {
			return errors.New("wrong delm")
		}

		if errors.Is(err, io.EOF) {
			return err
		}
	}

	if delm == workResultDelmReason {
	end0:
		for {
			var reason string

			switch b, n, err := frame.ReadFixed(stream); {
			case err != nil && !errors.Is(err, io.EOF):
				return err
			case n < 1 && errors.Is(err, io.EOF):
				return nil
			case len(b) < 1:
				return errors.New("empty delm")
			case len(b) == 1:
				if b[0] != workResultDelmStates {
					return errors.New("wrong delm; states")
				}

				break end0
			default:
				reason = string(b)
			}

			var opindex uint64
			if err := dutil.ReadInt(stream, &opindex); err != nil {
				return err
			}

			if !setReason(opindex, reason) {
				return errors.New("failed to set reason")
			}
		}
	}

end1:
	for {
		var key string
		switch n, err := readFixedFrameString(stream, &key); {
		case err != nil && !errors.Is(err, io.EOF):
			return err
		case n < 1 && errors.Is(err, io.EOF):
			break end1
		}

		var h util.Hash
		switch _, err := ensureReadFixedFrameHash(stream, &h); {
		case err != nil && !errors.Is(err, io.EOF):
			return err
		default:
			if err := setState(key, h); err != nil {
				return err
			}

			if errors.Is(err, io.EOF) {
				break end1
			}
		}
	}

	return nil
}

func requestHandlerWorkerErr(local string, err error) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		if err := frame.WriteFixed(stream, []byte(local)); err != nil {
			return ctx, err
		}

		var errb []byte
		if err != nil {
			errb = []byte(err.Error())
		}

		return ctx, frame.WriteFixed(stream, errb)
	}
}

func acceptHandlerWorkerErr(f dutil.In2ErrorFunc[string, error]) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var worker string
		if _, err := ensureReadFixedFrameString(stream, &worker); err != nil {
			return ctx, err
		}

		var errs string
		if _, err := ensureReadFixedFrameString(stream, &errs); err != nil {
			return ctx, err
		}

		return ctx, f(worker, errors.New(errs))
	}
}

func requestHandlerWorkerInsertDone() quicstream.StreamHandler {
	return func(ctx context.Context, _ quic.Stream) (context.Context, error) {
		return ctx, nil
	}
}

func acceptHandlerWorkerInsertDone(f dutil.ErrorFunc) quicstream.StreamHandler {
	return func(ctx context.Context, _ quic.Stream) (context.Context, error) {
		return ctx, f()
	}
}

func requestHandlerCloseWork(f dutil.InErrorFunc[error]) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var rerr error
		switch _, err := readError(stream, &rerr); {
		case err != nil:
			return ctx, err
		default:
			return ctx, f(rerr)
		}
	}
}

func acceptHandlerCloseWork(f dutil.ErrorFunc) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		return ctx, writeError(stream, f())
	}
}

func requestHandlerCancelWork(err error) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		return ctx, writeError(stream, err)
	}
}

func acceptHandlerCancelWork(f dutil.InErrorFunc[error]) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var rerr error
		switch _, err := readError(stream, &rerr); {
		case err != nil:
			return ctx, err
		default:
			return ctx, f(rerr)
		}
	}
}

func requestHandlerWaitWorkerClosed() quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var rerr error
		switch _, err := readError(stream, &rerr); {
		case err != nil:
			return ctx, err
		default:
			return ctx, rerr
		}
	}
}

func acceptHandlerWaitWorkerClosed(f dutil.ErrorFunc) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		return ctx, writeError(stream, f())
	}
}

func requestHandlerStopWorker() quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var rerr error
		if _, err := readError(stream, &rerr); err != nil {
			return ctx, err
		}

		return ctx, rerr
	}
}

func acceptHandlerStopWorker(f func() error) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		return ctx, writeError(stream, f())
	}
}

func requestHandlerWorkerDistributeSMVs(
	ctx context.Context,
	enc encoder.Encoder,
	local string,
	maxItems, maxSubStreamItems uint64,
	maxSubStreamsSize uint16,
) (
	dop5.WorkerProcessInsertSMVsFunc,
	dutil.In2ErrorFunc[[]string, []string],
	quicstream.OpenSubStreamsHandler,
) {
	chansize := maxItems
	if maxItems < 1 {
		chansize = maxSubStreamItems
	}

	addValues, addValuesDone, seq := dutil.NewUnlimitedChan[[3]interface{}](ctx, chansize, chansize)

	var notOK atomic.Value

	cancelf := func(err error) error {
		notOK.Store(true)

		addValuesDone()

		_, stop := iter.Pull(seq)
		stop()

		return err
	}

	seqHandler := func(
		ctx context.Context,
		openSubStreams quicstream.OpenSubStreamsHandlerOpenSubStreamsFunc,
	) (context.Context, error) {
		nsub := subStreamSize(maxItems, maxSubStreamItems, maxSubStreamsSize)

		var subStreams dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]]

		switch f, err := openSubStreams(ctx, nsub); {
		case err != nil:
			return ctx, err
		case nsub < 1:
			subStreams = func(f dutil.InErrorFunc[[]quic.Stream]) error { return f(nil) }
		default:
			subStreams = f
		}

		return ctx, subStreams(func(streams []quic.Stream) error {
			semSize := dutil.SafeUint64ToInt64(maxItems)
			if semSize < 1 {
				semSize = 1 << 9 //nolint:mnd
			}

			var newJob dutil.JobWorkerNewJobFunc
			var jobDone func()
			var jobWait dutil.InErrorFunc[error]

			switch n, d, w, err := defaultNewFuncJobWorker(ctx, semSize); {
			case err != nil:
				return err
			default:
				newJob, jobDone, jobWait = n, d, w
			}

			for i, v := range dutil.SeqAll(seq) {
				if err := newJob(func(context.Context) error {
					return writeSMVBundle(
						streams[i%len(streams)],
						enc,
						v[0].(uint64),     //nolint:forcetypeassert
						v[1].(util.Hash),  //nolint:forcetypeassert
						v[2].([]dop5.SMV), //nolint:forcetypeassert
					)
				}); err != nil {
					return err
				}
			}

			jobDone()

			return jobWait(nil)
		})
	}

	var subs, affected []string

	handler := func(
		ctx context.Context,
		stream quic.Stream,
		openSubStreams quicstream.OpenSubStreamsHandlerOpenSubStreamsFunc,
	) (context.Context, error) {
		if err := frame.WriteFixed(stream, []byte(local)); err != nil {
			return ctx, err
		}

		if ok, _, err := readOk(stream); err != nil || !ok {
			return ctx, cancelf(err)
		}

		if err := dutil.WriteInt(stream, maxItems); err != nil {
			return ctx, err
		}

		if _, err := seqHandler(ctx, openSubStreams); err != nil {
			return ctx, err
		}

		if err := writeFixedFrameStringSlice(stream, subs); err != nil {
			return ctx, err
		}

		return ctx, writeFixedFrameStringSlice(stream, affected)
	}

	return func(opindex uint64, facth util.Hash, values []dop5.SMV) error {
			if v := notOK.Load(); v != nil && v.(bool) { //nolint:forcetypeassert
				return nil
			}

			switch err := addValues([3]interface{}{opindex, facth, values}); {
			case err == nil,
				!errors.Is(err, dutil.ErrContextUnlimitedChanInsertDone):
				return err
			default:
				if v := notOK.Load(); v != nil && v.(bool) { //nolint:forcetypeassert
					return nil
				}

				return err
			}
		},
		func(rsubs, raffected []string) error {
			if v := notOK.Load(); v != nil && v.(bool) { //nolint:forcetypeassert
				return nil
			}

			subs, affected = rsubs, raffected

			addValuesDone()

			return nil
		},
		func(
			ctx context.Context,
			stream quic.Stream,
			openSubStreams quicstream.OpenSubStreamsHandlerOpenSubStreamsFunc,
		) (context.Context, error) {
			ctx, err := handler(ctx, stream, openSubStreams)
			if err != nil {
				return ctx, cancelf(err)
			}

			return ctx, nil
		}
}

func acceptHandlerWorkerDistributeSMVs(
	enc encoder.Encoder,
	w dop5.WorkerProcessWorkerInsertSMVsFunc,
) quicstream.AcceptSubStreamHandler {
	return func(
		ctx context.Context,
		stream quic.Stream,
		accept quicstream.AcceptSubStreamsHandlerOpenSubStreamsFunc,
	) (context.Context, error) {
		var worker string
		if _, err := ensureReadFixedFrameString(stream, &worker); err != nil {
			return ctx, err
		}

		var insertValues dop5.WorkerProcessInsertSMVsFunc
		var insertOwners dutil.In2ErrorFunc[[]string, []string]

		switch v, o, err := w(worker); {
		case err != nil:
			return ctx, err
		case v == nil:
			return ctx, writeOk(stream, false)
		default:
			if err := writeOk(stream, true); err != nil {
				return ctx, err
			}

			insertValues, insertOwners = v, o
		}

		var maxItems uint64
		if err := dutil.ReadInt(stream, &maxItems); err != nil {
			return ctx, err
		}

		var subStreams dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]]

		switch s, err := accept(ctx); {
		case err != nil:
			return ctx, err
		default:
			subStreams = s
		}

		var newJob dutil.JobWorkerNewJobFunc
		var jobDone func()
		var jobWait dutil.InErrorFunc[error]

		semSize := dutil.SafeUint64ToInt64(maxItems)
		if semSize < 1 {
			semSize = 1 << 9 //nolint:mnd
		}

		switch n, d, w, err := defaultNewFuncJobWorker(ctx, semSize); {
		case err != nil:
			return ctx, err
		default:
			newJob, jobDone, jobWait = n, d, w
		}

		if err := subStreams(func(streams []quic.Stream) error {
			defer jobDone()

			return jobsSubStreams(ctx, streams, func(_ context.Context, sub quic.Stream) error {
				for {
					switch b, n, err := frame.ReadFixed(sub); {
					case err != nil && !errors.Is(err, io.EOF):
						return err
					case n < 1 && errors.Is(err, io.EOF):
						return nil
					default:
						if err := newJob(func(context.Context) error {
							switch opindex, fact, values, err := decodeSMVBundle(b, enc); {
							case err != nil:
								return err
							default:
								return insertValues(opindex, fact, values)
							}
						}); err != nil {
							return err
						}

						if errors.Is(err, io.EOF) {
							return nil
						}
					}
				}
			})
		}); err != nil {
			return ctx, jobWait(err)
		}

		if err := jobWait(nil); err != nil {
			return ctx, err
		}

		var subs []string
		if err := readFixedFrameStringSlice(stream, &subs); err != nil {
			return ctx, err
		}

		var affected []string
		if err := readFixedFrameStringSlice(stream, &affected); err != nil {
			return ctx, err
		}

		return ctx, insertOwners(subs, affected)
	}
}

func requestHandlerWorkerUnmerged(local string, unmerged []uint64) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		if err := frame.WriteFixed(stream, []byte(local)); err != nil {
			return ctx, err
		}

		return ctx, frame.WriteFixedSlice(stream, dutil.SafeIntToUint32(len(unmerged)), func(yield func([]byte, error) bool) {
			for opindex := range slices.Values(unmerged) {
				b, err := dutil.EncodeInt(opindex)
				if !yield(b, err) {
					return
				}
			}
		})
	}
}

func acceptHandlerWorkerUnmerged(f func(string, []uint64) error) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var worker string
		if _, err := ensureReadFixedFrameString(stream, &worker); err != nil {
			return ctx, err
		}

		switch n, seq, err := frame.ReadFixedSlice(stream); {
		case err != nil:
			return ctx, err
		case n < 1:
			return ctx, f(worker, nil)
		default:
			unmerged := make([]uint64, n)

			var i int
			for b, err := range seq {
				if err != nil && errors.Is(err, io.EOF) {
					return ctx, err
				}

				if err := dutil.DecodeInt(b, &unmerged[i]); err != nil {
					return ctx, err
				}

				i++
			}

			if i < int(n) {
				return ctx, errors.Wrap(frame.ErrInsufficientRead, "unmerged")
			}

			return ctx, f(worker, unmerged)
		}
	}
}

func requestHandlerWorkerPingManager(local string) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		if err := frame.WriteFixed(stream, []byte(local)); err != nil {
			return ctx, err
		}

		var gerr error
		if _, err := readError(stream, &gerr); err != nil {
			return ctx, err
		}

		return ctx, gerr
	}
}

func acceptHandlerWorkerPingManager(f dutil.InErrorFunc[string]) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var worker string
		if _, err := ensureReadFixedFrameString(stream, &worker); err != nil {
			return ctx, err
		}

		return ctx, writeError(stream, f(worker))
	}
}

func subStreamSize(n, maxPerStream uint64, nmax uint16) uint16 {
	if n <= maxPerStream {
		return 1
	}

	var nsub uint16

	switch i := uint64(math.Ceil(float64(n) / float64(maxPerStream))); {
	case i > math.MaxUint16:
		nsub = math.MaxUint16
	case nmax > 0 && i > uint64(nmax):
		nsub = nmax
	default:
		nsub = uint16(i)
	}

	return nsub
}

func jobsSubStreams(
	ctx context.Context,
	streams []quic.Stream,
	f dutil.In2ErrorFunc[context.Context, quic.Stream],
) error {
	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := defaultNewFuncJobWorker(ctx, int64(len(streams))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	for sub := range slices.Values(streams) {
		if err := newJob(func(ctx context.Context) error {
			return f(ctx, sub)
		}); err != nil {
			return err
		}
	}

	jobDone()

	return jobWait(nil)
}

func requestHandlerPreloadOps(
	enc encoder.Encoder,
	maxItems, maxSubStreamItems uint64,
	maxSubStreamsSize uint16,
	seq iter.Seq[dop5.OperationFact],
) quicstream.OpenSubStreamsHandler {
	return func(
		ctx context.Context,
		stream quic.Stream,
		openSubStreams quicstream.OpenSubStreamsHandlerOpenSubStreamsFunc,
	) (context.Context, error) {
		switch err := dutil.WriteInt(stream, maxItems); {
		case err != nil:
			return ctx, err
		case maxItems < 1:
			return ctx, nil
		}

		nsub := subStreamSize(maxItems, maxSubStreamItems, maxSubStreamsSize)
		nsub--

		var subStreams dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]]

		switch f, err := openSubStreams(ctx, nsub); {
		case err != nil:
			return ctx, err
		case nsub < 1:
			subStreams = func(f dutil.InErrorFunc[[]quic.Stream]) error { return f(nil) }
		default:
			subStreams = f
		}

		var newJob dutil.JobWorkerNewJobFunc
		var jobDone func()
		var jobWait dutil.InErrorFunc[error]

		switch n, d, w, err := defaultNewFuncJobWorker(ctx, dutil.SafeUint64ToInt64(maxItems)); {
		case err != nil:
			return ctx, err
		default:
			newJob, jobDone, jobWait = n, d, w
		}

		next, stop := iter.Pull(seq)
		defer stop()

		return ctx, subStreams(func(streams []quic.Stream) error {
			defer func() { _ = stream.Close() }()

			nstreams := make([]quic.Stream, len(streams)+1)
			copy(nstreams, streams)
			nstreams[len(streams)] = stream

			var i int
			for {
				fact, ok := next()
				if !ok {
					break
				}

				sub := nstreams[i%len(nstreams)]

				if err := newJob(func(context.Context) error {
					return writePreLoadOp(sub, enc, fact)
				}); err != nil {
					return err
				}

				i++
			}

			jobDone()

			return jobWait(nil)
		})
	}
}

func writePreLoadOp(w io.Writer, enc encoder.Encoder, fact dop5.OperationFact) error {
	switch b, err := enc.Marshal(fact); {
	case err != nil:
		return err
	default:
		return frame.WriteFixed(w, b)
	}
}

func acceptHandlerPreloadOps(
	enc encoder.Encoder,
	w dutil.InErrorFunc[dop5.OperationFact],
) quicstream.AcceptSubStreamHandler {
	return func(
		ctx context.Context,
		stream quic.Stream,
		accept quicstream.AcceptSubStreamsHandlerOpenSubStreamsFunc,
	) (context.Context, error) {
		var maxItems uint64

		switch err := dutil.ReadInt(stream, &maxItems); {
		case err != nil:
			return ctx, err
		case maxItems < 1:
			return ctx, nil
		}

		var subStreams dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]]

		switch s, err := accept(ctx); {
		case err != nil:
			return ctx, err
		default:
			subStreams = s
		}

		var newJob dutil.JobWorkerNewJobFunc
		var jobDone func()
		var jobWait dutil.InErrorFunc[error]

		switch n, d, w, err := defaultNewFuncJobWorker(ctx, dutil.SafeUint64ToInt64(maxItems)); {
		case err != nil:
			return ctx, err
		default:
			newJob, jobDone, jobWait = n, d, w
		}

		if err := subStreams(func(streams []quic.Stream) error {
			nstreams := make([]quic.Stream, len(streams)+1)
			copy(nstreams, streams)
			nstreams[len(streams)] = stream

			return jobsSubStreams(ctx, nstreams, func(_ context.Context, sub quic.Stream) error {
				for {
					switch b, n, err := frame.ReadFixed(sub); {
					case err != nil && !errors.Is(err, io.EOF):
						return err
					case n < 1 && errors.Is(err, io.EOF):
						return nil
					default:
						if err := newJob(func(context.Context) error {
							var fact dop5.OperationFact
							if err := encoder.Unmarshal(enc, b, &fact); err != nil {
								return err
							}

							return w(fact)
						}); err != nil {
							return err
						}
					}
				}
			})
		}); err != nil {
			return ctx, jobWait(err)
		}

		jobDone()

		return ctx, jobWait(nil)
	}
}
