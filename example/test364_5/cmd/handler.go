package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"maps"
	"math"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	"github.com/spikeekips/mitum/example/test364_5/quicstream"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

var handlerPrefixesString = map[string]struct{}{
	"manager_start_worker": {},
	"manager_stop_worker":  {},
}

var workHandlerPrefixesString = map[string]struct{}{
	"manager_start_worker":       {},
	"manager_send_ops":           {},
	"manager_worker_owners":      {},
	"manager_worker_result":      {},
	"manager_worker_error":       {},
	"manager_worker_insert_done": {},
	"manager_worker_close":       {},
	"manager_cancel_worker":      {},
	"manager_wait_worker_closed": {},
	"worker_distribute_smvs":     {},
}

func requestHandlerStartWorker(
	workID string, workers, opsWorkers []string,
	opsShare, statesShare, maxOpsSize uint64,
) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		if err := frame.WriteFixed(stream, []byte(workID)); err != nil {
			return ctx, err
		}

		if err := writeFixedFrameStringSlice(stream, workers); err != nil {
			return ctx, err
		}

		if err := writeFixedFrameStringSlice(stream, opsWorkers); err != nil {
			return ctx, err
		}

		if err := writeUint64s(stream, opsShare, statesShare, maxOpsSize); err != nil {
			return ctx, err
		}

		return ctx, nil
	}
}

type startWorkerFunc = func(
	workID string,
	workers, opsWorkers []string,
	opsShare, statesShare, maxOpsSize uint64,
) error

func acceptHandlerStartWorker(f startWorkerFunc) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var workID string
		if _, err := ensureReadFixedFrameString(stream, &workID); err != nil {
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

		if err := readUint64s(stream, &opsShare, &statesShare, &maxOpsSize); err != nil && !errors.Is(err, io.EOF) {
			return ctx, err
		}

		return ctx, f(workID, workers, opsWorkers, opsShare, statesShare, maxOpsSize)
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
		add, addDone, seq = dutil.NewContextUnlimitedChans[[3]interface{}](ctx, 1<<9, 1<<9)
	}

	done, wait := dutil.NewContextDone(ctx)

	handler := func(
		ctx context.Context,
		stream quic.Stream,
		openSubStreams quicstream.OpenSubStreamsHandlerOpenSubStreamsFunc,
	) (context.Context, error) {
		switch err := writeUint64(stream, maxItems); {
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
			defer stream.Close()

			nstreams := make([]quic.Stream, len(streams)+1)
			copy(nstreams, streams)
			nstreams[len(streams)] = stream

			for i, v := range dutil.SeqAll(seq) {
				sub := nstreams[i%len(nstreams)]

				if err := writeUint64(sub, v[0].(uint64)); err != nil {
					return err
				}

				if err := writeFixedFrameHashes(sub, v[1].(util.Hash), v[2].(util.Hash)); err != nil {
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

		switch _, err := readUint64(stream, &maxItems); {
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

		add, addDone, seq := dutil.NewContextUnlimitedChans[[3]interface{}](ctx, 1<<9, 1<<9)

		done, wait := dutil.NewContextDone(ctx)

		go func() {
			_ = done(subStreams(func(streams []quic.Stream) error {
				nstreams := make([]quic.Stream, len(streams)+1)
				copy(nstreams, streams)
				nstreams[len(streams)] = stream

				return jobsSubStreams(ctx, nstreams, func(_ context.Context, sub quic.Stream) error {
					return readSendOpStream(sub, func(opindex uint64, oph, fact util.Hash) error {
						add([3]interface{}{opindex, oph, fact})

						return nil
					})
				})
			}))

			addDone()
		}()

		if err := w(maxItems, func(yield func(uint64, [2]util.Hash) bool) {
			for v := range seq {
				if !yield(v[0].(uint64), [2]util.Hash{v[1].(util.Hash), v[2].(util.Hash)}) {
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

		switch _, err := readUint64(r, &opindex); {
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

func requestHandlerWorkerOwners(worker string, owners []string) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		if err := frame.WriteFixed(stream, []byte(worker)); err != nil {
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
	worker string,
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
		if err := frame.WriteFixed(stream, []byte(worker)); err != nil {
			return ctx, err
		}

		if err := writeUint64(stream, statesSize); err != nil {
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
			defer stream.Close()

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
					return frame.WriteFixed(sub, []byte{0x01}) // start reason
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

					if err := writeUint64(sub, opindex); err != nil {
						return err
					}
				}
			}

			if statesSize < 1 {
				return nil
			}

			var sn int

			headWritten := dutil.NewSingleLockedMap[int, struct{}]()
			defer headWritten.Close()

			return statesf(func(st base.State) error {
				p := sn % len(nstreams)
				sub := nstreams[p]

				if _, err := headWritten.Set(p, func(_ struct{}, found bool) (struct{}, error) {
					if found {
						return struct{}{}, dutil.ErrLockedSetIgnore
					}

					return struct{}{}, frame.WriteFixed(sub, []byte{0x02}) // start states
				}); err != nil {
					return err
				}

				if err := frame.WriteFixed(sub, []byte(st.Key())); err != nil {
					return err
				}

				if err := frame.WriteFixed(sub, st.Hash().Bytes()); err != nil {
					return err
				}

				sn++

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

		var statesSize uint64
		if _, err := readUint64(stream, &statesSize); err != nil {
			return ctx, err
		}

		var subStreams dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]]

		switch s, err := accept(ctx); {
		case err != nil:
			return ctx, err
		default:
			subStreams = s
		}

		var setReasons dutil.InErrorFunc[map[uint64]string]
		var setState dutil.In2ErrorFunc[string, util.Hash]
		var setDone dutil.InErrorFunc[error]

		switch r, s, d, err := f(worker); {
		case err != nil:
			return ctx, err
		default:
			setReasons, setState, setDone = r, s, d
		}

		return ctx, subStreams(func(streams []quic.Stream) error {
			defer setDone(nil)

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

			return setReasons(reasons.Map())
		})
	}
}

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
		return errors.Errorf("empty delm")
	default:
		delm = b[0]

		if delm != 0x01 && delm != 0x02 {
			return errors.Errorf("wrong delm")
		}

		if errors.Is(err, io.EOF) {
			return err
		}
	}

	if delm == 0x01 {
	end0:
		for {
			var reason string

			switch b, n, err := frame.ReadFixed(stream); {
			case err != nil && !errors.Is(err, io.EOF):
				return err
			case n < 1 && errors.Is(err, io.EOF):
				return nil
			case len(b) < 1:
				return errors.Errorf("empty delm")
			case len(b) == 1:
				if b[0] != 0x02 {
					return errors.Errorf("wrong delm; states")
				}

				break end0
			default:
				reason = string(b)
			}

			var opindex uint64
			if _, err := readUint64(stream, &opindex); err != nil {
				return err
			}

			if !setReason(opindex, reason) {
				return errors.Errorf("failed to set reason")
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

func requestHandlerWorkerErr(worker string, err error) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		if err := frame.WriteFixed(stream, []byte(worker)); err != nil {
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

		return ctx, f(worker, errors.Errorf(errs))
	}
}

func requestHandlerWorkerInsertDone() quicstream.StreamHandler {
	return func(ctx context.Context, _ quic.Stream) (context.Context, error) {
		return ctx, nil
	}
}

func acceptHandlerWorkerInsertDone() quicstream.StreamHandler {
	return func(ctx context.Context, _ quic.Stream) (context.Context, error) {
		return ctx, nil
	}
}

func requestHandlerWorkerClose(f dutil.InErrorFunc[error]) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var err error

		var errs string
		switch _, eerr := ensureReadFixedFrameString(stream, &errs); {
		case eerr != nil:
			return ctx, eerr
		default:
			err = errors.Errorf(errs)
		}

		return ctx, f(err)
	}
}

func acceptHandlerWorkerClose(f dutil.ErrorFunc) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var errb []byte
		if err := f(); err != nil {
			errb = []byte(err.Error())
		}

		return ctx, frame.WriteFixed(stream, errb)
	}
}

func requestHandlerCancelWorker(err error) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var errb []byte
		if err != nil {
			errb = []byte(err.Error())
		}

		return ctx, frame.WriteFixed(stream, errb)
	}
}

func acceptHandlerCancelWorker(f dutil.InErrorFunc[error]) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var err error
		var errs string

		switch _, err := ensureReadFixedFrameString(stream, &errs); {
		case err != nil:
			return ctx, err
		case errs != "":
			err = errors.Errorf(errs)
		}

		return ctx, f(err)
	}
}

func requestHandlerWaitWorkerClosed() quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var err error
		var errs string

		switch _, err := ensureReadFixedFrameString(stream, &errs); {
		case err != nil:
			return ctx, err
		case errs != "":
			err = errors.Errorf(errs)
		}

		return ctx, err
	}
}

func acceptHandlerWaitWorkerClosed(f dutil.ErrorFunc) quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var errs string
		if err := f(); err != nil {
			errs = err.Error()
		}

		return ctx, frame.WriteFixed(stream, []byte(errs))
	}
}

func requestHandlerStopWorker() quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		return ctx, nil
	}
}

func acceptHandlerStopWorker() quicstream.StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		return ctx, nil
	}
}

func requestHandlerWorkerDistributeSMVs(
	ctx context.Context,
	enc encoder.Encoder,
	worker string,
	maxItems, maxSubStreamItems uint64,
	maxSubStreamsSize uint16,
) (
	dop5.WorkerProcessInsertSMVsFunc,
	dutil.In2ErrorFunc[[]string, []string],
	quicstream.OpenSubStreamsHandler,
) {
	addValues, addValuesDone, seq := dutil.NewContextUnlimitedChans[[3]interface{}](ctx, 1<<9, 1<<9)

	handler := func(
		ctx context.Context,
		stream quic.Stream,
		openSubStreams quicstream.OpenSubStreamsHandlerOpenSubStreamsFunc,
	) (context.Context, error) {
		if err := frame.WriteFixed(stream, []byte(worker)); err != nil {
			return ctx, err
		}

		if err := writeUint64(stream, maxItems); err != nil {
			return ctx, err
		}

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
			var newJob dutil.JobWorkerNewJobFunc
			var jobDone func()
			var jobWait dutil.InErrorFunc[error]

			switch n, d, w, err := dutil.NewFuncJobWorker(ctx, max(maxItems, 1<<9)); {
			case err != nil:
				return err
			default:
				newJob, jobDone, jobWait = n, d, w
			}

			for i, v := range dutil.SeqAll(seq) {
				if err := newJob(func(context.Context) error {
					switch b, err := enc.Marshal(newWorkerSMVsBundleMarshaller(v)); {
					case err != nil:
						return err
					default:
						return frame.WriteFixed(streams[i%len(streams)], b)
					}
				}); err != nil {
					return jobWait(err)
				}
			}

			jobDone()

			return jobWait(nil)
		})
	}

	var subs, affected []string

	return func(opindex uint64, facth util.Hash, values []dop5.SMV) error {
			return addValues([3]interface{}{opindex, facth, values})
		},
		func(rsubs, raffected []string) error {
			subs, affected = rsubs, raffected

			addValuesDone()

			return nil
		},
		func(
			ctx context.Context,
			stream quic.Stream,
			openSubStreams quicstream.OpenSubStreamsHandlerOpenSubStreamsFunc,
		) (context.Context, error) {
			if _, err := handler(ctx, stream, openSubStreams); err != nil {
				return ctx, err
			}

			if err := writeFixedFrameStringSlice(stream, subs); err != nil {
				return ctx, err
			}

			return ctx, writeFixedFrameStringSlice(stream, affected)
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

		var maxItems uint64
		if _, err := readUint64(stream, &maxItems); err != nil {
			return ctx, err
		}

		var insertValues dop5.WorkerProcessInsertSMVsFunc
		var insertOwners dutil.In2ErrorFunc[[]string, []string]

		switch v, o, err := w(worker); {
		case err != nil:
			return ctx, err
		default:
			insertValues, insertOwners = v, o
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

		switch n, d, w, err := dutil.NewFuncJobWorker(ctx, max(maxItems, 1<<9)); {
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
							var bundle WorkerSMVsBundle
							if err := encoder.Decode(enc, b, &bundle); err != nil {
								return err
							}

							return insertValues(bundle.O, bundle.F, bundle.V)
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

func subStreamSize(n, maxPerStream uint64, nmax uint16) (nsub uint16) {
	if s := uint64(maxPerStream); n <= s {
		return 1
	}

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

func jobsSubStreams(ctx context.Context, streams []quic.Stream, f dutil.In2ErrorFunc[context.Context, quic.Stream]) error {
	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := dutil.NewFuncJobWorker(ctx, uint64(len(streams))); {
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

func newFuncMustAddHandler(addHandler func([]byte, quicstream.StreamHandler) bool) func(string, quicstream.StreamHandler) {
	return func(prefix string, h quicstream.StreamHandler) {
		if !addHandler([]byte(prefix), h) {
			panic(fmt.Sprintf("failed to add handler, %q", prefix))
		}
	}
}
