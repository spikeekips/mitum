package main

import (
	"context"
	"io"
	"iter"
	"math"
	"slices"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	"github.com/spikeekips/mitum/example/test364_5/quicstream"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

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
		if err := readFixedFrameString(stream, &workID); err != nil {
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

		if err := readUint64s(stream, &opsShare, &statesShare, &maxOpsSize); err != nil {
			return ctx, err
		}

		return ctx, f(workID, workers, opsWorkers, opsShare, statesShare, maxOpsSize)
	}
}

func requestHandlerSendOps(
	ctx context.Context,
	maxItems, subStreamItems uint64,
	maxSubStreams uint16,
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

		var nsub uint16

		if s := uint64(subStreamItems); maxItems > s {
			switch n := uint64(math.Ceil(float64(maxItems) / float64(s))); {
			case n > math.MaxUint16:
				nsub = math.MaxUint16
			case maxSubStreamItems > 0 && n > uint64(maxSubStreamItems):
				nsub = maxSubStreamItems
			default:
				nsub = uint16(n)
			}

			nsub--
		}

		var subStreams dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]]

		switch f, err := openSubStreams(ctx, nsub); {
		case err != nil:
			return ctx, err
		case nsub < 1:
			subStreams = func(f dutil.InErrorFunc[[]quic.Stream]) error {
				return f(nil)
			}
		default:
			subStreams = f
		}

		return ctx, subStreams(func(streams []quic.Stream) error {
			defer stream.Close()

			for i, v := range dutil.SeqAll(seq) {
				nstreams := make([]quic.Stream, len(streams)+1)
				copy(nstreams, streams)
				nstreams[len(streams)] = stream

				cur := nstreams[i%len(nstreams)]

				if err := writeUint64(cur, v[0].(uint64)); err != nil {
					return err
				}

				if err := writeFixedFrameHashes(cur, v[1].(util.Hash), v[2].(util.Hash)); err != nil {
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
	w func(iter.Seq2[uint64, [2]util.Hash]) error,
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
			return ctx, w(func(yield func(uint64, [2]util.Hash) bool) {})
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

				var newJob dutil.JobWorkerNewJobFunc
				var jobDone func()
				var jobWait dutil.InErrorFunc[error]

				switch n, d, w, err := dutil.NewFuncJobWorker(ctx, uint64(len(nstreams))); {
				case err != nil:
					return err
				default:
					newJob, jobDone, jobWait = n, d, w
				}

				for sub := range slices.Values(nstreams) {
					if err := newJob(func(context.Context) error {
						return readSendOpStream(sub, func(opindex uint64, oph, fact util.Hash) error {
							add([3]interface{}{opindex, oph, fact})

							return nil
						})
					}); err != nil {
						return jobWait(err)
					}
				}

				jobDone()

				return jobWait(nil)
			}))

			addDone()
		}()

		if err := w(func(yield func(uint64, [2]util.Hash) bool) {
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

		if err := readFixedFrameHash(r, &oph); err != nil {
			return err
		}

		var fact util.Hash

		switch err := readFixedFrameHash(r, &fact); {
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
		if err := readFixedFrameString(stream, &worker); err != nil {
			return ctx, err
		}

		var owners []string
		if err := readFixedFrameStringSlice(stream, &owners); err != nil {
			return ctx, err
		}

		return ctx, f(worker, owners)
	}
}
