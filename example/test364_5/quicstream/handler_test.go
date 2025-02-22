package quicstream

import (
	"context"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

func TestBenchmarkSubStreamsHandler(tt *testing.T) {
	t := new(BaseTest)
	t.SetT(tt)
	t.SetupSuite()

	writeData := func(stream quic.Stream, s string) {
		_, err := stream.Write(append([]byte(s), byte('\n')))
		t.PrintIfErrPanic(err, "request: write stream")
	}

	readData := func(stream quic.Stream) {
		b, err := io.ReadAll(stream)
		t.PrintIfErrPanic(err, "handler: read stream, b=%d", len(b))
	}

	handler := func(size uint16) SubStreamsHandler {
		return SubStreamsHandler(func(
			ctx context.Context,
			stream quic.Stream,
			subStreams dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]],
		) (context.Context, error) {
			return ctx, subStreams(func(streams []quic.Stream) error {
				t.PrintIfTruePanic(len(streams) != int(size), "handler: wrong size: len(streams)%d != int(size)%d", len(streams), int(size))

				var wg sync.WaitGroup

				for stream := range slices.Values(streams) {
					wg.Add(1)

					go func() {
						defer wg.Done()

						readData(stream)
					}()
				}

				wg.Wait()

				return nil
			})
		})
	}

	request := func(ctx context.Context, conn quic.Connection, stream quic.Stream, size uint16, data []string) error {
		o, c, _ := newConnsAcceptSubStreams(ctx, time.Minute)
		defer c()

		subStreams, err := RequestOpenSubStreams(
			ctx,
			o,
			conn,
			stream,
			util.UUID().String(),
			size,
		)
		t.PrintIfErrPanic(err, "request: RequestOpenSubStreams")

		return subStreams(func(streams []quic.Stream) error {
			t.PrintIfTruePanic(len(streams) != int(size), "request: wrong size: len(streams)%d != int(size)%d", len(streams), int(size))

			var wg sync.WaitGroup

			n := int(math.Floor(float64(len(data)) / float64(int(size))))

			for i, stream := range slices.All(streams) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					e := (n * (i + 1))
					if i == int(size)-1 {
						e = len(data)
					}

					for s := range slices.Values(data[(n * i):e]) {
						writeData(stream, s)
					}

					_ = stream.Close()
				}()
			}

			wg.Wait()

			return nil
		})
	}

	ctx, cancel := t.Ctx()
	defer cancel(nil)

	_, _, localConn, remoteConn, closef := t.OpenConnections(ctx, ctx, nil, nil)
	defer closef()

	newFuncWaitLocalStream := func() func() quic.Stream {
		return dutil.AssertWaitChan(t.Assert(), time.Second, func() quic.Stream {
			stream, err := localConn.AcceptStream(ctx)
			t.PrintIfErrPanic(err, "local conn: AcceptStream")

			_, err = frame.ReadFixed(stream)
			t.IfErrPanicf(err, "local stream: read")

			return stream
		})
	}

	waitRemoteStream := func() quic.Stream {
		stream, err := remoteConn.OpenStream()
		t.PrintIfErrPanic(err, "remote conn: OpenStream")
		t.IfErrPanicf(frame.WriteFixed(stream, []byte{0}), "remote stream: write")

		return stream
	}

	runSingleStream := func(datasize uint64) func() {
		waitLocalStream := newFuncWaitLocalStream()
		remoteStream := waitRemoteStream()
		localStream := waitLocalStream()

		data := make([]string, datasize)
		for i := range datasize {
			data[i] = util.UUID().String()
		}

		return func() {
			defer localStream.Close()

			started := time.Now()

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				readData(localStream)
			}()

			go func() {
				defer wg.Done()

				for s := range slices.Values(data) {
					writeData(remoteStream, s)
				}

				remoteStream.Close()
			}()

			wg.Wait()

			d := time.Since(started)
			t.Print("single stream: elapsed: %dns (%s)", d.Nanoseconds(), d)
		}
	}

	runSubStreams := func(
		size uint16,
		datasize uint64,
	) func() {
		waitLocalStream := newFuncWaitLocalStream()
		remoteStream := waitRemoteStream()
		localStream := waitLocalStream()

		data := make([]string, datasize)
		for i := range datasize {
			data[i] = util.UUID().String()
		}

		return func() {
			defer localStream.Close()

			started := time.Now()

			go func() {
				requestCtx := context.WithValue(ctx, QuicConnectionContextKey, remoteConn)
				err := request(requestCtx, remoteConn, remoteStream, size, data)
				t.PrintIfErrPanic(err, "request")

				remoteStream.Close()
			}()

			handlerCtx := context.WithValue(ctx, QuicConnectionContextKey, localConn)
			_, err := handler(size).StreamHandler(math.MaxUint16)(handlerCtx, localStream)
			t.PrintIfErrPanic(err, "handler")

			d := time.Since(started)
			t.Print("sub streams: elapsed: %dns (%s)", d.Nanoseconds(), d)
		}
	}

	var maxDataSize uint64 = 300_033
	for dataSize := uint64(33); dataSize <= maxDataSize; dataSize += 50_000 {
		runSingleStream(dataSize)()

		for size := uint16(5); size < 672; size += 111 {
			if uint64(size) > dataSize {
				break
			}

			t.Run(fmt.Sprintf("size=%d data=%d", size, dataSize), func() {
				runSubStreams(size, dataSize)()
			})
		}
	}
}
