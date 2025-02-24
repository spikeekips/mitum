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
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
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

	handler := func(size uint16) AcceptSubStreamHandler {
		return AcceptSubStreamHandler(func(
			ctx context.Context,
			stream quic.Stream,
			openSubStreams AcceptSubStreamsHandlerOpenSubStreamsFunc,
		) (context.Context, error) {
			w, err := openSubStreams(ctx)
			t.PrintIfErrPanic(err, "handler: open sub streams")

			return ctx, w(func(streams []quic.Stream) error {
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

	requestHandler := func(size uint16, data []string) OpenSubStreamsHandler {
		return func(
			ctx context.Context,
			stream quic.Stream,
			open OpenSubStreamsHandlerOpenSubStreamsFunc,
		) (context.Context, error) {
			subStreams, err := open(ctx, size)
			t.PrintIfErrPanic(err, "request: open")

			return ctx, subStreams(func(streams []quic.Stream) error {
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

		o, c, _ := newConnsAcceptSubStreams(ctx, time.Minute)

		request := requestHandler(size, data).StreamHandler(newFuncOpenSubStreams(o))

		return func() {
			defer localStream.Close()
			defer c()

			started := time.Now()

			go func() {
				requestCtx := context.WithValue(ctx, QuicConnectionContextKey, remoteConn)
				_, err := request(requestCtx, remoteStream)
				t.PrintIfErrPanic(err, "request")

				remoteStream.Close()
			}()

			handlerCtx := context.WithValue(ctx, QuicConnectionContextKey, localConn)

			_, err := handler(size).StreamHandler(newFuncAcceptSubStreams(math.MaxUint16))(handlerCtx, localStream)
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

type testSubStreamsHandler struct {
	BaseTest
}

func (t *testSubStreamsHandler) TestOpen() {
	t.Run("multiple", func() {
		var size uint16 = 3

		handlerExhaustSubStreams := func(streams []quic.Stream) error {
			t.PrintIfTruePanic(len(streams) != int(size), "handler: wrong size: len(streams)%d != int(size)%d", len(streams), int(size))

			var wg sync.WaitGroup

			for stream := range slices.Values(streams) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					b, err := io.ReadAll(stream)
					t.PrintIfErrPanic(err, "handler: stream read: b=%d", len(b))
				}()
			}

			wg.Wait()

			return nil
		}

		requestExhaustSubStreams := func(streams []quic.Stream) error {
			t.PrintIfTruePanic(len(streams) != int(size), "request: wrong size: len(streams)%d != int(size)%d", len(streams), int(size))

			var wg sync.WaitGroup

			for stream := range slices.Values(streams) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					n, err := stream.Write([]byte{1, 2, 3})
					t.PrintIfErrPanic(err, "request: stream write: n=%d", n)
					stream.Close()
				}()
			}

			wg.Wait()

			return nil
		}

		ctx, cancel := t.Ctx()
		defer cancel(nil)

		_, _, localConn, remoteConn, closef := t.OpenConnections(ctx, ctx, nil, nil)
		defer closef()

		waitLocalStream := dutil.AssertWaitChan(t.Assert(), time.Second, func() quic.Stream {
			stream, err := localConn.AcceptStream(ctx)
			t.PrintIfErrPanic(err, "local conn: AcceptStream")

			_, err = frame.ReadFixed(stream)
			t.IfErrPanicf(err, "local stream: read")

			return stream
		})

		remoteStream, err := remoteConn.OpenStream()
		t.PrintIfErrPanic(err, "remote conn: OpenStream")
		t.IfErrPanicf(frame.WriteFixed(remoteStream, []byte{0}), "remote stream: write")

		localStream := waitLocalStream()

		handler := func() AcceptSubStreamHandler {
			return AcceptSubStreamHandler(func(
				ctx context.Context,
				stream quic.Stream,
				openSubStreams AcceptSubStreamsHandlerOpenSubStreamsFunc,
			) (context.Context, error) {
				w, err := openSubStreams(ctx)
				t.PrintIfErrPanic(err, "handler: open sub streams")

				t.PrintIfErrPanic(w(handlerExhaustSubStreams), "handler: 1st sub streams")

				t.PrintIfErrPanic(frame.WriteFixed(stream, []byte{0}), "handler: stream write")

				w, err = openSubStreams(ctx)
				t.PrintIfErrPanic(err, "handler: open sub streams")

				t.PrintIfErrPanic(w(handlerExhaustSubStreams), "handler: 2nd sub streams")

				return ctx, nil
			})
		}

		handlerdone := make(chan struct{})
		go func() {
			handlerCtx := context.WithValue(ctx, QuicConnectionContextKey, localConn)

			_, err := handler().StreamHandler(newFuncAcceptSubStreams(math.MaxUint16))(handlerCtx, localStream)
			t.PrintIfErrPanic(err, "handler")

			close(handlerdone)
		}()

		requestHandler := OpenSubStreamsHandler(func(
			ctx context.Context,
			stream quic.Stream,
			open OpenSubStreamsHandlerOpenSubStreamsFunc,
		) (context.Context, error) {
			subStreams, err := open(ctx, size)
			t.PrintIfErrPanic(err, "request: 1st open")

			t.PrintIfErrPanic(subStreams(requestExhaustSubStreams), "request: 1st sub streams")

			_, err = frame.ReadFixed(remoteStream)
			t.NoError(err)

			subStreams, err = open(ctx, size)
			t.PrintIfErrPanic(err, "request: 2nd open")

			t.PrintIfErrPanic(subStreams(requestExhaustSubStreams), "request: 2nd sub streams")
			t.T().Log("done")

			return ctx, nil
		})

		o, c, _ := newConnsAcceptSubStreams(ctx, time.Minute)
		defer c()

		requestCtx := context.WithValue(ctx, QuicConnectionContextKey, remoteConn)

		requestHandler.StreamHandler(newFuncOpenSubStreams(o))(requestCtx, remoteStream)

		select {
		case <-time.After(time.Second):
			t.Fail("wait handler done")
		case <-handlerdone:
		}
	})
}

func TestSubStreamsHandler(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testSubStreamsHandler))
}
