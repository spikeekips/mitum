package quicstream

import (
	"bytes"
	"context"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/example/test364_7/frame"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/errgroup"
)

type testSubStreams struct {
	BaseTest
}

func (t *testSubStreams) Run(a string, f func()) {
	t.BaseTest.RunWithGoLeak(a, f)
}

func (t *testSubStreams) openConnections(
	reqCtx, serveCtx context.Context,
	reqQuicConfig, serveQuicConfig *quic.Config,
) (net.PacketConn, net.PacketConn, Conn, Conn, func()) {
	if reqCtx == nil {
		reqCtx = context.Background()
	}
	if serveCtx == nil {
		serveCtx = context.Background()
	}

	if reqQuicConfig == nil {
		reqQuicConfig = DefaultQuicConfig()
	}
	if serveQuicConfig == nil {
		serveQuicConfig = DefaultQuicConfig()
	}

	reqpconn := t.RandomPacketConn()
	servepconn, l := t.RandomListener(nil, serveQuicConfig)

	servech := make(chan Conn, 1)
	go func() {
		conn, err := l.Accept(serveCtx)
		t.PrintIfErrPanic(err, "accept")

		servech <- conn
	}()

	reqconn, err := dialWithPacketConn(reqCtx, reqpconn, servepconn.LocalAddr(), t.NewTLSConfig(), reqQuicConfig)
	t.PrintIfErrPanic(err, "req dial")

	serveconn := <-servech

	return reqpconn, servepconn, reqconn, serveconn, sync.OnceFunc(func() {
		err = reqpconn.Close()
		t.PrintIfErrCaller(err, 2, "req: PacketConn closed")
		err := servepconn.Close()
		t.PrintIfErrCaller(err, 2, "serve: PacketConn closed")

		closeConnection(reqconn, nil)
		closeConnection(serveconn, nil)
	})
}

func (t *testSubStreams) openControlStreams(ctx context.Context, reqconn, serveconn Conn) (
	Stream, Stream,
	func(),
) {
	if ctx == nil {
		ctx = context.Background()
	}

	sctx, sctxCancel := context.WithTimeout(ctx, time.Second*3)
	defer sctxCancel()

	servestreamch := make(chan Stream, 1)
	go func() {
		stream, err := contextEnsureT(sctx, func(ctx context.Context) (Stream, error) {
			stream, err := serveconn.AcceptStream(ctx)
			if err != nil {
				return nil, err
			}
			t.PrintIfErr(err, "serve conn: AcceptStream")

			_, _, err = frame.ReadBytes(stream)
			t.PrintIfErr(err, "serve stream: read")

			return stream, err
		})
		t.IfErrPanicf(err, "serve")

		servestreamch <- stream
	}()

	reqstream, err := reqconn.OpenStream()
	t.IfErrPanicf(err, "req conn: OpenStream")
	_, err = frame.WriteBytes(reqstream, []byte{0})
	t.IfErrPanicf(err, "req stream: write")

	var servestream Stream

	select {
	case <-sctx.Done():
		t.PrintIfErrPanic(err, "wait serve stream")
	case servestream = <-servestreamch:
	}

	return reqstream, servestream, sync.OnceFunc(func() {
		closeStream(reqstream, nil)
		closeStream(servestream, nil)
	})
}

type dummyStream struct {
	w               *bytes.Buffer
	ctx             context.Context
	ctxCancel       func()
	rerr            error
	werr            error
	onceCancelRead  sync.Once
	onceCancelWrite sync.Once
	isClosed        int64
}

var _ Stream = &dummyStream{}

func newDmmyStream(ctx context.Context) *dummyStream {
	s := &dummyStream{}

	s.w = bytes.NewBuffer(nil)

	s.ctx, s.ctxCancel = context.WithCancel(ctx)

	return s
}

func (s *dummyStream) CancelRead(c quic.StreamErrorCode) {
	s.onceCancelRead.Do(func() {
		s.rerr = &quic.StreamError{ErrorCode: c}
	})
}

func (s *dummyStream) CancelWrite(c quic.StreamErrorCode) {
	s.onceCancelWrite.Do(func() {
		s.werr = &quic.StreamError{ErrorCode: c}
	})
}

func (s *dummyStream) Close() error {
	if !atomic.CompareAndSwapInt64(&s.isClosed, 0, 1) {
		return &net.OpError{Op: "closed", Err: errors.New("write on closed stream")}
	}

	s.ctxCancel()

	return nil
}
func (s *dummyStream) Context() context.Context { return s.ctx }
func (s *dummyStream) Read(p []byte) (int, error) {
	if s.ctx.Err() != nil {
		return 0, &net.OpError{Op: "closed", Err: errors.New("read on closed stream")}
	}

	return s.w.Read(p)
}
func (s *dummyStream) SetDeadline(time.Time) error      { return nil }
func (s *dummyStream) SetReadDeadline(time.Time) error  { return nil }
func (s *dummyStream) SetWriteDeadline(time.Time) error { return nil }
func (s *dummyStream) StreamID() quic.StreamID          { return 0 }
func (s *dummyStream) Write(b []byte) (int, error) {
	if s.ctx.Err() != nil {
		return 0, &net.OpError{Op: "closed", Err: errors.New("write on closed stream")}
	}

	return s.w.Write(b)
}

func (t *testSubStreams) rawRequest() (
	context.Context, dutil.InErrorFunc[error],
	context.Context, dutil.InErrorFunc[error],
	context.Context, dutil.InErrorFunc[error],
	Stream, Stream,
	func(time.Duration) requestSubStreamsFunc,
	openStreamFunc,
	func(),
) {
	ctx, ctxCancel := t.Ctx()

	rctx, rctxCancel := contextWithCancelCause(ctx)
	sctx, sctxCancel := contextWithCancelCause(ctx)

	_, _, rconn, sconn, closef := t.openConnections(rctx, sctx, nil, nil)
	rcstream, scstream, cclosef := t.openControlStreams(context.Background(), rconn, sconn)

	return ctx, ctxCancel,
		rctx, rctxCancel,
		sctx, sctxCancel,
		rcstream, scstream,
		func(timeout time.Duration) requestSubStreamsFunc {
			return defaultRequestSubStreams(
				rctx,
				func() (context.Context, openStreamFunc) {
					return rctx, quicOpenStreamFunc(rconn.AcceptStream)
				},
				timeout,
			)
		},
		quicOpenStreamFunc(sconn.OpenStreamSync),
		func() {
			ctxCancel(nil)
			rctxCancel(nil)
			sctxCancel(nil)

			cclosef()
			closef()
		}
}

func (t *testSubStreams) serveOpenStream(
	ctx context.Context,
	openStream openStreamFunc,
	n uint64,
	uid subStreamsRequestUID,
	handler subStreamHandlerkFunc,
) (dutil.ErrorFunc, error) {
	eg, ectx := errgroup.WithContext(ctx)

	for range n {
		stream, err := openStream(ectx)
		if err != nil {
			return nil, err
		}

		eg.Go(func() error {
			if _, err := frame.WriteBytes(stream, uid[:]); err != nil {
				return closeStream(stream, err)
			}

			return closeStream(stream, handler(ectx, stream))
		})
	}

	return eg.Wait, nil
}

func (t *testSubStreams) TestRequest() {
	var gn uint64 = 3

	sentb := make([]string, gn)
	for i := range sentb {
		sentb[i] = util.UUID().String()
	}
	slices.Sort(sentb)

	t.Run("ok; n=3", func() {
		n := gn

		_, _, rctx, rctxCancel, sctx, sctxCancel, rcstream, scstream, requestf, openStream, closef := t.rawRequest()
		defer closef()

		request := requestf(time.Minute)

		go func() {
			sctxCancel(contextEnsure(sctx, func(ctx context.Context) error {
				uid, err := readSubStreamRequestUID(scstream)
				if err != nil {
					return err
				}

				n, _, err := frame.ReadUint64(scstream)
				if err != nil {
					return err
				}

				_, err = frame.WriteUint64(scstream, n)
				if err != nil {
					return err
				}

				rerr, err := readSubStreamError(scstream)
				if err != nil {
					return err
				}
				if rerr != nil {
					return rerr
				}

				var i int64
				swait, err := t.serveOpenStream(ctx, openStream, n, uid, func(_ context.Context, stream Stream) error {
					j := atomic.AddInt64(&i, 1)

					_, err := frame.WriteBytes(stream, []byte(sentb[j-1]))
					return err
				})
				if err != nil {
					return err
				}

				err = func() error {
					if err := swait(); err != nil {
						return err
					}

					rerr, err = readSubStreamError(scstream)
					if err != nil {
						return err
					}
					if rerr != nil {
						return rerr
					}

					return nil
				}()

				return writeSubStreamError(scstream, err)
			}))
		}()

		var recvb []string

		t.NoError(request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			if nn != n {
				return nil, errors.Errorf("nn != n; %d != %d", nn, n)
			}

			var l sync.Mutex

			return func(_ context.Context, stream Stream) error {
					_, b, err := frame.ReadBytes(stream)
					if err != nil {
						return err
					}

					l.Lock()
					recvb = append(recvb, string(b))
					l.Unlock()

					return nil
				},
				nil
		}))
		rctxCancel(nil)

		slices.Sort(recvb)
		t.Equal(sentb, recvb)

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait serve done")
		case <-sctx.Done():
			t.NoError(sctxCancel(nil))
		}
	})

	t.Run("ok; n > nn", func() {
		var n, nn uint64 = 4, gn

		_, _, rctx, rctxCancel, sctx, sctxCancel, rcstream, scstream, requestf, openStream, closef := t.rawRequest()
		defer closef()

		request := requestf(time.Minute)

		go func() {
			sctxCancel(contextEnsure(sctx, func(ctx context.Context) error {
				uid, err := readSubStreamRequestUID(scstream)
				if err != nil {
					return err
				}

				rn, _, err := frame.ReadUint64(scstream)
				if err != nil {
					return err
				}
				if rn != n {
					return errors.Errorf("wrong request n, %d", rn)
				}

				_, err = frame.WriteUint64(scstream, nn)
				if err != nil {
					return err
				}

				rerr, err := readSubStreamError(scstream)
				if err != nil {
					return err
				}
				if rerr != nil {
					return rerr
				}

				var i int64
				swait, err := t.serveOpenStream(ctx, openStream, nn, uid, func(_ context.Context, stream Stream) error {
					j := atomic.AddInt64(&i, 1)

					_, err := frame.WriteBytes(stream, []byte(sentb[j-1]))
					return err
				})
				if err != nil {
					return err
				}

				err = func() error {
					if err := swait(); err != nil {
						return err
					}

					rerr, err = readSubStreamError(scstream)
					if err != nil {
						return err
					}
					if rerr != nil {
						return rerr
					}

					return nil
				}()

				return writeSubStreamError(scstream, err)
			}))
		}()

		var recvb []string

		t.NoError(request(rctx, rcstream, n, func(sn uint64) (subStreamHandlerkFunc, error) {
			if nn != sn {
				return nil, errors.Errorf("nn != n; %d != %d", nn, sn)
			}

			var l sync.Mutex

			return func(_ context.Context, stream Stream) error {
					_, b, err := frame.ReadBytes(stream)
					if err != nil {
						return err
					}

					l.Lock()
					recvb = append(recvb, string(b))
					l.Unlock()

					return nil
				},
				nil
		}))
		rctxCancel(nil)

		slices.Sort(recvb)
		t.Equal(sentb, recvb)

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait serve done")
		case <-sctx.Done():
			t.NoError(sctxCancel(nil))
		}
	})

	t.Run("ok; n=0 || nn=0", func() {
		t.Run("ok; n=0", func() {
			var n uint64 = 0

			_, _, rctx, rctxCancel, sctx, sctxCancel, rcstream, scstream, _, _, closef := t.rawRequest()
			defer closef()

			go func() {
				sctxCancel(contextEnsure(sctx, func(ctx context.Context) error {
					if _, err := readSubStreamRequestUID(scstream); err != nil {
						return err
					}

					n, _, err := frame.ReadUint64(scstream)
					if err != nil {
						return err
					}

					_, err = frame.WriteUint64(scstream, n)
					if err != nil {
						return err
					}

					rerr, err := readSubStreamError(scstream)
					if err != nil {
						return err
					}
					if rerr != nil {
						return rerr
					}

					return nil
				}))
			}()

			request := defaultRequestSubStreams(
				rctx,
				func() (context.Context, openStreamFunc) {
					return rctx, func(context.Context) (Stream, error) {
						return nil, errors.Errorf("request AcceptStream called")
					}
				},
				time.Minute,
			)

			t.NoError(request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
				if nn != n {
					return nil, errors.Errorf("nn != n; %d != %d", nn, n)
				}

				return nil, nil
			}))
			rctxCancel(nil)

			select {
			case <-time.After(time.Second * 3):
				t.Fail("wait serve done")
			case <-sctx.Done():
				t.NoError(sctxCancel(nil))
			}
		})

		t.Run("ok; n=3 nn=0", func() {
			var n, nn uint64 = gn, 0

			_, _, rctx, rctxCancel, sctx, sctxCancel, rcstream, scstream, _, _, closef := t.rawRequest()
			defer closef()

			go func() {
				sctxCancel(contextEnsure(sctx, func(ctx context.Context) error {
					if _, err := readSubStreamRequestUID(scstream); err != nil {
						return err
					}

					rn, _, err := frame.ReadUint64(scstream)
					if err != nil {
						return err
					}
					if rn != n {
						return errors.Errorf("wrong request n, %d", rn)
					}

					_, err = frame.WriteUint64(scstream, nn)
					if err != nil {
						return err
					}

					rerr, err := readSubStreamError(scstream)
					if err != nil {
						return err
					}
					if rerr != nil {
						return rerr
					}

					return nil
				}))
			}()

			request := defaultRequestSubStreams(
				rctx,
				func() (context.Context, openStreamFunc) {
					return rctx, func(context.Context) (Stream, error) {
						return nil, errors.Errorf("request AcceptStream called")
					}
				},
				time.Minute,
			)

			t.NoError(request(rctx, rcstream, n, func(sn uint64) (subStreamHandlerkFunc, error) {
				if sn != nn {
					return nil, errors.Errorf("nn != n; %d != %d", sn, n)
				}

				return nil, nil
			}))
			rctxCancel(nil)

			select {
			case <-time.After(time.Second * 3):
				t.Fail("wait serve done")
			case <-sctx.Done():
				t.NoError(sctxCancel(nil))
			}
		})
	})
}

func TestSubStreams(t *testing.T) {
	suite.Run(t, new(testSubStreams))
}

func quicOpenStreamFunc(f func(context.Context) (*quic.Stream, error)) func(context.Context) (Stream, error) {
	return func(ctx context.Context) (Stream, error) {
		s, err := f(ctx)

		return s, err
	}
}
