package quicstream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testQuic struct {
	BaseTest
}

func (t *testQuic) OpenConnections(
	localCtx, remoteCtx context.Context,
	localQuicConfig, remoteQuicConfig *quic.Config,
) (net.PacketConn, net.PacketConn, Conn, Conn, func()) {
	lpconn, l := t.RandomListener(nil, localQuicConfig)
	rpconn := t.RandomPacketConn()

	localConnCh := make(chan Conn, 1)
	go func() {
		conn, err := l.Accept(localCtx)
		t.PrintIfErrPanic(err, "accept")

		localConnCh <- conn
	}()

	if remoteQuicConfig == nil {
		remoteQuicConfig = DefaultQuicConfig()
	}

	remoteConn, err := dialWithPacketConn(remoteCtx, rpconn, lpconn.LocalAddr(), t.NewTLSConfig(), remoteQuicConfig)
	t.PrintIfErrPanic(err, "remote dial")

	localConn := <-localConnCh

	closef := sync.OnceFunc(func() {
		err := lpconn.Close()
		t.PrintIfErrCaller(err, 2, "local: PacketConn closed")
		err = rpconn.Close()
		t.PrintIfErrCaller(err, 2, "remote: PacketConn closed")
	})

	t.T().Cleanup(closef)

	return lpconn, rpconn, localConn, remoteConn, closef
}

func (t *testQuic) TestPacketConn() {
	t.RunWithGoLeak("PacketConn closed; before listener accept", func() {
		pconn, l := t.RandomListener(nil, nil)

		t.NoError(pconn.Close())
		t.T().Log("PacketConn closed")

		conn, err := l.Accept(context.Background())
		t.T().Logf("accept err=%v %T", err, err)
		t.Nil(conn)
		t.ErrorIs(err, quic.ErrTransportClosed)
	})

	t.RunWithGoLeak("PacketConn closed; before remote dial", func() {
		pconn, l := t.RandomListener(nil, nil)

		t.NoError(pconn.Close())

		wait := dutil.AssertWaitChan(t.Assert(), time.Second, func() error {
			_, err := l.Accept(context.Background())
			t.PrintErr(err, "local listener: Accept")

			return err
		})

		remoteQuicConfig := DefaultQuicConfig()
		remoteQuicConfig.HandshakeIdleTimeout = time.Millisecond * 333
		remoteQuicConfig.MaxIdleTimeout = time.Millisecond * 3

		_, err := Dial(context.Background(), pconn.LocalAddr(), t.NewTLSConfig(), remoteQuicConfig)
		t.PrintErr(err, "remote dial")

		var ierr *quic.IdleTimeoutError
		t.ErrorAs(err, &ierr)

		err = wait()
		t.ErrorIs(err, quic.ErrTransportClosed)
	})

	t.RunWithGoLeak("PacketConn closed; before accept stream", func() {
		pconn, l := t.RandomListener(nil, nil)

		wait := dutil.AssertWaitChan(t.Assert(), time.Second, func() Conn {
			conn, err := l.Accept(context.Background())
			t.PrintIfErrPanic(err, "local listener: accept")

			return conn
		})

		remoteConn, err := Dial(context.Background(), pconn.LocalAddr(), t.NewTLSConfig(), DefaultQuicConfig())
		t.NoError(err, "remote dial")
		defer remoteConn.CloseWithError(0, "")

		localConn := wait()

		t.NoError(pconn.Close())
		t.T().Log("PacketConn closed")

		_, err = localConn.AcceptStream(context.Background())
		t.T().Logf("accept stream err=%v %T", err, err)

		var operr *net.OpError

		switch {
		case errors.Is(err, quic.ErrTransportClosed),
			errors.As(err, &operr):
		default:
			t.Fail("unexpected", "%T %v", err, err)
		}
	})

	t.RunWithGoLeak("PacketConn closed; before open stream", func() {
		remoteQuicConfig := DefaultQuicConfig()
		remoteQuicConfig.MaxIdleTimeout = time.Millisecond * 33
		remoteQuicConfig.KeepAlivePeriod = 0

		lpconn, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, remoteQuicConfig)
		defer closef()

		waitLocalConn := dutil.AssertWaitChan(t.Assert(), time.Second, func() error {
			_, err := localConn.AcceptStream(context.Background())
			t.PrintErr(err, "local conn: accept stream")

			return err
		})

		lpconn.Close()

		remoteStream, err := remoteConn.OpenStream()
		t.NoError(err)

		var nerr *net.OpError

		switch err := waitLocalConn(); {
		case errors.Is(err, quic.ErrTransportClosed):
		case errors.As(err, &nerr):
		default:
			t.Fail("unexpected", "%T %v", err, err)
		}

		_, err = remoteStream.Write([]byte{0})
		t.PrintErr(err, "remote stream: write")
		t.NoError(err)

		_, err = io.ReadAll(remoteStream)
		t.PrintErr(err, "remote stream: read all")

		var ierr *quic.IdleTimeoutError
		t.ErrorAs(err, &ierr)
	})

	t.RunWithGoLeak("PacketConn closed; before stream read; idle timeout", func() {
		remoteQuicConfig := DefaultQuicConfig()
		remoteQuicConfig.MaxIdleTimeout = time.Second * 3
		remoteQuicConfig.KeepAlivePeriod = 0

		lpconn, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, remoteQuicConfig)
		defer closef()

		waitLocalStream := dutil.AssertWaitChan(t.Assert(), time.Second, func() error {
			stream, err := localConn.AcceptStream(context.Background())
			t.PrintIfErrPanic(err, "local conn: accept stream")

			{
				err := lpconn.Close()
				t.PrintErr(err, "local PacketConn: closed")
			}

			_, err = io.ReadAll(stream)
			t.PrintErr(err, "local stream: read all")

			return err
		})

		remoteStream, err := remoteConn.OpenStream()
		t.NoError(err, "remote open stream")

		// NOTE before writing stream by remote, idle timeout will never be
		// occurred.
		_, err = remoteStream.Write([]byte{0})
		t.NoError(err, "remote stream write")

		err = waitLocalStream()

		var operr *net.OpError

		switch {
		case errors.Is(err, quic.ErrTransportClosed),
			errors.As(err, &operr):
		default:
			t.Fail("unexpected", "%T %v", err, err)
		}

		t.T().Log("remote stream: trying to read all")

		_, err = io.ReadAll(remoteStream)
		t.PrintErr(err, "remote stream: read all")

		var ierr *quic.IdleTimeoutError
		t.ErrorAs(err, &ierr)
	})

	t.RunWithGoLeak("PacketConn closed; before stream read; remote write stream", func() {
		remoteQuicConfig := DefaultQuicConfig()
		remoteQuicConfig.MaxIdleTimeout = time.Millisecond * 333

		lpconn, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, remoteQuicConfig)
		defer closef()

		waitLocalStream := dutil.AssertWaitChan(t.Assert(), time.Second, func() error {
			stream, err := localConn.AcceptStream(context.Background())
			t.PrintIfErrPanic(err, "local conn: accept stream")

			{
				err := lpconn.Close()
				t.PrintErr(err, "local PacketConn: closed")
			}

			_, err = io.ReadAll(stream)
			t.PrintErr(err, "local stream: read all")

			return err
		})

		remoteStream, err := remoteConn.OpenStream()
		t.NoError(err, "remote open stream")

		_, err = remoteStream.Write([]byte{0}) // NOTE before writing stream by remote, local stream will never be opened
		t.NoError(err, "remote stream write")

		var operr *net.OpError

		switch err = waitLocalStream(); {
		case errors.Is(err, quic.ErrTransportClosed),
			errors.As(err, &operr):
		default:
			t.Fail("unexpected", "%T %v", err, err)
		}

		t.T().Log("remote stream: trying to read all")

		wait := dutil.AssertWaitChan(t.Assert(), remoteQuicConfig.MaxIdleTimeout*3, func() error {
			_, err := io.ReadAll(remoteStream)
			t.PrintErr(err, "remote stream: read all")

			return err
		})

		var ierr *quic.IdleTimeoutError
		t.ErrorAs(wait(), &ierr)
	})
}

func (t *testQuic) TestListener() {
	t.RunWithGoLeak("context canceled", func() {
		pconn, l := t.RandomListener(nil, nil)
		defer pconn.Close()

		ctx, cancel := t.Ctx()
		cancel(nil)

		conn, err := l.Accept(ctx)
		t.T().Log("err:", err)
		t.Nil(conn)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testQuic) TestConnection() {
	t.RunWithGoLeak("closed", func() {
		remoteQuicConfig := DefaultQuicConfig()
		remoteQuicConfig.KeepAlivePeriod = 0
		remoteQuicConfig.MaxIdleTimeout = time.Minute
		remoteQuicConfig.HandshakeIdleTimeout = time.Millisecond * 33

		lpconn, _ := t.RandomListener(nil, DefaultQuicConfig())
		t.NoError(lpconn.Close())

		_, err := Dial(context.Background(), lpconn.LocalAddr(), t.NewTLSConfig(), remoteQuicConfig)
		var ierr *quic.IdleTimeoutError
		t.ErrorAs(err, &ierr, "%T", err)
	})

	t.RunWithGoLeak("idle timeout", func() {
		localQuicConfig := DefaultQuicConfig()
		localQuicConfig.KeepAlivePeriod = 0

		remoteQuicConfig := DefaultQuicConfig()
		remoteQuicConfig.KeepAlivePeriod = 0
		remoteQuicConfig.HandshakeIdleTimeout = time.Millisecond * 33

		lpconn, l := t.RandomListener(nil, localQuicConfig)
		defer lpconn.Close()
		defer l.Close()

		conn, err := Dial(context.Background(), lpconn.LocalAddr(), t.NewTLSConfig(), remoteQuicConfig)
		t.NoError(err)
		defer conn.CloseWithError(3, "")

		t.T().Logf("wait more than MaxIdleTimeout, %s", localQuicConfig.MaxIdleTimeout)
		<-time.After(localQuicConfig.MaxIdleTimeout + time.Second)

		_, err = conn.OpenStream()
		t.Error(err)

		var ierr *quic.IdleTimeoutError
		t.ErrorAs(err, &ierr, "%T", err)
	})

	t.RunWithGoLeak("idle timeout, but send datagram", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		localQuicConfig := DefaultQuicConfig()
		localQuicConfig.KeepAlivePeriod = 0

		remoteQuicConfig := DefaultQuicConfig()
		remoteQuicConfig.KeepAlivePeriod = 0
		remoteQuicConfig.HandshakeIdleTimeout = time.Millisecond * 33

		lpconn, l := t.RandomListener(nil, localQuicConfig)
		defer lpconn.Close()
		defer l.Close()

		conn, err := Dial(context.Background(), lpconn.LocalAddr(), t.NewTLSConfig(), remoteQuicConfig)
		t.NoError(err)
		defer conn.CloseWithError(3, "")

		go func() {
			ticker := time.NewTicker(time.Millisecond * 33)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					err := conn.SendDatagram([]byte{0x00})
					if err != nil {
						return
					}
				}
			}
		}()

		t.T().Logf("wait more than MaxIdleTimeout, %s", localQuicConfig.MaxIdleTimeout)
		<-time.After(localQuicConfig.MaxIdleTimeout + time.Second)

		stream, err := conn.OpenStream()
		t.NoError(err)
		defer stream.Close()
	})
}

func (t *testQuic) TestLocalConnection() {
	t.RunWithGoLeak("conn closed; accept stream", func() {
		_, _, localConn, _, closef := t.OpenConnections(context.Background(), context.Background(), nil, nil)
		defer closef()

		t.NoError(localConn.CloseWithError(0, ""))
		t.T().Log("local conn: CloseWithError")

		_, err := localConn.AcceptStream(context.Background())
		t.PrintErr(err, "local conn: AcceptStream")

		var aerr *quic.ApplicationError
		switch {
		case errors.As(err, &aerr):
		default:
			t.NoError(err, "local conn: AcceptStream")
		}
	})

	t.RunWithGoLeak("conn closed; local stream read", func() {
		_, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, nil)
		defer closef()

		waitLocalStream := dutil.AssertWaitChan(t.Assert(), time.Second, func() *quic.Stream {
			stream, err := localConn.AcceptStream(context.Background())
			t.PrintIfErrPanic(err, "local conn: AcceptStream")

			return stream
		})

		remoteStream, err := remoteConn.OpenStream()
		t.NoError(err)

		b := util.UUID().Bytes()
		_, err = remoteStream.Write(b)
		t.NoError(err)
		t.NoError(remoteStream.Close())

		localStream := waitLocalStream()

		t.NoError(localConn.CloseWithError(9, "showme"))
		t.T().Log("local conn: CloseWithError")

		_, err = io.ReadAll(localStream)
		t.PrintErr(err, "local stream: read all")

		var aerr *quic.ApplicationError
		t.ErrorAs(err, &aerr)

		t.EqualValues(9, aerr.ErrorCode)
		t.Equal("showme", aerr.ErrorMessage)
	})

	t.RunWithGoLeak("conn closed; remote stream read", func() {
		_, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, nil)
		defer closef()

		waitLocalStream := dutil.AssertWaitChan(t.Assert(), time.Second, func() *quic.Stream {
			stream, err := localConn.AcceptStream(context.Background())
			t.PrintIfErrPanic(err, "local conn: AcceptStream")

			return stream
		})

		remoteStream, err := remoteConn.OpenStream()
		t.NoError(err)

		_, err = remoteStream.Write(util.UUID().Bytes())
		t.NoError(err)

		localStream := waitLocalStream()
		t.NoError(localConn.CloseWithError(9, "showme"))
		t.T().Log("local conn: CloseWithError")

		_, err = localStream.Write(util.UUID().Bytes())

		var aerr *quic.ApplicationError
		t.ErrorAs(err, &aerr)

		t.EqualValues(9, aerr.ErrorCode)
		t.Equal("showme", aerr.ErrorMessage)

		_, err = io.ReadAll(remoteStream)
		t.T().Logf("remote stream: read all: err=%v %T", err, err)

		t.ErrorAs(err, &aerr)

		t.EqualValues(9, aerr.ErrorCode)
		t.Equal("showme", aerr.ErrorMessage)
	})
}

func (t *testQuic) TestRemoteConnection() {
	t.RunWithGoLeak("dial after context canceled", func() {
		pconn, _ := t.RandomListener(nil, nil)
		defer pconn.Close()

		ctx, cancel := t.Ctx()
		cancel(nil)

		_, err := Dial(ctx, pconn.LocalAddr(), t.NewTLSConfig(), nil)
		t.PrintErr(err, "remote dial")
		t.ErrorIs(err, context.Canceled)
	})

	t.RunWithGoLeak("conn closed; during local stream read", func() {
		_, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, nil)
		defer closef()

		waitLocalStream := dutil.AssertWaitChan(t.Assert(), time.Second, func() *quic.Stream {
			stream, err := localConn.AcceptStream(context.Background())
			t.PrintIfErrPanic(err, "local conn: accept stream")

			return stream
		})

		remoteStream, err := remoteConn.OpenStream()
		t.NoError(err)
		_, err = remoteStream.Write([]byte{0})
		t.PrintErr(err, "remote stream: write")
		t.NoError(err)

		localStream := waitLocalStream()

		err = remoteConn.CloseWithError(9, "findme")
		t.PrintErr(err, "remote conn: closed")

		waitReadAll := dutil.AssertWaitChan(t.Assert(), time.Second, func() error {
			_, err := io.ReadAll(localStream)
			t.PrintErr(err, "local stream: read all")

			return err
		})

		err = waitReadAll()
		var aerr *quic.ApplicationError
		t.ErrorAs(err, &aerr)

		t.EqualValues(9, aerr.ErrorCode)
		t.Equal("findme", aerr.ErrorMessage)
	})

	t.RunWithGoLeak("accept stream from remote dialed conn", func() {
		_, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, nil)
		defer closef()

		waitRemoteStream := dutil.AssertWaitChan(t.Assert(), time.Second, func() *quic.Stream {
			stream, err := remoteConn.AcceptStream(context.Background())
			t.PrintIfErrPanic(err, "remote accept stream")

			return stream
		})

		localStream, err := localConn.OpenStream()
		t.NoError(err)

		b := util.UUID().Bytes()
		_, err = localStream.Write(b)
		t.NoError(err)
		t.NoError(localStream.Close())

		remoteStream := waitRemoteStream()
		rb, err := io.ReadAll(remoteStream)
		t.PrintErr(err, "remote stream: read all")

		t.NoError(err)
		t.Equal(b, rb)
	})
}

func (t *testQuic) TestLocalStream() {
	t.RunWithGoLeak("accept stream after context canceled", func() {
		_, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, nil)
		defer closef()

		ctx, cancel := t.Ctx()
		cancel(nil)

		_, err := localConn.AcceptStream(ctx)
		t.PrintErr(err, "local conn: AcceptStream; expected canceled")
		t.ErrorIs(err, context.Canceled)

		nctx, ncancel := t.Ctx()
		defer ncancel(nil)

		accepted := make(chan struct{})
		go func() {
			_, err := localConn.AcceptStream(nctx)
			t.PrintIfErrPanic(err, "local conn: AcceptStream; expected ok")
			close(accepted)
		}()

		t.T().Log("remote conn: OpenStream")
		remoteStream, err := remoteConn.OpenStream()
		t.NoError(err)
		_, err = remoteStream.Write(util.UUID().Bytes())
		t.NoError(err)
		remoteStream.Close()

		select {
		case <-time.After(time.Second):
			t.Fail("wait accept stream")
		case <-accepted:
		}
	})

	t.RunWithGoLeak("local stream read after context canceled", func() {
		// NOTE context for AcceptStream does not work after AcceptStream.
		_, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, nil)
		defer closef()

		ctx, cancel := t.Ctx()

		waitLocalStream := dutil.AssertWaitChan(t.Assert(), time.Second, func() *quic.Stream {
			stream, err := localConn.AcceptStream(ctx)
			t.PrintErr(err, "local conn: AcceptStream")

			return stream
		})

		remoteStream, err := remoteConn.OpenStream()
		t.NoError(err)

		b := util.UUID().Bytes()
		_, err = remoteStream.Write(b)
		t.NoError(err)
		t.NoError(remoteStream.Close())

		localStream := waitLocalStream()
		cancel(nil)
		t.T().Log("local stream: context canceled")

		rb, err := io.ReadAll(localStream)
		t.PrintIfErrPanic(err, "local stream: read all: b=%v", rb)
		t.Equal(b, rb)
	})

	t.RunWithGoLeak("AcceptStream: before remote stream write", func() {
		_, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, nil)
		defer closef()

		waitch := make(chan *quic.Stream)

		go func() {
			stream, err := localConn.AcceptStream(context.Background())
			t.PrintErr(err, "local conn: AcceptStream")

			var aerr *quic.ApplicationError
			switch {
			case err == nil:
				waitch <- stream
			case errors.Is(err, quic.ErrTransportClosed),
				errors.As(err, &aerr):
			default:
				t.PrintErr(err, "local conn: AcceptStream")
			}
		}()

		_, err := remoteConn.OpenStream()
		t.NoError(err, "remote open stream")

		select {
		case <-time.After(time.Second * 2):
		case <-waitch:
			t.Fail("unexpected local stream")
		}
	})

	t.RunWithGoLeak("AcceptStream: before remote stream write; local idle timeout", func() {
		localQuicConfig := DefaultQuicConfig()
		localQuicConfig.MaxIdleTimeout = time.Millisecond * 3
		localQuicConfig.KeepAlivePeriod = 0

		_, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), localQuicConfig, nil)
		defer closef()

		waitLocalStream := dutil.AssertWaitChan(t.Assert(), time.Second*2, func() error {
			_, err := localConn.AcceptStream(context.Background())
			t.PrintErr(err, "local conn: AcceptStream")

			return err
		})

		_, err := remoteConn.OpenStream()
		t.NoError(err, "remote open stream")

		err = waitLocalStream()

		var ierr *quic.IdleTimeoutError
		t.ErrorAs(err, &ierr)
	})

	t.RunWithGoLeak("AcceptStream: remote stream write", func() {
		_, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, nil)
		defer closef()

		waitLocalStream := dutil.AssertWaitChan(t.Assert(), time.Second, func() error {
			_, err := localConn.AcceptStream(context.Background())

			return err
		})

		remoteStream, err := remoteConn.OpenStream()
		t.NoError(err, "remote open stream")
		_, err = remoteStream.Write([]byte{0})
		t.NoError(err)

		t.NoError(waitLocalStream(), "local conn: AcceptStream")
	})

	t.RunWithGoLeak("after idle timeout, remote OpenStream", func() {
		// NOTE after idle timeout, local connection is not able to use.
		localQuicConfig := DefaultQuicConfig()
		localQuicConfig.MaxIdleTimeout = time.Millisecond * 3
		localQuicConfig.KeepAlivePeriod = 0

		_, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), localQuicConfig, nil)
		defer closef()

		go func() {
			remoteStream, err := remoteConn.OpenStream()
			t.PrintIfErrPanic(err, "remote OpenStream")

			_, err = remoteStream.Write([]byte{0})
			t.PrintIfErrPanic(err, "remote stream: write")
		}()

		localStream, err := localConn.AcceptStream(context.Background())
		t.NoError(err)

		_, err = io.ReadAll(localStream)
		t.PrintErr(err, "local stream: read all")
		var ierr *quic.IdleTimeoutError
		t.ErrorAs(err, &ierr)

		_, err = localConn.AcceptStream(context.Background())
		t.ErrorAs(err, &ierr)
	})

	t.RunWithGoLeak("remote stream: CancelRead()", func() {
		_, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, nil)
		defer closef()

		go func() {
			remoteStream, err := remoteConn.OpenStream()
			t.PrintIfErrPanic(err, "remote OpenStream")

			remoteStream.CancelRead(9)
		}()

		localStream, err := localConn.AcceptStream(context.Background())
		t.NoError(err)

		err = nil

		for i := range 333 {
			_, err = localStream.Write([]byte{0})
			t.PrintErr(err, "local stream: write: %d", i)
			if err != nil {
				break
			}

			<-time.After(time.Millisecond * 33)
		}

		var serr *quic.StreamError
		t.ErrorAs(err, &serr)
		t.EqualValues(serr.ErrorCode, 9)
		t.True(serr.Remote)
	})

	t.RunWithGoLeak("remote stream: CancelWrite()", func() {
		_, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, nil)
		defer closef()

		go func() {
			remoteStream, err := remoteConn.OpenStream()
			t.PrintIfErrPanic(err, "remote OpenStream")

			remoteStream.CancelWrite(9)
		}()

		localStream, err := localConn.AcceptStream(context.Background())
		t.NoError(err)

		_, err = io.ReadAll(localStream)
		t.PrintErr(err, "local stream: read all")

		var serr *quic.StreamError
		t.ErrorAs(err, &serr)
		t.EqualValues(serr.ErrorCode, 9)
		t.True(serr.Remote)
	})

	t.RunWithGoLeak("remote OpenStreamSync", func() {
		_, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, nil)
		defer closef()

		opened := make(chan struct{})
		go func() {
			stream, err := remoteConn.OpenStreamSync(context.Background())
			t.PrintIfErrPanic(err, "remote OpenStream")

			_, err = stream.Write([]byte{0}) // NOTE without stream write, other side stream will not be opened.
			t.PrintIfErrPanic(err, "remote stream: write")

			close(opened)
		}()

		_, err := localConn.AcceptStream(context.Background())
		t.NoError(err)

		select {
		case <-time.After(time.Second):
			t.Fail("failed AcceptStream")
		case <-opened:
		}
	})
}

func (t *testQuic) TestRemoteStream() {
	t.RunWithGoLeak("OpenStreamSync after context canceled", func() {
		_, _, _, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, nil)
		defer closef()

		ctx, cancel := t.Ctx()
		cancel(nil)

		_, err := remoteConn.OpenStreamSync(ctx)
		t.PrintErr(err, "remote conn: OpenStreamSync")
		t.ErrorIs(err, context.Canceled)
	})

	t.RunWithGoLeak("after idle timeout, remote OpenStream", func() {
		// NOTE after idle timeout, remote connection is not able to use.
		remoteQuicConfig := DefaultQuicConfig()
		remoteQuicConfig.MaxIdleTimeout = time.Millisecond * 3
		remoteQuicConfig.KeepAlivePeriod = 0

		_, _, _, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, remoteQuicConfig)
		defer closef()

		remoteStream, err := remoteConn.OpenStream()
		t.NoError(err)

		_, err = io.ReadAll(remoteStream)
		t.PrintErr(err, "remote stream: read all")

		var ierr *quic.IdleTimeoutError
		t.ErrorAs(err, &ierr)

		_, err = remoteConn.OpenStream()
		t.ErrorAs(err, &ierr)
	})

	t.RunWithGoLeak("local stream: CancelRead()", func() {
		_, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, nil)
		defer closef()

		go func() {
			localStream, err := localConn.AcceptStream(context.Background())
			t.PrintIfErrPanic(err, "local AcceptStream")

			localStream.CancelRead(9)
		}()

		remoteStream, err := remoteConn.OpenStream()
		t.NoError(err)

		for i := range 333 {
			_, err = remoteStream.Write([]byte{0})
			t.PrintErr(err, "remote stream: write: %d", i)
			if err != nil {
				break
			}

			<-time.After(time.Millisecond * 33)
		}

		var serr *quic.StreamError
		t.ErrorAs(err, &serr)
		t.EqualValues(serr.ErrorCode, 9)
		t.True(serr.Remote)
	})

	t.RunWithGoLeak("local stream: CancelWrite()", func() {
		_, _, localConn, remoteConn, closef := t.OpenConnections(context.Background(), context.Background(), nil, nil)
		defer closef()

		go func() {
			localStream, err := localConn.AcceptStream(context.Background())
			t.PrintIfErrPanic(err, "local AcceptStream")

			localStream.CancelWrite(9)
		}()

		remoteStream, err := remoteConn.OpenStream()
		t.NoError(err)
		_, err = remoteStream.Write([]byte{0})
		t.PrintErr(err, "remote stream: write")

		_, err = io.ReadAll(remoteStream)
		t.PrintErr(err, "remote stream: read all")

		var serr *quic.StreamError
		t.ErrorAs(err, &serr)
		t.EqualValues(serr.ErrorCode, 9)
		t.True(serr.Remote)
	})
}

func (t *testQuic) TestRemoteReboot() {
	remoteQuicConfig := DefaultQuicConfig()
	remoteQuicConfig.MaxIdleTimeout = time.Millisecond * 33

	b := util.UUID().Bytes()

	localReady := func() net.PacketConn {
		lpconn, l := t.RandomListener(nil, nil)

		go t.ListenerAccept(context.Background(), l, func(stream *quic.Stream) error {
			_, err := stream.Write(b)
			t.PrintIfErrPanic(err, "local stream: write")
			err = stream.Close()
			t.PrintIfErrPanic(err, "local stream: close")

			return nil
		})

		t.T().Log("local addr:", lpconn.LocalAddr())

		return lpconn
	}

	remoteWrite := func(remoteConn Conn) error {
		remoteStream, err := remoteConn.OpenStream()
		if err != nil {
			return err
		}

		if _, err := remoteStream.Write([]byte{0}); err != nil {
			return err
		}

		if err := remoteStream.Close(); err != nil {
			return err
		}

		rb, err := io.ReadAll(remoteStream)
		if err != nil {
			return err
		}

		t.IfTruePanic(!bytes.Equal(b, rb), "")

		return nil
	}

	t.RunWithGoLeak("same tls key", func() {
		lpconn := localReady()
		defer lpconn.Close()

		remoteTLSConfig := t.NewTLSConfig()

		t.Run("remote connect", func() {
			remoteConn, err := Dial(context.Background(), lpconn.LocalAddr(), remoteTLSConfig, remoteQuicConfig)
			t.NoError(err)

			t.NoError(remoteWrite(remoteConn))
			t.NoError(remoteConn.CloseWithError(0, ""))
		})

		t.Run("remote reboot", func() {
			remoteConn, err := Dial(context.Background(), lpconn.LocalAddr(), remoteTLSConfig, remoteQuicConfig)
			t.NoError(err)

			t.NoError(remoteWrite(remoteConn))
			t.NoError(remoteConn.CloseWithError(0, ""))
		})
	})

	t.RunWithGoLeak("changed tls key", func() {
		lpconn := localReady()
		defer lpconn.Close()

		t.Run("remote connect", func() {
			remoteConn, err := Dial(context.Background(), lpconn.LocalAddr(), t.NewTLSConfig(), remoteQuicConfig)
			t.PrintIfErrPanic(err, "remote dial")

			t.NoError(remoteWrite(remoteConn))
			t.NoError(remoteConn.CloseWithError(0, ""))
		})

		t.Run("remote reboot", func() {
			remoteConn, err := Dial(context.Background(), lpconn.LocalAddr(), t.NewTLSConfig(), remoteQuicConfig)
			t.NoError(err)

			t.NoError(remoteWrite(remoteConn))
			remoteConn.CloseWithError(0, "")
		})
	})
}

func (t *testQuic) TestLocalReboot() {
	localQuicConfig := DefaultQuicConfig()
	localQuicConfig.MaxIdleTimeout = time.Millisecond * 3
	localQuicConfig.KeepAlivePeriod = 0

	remoteQuicConfig := DefaultQuicConfig()
	remoteQuicConfig.MaxIdleTimeout = time.Millisecond * 3

	localb := util.UUID().Bytes()
	remoteb := util.UUID().Bytes()

	listenerAcceptStream := func(l *quic.EarlyListener) func(stream *quic.Stream) error {
		return func(stream *quic.Stream) error {
			_, err := stream.Write(localb)
			t.PrintIfErrPanic(err, "%p: local stream: write", l)
			err = stream.Close()
			t.PrintIfErrPanic(err, "%p: local stream: close", l)

			_, err = io.ReadAll(stream)
			t.PrintErr(err, "%p: local stream: read", l)

			return nil
		}
	}

	remoteWrite := func(remoteConn Conn) error {
		t.Print("> remote write")

		remoteStream, err := remoteConn.OpenStream()
		if err != nil {
			return err
		}
		defer remoteStream.Close()

		if _, err := remoteStream.Write(remoteb); err != nil {
			return err
		}

		rb, err := io.ReadAll(remoteStream)
		if err != nil {
			return err
		}

		t.Equal(localb, rb)

		return nil
	}

	t.Run("remote send to new local listener", func() {
		t.RunWithGoLeak("reboot listener", func() {
			lpconn := t.RandomPacketConn()
			defer lpconn.Close()

			l := t.NewListener(lpconn, nil, localQuicConfig)

			go t.ListenerAccept(context.Background(), l, listenerAcceptStream(l))

			rpconn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
			t.NoError(err, "remote: new PacketConn")

			t.T().Log("remote connect")
			remoteConn, err := dialWithPacketConn(context.Background(), rpconn, lpconn.LocalAddr(), t.NewTLSConfig(), remoteQuicConfig)
			t.NoError(err)
			defer remoteConn.CloseWithError(0, "")

			t.NoError(remoteWrite(remoteConn))

			t.T().Log("> local reboot")
			t.NoError(l.Close()) // NOTE after listener Close(), listener stil keeps the existing remote connections.

			newl := t.NewListener(lpconn, nil, nil)

			go t.ListenerAccept(context.Background(), newl, listenerAcceptStream(newl))

			if err != nil { // NOTE after listener closed, remote write may success or fail
				var ierr *quic.IdleTimeoutError
				var serr *quic.StatelessResetError

				switch err = remoteWrite(remoteConn); {
				case errors.As(err, &ierr):
				case errors.As(err, &serr):
				default:
					t.Failf("unexpected error", "%T %v", err, err)
				}
			}
		})

		t.RunWithGoLeak("reboot PacketConn; same tls key", func() {
			// localQuicConfig.Tracer = DebugQLogTracer(os.Stderr)

			localTLSConfig := t.NewTLSConfig()

			lpconn := t.RandomPacketConn()
			defer lpconn.Close()

			l := t.NewListener(lpconn, localTLSConfig, localQuicConfig)

			go t.ListenerAccept(context.Background(), l, listenerAcceptStream(l))

			rpconn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
			t.NoError(err, "remote: new PacketConn")

			t.T().Log("remote connect")
			remoteConn, err := dialWithPacketConn(context.Background(), rpconn, lpconn.LocalAddr(), t.NewTLSConfig(), remoteQuicConfig)
			t.NoError(err)
			defer remoteConn.CloseWithError(0, "")

			t.NoError(remoteWrite(remoteConn))
			t.PrintErr(err, "< remote write")

			t.T().Log("> local reboot")
			t.NoError(lpconn.Close())

			newlpconn, err := NewPacketConn(lpconn.LocalAddr().(*net.UDPAddr))
			t.NoError(err, "local new PacketConn")
			defer newlpconn.Close()

			t.Print("local new PacketConn: addr=%q", newlpconn.LocalAddr())

			newl := t.NewListener(newlpconn, localTLSConfig, localQuicConfig)

			go t.ListenerAccept(context.Background(), newl, listenerAcceptStream(newl))

			err = remoteWrite(remoteConn)
			t.PrintErr(err, "< remote write")

			var ierr *quic.StatelessResetError
			t.ErrorAs(err, &ierr, "%T", err)
		})
	})
}

func TestQuic(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testQuic))
}
