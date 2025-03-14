//go:build test
// +build test

package quicstream

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/madflojo/testcerts"
	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	quiclogging "github.com/quic-go/quic-go/logging"
	"github.com/quic-go/quic-go/qlog"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type BaseTest struct {
	dutil.BaseSuite
	tlsPool *sync.Pool
	tlsUsed [][2]interface{}
}

func (t *BaseTest) SetupSuite() {
	t.tlsPool = &sync.Pool{
		New: func() any {
			kp, tlsConfig := NewTestTLSConfig(util.UUID().String())

			return [2]interface{}{kp, tlsConfig}
		},
	}
}

func (t *BaseTest) TearDownTest() {
	for i := range slices.Values(t.tlsUsed) {
		t.tlsPool.Put(i)
	}
}

func (t *BaseTest) NewTLSConfig() (*testcerts.KeyPair, *tls.Config) {
	i := t.tlsPool.Get().([2]interface{})

	t.tlsUsed = append(t.tlsUsed, i)

	return i[0].(*testcerts.KeyPair), i[1].(*tls.Config)
}

func (t *BaseTest) NewPacketConn() net.PacketConn {
	pconn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	t.PrintIfErrPanic(err, "new net.PacketConn")

	return pconn
}

func (t *BaseTest) NewDial(ctx context.Context, addr net.Addr, quicConfig *quic.Config) (quic.Connection, error) {
	pconn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return nil, err
	}

	return t.Dial(ctx, pconn, addr, quicConfig)
}

func (t *BaseTest) Dial(ctx context.Context, pconn net.PacketConn, addr net.Addr, quicConfig *quic.Config) (quic.Connection, error) {
	_, tlsConfig := t.NewTLSConfig()

	if quicConfig == nil {
		quicConfig = DefaultQuicConfig()
	}

	return DialWithPacketConn(ctx, pconn, addr, tlsConfig, quicConfig)
}

func (t *BaseTest) RandomListener(
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
) (net.PacketConn, *quic.EarlyListener) {
	pconn, err := NewPacketConn(0)
	t.PrintIfErrPanic(err, "new net.PacketConn")

	return pconn, t.NewListener(pconn, tlsConfig, quicConfig)
}

func (t *BaseTest) NewListener(
	pconn net.PacketConn,
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
) *quic.EarlyListener {
	if tlsConfig == nil {
		_, tlsConfig = t.NewTLSConfig()
	}

	statelessResetKey := NewStatelessResetKey(tlsConfig.Certificates[0].Certificate[0])

	tp := quic.Transport{
		Conn:              pconn,
		StatelessResetKey: statelessResetKey,
	}

	if quicConfig == nil {
		quicConfig = DefaultQuicConfig()
	}

	l, err := tp.ListenEarly(tlsConfig, quicConfig)
	t.PrintIfErrPanic(err, "new listener")

	return l
}

func (t *BaseTest) OpenConnections(
	localCtx, remoteCtx context.Context,
	localQuicConfig, remoteQuicConfig *quic.Config,
) (net.PacketConn, net.PacketConn, quic.Connection, quic.Connection, func()) {
	lpconn, l := t.RandomListener(nil, localQuicConfig)

	rpconn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	t.PrintIfErrPanic(err, "remote: new PacketConn")

	localConnCh := make(chan quic.Connection, 1)
	go func() {
		conn, err := l.Accept(localCtx)
		t.PrintIfErrPanic(err, "accept")

		localConnCh <- conn
	}()

	if remoteQuicConfig == nil {
		remoteQuicConfig = DefaultQuicConfig()
	}

	remoteConn, err := t.Dial(remoteCtx, rpconn, lpconn.LocalAddr(), remoteQuicConfig)
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

func (t *BaseTest) ListenerAccept(ctx context.Context, l *quic.EarlyListener, f func(quic.Stream) error) {
	for {
		conn, err := l.Accept(ctx)
		t.PrintIfErr(err, "%p: local PacketConn: accept", l)
		if err != nil {
			return
		}

		go t.StreamsAccept(ctx, conn, f)
	}
}

func (t *BaseTest) StreamsAccept(ctx context.Context, conn quic.Connection, f func(quic.Stream) error) {
	for {
		stream, err := conn.AcceptStream(ctx)
		t.PrintIfErrCaller(err, 3, "AcceptStream")
		if err != nil {
			return
		}

		err = f(stream)
		t.PrintIfErr(err, "stream")
	}
}

func (t *BaseTest) OpenStreams(
	ctx context.Context,
) (
	net.PacketConn, net.PacketConn, quic.Connection, quic.Connection,
	quic.Stream, quic.Stream,
	func(),
) {
	lpconn, rpconn, localConn, remoteConn, closef := t.OpenConnections(ctx, ctx, nil, nil)

	waitLocalStream := dutil.AssertWaitChan(t.Assert(), time.Second, func() quic.Stream {
		stream, err := localConn.AcceptStream(ctx)
		t.PrintIfErrPanic(err, "local conn: AcceptStream")

		_, _, err = frame.ReadFixed(stream)
		t.IfErrPanicf(err, "local stream: read")

		return stream
	})

	remoteStream, err := remoteConn.OpenStream()
	t.PrintIfErrPanic(err, "remote conn: OpenStream")
	t.IfErrPanicf(frame.WriteFixed(remoteStream, []byte{0}), "remote stream: write")

	return lpconn, rpconn, localConn, remoteConn, waitLocalStream(), remoteStream, closef
}

func (t *BaseTest) NewDialPool() dutil.LockedMap[string, quic.Connection] {
	p, err := dutil.NewShardedLockedMap[string, quic.Connection](nil, 1<<9, 1<<9)

	t.PrintIfErrPanic(err, "new pool")

	return p
}

func NewTestTLSConfig(serverName string) (*testcerts.KeyPair, *tls.Config) {
	ca := testcerts.NewCA()

	kp, err := ca.NewKeyPair(serverName)
	if err != nil {
		panic(err)
	}

	tlsConfig, err := kp.ConfigureTLSConfig(ca.GenerateTLSConfig())
	if err != nil {
		panic(err)
	}
	tlsConfig.InsecureSkipVerify = true

	return kp, tlsConfig
}

func TestNewDail(
	ctx context.Context,
	addr net.Addr,
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
) (quic.Connection, error) {
	switch c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0}); {
	case err != nil:
		return nil, err
	default:
		return DialWithPacketConn(ctx, c, addr, tlsConfig, quicConfig)
	}
}

func RandomStatelessResetKey() *quic.StatelessResetKey {
	return NewStatelessResetKey(util.UUID().Bytes())
}

func DebugQLogTracer(out io.WriteCloser) func(context.Context, quiclogging.Perspective, quic.ConnectionID) *quiclogging.ConnectionTracer {
	return func(_ context.Context, p quiclogging.Perspective, connID quiclogging.ConnectionID) *quiclogging.ConnectionTracer {
		return qlog.NewConnectionTracer(out, p, connID)
	}
}

var (
	CloseStreamWithError  = closeStream
	CloseStreamsWithError = closeStreams
)

type DummyStream struct {
	*io.PipeReader
	*io.PipeWriter
}

func (DummyStream) StreamID() quic.StreamID {
	return 0
}

func (s DummyStream) Close() error {
	return s.PipeWriter.Close()
}

func (s DummyStream) CancelRead(c quic.StreamErrorCode) {
	s.PipeReader.CloseWithError(errors.Errorf("cancel read error: %d", c))
}

func (s DummyStream) CancelWrite(c quic.StreamErrorCode) {
	s.PipeReader.CloseWithError(errors.Errorf("cancel write error: %d", c))
}

func (s DummyStream) Context() context.Context {
	return nil
}

func (s DummyStream) SetDeadline(time.Time) error {
	return nil
}

func (s DummyStream) SetReadDeadline(time.Time) error {
	return nil
}

func (s DummyStream) SetWriteDeadline(time.Time) error {
	return nil
}

func NewDummyStreams() (DummyStream, DummyStream) {
	ar, bw := io.Pipe()
	br, aw := io.Pipe()

	return DummyStream{PipeReader: ar, PipeWriter: aw},
		DummyStream{PipeReader: br, PipeWriter: bw}
}
