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

	"github.com/madflojo/testcerts"
	"github.com/quic-go/quic-go"
	quiclogging "github.com/quic-go/quic-go/logging"
	"github.com/quic-go/quic-go/qlog"
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

	return Dial(ctx, pconn, addr, tlsConfig, quicConfig)
}

func (t *BaseTest) RandomListener(
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
) (net.PacketConn, *quic.EarlyListener, error) {
	pconn, err := NewTestPacketConn(0)
	if err != nil {
		return nil, nil, err
	}

	l, err := t.NewListener(pconn, tlsConfig, quicConfig)

	return pconn, l, err
}

func (t *BaseTest) NewListener(
	pconn net.PacketConn,
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
) (*quic.EarlyListener, error) {
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

	return l, err
}

func (t *BaseTest) OpenConnections(
	localCtx, remoteCtx context.Context,
	localQuicConfig, remoteQuicConfig *quic.Config,
) (net.PacketConn, net.PacketConn, quic.Connection, quic.Connection, func()) {
	lpconn, l, err := t.RandomListener(nil, localQuicConfig)
	t.PrintIfErrPanic(err, "local: new listener: %q", lpconn.LocalAddr())

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

func NewTestPacketConn(port int) (net.PacketConn, error) {
	return net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: port})
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
		return Dial(ctx, c, addr, tlsConfig, quicConfig)
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
