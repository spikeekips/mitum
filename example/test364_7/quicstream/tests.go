//go:build test
// +build test

package quicstream

import (
	"context"
	"crypto/tls"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/madflojo/testcerts"
	"github.com/quic-go/quic-go"
	"github.com/rs/zerolog"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

var TLSPool = sync.Pool{
	New: func() any {
		kp, tlsConfig := NewTestTLSConfig(util.UUID().String())

		i := [2]any{kp, tlsConfig}

		return &i
	},
}

type BaseTest struct {
	dutil.BaseSuite
	tlsPool *sync.Pool
	tlsUsed []*[2]any
}

func (t *BaseTest) Ctx() (context.Context, dutil.InErrorFunc[error]) {
	ctx, cancel := t.BaseSuite.Ctx()

	i := dutil.TestLogger.Level(zerolog.TraceLevel)

	return dutil.SetContextValueLogger(ctx, &i), cancel
}

func (t *BaseTest) TearDownTest() {
	for i := range slices.Values(t.tlsUsed) {
		TLSPool.Put(i)
	}
}

func (t *BaseTest) NewTLS() (*testcerts.KeyPair, *tls.Config) {
	i := TLSPool.Get().(*[2]any)

	t.tlsUsed = append(t.tlsUsed, i)

	j := *i

	return j[0].(*testcerts.KeyPair), j[1].(*tls.Config)
}

func (t *BaseTest) NewTLSConfig() *tls.Config {
	_, c := t.NewTLS()

	return c
}

func (t *BaseTest) RandomPacketConn() net.PacketConn {
	pconn, err := NewPacketConn(&net.UDPAddr{IP: net.IPv4zero, Port: 0})
	t.PrintIfErrPanic(err, "new net.PacketConn")

	return pconn
}

func (t *BaseTest) RandomListener(
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
) (net.PacketConn, *quic.EarlyListener) {
	pconn := t.RandomPacketConn()
	return pconn, t.NewListener(pconn, tlsConfig, quicConfig)
}

func (t *BaseTest) NewListener(
	pconn net.PacketConn,
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
) *quic.EarlyListener {
	if tlsConfig == nil {
		tlsConfig = t.NewTLSConfig()
	}

	tp := quic.Transport{
		Conn:              pconn,
		StatelessResetKey: DefaultStatelessResetKey(tlsConfig),
	}

	if quicConfig == nil {
		quicConfig = DefaultQuicConfig()
	}

	l, err := tp.ListenEarly(tlsConfig, quicConfig)
	t.PrintIfErrPanic(err, "new listener")

	return l
}

func (t *BaseTest) ListenerAccept(ctx context.Context, l *quic.EarlyListener, f func(*quic.Stream) error) {
	for {
		conn, err := l.Accept(ctx)
		t.PrintIfErr(err, "%p: local PacketConn: accept", l)
		if err != nil {
			return
		}

		go t.StreamsAccept(ctx, conn, f)
	}
}

func (t *BaseTest) StreamsAccept(ctx context.Context, conn Conn, f func(*quic.Stream) error) {
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

func (t *BaseTest) NewDialPool(ctx context.Context, idleTimeout, cleanInterval time.Duration) (DialFunc, dutil.InErrorFunc[error]) {
	d, e, err := NewConnPool(ctx, nil, 1<<14, idleTimeout, cleanInterval)
	t.IfErrPanic(err)

	return d, e
}

func (t *BaseTest) newConnPoolLockedMap() dutil.LockedMap[string, Conn] {
	p, err := dutil.NewLockedMap[string, Conn](nil, 1<<9)
	t.IfErrPanic(err)

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

func RandomStatelessResetKey() *quic.StatelessResetKey {
	return NewStatelessResetKey(util.UUID().Bytes())
}
