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
	"github.com/spikeekips/mitum/util"
)

type BaseTest struct {
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
