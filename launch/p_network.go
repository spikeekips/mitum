package launch

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/base"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameNetwork                 = ps.Name("network")
	PNameStartNetwork            = ps.Name("start-network")
	PNameQuicstreamClient        = ps.Name("network-client")
	QuicstreamClientContextKey   = util.ContextKey("network-client")
	QuicstreamServerContextKey   = util.ContextKey("quicstream-server")
	QuicstreamHandlersContextKey = util.ContextKey("quicstream-handlers")
	ConnectionPoolContextKey     = util.ContextKey("network-connection-pool")
)

func PQuicstreamClient(pctx context.Context) (context.Context, error) {
	var encs *encoder.Encoders
	var params *LocalParams

	if err := util.LoadFromContextOK(pctx,
		EncodersContextKey, &encs,
		LocalParamsContextKey, &params,
	); err != nil {
		return pctx, errors.WithMessage(err, "network client")
	}

	connectionPool, err := NewConnectionPool(
		params.Network.ConnectionPoolSize(),
		params.ISAAC.NetworkID(),
		params.Network,
	)
	if err != nil {
		return pctx, err
	}

	client := NewNetworkClient(encs, encs.Default(), connectionPool) //nolint:gomnd //...

	return util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
		QuicstreamClientContextKey: client,
		ConnectionPoolContextKey:   connectionPool,
	}), nil
}

func PNetwork(pctx context.Context) (context.Context, error) {
	e := util.StringError("prepare network")

	var log *logging.Logging
	var design NodeDesign
	var params *LocalParams

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
		LocalParamsContextKey, &params,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	handlers := quicstream.NewPrefixHandler(nil)

	quicconfig := ServerQuicConfig(params.Network)

	server, err := quicstream.NewServer(
		design.Network.Bind,
		GenerateNewTLSConfig(params.ISAAC.NetworkID()),
		quicconfig,
		handlers.Handler,
		func() time.Duration {
			return params.Network.MaxStreamTimeout()
		},
	)
	if err != nil {
		return pctx, err
	}

	_ = server.SetLogging(log)

	return util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
		QuicstreamServerContextKey:   server,
		QuicstreamHandlersContextKey: handlers,
	}), nil
}

func PStartNetwork(pctx context.Context) (context.Context, error) {
	var server *quicstream.Server
	if err := util.LoadFromContextOK(pctx, QuicstreamServerContextKey, &server); err != nil {
		return pctx, err
	}

	return pctx, server.Start(context.Background())
}

func PCloseNetwork(pctx context.Context) (context.Context, error) {
	var server *quicstream.Server
	if err := util.LoadFromContext(pctx, QuicstreamServerContextKey, &server); err != nil {
		return pctx, err
	}

	if server != nil {
		if err := server.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
			return pctx, err
		}
	}

	return pctx, nil
}

func NewNetworkClient(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	connectionPool *quicstream.ConnectionPool,
) *isaacnetwork.BaseClient {
	return isaacnetwork.NewBaseClient(
		encs, enc,
		connectionPool.Dial,
		connectionPool.CloseAll,
	)
}

func NewConnectionPool(
	size uint64, networkID base.NetworkID, params *NetworkParams,
) (*quicstream.ConnectionPool, error) {
	return quicstream.NewConnectionPool(
		size,
		NewConnInfoDialFunc(networkID, params),
	)
}

func NewConnInfoDialFunc(networkID base.NetworkID, params *NetworkParams) quicstream.ConnInfoDialFunc {
	return quicstream.NewConnInfoDialFunc(
		func() *quic.Config {
			return DialQuicConfig(params)
		},
		func() *tls.Config {
			return &tls.Config{
				NextProtos: []string{string(networkID)},
			}
		},
	)
}

func GenerateNewTLSConfig(networkID base.NetworkID) *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 1024) //nolint:gomnd //...
	if err != nil {
		panic(err)
	}

	template := x509.Certificate{SerialNumber: big.NewInt(1)}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{string(networkID)},
	}
}

func DefaultServerQuicConfig() *quic.Config {
	return &quic.Config{
		HandshakeIdleTimeout: time.Second * 2,
		MaxIdleTimeout:       time.Second * 30, //nolint:gomnd //...
		KeepAlivePeriod:      time.Second * 6,  //nolint:gomnd //...
		MaxIncomingStreams:   33,               //nolint:gomnd // default will be enough :)
	}
}

func ServerQuicConfig(params *NetworkParams) *quic.Config {
	config := DefaultServerQuicConfig()

	config.HandshakeIdleTimeout = params.HandshakeIdleTimeout()
	config.MaxIdleTimeout = params.MaxIdleTimeout()
	config.KeepAlivePeriod = params.KeepAlivePeriod()
	config.MaxIncomingStreams = int64(params.MaxIncomingStreams())

	config.RequireAddressValidation = func(net.Addr) bool {
		return true // TODO NOTE manage blacklist
	}

	return config
}

func DefaultDialQuicConfig() *quic.Config {
	return &quic.Config{
		HandshakeIdleTimeout: time.Second * 2,
		MaxIdleTimeout:       time.Second * 30, //nolint:gomnd //...
		KeepAlivePeriod:      time.Second * 6,  //nolint:gomnd //...
	}
}

func DialQuicConfig(params *NetworkParams) *quic.Config {
	config := DefaultDialQuicConfig()
	if params != nil {
		config.HandshakeIdleTimeout = params.HandshakeIdleTimeout()
		config.MaxIdleTimeout = params.MaxIdleTimeout()
		config.KeepAlivePeriod = params.KeepAlivePeriod()
	}

	return config
}
