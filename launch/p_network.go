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

	"github.com/lucas-clemente/quic-go"
	"github.com/pkg/errors"
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
	QuicstreamClientContextKey   = ps.ContextKey("network-client")
	QuicstreamServerContextKey   = ps.ContextKey("quicstream-server")
	QuicstreamHandlersContextKey = ps.ContextKey("quicstream-handlers")
)

func PQuicstreamClient(ctx context.Context) (context.Context, error) {
	var encs *encoder.Encoders
	var enc encoder.Encoder
	var params base.LocalParams

	if err := ps.LoadFromContextOK(ctx,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
		LocalParamsContextKey, &params,
	); err != nil {
		return ctx, errors.WithMessage(err, "failed network client")
	}

	client := NewNetworkClient(encs, enc, time.Second*2, params.NetworkID()) //nolint:gomnd //...

	ctx = context.WithValue(ctx, QuicstreamClientContextKey, client) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func PNetwork(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare network")

	var log *logging.Logging
	var encs *encoder.Encoders
	var enc encoder.Encoder
	var design NodeDesign
	var params base.LocalParams

	if err := ps.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
		DesignContextKey, &design,
		LocalParamsContextKey, &params,
	); err != nil {
		return ctx, e(err, "")
	}

	handlers := quicstream.NewPrefixHandler(isaacnetwork.QuicstreamErrorHandler(enc))

	quicconfig := DefaultQuicConfig()
	quicconfig.AcceptToken = func(remote net.Addr, token *quic.Token) bool {
		return true // FIXME NOTE manage blacklist
	}

	server := quicstream.NewServer(
		design.Network.Bind,
		GenerateNewTLSConfig(params.NetworkID()),
		quicconfig,
		handlers.Handler,
	)
	_ = server.SetLogging(log)

	ctx = context.WithValue(ctx, QuicstreamServerContextKey, server)     //revive:disable-line:modifies-parameter
	ctx = context.WithValue(ctx, QuicstreamHandlersContextKey, handlers) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func PStartNetwork(ctx context.Context) (context.Context, error) {
	var server *quicstream.Server
	if err := ps.LoadFromContextOK(ctx, QuicstreamServerContextKey, &server); err != nil {
		return ctx, err
	}

	return ctx, server.Start()
}

func PCloseNetwork(ctx context.Context) (context.Context, error) {
	var server *quicstream.Server
	if err := ps.LoadFromContext(ctx, QuicstreamServerContextKey, &server); err != nil {
		return ctx, err
	}

	if server != nil {
		if err := server.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
			return ctx, err
		}
	}

	return ctx, nil
}

func NewNetworkClient(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	idleTimeout time.Duration,
	networkID base.NetworkID,
) *isaacnetwork.QuicstreamClient {
	return isaacnetwork.NewQuicstreamClient(encs, enc, idleTimeout, string(networkID), DefaultQuicConfig())
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

func DefaultQuicConfig() *quic.Config {
	return &quic.Config{
		HandshakeIdleTimeout: time.Second * 2,  //nolint:gomnd //...
		MaxIdleTimeout:       time.Second * 30, //nolint:gomnd //...
		KeepAlivePeriod:      time.Second * 6,  //nolint:gomnd //...
	}
}
