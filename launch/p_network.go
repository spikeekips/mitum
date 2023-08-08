package launch

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
	"golang.org/x/sync/singleflight"
)

var (
	PNameNetwork                 = ps.Name("network")
	PNameStartNetwork            = ps.Name("start-network")
	PNameQuicstreamClient        = ps.Name("network-client")
	PNameRateLimiterContextKey   = ps.Name("network-ratelimiter")
	QuicstreamClientContextKey   = util.ContextKey("network-client")
	QuicstreamServerContextKey   = util.ContextKey("quicstream-server")
	QuicstreamHandlersContextKey = util.ContextKey("quicstream-handlers")
	ConnectionPoolContextKey     = util.ContextKey("network-connection-pool")
	RateLimiterContextKey        = util.ContextKey("network-ratelimiter")
)

func PQuicstreamClient(pctx context.Context) (context.Context, error) {
	var encs *encoder.Encoders
	var enc encoder.Encoder
	var params *LocalParams

	if err := util.LoadFromContextOK(pctx,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
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

	client := NewNetworkClient(encs, enc, connectionPool) //nolint:gomnd //...

	nctx := context.WithValue(pctx, QuicstreamClientContextKey, client)

	return context.WithValue(nctx, ConnectionPoolContextKey, connectionPool), nil
}

func PNetwork(pctx context.Context) (context.Context, error) {
	e := util.StringError("prepare network")

	var log *logging.Logging
	var encs *encoder.Encoders
	var enc encoder.Encoder
	var design NodeDesign
	var params *LocalParams

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
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

	nctx := context.WithValue(pctx, QuicstreamServerContextKey, server)
	nctx = context.WithValue(nctx, QuicstreamHandlersContextKey, handlers)

	return nctx, nil
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

func PNetworkRateLimiter(pctx context.Context) (context.Context, error) {
	e := util.StringError("ratelimiter")

	var log *logging.Logging
	var design NodeDesign
	var handlers *quicstream.PrefixHandler
	var watcher *isaac.LastConsensusNodesWatcher

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
		QuicstreamHandlersContextKey, &handlers,
		LastConsensusNodesWatcherContextKey, &watcher,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	args := NewRateLimitHandlerArgs()
	args.Rules = design.LocalParams.Network.RateLimit().RateLimiterRules
	args.Rules.SetIsInConsensusNodesFunc(rateLimiterIsInConsensusNodesFunc(watcher))

	var rateLimitHandler *RateLimitHandler

	switch i, err := NewRateLimitHandler(args); {
	case err != nil:
		return pctx, e.Wrap(err)
	default:
		rateLimitHandler = i
	}

	rslog := logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
		return zctx.Str("module", "ratelimit")
	}).SetLogging(log)

	handlers.SetHandlerFunc(func(handler quicstream.Handler) quicstream.Handler {
		return func(ctx context.Context, addr net.Addr, r io.Reader, w io.WriteCloser) (context.Context, error) {
			ctx, err := handler(ctx, addr, r, w)
			if ctx != nil && ctx.Value(RateLimiterResultContextKey) != nil {
				rslog.Log().Debug().
					Stringer("remote", addr).
					Func(func(e *zerolog.Event) {
						i, ok := ctx.Value(RateLimiterResultContextKey).(func() RateLimiterResult)
						if !ok {
							return
						}

						e.Interface("ratelimit_result", i())
					}).
					Msg("check ratelimit")
			}

			return ctx, err
		}
	})

	return context.WithValue(pctx, RateLimiterContextKey, rateLimitHandler), nil
}

func rateLimiterIsInConsensusNodesFunc(
	watcher *isaac.LastConsensusNodesWatcher,
) func() (util.Hash, func(base.Address) bool, error) {
	var sg singleflight.Group
	var sgf singleflight.Group

	return func() (util.Hash, func(base.Address) bool, error) {
		// NOTE suffrage and candidates
		i, err, _ := util.SingleflightDo[[2]interface{}](&sg, "", func() (r [2]interface{}, _ error) {
			var proof base.SuffrageProof
			var candidates base.State

			switch i, j, err := watcher.Last(); {
			case err != nil:
				return r, err
			case i == nil:
				return r, errors.Errorf("proof not found")
			default:
				proof = i
				candidates = j
			}

			return [2]interface{}{
				proof.State().Hash(),
				func(node base.Address) bool {
					exists, _, _ := util.SingleflightDo[bool](&sgf, node.String(), func() (bool, error) {
						_, exists, _ := isaac.IsNodeAddressInLastConsensusNodes(node, proof, candidates)

						return exists, nil
					})

					return exists
				},
			}, nil
		})

		if err != nil {
			return nil, nil, err
		}

		return i[0].(util.Hash), i[1].(func(base.Address) bool), nil //nolint:forcetypeassert //...
	}
}
