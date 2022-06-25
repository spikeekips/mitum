package launch

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"time"

	"github.com/lucas-clemente/quic-go"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util/encoder"
)

var QuicStreamNetworkProto = "mitum-example-network"

func NewNetworkClient(
	encs *encoder.Encoders, enc encoder.Encoder, idleTimeout time.Duration,
) *isaacnetwork.QuicstreamClient {
	return isaacnetwork.NewQuicstreamClient(encs, enc, idleTimeout, QuicStreamNetworkProto, DefaultQuicConfig())
}

func GenerateNewTLSConfig() *tls.Config {
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
		NextProtos:   []string{QuicStreamNetworkProto},
	}
}

func DefaultQuicConfig() *quic.Config {
	return &quic.Config{
		HandshakeIdleTimeout: time.Second * 2,  //nolint:gomnd //...
		MaxIdleTimeout:       time.Second * 30, //nolint:gomnd //...
		KeepAlive:            true,
	}
}

func Handlers(handlers *isaacnetwork.QuicstreamHandlers) *quicstream.PrefixHandler {
	prefix := quicstream.NewPrefixHandler(handlers.ErrorHandler)
	prefix.
		Add(isaacnetwork.HandlerPrefixRequestProposal, handlers.RequestProposal).
		Add(isaacnetwork.HandlerPrefixProposal, handlers.Proposal).
		Add(isaacnetwork.HandlerPrefixLastSuffrageProof, handlers.LastSuffrageProof).
		Add(isaacnetwork.HandlerPrefixSuffrageProof, handlers.SuffrageProof).
		Add(isaacnetwork.HandlerPrefixLastBlockMap, handlers.LastBlockMap).
		Add(isaacnetwork.HandlerPrefixBlockMap, handlers.BlockMap).
		Add(isaacnetwork.HandlerPrefixBlockMapItem, handlers.BlockMapItem).
		Add(isaacnetwork.HandlerPrefixMemberlistNodeChallenge, handlers.MemberlistNodeChallenge)

	return prefix
}

func BroadcastThruMemberlist(memberlist *quictransport.Memberlist, id string, b []byte) error {
	donech := make(chan struct{})

	body := quictransport.NewBroadcast(b, id, donech)

	memberlist.Broadcast(body)

	<-donech

	return nil
}
