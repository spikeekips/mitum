//go:build test
// +build test

package quicstream

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"sync"

	"github.com/lucas-clemente/quic-go"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
)

type BaseTest struct {
	sync.Mutex
	suite.Suite
	Bind      *net.UDPAddr
	TLSConfig *tls.Config
	Proto     string
	binded    []*net.UDPAddr
}

func (t *BaseTest) SetupSuite() {
	t.Proto = "quicstream"
	t.TLSConfig = t.NewTLSConfig(t.Proto)
}

func (t *BaseTest) SetupTest() {
	t.Bind = t.NewBind()
}

func (t *BaseTest) NewBind() *net.UDPAddr {
	t.Lock()
	defer t.Unlock()

	addr, err := freePort(t.binded)
	t.NoError(err)

	t.binded = append(t.binded, addr)

	return addr
}

func (t *BaseTest) NewTLSConfig(proto string) *tls.Config {
	return generateTLSConfig(proto)
}

func (t *BaseTest) NewServer(bind *net.UDPAddr, tlsconfig *tls.Config) *Server {
	srv := NewServer(bind, tlsconfig, &quic.Config{}, func(_ net.Addr, r io.Reader, w io.Writer) error {
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}

		_, _ = w.Write(b)

		return nil
	})
	srv.SetLogging(logging.TestNilLogging)

	return srv
}

func (t *BaseTest) NewDefaultServer() *Server {
	return t.NewServer(t.Bind, t.TLSConfig)
}

func (t *BaseTest) NewClient(addr *net.UDPAddr) *Client {
	return NewClient(
		addr,
		&tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{t.Proto},
		},
		nil,
		nil,
	)
}

func freePort(excludes []*net.UDPAddr) (*net.UDPAddr, error) {
	for {
		zero, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}

		l, err := net.ListenUDP("udp", zero)
		if err != nil {
			return nil, err
		}

		_ = l.Close()

		addr := l.LocalAddr().(*net.UDPAddr)

		var found bool
		for i := range excludes {
			if addr.Port == excludes[i].Port {
				found = true

				break
			}
		}

		if !found {
			return addr, nil
		}
	}
}

func generateTLSConfig(proto string) *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
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
		NextProtos:   []string{proto},
	}
}
