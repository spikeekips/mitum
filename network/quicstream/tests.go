//go:build test
// +build test

package quicstream

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"io"
	"math"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
)

func (srv *Server) StopWait() error {
	if err := srv.Stop(); err != nil {
		return err
	}

	ticker := time.NewTicker(time.Millisecond * 33)

	for range ticker.C {
		if func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
			defer cancel()

			conn, err := quic.DialAddrEarlyContext(ctx, srv.bind.String(), srv.tlsconfig, nil)
			if err != nil {
				return true
			}

			defer func() {
				_ = conn.CloseWithError(0x100, "")
			}()

			return false
		}() {
			break
		}
	}

	return nil
}

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
	t.Lock()
	defer t.Unlock()

	addr := t.NewAddr()
	t.binded = append(t.binded, addr)
	t.Bind = addr
}

func (t *BaseTest) NewAddr() *net.UDPAddr {
	addr, err := freeUDPAddr(t.binded)
	t.NoError(err)

	return addr
}

func (t *BaseTest) NewTLSConfig(proto string) *tls.Config {
	return generateTLSConfig(proto)
}

func (t *BaseTest) NewServer(bind *net.UDPAddr, tlsconfig *tls.Config, qconfig *quic.Config, handler Handler) *Server {
	srv, err := NewServer(bind, tlsconfig, qconfig, handler)
	t.NoError(err)

	srv.SetLogging(logging.TestNilLogging)

	return srv
}

func (t *BaseTest) NewDefaultServer(qconfig *quic.Config, handler Handler) *Server {
	if qconfig == nil {
		qconfig = &quic.Config{}
	}

	if handler == nil {
		handler = func(net.Addr, io.Reader, io.Writer) error { return nil }
	}

	return t.NewServer(t.Bind, t.TLSConfig, qconfig, handler)
}

func (t *BaseTest) EchoHandler() Handler {
	return func(_ net.Addr, r io.Reader, w io.Writer) error {
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}

		_, _ = w.Write(b)

		return nil
	}
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

func freeUDPAddr(excludes []*net.UDPAddr) (*net.UDPAddr, error) {
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

var (
	randomConnInfoLock sync.Mutex
	randomConnInfoMap  map[string]UDPConnInfo
)

func init() {
	randomConnInfoMap = map[string]UDPConnInfo{}
}

func RandomConnInfo() UDPConnInfo {
	randomConnInfoLock.Lock()
	defer randomConnInfoLock.Unlock()

	for {
		ci := randomConnInfo()

		if _, found := randomConnInfoMap[ci.String()]; !found {
			randomConnInfoMap[ci.String()] = ci

			return ci
		}
	}
}

func randomConnInfo() UDPConnInfo {
	for {
		bip, err := rand.Int(rand.Reader, big.NewInt(math.MaxUint32))
		if err != nil {
			continue
		}
		bport, err := rand.Int(rand.Reader, big.NewInt(math.MaxUint16))
		if err != nil {
			continue
		}

		buf := make([]byte, 4)

		binary.LittleEndian.PutUint32(buf, uint32(bip.Uint64()))

		ci := NewUDPConnInfo(&net.UDPAddr{IP: net.IP(buf), Port: int(bport.Uint64())}, true)

		if ci.isValid() != nil {
			continue
		}

		return ci
	}
}
