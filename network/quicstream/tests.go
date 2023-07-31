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

type TestServer struct {
	*Server
	Bind      *net.UDPAddr
	TLSConfig *tls.Config
}

func NewTestServer(
	bind *net.UDPAddr,
	tlsconfig *tls.Config,
	quicconfig *quic.Config,
	handler Handler,
) (*TestServer, error) {
	srv, err := NewServer(bind, tlsconfig, quicconfig, handler, func() time.Duration { return time.Second * 33 })
	if err != nil {
		return nil, err
	}

	return &TestServer{
		Server:    srv,
		Bind:      bind,
		TLSConfig: tlsconfig,
	}, nil
}

func (srv *TestServer) EnsureStart(ctx context.Context) error {
	if err := srv.Start(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(time.Millisecond * 33)
	defer ticker.Stop()

	for range ticker.C {
		if srv.checkListen() {
			break
		}
	}

	return nil
}

func (srv *TestServer) StopWait() error {
	if err := srv.Stop(); err != nil {
		return err
	}

	ticker := time.NewTicker(time.Millisecond * 33)
	defer ticker.Stop()

	for range ticker.C {
		if !srv.checkListen() {
			break
		}
	}

	return nil
}

func (srv *TestServer) checkListen() bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := quic.DialAddrEarly(ctx, srv.Bind.String(), &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         srv.TLSConfig.NextProtos,
	}, nil)
	if err != nil {
		return false
	}

	defer func() {
		_ = conn.CloseWithError(0x999, "")
	}()

	return true
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

func (t *BaseTest) NewServer(bind *net.UDPAddr, tlsconfig *tls.Config, qconfig *quic.Config, handler Handler) *TestServer {
	srv, err := NewTestServer(bind, tlsconfig, qconfig, handler)
	t.NoError(err)

	srv.SetLogging(logging.TestNilLogging)

	return srv
}

func (t *BaseTest) NewDefaultServer(qconfig *quic.Config, handler Handler) *TestServer {
	if qconfig == nil {
		qconfig = &quic.Config{}
	}

	if handler == nil {
		handler = t.EchoHandler()
	}

	return t.NewServer(t.Bind, t.TLSConfig, qconfig, handler)
}

func (t *BaseTest) EmptyHandler() Handler {
	return func(_ context.Context, _ net.Addr, r io.Reader, w io.WriteCloser) error {
		defer func() {
			_ = w.Close()
		}()

		b := make([]byte, 1)
		_, err := r.Read(b)

		return err
	}
}

func (t *BaseTest) EchoHandler() Handler {
	return func(_ context.Context, _ net.Addr, r io.Reader, w io.WriteCloser) error {
		defer func() {
			_ = w.Close()
		}()

		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}

		_, err = w.Write(b)
		if err != nil {
			return err
		}

		return nil
	}
}

func (t *BaseTest) NewConnection(addr *net.UDPAddr, quicconfig *quic.Config) *Connection {
	if quicconfig == nil {
		quicconfig = &quic.Config{
			HandshakeIdleTimeout: time.Millisecond * 100,
		}
	}

	conn, err := Dial(
		context.Background(),
		addr,
		&tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{t.Proto},
		},
		quicconfig,
	)
	if err != nil {
		panic(err)
	}

	return conn
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

func RandomConnInfo() ConnInfo {
	return randomConnInfo()
}

func RandomConnInfos(n int) []ConnInfo {
	m := map[string]struct{}{}

	us := make([]ConnInfo, n)

	var i int
	for {
		ci := randomConnInfo()
		if _, found := m[ci.String()]; found {
			continue
		}

		us[i] = ci
		m[ci.String()] = struct{}{}
		i++

		if i == n {
			return us
		}
	}
}

func RandomUDPAddr() *net.UDPAddr {
	for {
		bip, err := rand.Int(rand.Reader, big.NewInt(math.MaxUint32))
		if err != nil {
			continue
		}
		bport, err := rand.Int(rand.Reader, big.NewInt(math.MaxUint16))
		if err != nil || bport.Int64() < 1024 {
			continue
		}

		buf := make([]byte, 4)

		binary.LittleEndian.PutUint32(buf, uint32(bip.Uint64()))

		return &net.UDPAddr{
			IP:   net.IP(buf),
			Port: int(bport.Uint64()),
		}
	}
}

func randomConnInfo() ConnInfo {
	return MustConnInfo(RandomUDPAddr(), true)
}
