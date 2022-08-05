package quicmemberlist

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/lucas-clemente/quic-go"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	TransportDialFunc    func(context.Context, quicstream.UDPConnInfo) (quic.EarlyConnection, error)
	TransportWriteFunc   func(context.Context, quicstream.UDPConnInfo, []byte) error
	TransportGetConnInfo func(*net.UDPAddr) quicstream.UDPConnInfo
)

type Transport struct {
	streamch chan net.Conn
	*logging.Logging
	laddr        *net.UDPAddr
	dialf        TransportDialFunc
	writef       TransportWriteFunc
	packetch     chan *memberlist.Packet
	conns        *util.ShardedMap
	getconninfof TransportGetConnInfo
	sync.RWMutex
	shutdowned bool
}

func NewTransport(
	laddr *net.UDPAddr,
	dialf TransportDialFunc,
	writef TransportWriteFunc,
) *Transport {
	return &Transport{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "memberlist-quicmemberlist")
		}),
		laddr:        laddr,
		dialf:        dialf,
		writef:       writef,
		packetch:     make(chan *memberlist.Packet),
		streamch:     make(chan net.Conn),
		conns:        util.NewShardedMap(1 << 9), //nolint:gomnd //...
		getconninfof: func(addr *net.UDPAddr) quicstream.UDPConnInfo { return quicstream.NewUDPConnInfo(addr, true) },
	}
}

func NewTransportWithQuicstream(
	laddr *net.UDPAddr,
	handlerPrefix string,
	poolclient *quicstream.PoolClient,
	newClient func(quicstream.UDPConnInfo) func(*net.UDPAddr) *quicstream.Client,
) *Transport {
	makebody := func(b []byte) []byte {
		return b
	}
	if len(handlerPrefix) > 0 {
		makebody = func(b []byte) []byte {
			return quicstream.BodyWithPrefix(handlerPrefix, b)
		}
	}

	return NewTransport(
		laddr,
		func(ctx context.Context, ci quicstream.UDPConnInfo) (quic.EarlyConnection, error) {
			return poolclient.Dial(
				ctx,
				ci.UDPAddr(),
				newClient(ci),
			)
		},
		func(ctx context.Context, ci quicstream.UDPConnInfo, b []byte) error {
			tctx, cancel := context.WithTimeout(ctx, time.Second*2) //nolint:gomnd //...
			defer cancel()

			r, err := poolclient.Write(
				tctx,
				ci.UDPAddr(),
				func(w io.Writer) error {
					_, err := w.Write(makebody(b))

					return errors.WithStack(err)
				},
				newClient(ci),
			)
			if err != nil {
				return err
			}

			r.CancelRead(0)

			return nil
		},
	)
}

func (t *Transport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return t.DialAddressTimeout(memberlist.Address{Addr: addr}, timeout)
}

func (t *Transport) DialAddressTimeout(addr memberlist.Address, timeout time.Duration) (net.Conn, error) {
	l := t.Log().With().Stringer("remote_address", &addr).Logger()

	l.Trace().Msg("trying to dial")

	e := util.StringErrorFunc("failed DialAddressTimeout")

	raddr, err := net.ResolveUDPAddr("udp", addr.Addr)
	if err != nil {
		l.Error().Err(err).Msg("failed to resolve udp address")

		return nil, e(err, "failed to resolve udp address")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ci := t.getconninfof(raddr)

	if _, err := t.dialf(ctx, ci); err != nil {
		l.Error().Err(err).Interface("conn_info", ci).Msg("failed to dial")

		return nil, &net.OpError{
			Net: "tcp", Op: "dial",
			Err: errors.WithMessagef(err, "failed to dial"),
		}
	}

	l.Trace().Msg("successfully dial")

	return t.newConn(raddr), nil
}

func (t *Transport) FinalAdvertiseAddr(ip string, port int) (net.IP, int, error) {
	if len(ip) < 1 {
		return t.laddr.IP, t.laddr.Port, nil
	}

	addr := net.ParseIP(ip)
	if addr == nil {
		return nil, 0, errors.Errorf("failed to parse advertise address %q", ip)
	}

	if ip4 := addr.To4(); ip4 != nil {
		addr = ip4
	}

	return addr, port, nil
}

func (t *Transport) PacketCh() <-chan *memberlist.Packet {
	return t.packetch
}

func (t *Transport) Start() error {
	t.Lock()
	defer t.Unlock()

	t.shutdowned = false

	return nil
}

func (t *Transport) Shutdown() error {
	t.Lock()
	defer t.Unlock()

	t.shutdowned = true

	return nil
}

func (t *Transport) isShutdowned() bool {
	t.RLock()
	defer t.RUnlock()

	return t.shutdowned
}

func (t *Transport) StreamCh() <-chan net.Conn {
	return t.streamch
}

func (t *Transport) WriteTo(b []byte, addr string) (time.Time, error) {
	return t.WriteToAddress(b, memberlist.Address{Addr: addr})
}

func (t *Transport) WriteToAddress(b []byte, addr memberlist.Address) (time.Time, error) {
	if t.isShutdowned() {
		return time.Time{}, nil
	}

	e := util.StringErrorFunc("failed to WriteToAddress")

	raddr, err := net.ResolveUDPAddr("udp", addr.Addr)
	if err != nil {
		return time.Time{}, e(err, "failed to resolve udp address")
	}

	ci := t.getconninfof(raddr)

	if err := t.writef(context.Background(), ci, marshalMsg(packetDataType, t.laddr, b)); err != nil {
		return time.Time{}, &net.OpError{
			Net: "udp", Op: "write",
			Err: errors.WithMessagef(err, "failed to write"),
		}
	}

	return time.Now(), nil
}

func (t *Transport) ReceiveRaw(b []byte, addr net.Addr) error {
	if t.isShutdowned() {
		return nil
	}

	e := util.StringErrorFunc("failed to receive raw data")

	dt, raddr, rb, err := unmarshalMsg(b)
	if err != nil {
		t.Log().Error().Err(err).Stringer("remote_address", addr).Msg("invalid message received")

		return e(err, "")
	}

	t.Log().Trace().Stringer("remote_address", raddr).Stringer("message_type", dt).Msg("raw data received")

	switch {
	case dt == packetDataType:
		go t.receivePacket(rb, raddr)
	case dt == streamDataType:
		go t.receiveStream(rb, raddr)
	default:
		return e(nil, "unknown raw data type, %v", dt)
	}

	return nil
}

func (t *Transport) receivePacket(b []byte, raddr net.Addr) {
	if t.isShutdowned() {
		return
	}

	donech := make(chan struct{})

	go func() {
		t.packetch <- &memberlist.Packet{
			Buf:       b,
			From:      raddr,
			Timestamp: time.Now(),
		}

		donech <- struct{}{}
	}()

	select {
	case <-time.After(time.Second * 2):
		t.Log().Warn().Msg("receive packet blocked")
	case <-donech:
	}
}

func (t *Transport) receiveStream(b []byte, raddr net.Addr) {
	if t.isShutdowned() {
		return
	}

	var conn *qconn
	i, _ := t.conns.Value(raddr.String())

	switch {
	case i != nil:
		conn = i.(*qconn) //nolint:forcetypeassert // ...
	case i == nil:
		conn = t.newConn(raddr.(*net.UDPAddr)) //nolint:forcetypeassert // ...
	}

	if conn.writeClose(b) {
		t.streamch <- conn
	}
}

func (t *Transport) newConn(raddr *net.UDPAddr) *qconn {
	ci := t.getconninfof(raddr)

	conn := newQConn(
		t.laddr,
		raddr,
		func(ctx context.Context, b []byte) (int, error) {
			err := t.writef(ctx, ci, marshalMsg(streamDataType, t.laddr, b))
			var n int
			if err == nil {
				n = len(b)
			}

			return n, err
		},
		func() {
			_ = t.conns.Remove(raddr.String(), nil)
		},
	)

	t.conns.SetValue(raddr.String(), conn)

	return conn
}

func marshalMsg(t rawDataType, addr net.Addr, b []byte) []byte {
	y := make([]byte, len(b)+200)
	a := []byte(addr.String())

	y[0] = byte(t)
	copy(y[1:200], a)
	copy(y[200:], b)

	return y
}

func unmarshalMsg(b []byte) (rawDataType, net.Addr, []byte, error) {
	dt := b[0]
	h := make([]byte, 199)
	copy(h, b[1:])

	s := strings.TrimRight(string(h), string(make([]byte, 1)))

	addr, err := net.ResolveUDPAddr("udp", s)
	if err != nil {
		return noneDataType, nil, nil, errors.WithStack(err)
	}

	return rawDataType(dt), addr, b[200:], nil
}
