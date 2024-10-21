package quicmemberlist

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	TransportDialFunc    func(context.Context, quicstream.ConnInfo) error
	TransportWriteFunc   func(context.Context, quicstream.ConnInfo, []byte) error
	TransportGetConnInfo func(*net.UDPAddr) (quicstream.ConnInfo, error)
)

type TransportArgs struct {
	DialFunc     TransportDialFunc
	WriteFunc    TransportWriteFunc
	NotAllowFunc func(string) bool
}

func NewTransportArgs() *TransportArgs {
	return &TransportArgs{
		DialFunc: func(context.Context, quicstream.ConnInfo) error {
			return util.ErrNotImplemented.Errorf("DialFunc")
		},
		WriteFunc: func(context.Context, quicstream.ConnInfo, []byte) error {
			return util.ErrNotImplemented.Errorf("WriteFunc")
		},
		NotAllowFunc: func(string) bool { return false }, // NOTE by default, allows all nodes.
	}
}

type Transport struct {
	streamch chan net.Conn
	*logging.Logging
	laddr        *net.UDPAddr
	args         *TransportArgs
	packetch     chan *memberlist.Packet
	conns        *util.ShardedMap[string, *qconn]
	getconninfof TransportGetConnInfo
	l            sync.RWMutex
	shutdowned   bool
}

func NewTransport(
	laddr *net.UDPAddr,
	args *TransportArgs,
) *Transport {
	conns, _ := util.NewShardedMap[string, *qconn](1<<9, nil) //nolint:mnd //...

	return &Transport{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "memberlist-quicmemberlist")
		}),
		laddr:    laddr,
		args:     args,
		packetch: make(chan *memberlist.Packet),
		streamch: make(chan net.Conn),
		conns:    conns,
		getconninfof: func(addr *net.UDPAddr) (quicstream.ConnInfo, error) {
			return quicstream.NewConnInfo(addr, true)
		},
	}
}

func NewTransportWithQuicstream(
	laddr *net.UDPAddr,
	handler quicstream.HandlerName,
	dialf quicstream.ConnInfoDialFunc,
	notallowf func(string) bool,
	requestTimeoutf func() time.Duration,
) *Transport {
	nrequestTimeoutf := func() time.Duration {
		return time.Second * 2
	}

	if requestTimeoutf != nil {
		nrequestTimeoutf = requestTimeoutf
	}

	writeBody := func(ctx context.Context, w io.Writer, b []byte) error {
		return util.AwareContext(ctx, func(context.Context) error {
			_, err := w.Write(b)

			return errors.WithStack(err)
		})
	}

	if len(handler) > 0 {
		prefix := quicstream.HashPrefix(handler)

		writeBody = func(ctx context.Context, w io.Writer, b []byte) error {
			return util.AwareContext(ctx, func(context.Context) error {
				if err := quicstream.WritePrefix(ctx, w, prefix); err != nil {
					return err
				}

				_, err := w.Write(b)

				return errors.WithStack(err)
			})
		}
	}

	args := NewTransportArgs()

	args.DialFunc = func(ctx context.Context, ci quicstream.ConnInfo) error {
		streamer, err := dialf(ctx, ci)
		if err != nil {
			return err
		}

		return streamer.Stream(ctx, func(context.Context, io.Reader, io.WriteCloser) error {
			return nil
		})
	}

	args.WriteFunc = func(ctx context.Context, ci quicstream.ConnInfo, b []byte) error {
		streamer, err := dialf(ctx, ci)
		if err != nil {
			return err
		}

		cctx, cancel := context.WithTimeout(ctx, nrequestTimeoutf())
		defer cancel()

		return streamer.Stream(cctx, func(ctx context.Context, r io.Reader, w io.WriteCloser) error {
			if err := writeBody(ctx, w, b); err != nil {
				return errors.WithStack(err)
			}

			_ = w.Close()
			_, _ = io.ReadAll(r)

			return nil
		})
	}

	args.NotAllowFunc = notallowf

	return NewTransport(laddr, args)
}

func (t *Transport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return t.DialAddressTimeout(memberlist.Address{Addr: addr}, timeout)
}

func (t *Transport) DialAddressTimeout(addr memberlist.Address, timeout time.Duration) (net.Conn, error) {
	l := t.Log().With().Stringer("remote_address", &addr).Logger()

	l.Trace().Msg("trying to dial")

	if t.args.NotAllowFunc(addr.Addr) {
		return nil, &net.OpError{
			Net: "tcp", Op: "dial",
			Err: errors.Errorf("dial; not allowed"),
		}
	}

	e := util.StringError("DialAddressTimeout")

	raddr, err := net.ResolveUDPAddr("udp", addr.Addr)
	if err != nil {
		l.Error().Err(err).Msg("failed to resolve udp address")

		return nil, e.WithMessage(err, "resolve udp address")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var ci quicstream.ConnInfo

	switch i, err := t.getconninfof(raddr); {
	case err != nil:
		return nil, err
	default:
		ci = i
	}

	if err := t.args.DialFunc(ctx, ci); err != nil {
		l.Error().Err(err).Interface("conn_info", ci).Msg("failed to dial")

		return nil, &net.OpError{Net: "tcp", Op: "dial", Err: err}
	}

	l.Trace().Msg("successfully dial")

	return t.newConn(raddr)
}

func (t *Transport) FinalAdvertiseAddr(ip string, port int) (net.IP, int, error) {
	if len(ip) < 1 {
		return t.laddr.IP, t.laddr.Port, nil
	}

	addr := net.ParseIP(ip)
	if addr == nil {
		return nil, 0, errors.Errorf("parse advertise address %q", ip)
	}

	if ip4 := addr.To4(); ip4 != nil {
		addr = ip4
	}

	return addr, port, nil
}

func (t *Transport) PacketCh() <-chan *memberlist.Packet {
	return t.packetch
}

func (t *Transport) Start(context.Context) error {
	t.l.Lock()
	defer t.l.Unlock()

	t.shutdowned = false

	return nil
}

func (t *Transport) Shutdown() error {
	t.l.Lock()
	defer t.l.Unlock()

	t.shutdowned = true

	return nil
}

func (t *Transport) isShutdowned() bool {
	t.l.RLock()
	defer t.l.RUnlock()

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

	e := util.StringError("WriteToAddress")

	raddr, err := net.ResolveUDPAddr("udp", addr.Addr)
	if err != nil {
		return time.Time{}, e.WithMessage(err, "resolve udp address")
	}

	var ci quicstream.ConnInfo

	switch i, err := t.getconninfof(raddr); {
	case err != nil:
		return time.Time{}, err
	default:
		ci = i
	}

	if err := t.args.WriteFunc(context.Background(), ci, marshalMsg(packetDataType, t.laddr, b)); err != nil {
		return time.Time{}, &net.OpError{
			Net: "udp", Op: "write",
			Err: errors.WithMessagef(err, "write"),
		}
	}

	return time.Now(), nil
}

func (t *Transport) receiveRaw(id string, b []byte, addr net.Addr) error {
	if t.isShutdowned() {
		return nil
	}

	e := util.StringError("receive raw data")

	if len(b) < 1 {
		return nil
	}

	dt, raddr, rb, err := unmarshalMsg(b)
	if err != nil {
		return e.Wrap(err)
	}

	t.Log().Trace().
		Str("id", id).
		Stringer("remote_address", addr).
		Stringer("remote_address_in_message", raddr).
		Stringer("message_type", dt).
		Msg("raw data received")

	if t.args.NotAllowFunc(raddr.String()) {
		return e.Errorf("not allowed")
	}

	switch {
	case dt == packetDataType:
		go t.receivePacket(rb, raddr)
	case dt == streamDataType:
		go t.receiveStream(rb, raddr)
	default:
		return e.Errorf("unknown raw data type, %v", dt)
	}

	return nil
}

func (t *Transport) QuicstreamHandler(
	ctx context.Context, addr net.Addr, r io.Reader, _ io.WriteCloser,
) (context.Context, error) {
	id := util.UUID().String()

	l := t.Log().With().Str("id", id).Stringer("remote_address", addr).Logger()

	b, err := io.ReadAll(r)
	if err != nil {
		l.Trace().Err(err).Msg("failed to read")

		return ctx, errors.WithStack(err)
	}

	if err := t.receiveRaw(id, b, addr); err != nil {
		l.Trace().Err(err).Msg("invalid message received")

		return ctx, err
	}

	return ctx, nil
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
		t.Log().Warn().Interface("remote", raddr).Msg("receive packet blocked")
	case <-donech:
	}
}

func (t *Transport) receiveStream(b []byte, raddr net.Addr) {
	if t.isShutdowned() {
		return
	}

	switch conn, err := t.newConn(raddr.(*net.UDPAddr)); {
	case err != nil:
		return
	case conn.writeClose(b):
		t.streamch <- conn
	}
}

func (t *Transport) newConn(raddr *net.UDPAddr) (*qconn, error) {
	var ci quicstream.ConnInfo

	switch i, err := t.getconninfof(raddr); {
	case err != nil:
		return nil, err
	default:
		ci = i
	}

	var conn *qconn

	err := t.conns.GetOrCreate(
		raddr.String(),
		func(i *qconn, _ bool) error {
			conn = i

			return nil
		},
		func() (*qconn, error) {
			return newQConn(
				t.laddr,
				raddr,
				func(ctx context.Context, b []byte) (int, error) {
					var n int

					err := t.args.WriteFunc(ctx, ci, marshalMsg(streamDataType, t.laddr, b))
					if err == nil {
						n = len(b)
					}

					return n, err
				},
				func() {
					_ = t.conns.RemoveValue(raddr.String())
				},
			), nil
		},
	)

	return conn, err
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
