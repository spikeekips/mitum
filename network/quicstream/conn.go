package quicstream

import (
	"net"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/util"
)

type ConnInfo interface {
	Addr() net.Addr
	UDPAddr() *net.UDPAddr
	TLSInsecure() bool
}

type BaseConnInfo struct {
	addr        *net.UDPAddr
	tlsinsecure bool
}

func NewBaseConnInfo(addr *net.UDPAddr, tlsinsecure bool) BaseConnInfo {
	return BaseConnInfo{addr: addr, tlsinsecure: tlsinsecure}
}

func NewBaseConnInfoFromString(s string) (BaseConnInfo, error) {
	as, tlsinsecure := network.ParseInsecure(s)

	return NewBaseConnInfoFromStringAddress(as, tlsinsecure)
}

func NewBaseConnInfoFromStringAddress(s string, tlsinsecure bool) (BaseConnInfo, error) {
	addr, err := net.ResolveUDPAddr("udp", s)
	if err != nil {
		return BaseConnInfo{}, util.ErrInvalid.Wrap(errors.Wrap(err, "failed to parse BaseConnInfo"))
	}

	return NewBaseConnInfo(addr, tlsinsecure), nil
}

func (c BaseConnInfo) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid BaseConnInfo")

	switch {
	case c.addr == nil:
		return e.Errorf("empty addr")
	case c.addr.IP.Equal(net.IPv4zero), c.addr.IP.Equal(net.IPv6zero):
		return e.Errorf("empty addr ip")
	case c.addr.Port < 1:
		return e.Errorf("empty addr port")
	}

	return nil
}

func (c BaseConnInfo) Addr() net.Addr {
	return c.addr
}

func (c BaseConnInfo) UDPAddr() *net.UDPAddr {
	return c.addr
}

func (c BaseConnInfo) TLSInsecure() bool {
	return c.tlsinsecure
}

func (c BaseConnInfo) String() string {
	var addr string
	if c.addr != nil {
		addr = c.addr.String()
	}

	return network.ConnInfoToString(addr, c.tlsinsecure)
}

func (c BaseConnInfo) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

func (c *BaseConnInfo) UnmarshalText(b []byte) error {
	ci, err := NewBaseConnInfoFromString(string(b))
	if err != nil {
		return errors.WithMessage(err, "failed to unmarshal BaseConnInfo")
	}

	*c = ci

	return nil
}

func (c BaseConnInfo) MarshalZerologObject(e *zerolog.Event) {
	e.
		Stringer("address", c.addr).
		Bool("tls_insecure", c.tlsinsecure)
}

func EqualConnInfo(a, b ConnInfo) bool {
	switch {
	case a == nil, b == nil:
		return false
	case a.Addr() == nil, b.Addr() == nil:
		return false
	case a.TLSInsecure() != b.TLSInsecure():
		return false
	case a.Addr().String() != b.Addr().String():
		return false
	default:
		return true
	}
}
