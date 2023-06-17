package quicstream

import (
	"net"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/util"
)

type ConnInfo struct {
	addr        *net.UDPAddr
	tlsinsecure bool
}

func NewConnInfo(addr *net.UDPAddr, tlsinsecure bool) ConnInfo {
	return ConnInfo{addr: addr, tlsinsecure: tlsinsecure}
}

func NewConnInfoFromString(s string) (ConnInfo, error) {
	as, tlsinsecure := network.ParseTLSInsecure(s)

	return NewConnInfoFromStringAddress(as, tlsinsecure)
}

func MustNewConnInfoFromString(s string) ConnInfo {
	as, tlsinsecure := network.ParseTLSInsecure(s)

	ci, err := NewConnInfoFromStringAddress(as, tlsinsecure)
	if err != nil {
		panic(err)
	}

	return ci
}

func NewConnInfoFromStringAddress(s string, tlsinsecure bool) (ci ConnInfo, _ error) {
	addr, err := net.ResolveUDPAddr("udp", s)
	if err == nil {
		return NewConnInfo(addr, tlsinsecure), nil
	}

	var dnserr *net.DNSError

	if errors.As(err, &dnserr) {
		return ci, errors.Wrap(err, "parse ConnInfo")
	}

	return ci, util.ErrInvalid.WithMessage(err, "parse ConnInfo")
}

func (c ConnInfo) IsValid([]byte) error {
	return c.isValid()
}

func (c ConnInfo) isValid() error {
	e := util.ErrInvalid.Errorf("invalid ConnInfo")

	switch {
	case c.addr == nil:
		return e.Errorf("empty addr")
	case c.addr.IP.IsUnspecified():
		return e.Errorf("empty addr ip")
	case c.addr.Port < 1:
		return e.Errorf("empty addr port")
	}

	return nil
}

func (c ConnInfo) Addr() net.Addr {
	if c.isValid() != nil {
		return nil
	}

	return c.addr
}

func (c ConnInfo) TLSInsecure() bool {
	return c.tlsinsecure
}

func (c ConnInfo) String() string {
	var addr string
	if c.addr != nil {
		addr = c.addr.String()
	}

	return network.ConnInfoToString(addr, c.tlsinsecure)
}

func (c ConnInfo) UDPAddr() *net.UDPAddr {
	return c.addr
}

func (c ConnInfo) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

func (c *ConnInfo) UnmarshalText(b []byte) error {
	ci, err := NewConnInfoFromString(string(b))
	if err != nil {
		return errors.WithMessage(err, "unmarshal ConnInfo")
	}

	*c = ci

	return nil
}
