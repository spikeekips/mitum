package quicstream

import (
	"net"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/util"
)

type UDPConnInfo struct {
	addr        *net.UDPAddr
	tlsinsecure bool
}

func NewUDPConnInfo(addr *net.UDPAddr, tlsinsecure bool) UDPConnInfo {
	return UDPConnInfo{addr: addr, tlsinsecure: tlsinsecure}
}

func NewUDPConnInfoFromString(s string) (UDPConnInfo, error) {
	as, tlsinsecure := network.ParseTLSInsecure(s)

	return NewUDPConnInfoFromStringAddress(as, tlsinsecure)
}

func NewUDPConnInfoFromStringAddress(s string, tlsinsecure bool) (ci UDPConnInfo, _ error) {
	addr, err := net.ResolveUDPAddr("udp", s)
	if err == nil {
		return NewUDPConnInfo(addr, tlsinsecure), nil
	}

	var dnserr *net.DNSError

	if errors.As(err, &dnserr) {
		return ci, errors.Wrap(err, "failed to parse UDPConnInfo")
	}

	return ci, util.ErrInvalid.Wrap(errors.Wrap(err, "failed to parse UDPConnInfo"))
}

func (c UDPConnInfo) IsValid([]byte) error {
	return c.isValid()
}

func (c UDPConnInfo) isValid() error {
	e := util.ErrInvalid.Errorf("invalid UDPConnInfo")

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

func (c UDPConnInfo) Addr() net.Addr {
	if c.isValid() != nil {
		return nil
	}

	return c.addr
}

func (c UDPConnInfo) TLSInsecure() bool {
	return c.tlsinsecure
}

func (c UDPConnInfo) String() string {
	var addr string
	if c.addr != nil {
		addr = c.addr.String()
	}

	return network.ConnInfoToString(addr, c.tlsinsecure)
}

func (c UDPConnInfo) UDPAddr() *net.UDPAddr {
	return c.addr
}

func (c UDPConnInfo) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

func (c *UDPConnInfo) UnmarshalText(b []byte) error {
	ci, err := NewUDPConnInfoFromString(string(b))
	if err != nil {
		return errors.WithMessage(err, "failed to unmarshal UDPConnInfo")
	}

	*c = ci

	return nil
}

func (c UDPConnInfo) MarshalZerologObject(e *zerolog.Event) { // FIXME use network.ConnInfoLog
	e.
		Str("type", "udpconninfo").
		Stringer("addr", c.addr).
		Bool("tls_insecure", c.tlsinsecure)
}
