package network

import (
	"fmt"
	"net"
	"strings"

	"github.com/spikeekips/mitum/util"
)

type ConnInfo interface {
	fmt.Stringer
	Addr() net.Addr
	TLSInsecure() bool
}

type NamedAddr struct {
	addr string
}

func (NamedAddr) Network() string {
	return "udp"
}

func (c NamedAddr) String() string {
	return c.addr
}

type NamedConnInfo struct {
	addr        NamedAddr
	tlsinsecure bool
}

func (c NamedConnInfo) Addr() net.Addr {
	return c.addr
}

func (c NamedConnInfo) TLSInsecure() bool {
	return c.tlsinsecure
}

func (c NamedConnInfo) String() string {
	return ConnInfoToString(c)
}

func ParseConnInfo(s string) (ConnInfo, error) {
	e := util.StringErrorFunc("failed to parse ConnInfo")

	if _, _, err := net.SplitHostPort(s); err != nil {
		return nil, e(err, "")
	}

	as, tlsinsecure := ParseInsecure(s)

	return NamedConnInfo{
		addr:        NamedAddr{addr: as},
		tlsinsecure: tlsinsecure,
	}, nil
}

func ParseInsecure(s string) (string, bool) {
	switch i := strings.Index(s, "#"); {
	case i < 0:
		return s, false
	case len(s[i:]) > 0:
		return s[:i], strings.ToLower(s[i+1:]) == "tls_insecure"
	default:
		return s[:i], false
	}
}

func ConnInfoToString(ci ConnInfo) string {
	tlsinsecure := ""
	if ci.TLSInsecure() {
		tlsinsecure = "#tls_insecure"
	}

	return ci.Addr().String() + tlsinsecure
}
