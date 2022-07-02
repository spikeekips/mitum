package network

import (
	"fmt"
	"net"
	"strings"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
)

type ConnInfo interface {
	fmt.Stringer
	util.IsValider
	Addr() net.Addr
	TLSInsecure() bool
}

func ParseTLSInsecure(s string) (string, bool) {
	// FIXME parse url fragment
	switch i := strings.Index(s, "#"); {
	case i < 0:
		return s, false
	case len(s[i:]) > 0:
		return s[:i], strings.ToLower(s[i+1:]) == "tls_insecure"
	default:
		return s[:i], false
	}
}

func ConnInfoToString(addr string, tlsinsecure bool) string { // revive:disable-line:flag-parameter
	// FIXME get ConnInfo
	ti := ""
	if tlsinsecure {
		ti = "#tls_insecure"
	}

	return addr + ti
}

func EqualConnInfo(a, b ConnInfo) bool {
	switch {
	case a == nil, b == nil:
		return false
	case a.Addr() == nil, b.Addr() == nil:
		return false
	case a.TLSInsecure() != b.TLSInsecure():
		return false
	case a.String() != b.String():
		return false
	default:
		return true
	}
}

func ConnInfoLog(ci ConnInfo) *zerolog.Event {
	return zerolog.Dict().
		Str("type", fmt.Sprintf("%T", ci)).
		Stringer("addr", ci.Addr()).
		Bool("tls_insecure", ci.TLSInsecure())
}
