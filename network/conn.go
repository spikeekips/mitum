package network

import (
	"fmt"
	"net"
	"net/url"
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

func HasTLSInsecure(s string) bool {
	v, err := url.ParseQuery(s)
	if err != nil {
		return false
	}

	return v.Has("tls_insecure")
}

func ParseTLSInsecure(s string) (string, bool) {
	switch i := strings.Index(s, "#"); {
	case i < 0:
		return s, false
	case len(s[i:]) > 0:
		return s[:i], HasTLSInsecure(s[i+1:])
	default:
		return s[:i], false
	}
}

func ConnInfoToString(addr string, tlsinsecure bool) string { // revive:disable-line:flag-parameter
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
	case a.Addr().String() != b.Addr().String():
		return false
	default:
		return true
	}
}

func DeepEqualConnInfo(a, b ConnInfo) bool {
	switch {
	case !EqualConnInfo(a, b):
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
