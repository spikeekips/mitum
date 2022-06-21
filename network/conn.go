package network

import (
	"strings"
)

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

func ConnInfoToString(addr string, tlsinsecure bool) string { // revive:disable-line:flag-parameter
	ti := ""
	if tlsinsecure {
		ti = "#tls_insecure"
	}

	return addr + ti
}
