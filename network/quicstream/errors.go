package quicstream

import (
	"net"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
)

func IsNetworkError(err error) bool {
	if err == nil {
		return false
	}

	if e := (&quic.StreamError{}); errors.As(err, &e) {
		return true
	}

	if e := (&quic.TransportError{}); errors.As(err, &e) {
		return true
	}

	if e := (&quic.ApplicationError{}); errors.As(err, &e) {
		return true
	}

	var nerr net.Error

	return errors.As(err, &nerr)
}
