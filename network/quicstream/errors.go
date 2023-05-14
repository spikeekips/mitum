package quicstream

import (
	"net"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/util"
)

var ErrNetwork = util.NewIDError("network error")

func IsNetworkError(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, ErrNetwork):
		return true
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
