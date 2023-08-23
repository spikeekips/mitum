package network

import (
	"net"

	"github.com/pkg/errors"
)

func IsValidAddr(s string) error {
	if len(s) < 1 {
		return errors.Errorf("empty publish")
	}

	switch host, port, err := net.SplitHostPort(s); {
	case err != nil:
		return errors.WithStack(err)
	case len(host) < 1:
		return errors.Errorf("empty host")
	case len(port) < 1:
		return errors.Errorf("empty port")
	}

	return nil
}
