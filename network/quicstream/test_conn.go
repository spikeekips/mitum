//go:build test
// +build test

package quicstream

import (
	"net"
	"sync"
)

func RandomConnInfoGenerator() func() ConnInfo {
	var (
		lastrandomport     int
		randomConnInfoLock sync.Mutex
	)

	return func() ConnInfo {
		randomConnInfoLock.Lock()
		defer randomConnInfoLock.Unlock()

		lastrandomport++

		return MustConnInfo(&net.UDPAddr{IP: net.IPv6loopback, Port: lastrandomport}, true)
	}
}
