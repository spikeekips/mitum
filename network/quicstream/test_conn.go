//go:build test
// +build test

package quicstream

import (
	"net"
	"sync"
)

func RandomConnInfoGenerator() func() UDPConnInfo {
	var (
		lastrandomport     int
		randomConnInfoLock sync.Mutex
	)

	return func() UDPConnInfo {
		randomConnInfoLock.Lock()
		defer randomConnInfoLock.Unlock()

		lastrandomport++

		return NewUDPConnInfo(&net.UDPAddr{IP: net.IPv6loopback, Port: lastrandomport}, true)
	}
}
