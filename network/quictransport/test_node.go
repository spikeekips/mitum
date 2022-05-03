//go:build test
// +build test

package quictransport

import (
	"net"
	"sync"
)

func RandomConnInfoGenerator() func() BaseConnInfo {
	var (
		lastrandomport     int
		randomConnInfoLock sync.Mutex
	)

	return func() BaseConnInfo {
		randomConnInfoLock.Lock()
		defer randomConnInfoLock.Unlock()

		lastrandomport++

		return NewBaseConnInfo(&net.UDPAddr{IP: net.IPv6loopback, Port: lastrandomport}, true)
	}
}
