//go:build test
// +build test

package base

import "github.com/spikeekips/mitum/util"

func RandomNetworkID() NetworkID {
	return NetworkID(util.UUID().Bytes())
}
