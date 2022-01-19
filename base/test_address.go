//go:build test
// +build test

package base

import "github.com/spikeekips/mitum/util"

func RandomAddress(prefix string) Address {
	return NewStringAddress(prefix + util.UUID().String())
}
