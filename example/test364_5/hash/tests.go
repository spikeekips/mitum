//go:build test
// +build test

package hash

import (
	"crypto/rand"

	"github.com/spikeekips/mitum/util"
)

func RandomHex64() util.Hash {
	b := make([]byte, 32)
	_, _ = rand.Read(b)

	return NewSHA3(b)
}
