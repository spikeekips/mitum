//go:build test
// +build test

package dop5

import (
	"github.com/spikeekips/mitum/base"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
)

func DecodeJSONDummyPublickey(enc *jsonenc.Encoder, s string) (any, error) {
	return base.NewDummyPublickey(s[:len(s)-3]), nil
}
