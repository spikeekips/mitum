package util

import (
	"fmt"

	"github.com/btcsuite/btcutil/base58"
)

type Hash interface {
	Equal(Hash) bool
	fmt.Stringer // NOTE usually String() value is the base58 encoded of Bytes()
	Byter
	IsValider
}

type Hasher interface {
	Hash() Hash
}

type HashByter interface {
	// HashBytes is uses to generate hash
	HashBytes() []byte
}

func EncodeHash(b []byte) string {
	return base58.Encode(b)
}

func DecodeHash(s string) []byte {
	return base58.Decode(s)
}
