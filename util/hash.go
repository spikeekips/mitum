package util

import (
	"encoding/hex"
	"fmt"
)

type Hash interface {
	Equal(Hash) bool
	fmt.Stringer // NOTE usually String() value is the hex encoded of Bytes()
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
	return hex.EncodeToString(b)
}

func DecodeHash(s string) ([]byte, error) {
	return hex.DecodeString(s)
}
