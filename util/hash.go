package util

import (
	"fmt"
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
