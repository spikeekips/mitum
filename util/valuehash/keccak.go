package valuehash

import (
	"golang.org/x/crypto/sha3"
)

const (
	SHA256Size int = 32
	SHA512Size int = 64
)

func NewSHA512(b []byte) L64 {
	return L64(sha3.Sum512(b))
}

func NewSHA256(b []byte) L32 {
	return L32(sha3.Sum256(b))
}
