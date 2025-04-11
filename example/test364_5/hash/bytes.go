package hash

import (
	"crypto/sha3"
	"encoding/hex"

	"github.com/spikeekips/mitum/util"
)

type Bytes []byte

func NewBytes(b []byte) Bytes {
	i := sha3.Sum256(b)

	s := make([]byte, hex64Size)
	_ = hex.Encode(s, i[:])

	return Bytes(s)
}

func BytesFromBytes(b []byte) Bytes {
	return Bytes(b)
}

func BytesFromString(s string) Bytes {
	return Bytes([]byte(s))
}

func (h Bytes) IsValid([]byte) error {
	if len(h) == 0 {
		return util.ErrInvalid.Errorf("empty")
	}

	return nil
}

func (h Bytes) Bytes() []byte {
	return h
}

func (h Bytes) String() string {
	return string(h)
}

func (h Bytes) Equal(b util.Hash) bool {
	if b == nil {
		return false
	}

	return h.String() == b.String()
}

func (h Bytes) MarshalText() ([]byte, error) {
	return h.Bytes(), nil
}

func (h *Bytes) UnmarshalText(b []byte) error {
	*h = b

	return nil
}
