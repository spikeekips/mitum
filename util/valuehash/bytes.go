package valuehash

import (
	"bytes"

	"github.com/btcsuite/btcutil/base58"
	"github.com/spikeekips/mitum/util"
)

const maxBytesHashSize = 100

type Bytes []byte

func NewBytes(b []byte) Bytes {
	return Bytes(b)
}

func NewBytesFromString(s string) Bytes {
	return NewBytes(base58.Decode(s))
}

func (hs Bytes) String() string {
	return toString(hs)
}

func (hs Bytes) IsValid([]byte) error {
	if hs == nil || len(hs) < 1 {
		return util.InvalidError.Errorf("empty hash")
	}

	if len(hs) > maxBytesHashSize {
		return util.InvalidError.Errorf("over max: %d > %d", len(hs), maxBytesHashSize)
	}

	return nil
}

func (hs Bytes) Bytes() []byte {
	return []byte(hs)
}

func (hs Bytes) Equal(h util.Hash) bool {
	return bytes.Equal(hs, h.Bytes())
}

func (hs Bytes) MarshalText() ([]byte, error) {
	return []byte(hs.String()), nil
}

func (hs *Bytes) UnmarshalText(b []byte) error {
	*hs = NewBytesFromString(string(b))

	return nil
}
