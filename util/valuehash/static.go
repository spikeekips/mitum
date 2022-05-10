package valuehash

import (
	"bytes"

	"github.com/spikeekips/mitum/util"
)

type (
	L32 [32]byte
	L64 [64]byte
)

var (
	emptyL32 [32]byte
	emptyL64 [64]byte
)

func (h L32) IsValid([]byte) error {
	if emptyL32 == h {
		return util.ErrInvalid.Errorf("empty hash")
	}

	return nil
}

func (h L32) Bytes() []byte {
	return h[:]
}

func (h L32) String() string {
	return util.EncodeHash(h[:])
}

func (h L32) Equal(b util.Hash) bool {
	if b == nil {
		return false
	}

	return bytes.Equal(h[:], b.Bytes())
}

func (h L32) MarshalText() ([]byte, error) {
	return []byte(h.String()), nil
}

func (h L64) IsValid([]byte) error {
	if emptyL64 == h {
		return util.ErrInvalid.Errorf("empty hash")
	}

	return nil
}

func (h L64) Bytes() []byte {
	return h[:]
}

func (h L64) String() string {
	return util.EncodeHash(h[:])
}

func (h L64) Equal(b util.Hash) bool {
	if b == nil {
		return false
	}

	return bytes.Equal(h[:], b.Bytes())
}

func (h L64) MarshalText() ([]byte, error) {
	return []byte(h.String()), nil
}
