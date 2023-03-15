package valuehash

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

const maxBytesHashSize = 100

type Bytes []byte

func NewBytes(b []byte) Bytes {
	return Bytes(b)
}

func NewHashFromBytes(b []byte) util.Hash {
	if b == nil {
		return nil
	}

	return NewBytes(b)
}

func NewBytesFromString(s string) Bytes {
	return NewBytes(util.DecodeHash(s))
}

func (h Bytes) String() string {
	return util.EncodeHash(h.Bytes())
}

func (h Bytes) IsValid([]byte) error {
	if h == nil || len(h) < 1 {
		return util.ErrInvalid.Errorf("empty hash")
	}

	if len(h) > maxBytesHashSize {
		return util.ErrInvalid.Errorf("over max: %d > %d", len(h), maxBytesHashSize)
	}

	return nil
}

func (h Bytes) Bytes() []byte {
	return []byte(h)
}

func (h Bytes) Equal(b util.Hash) bool {
	if b == nil {
		return false
	}

	return bytes.Equal(h, b.Bytes())
}

func (h Bytes) MarshalText() ([]byte, error) {
	return []byte(h.String()), nil
}

func (h *Bytes) UnmarshalText(b []byte) error {
	*h = NewBytesFromString(string(b))

	return nil
}

type HashDecoder struct {
	h util.Hash
}

func (d *HashDecoder) UnmarshalText(b []byte) error {
	if len(b) < 1 {
		return nil
	}

	var u Bytes
	if err := u.UnmarshalText(b); err != nil {
		return errors.WithMessage(err, "decode hash by Bytes")
	}

	d.h = u

	return nil
}

func (d HashDecoder) Hash() util.Hash {
	return d.h
}
