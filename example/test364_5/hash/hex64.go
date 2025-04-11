package hash

import (
	"crypto/sha3"
	"encoding/hex"
	"unsafe"

	"github.com/spikeekips/mitum/util"
	"github.com/zeebo/blake3"
)

type Hex64 []byte

var (
	_         = any(Hex64([]byte{0x00})).(util.Hash) //nolint:forcetypeassert
	hex64Size = 64
)

func NewBlake3(b []byte) Hex64 {
	i := blake3.Sum256(b)

	return hext64FromSum(i[:])
}

func NewSHA3(b []byte) Hex64 {
	i := sha3.Sum256(b)

	return hext64FromSum(i[:])
}

func NewHex64Writer() (
	func([]byte),
	func() Hex64,
) {
	h := sha3.New256()

	return func(b []byte) {
			_, _ = h.Write(b)
		},
		func() Hex64 {
			defer h.Reset()

			return hext64FromSum(h.Sum(nil))
		}
}

func hext64FromSum(b []byte) Hex64 {
	s := make([]byte, hex64Size)
	_ = hex.Encode(s, b)

	return Hex64(s)
}

func Hex64FromBytes(b []byte) Hex64 {
	return Hex64(b)
}

func Hex64FromString(s string) Hex64 {
	//nolint:gosec // G103
	return Hex64FromBytes(unsafe.Slice(unsafe.StringData(s), len(s)))
}

func (h Hex64) IsValid([]byte) error {
	if len(h) != hex64Size {
		return util.ErrInvalid.Errorf("wrong size")
	}

	return nil
}

func (h Hex64) Bytes() []byte {
	return h
}

func (h Hex64) String() string {
	return string(h)
}

func (h Hex64) Equal(b util.Hash) bool {
	if b == nil {
		return false
	}

	return h.String() == b.String()
}

func (h Hex64) MarshalText() ([]byte, error) {
	return h.Bytes(), nil
}

func (h *Hex64) UnmarshalText(b []byte) error {
	*h = b

	return nil
}
