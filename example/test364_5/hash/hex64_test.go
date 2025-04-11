package hash

import (
	"crypto/rand"
	"crypto/sha3"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testHex64 struct {
	suite.Suite
}

func (t *testHex64) sum(b []byte) string {
	i := sha3.Sum256(b)

	return hex.EncodeToString(i[:])
}

func (t *testHex64) random() []byte {
	b := make([]byte, 100)
	_, _ = rand.Read(b)

	return b
}

func (t *testHex64) TestNew() {
	t.Run("nil", func() {
		h := NewSHA3(nil)
		t.T().Logf("new: %q", h)

		t.NoError(h.IsValid(nil))

		t.Equal(t.sum(nil), h.String())
	})

	t.Run("not empty", func() {
		b := t.random()

		h := NewSHA3(b)
		t.T().Logf("new: %q", h)

		t.NoError(h.IsValid(nil))

		t.Equal(t.sum(b), h.String())
	})
}

func (t *testHex64) TestInvalid() {
	t.Run("empty", func() {
		var h Hex64
		err := h.IsValid(nil)
		t.ErrorIs(err, util.ErrInvalid)
	})

	t.Run("wrong size", func() {
		b := make([]byte, 3)
		h := Hex64(b)
		err := h.IsValid(nil)
		t.ErrorIs(err, util.ErrInvalid)
	})
}

func (t *testHex64) TestEncode() {
	h := NewSHA3(t.random())

	b, err := json.Marshal(h)
	t.NoError(err)

	t.T().Log("marshaled:", string(b))

	var m Hex64
	t.NoError(json.Unmarshal(b, &m))

	t.T().Log("unmarshaled:", m)

	t.True(h.Equal(m))
	t.Equal(h, m)
	t.Equal(h.String(), m.String())
}

func TestHex64(t *testing.T) {
	suite.Run(t, new(testHex64))
}

func runBenchmarkNewHash(bb *testing.B, name string, f func() util.Hash) {
	bb.Run(name, func(b *testing.B) {
		for b.Loop() {
			_ = f()
		}
	})

	bb.Run(name+"-ToString", func(b *testing.B) {
		for b.Loop() {
			_ = f().String()
		}
	})
}

func BenchmarkNewHashSHA3(bb *testing.B) {
	i := valuehash.RandomSHA256().Bytes()

	runBenchmarkNewHash(bb, "sha3", func() util.Hash {
		return valuehash.NewSHA256(i)
	})
}

func BenchmarkNewHashHex64SHA3(bb *testing.B) {
	i := util.UUID().Bytes()

	runBenchmarkNewHash(bb, "Hex64-SHA3", func() util.Hash {
		return NewSHA3(i)
	})
}

func BenchmarkNewHashHex64Blake3(bb *testing.B) {
	i := util.UUID().Bytes()

	runBenchmarkNewHash(bb, "Hex64-Blake3", func() util.Hash {
		return NewBlake3(i)
	})
}

func runBenchmarkFromBytesHash(bb *testing.B, name string, f func() util.Hash) {
	bb.Run(name, func(b *testing.B) {
		for b.Loop() {
			_ = f()
		}
	})

	bb.Run(name+"-ToString", func(b *testing.B) {
		for b.Loop() {
			_ = f().String()
		}
	})
}

func BenchmarkFromBytesHashValueHashBytes(bb *testing.B) {
	i := valuehash.RandomSHA256().Bytes()

	runBenchmarkFromBytesHash(bb, "valuehash.Bytes", func() util.Hash {
		return valuehash.NewBytes(i)
	})
}

func BenchmarkFromBytesHashHex64(bb *testing.B) {
	bl := sha3.Sum256(util.UUID().Bytes())
	i := []byte(hex.EncodeToString(bl[:]))

	runBenchmarkFromBytesHash(bb, "Hex64", func() util.Hash {
		return Hex64FromBytes(i)
	})
}

func runBenchmarkFromStringHash(bb *testing.B, name string, f func() util.Hash) {
	bb.Run(name, func(b *testing.B) {
		for b.Loop() {
			_ = f()
		}
	})

	bb.Run(name+"-ToString", func(b *testing.B) {
		for b.Loop() {
			_ = f().String()
		}
	})
}

func BenchmarkFromStringHashValueHashBytes(bb *testing.B) {
	i := valuehash.RandomSHA256().String()

	runBenchmarkFromStringHash(bb, "valuehash.Bytes", func() util.Hash {
		h, err := valuehash.NewBytesFromString(i)
		if err != nil {
			panic(err)
		}

		return h
	})
}

func BenchmarkFromStringHashHex64(bb *testing.B) {
	i := NewSHA3(util.UUID().Bytes()).String()

	runBenchmarkFromStringHash(bb, "Hex64", func() util.Hash {
		return Hex64FromString(i)
	})
}

func runBenchmarkJSONMarshalHash(bb *testing.B, name string, f func() util.Hash) {
	bb.Run(name, func(b *testing.B) {
		for b.Loop() {
			if _, err := util.MarshalJSON(f()); err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkJSONMarshalHashValueHashBytes(bb *testing.B) {
	h := valuehash.RandomSHA256()

	runBenchmarkJSONMarshalHash(bb, "valuehash.Bytes", func() util.Hash {
		return h
	})
}

func BenchmarkJSONMarshalHashHex64(bb *testing.B) {
	h := NewSHA3(util.UUID().Bytes())

	runBenchmarkJSONMarshalHash(bb, "Hex64", func() util.Hash {
		return h
	})
}

func runBenchmarkJSONUnmarshalHash[T any](bb *testing.B, name string, f func() []byte) {
	bb.Run(name, func(b *testing.B) {
		for b.Loop() {
			var t T

			if err := util.UnmarshalJSON(f(), &t); err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkJSONUnmarshalHashValueHashBytes(bb *testing.B) {
	i, _ := util.MarshalJSON(valuehash.RandomSHA256())

	runBenchmarkJSONUnmarshalHash[valuehash.Bytes](bb, "valuehash.Bytes", func() []byte {
		return i
	})
}

func BenchmarkJSONUnmarshalHashHex64(bb *testing.B) {
	i, _ := util.MarshalJSON(NewSHA3(util.UUID().Bytes()).Bytes())

	runBenchmarkJSONUnmarshalHash[Hex64](bb, "Hex64", func() []byte {
		return i
	})
}
