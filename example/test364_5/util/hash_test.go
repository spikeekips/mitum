package util

import (
	"maps"
	"slices"
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/sha3"
)

type testMurmur3 struct {
	BaseSuite
}

func (t *testMurmur3) TestDistribution() {
	var size uint64 = 333

	counts := map[uint64]int{}

	for range 33333 {
		index := Murmur3(0, util.UUID().String()) % size
		counts[index]++
	}

	indices := slices.Collect(maps.Keys(counts))
	slices.Sort(indices)
	for index := range slices.Values(indices) {
		t.T().Logf("index=%d count=%d", index, counts[index])
	}
}

func TestMurmur3(t *testing.T) {
	suite.Run(t, new(testMurmur3))
}

func TestSHA256HashWriter(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	t.Run("ok", func() {
		b := []byte{0, 1, 2, 3, 4}

		hb := sha3.Sum256(b)
		expected := util.EncodeHash(hb[:])

		w, s := NewSHA256HashWriter()
		w(b)
		h := s()

		t.Equal(expected, h.String())
	})

	t.Run("empty", func() {
		hb := sha3.Sum256(nil)
		expected := util.EncodeHash(hb[:])

		_, s := NewSHA256HashWriter()
		h := s()

		t.Equal(expected, h.String())
	})
}
