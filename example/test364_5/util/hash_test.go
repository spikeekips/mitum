package util

import (
	"maps"
	"slices"
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
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
