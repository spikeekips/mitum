package util

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testDistributeByCount struct {
	BaseSuite
}

func (t *testDistributeByCount) print(a []uint64) {
	slices.Sort(a)

	fmt.Printf("> %v\n", a)
}

func (t *testDistributeByCount) TestCases() {
	t.Run("1 total, 1 share", func() {
		g, c, err := DistributeByCount[string](1, 1)
		t.NoError(err)

		defer c()

		for range 33 {
			t.Equal([]uint64{0}, g(util.UUID().String()))
		}
	})

	t.Run("2 total, 1 share", func() {
		g, c, err := DistributeByCount[string](2, 1)
		t.NoError(err)

		defer c()

		m := map[uint64]int{}

		for range 36 {
			for p := range slices.Values(g(util.UUID().String())) {
				m[p]++
			}
		}

		t.Equal(2, len(m))
		t.Equal(18, m[0])
		t.Equal(18, m[1])
	})

	t.Run("3 total, 3 share", func() {
		g, c, err := DistributeByCount[string](3, 3)
		t.NoError(err)

		defer c()

		t.print(g(util.UUID().String()))
		t.print(g(util.UUID().String()))
		t.print(g(util.UUID().String()))
		t.print(g(util.UUID().String()))
		t.print(g(util.UUID().String()))
		t.print(g(util.UUID().String()))
		t.print(g(util.UUID().String()))
		t.print(g(util.UUID().String()))
	})

	t.Run("4 total, 3 share", func() {
		g, c, err := DistributeByCount[string](4, 3)
		t.NoError(err)

		defer c()

		t.print(g(util.UUID().String()))
		t.print(g(util.UUID().String()))
		t.print(g(util.UUID().String()))
		t.print(g(util.UUID().String()))
		t.print(g(util.UUID().String()))
		t.print(g(util.UUID().String()))
		t.print(g(util.UUID().String()))
		t.print(g(util.UUID().String()))
	})

	t.Run("permutation", func() {
		var maxTotal uint64 = 33

		for total := uint64(1); total < maxTotal; total++ {
			for share := uint64(1); share <= total; share++ {
				t.Run(fmt.Sprintf("total=%d share=%d", total, share), func() {
					g, c, err := DistributeByCount[string](total, share)
					t.NoError(err)

					defer c()

					var wg sync.WaitGroup

					for range 111 {
						wg.Add(1)

						go func() {
							defer wg.Done()

							a := g(util.UUID().String())
							slices.Sort(a)

							b := slices.Clone(a)
							slices.Sort(b)
							b = slices.Compact(b)

							if len(a) != len(b) {
								t.Panicf(nil, "a=%v b=%v", a, b)
							}
						}()
					}

					wg.Wait()
				})
			}
		}
	})
}

func TestDistributeByCount(t *testing.T) {
	suite.Run(t, new(testDistributeByCount))
}
