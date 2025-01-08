package util

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

func DistributeByCount[K comparable](total, share uint64) (
	_ func(K) []uint64,
	closef func(),
	_ error,
) {
	switch {
	case total < 1:
		return nil, nil, errors.Errorf("empty size")
	case total < 2:
		return func(K) []uint64 { return []uint64{0} }, func() {}, nil
	case share > total:
		share = total
	}

	c := map[uint64]uint64{}

	var l sync.Mutex

	findLow := func() uint64 {
		l.Lock()
		defer l.Unlock()

		var p *uint64
		var low uint64

		for i := range total {
			switch {
			case p == nil:
				p, low = &i, c[i]
			case c[i] < low:
				p, low = &i, c[i]
			}
		}

		var pos uint64

		if p != nil {
			pos = *p
		}

		c[pos]++

		return pos
	}

	var cache util.LockedMap[K, []uint64]

	switch i, err := util.NewLockedMap[K, []uint64](total, nil); {
	case err != nil:
		return nil, nil, err
	default:
		cache = i
	}

	return func(k K) (poses []uint64) {
			_ = cache.GetOrCreate(
				k,
				func(i []uint64, _ bool) error {
					poses = i

					return nil
				},
				func() ([]uint64, error) {
					n := make([]uint64, share)
					for i := range n {
						p := findLow()

						n[i] = p
					}

					return n, nil
				},
			)

			return poses
		},
		func() {
			ClearMap(c)
			cache.Close()
		},
		nil
}
