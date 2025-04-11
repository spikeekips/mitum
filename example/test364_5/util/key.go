package util

import (
	"sync"

	"github.com/pkg/errors"
)

func DistributeByCount[K mapIndexType](total, share uint64) (
	func(K) []uint64,
	func(), /* close */
	error,
) {
	switch {
	case total < 1:
		return nil, nil, errors.New("empty size")
	case total < 2: //nolint:mnd
		return func(K) []uint64 { return []uint64{0} }, func() {}, nil
	case share > total:
		return nil, nil, errors.New("share > total")
	}

	c := make(map[uint64]uint64, total)

	var l sync.Mutex

	findLow := func() uint64 {
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

		if p == nil {
			return 0
		}

		pos = *p
		c[pos]++

		return pos
	}

	var cache LockedMap[K, []uint64]

	switch i, err := NewLockedMap[K, []uint64](nil, total); {
	case err != nil:
		return nil, nil, err
	default:
		cache = i
	}

	return func(k K) []uint64 {
			l.Lock()
			defer l.Unlock()

			if i, found := cache.Value(k); found {
				return i
			}

			poses := make([]uint64, share)

			for i := range share {
				p := findLow()
				poses[i] = p
			}

			cache.SetValue(k, poses)

			return poses
		},
		func() {
			clear(c)
			cache.Close()
		},
		nil
}
