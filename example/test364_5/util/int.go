package util

import "math"

func SafeUint64ToInt64(i uint64) int64 {
	switch {
	case i > uint64(math.MaxInt64):
		return int64(math.MaxInt64)
	default:
		return int64(i)
	}
}

func SafeIntToUint64(i int) uint64 {
	switch {
	case i < 0:
		return 0
	default:
		return uint64(i)
	}
}

func SafeUint64ToInt(i uint64) int {
	return int(i) //nolint:gosec
}

func SafeIntToUint32(i int) uint32 {
	switch {
	case i < 0:
		return 0
	case i > math.MaxUint32:
		return math.MaxUint32
	default:
		return uint32(i)
	}
}
