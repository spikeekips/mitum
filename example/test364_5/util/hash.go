package util

import (
	"crypto/rand"
	"math"
	"math/big"
	mathrand "math/rand"

	"github.com/twmb/murmur3"
)

func NewHashSeed() uint64 {
	maxv := big.NewInt(0).SetUint64(uint64(math.MaxUint64))

	switch rnd, err := rand.Int(rand.Reader, maxv); {
	case err != nil:
		return mathrand.Uint64()
	default:
		return uint64(rnd.Uint64())
	}
}

// StringDjb2 is derived from
// https://github.com/patrickmn/go-cache/blob/master/sharded.go .
func StringDjb2(seed uint64, k string) uint64 {
	var (
		l = uint64(len(k))
		d = 5381 + seed + l
		i = uint64(0)
	)

	if l >= 4 {
		for i < l-4 {
			d = (d * 33) ^ uint64(k[i])
			d = (d * 33) ^ uint64(k[i+1])
			d = (d * 33) ^ uint64(k[i+2])
			d = (d * 33) ^ uint64(k[i+3])
			i += 4
		}
	}

	switch l - i {
	case 1:
	case 2:
		d = (d * 33) ^ uint64(k[i])
	case 3:
		d = (d * 33) ^ uint64(k[i])
		d = (d * 33) ^ uint64(k[i+1])
	case 4:
		d = (d * 33) ^ uint64(k[i])
		d = (d * 33) ^ uint64(k[i+1])
		d = (d * 33) ^ uint64(k[i+2])
	}

	return uint64(d ^ (d >> 16))
}

func Murmur3(seed uint64, s string) uint64 {
	return murmur3.SeedStringSum64(seed, s)
}
