package util

import (
	"crypto/rand"
	"math"
	"math/big"

	"github.com/twmb/murmur3"
)

var maxv = big.NewInt(0).SetUint64(uint64(math.MaxUint64))

func NewHashSeed() uint64 {
	for {
		if rnd, err := rand.Int(rand.Reader, maxv); err == nil {
			return rnd.Uint64()
		}
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

	if l >= 4 { //nolint:mnd
		for i < l-4 {
			d = (d * 33) ^ uint64(k[i])   //nolint:mnd
			d = (d * 33) ^ uint64(k[i+1]) //nolint:mnd
			d = (d * 33) ^ uint64(k[i+2]) //nolint:mnd
			d = (d * 33) ^ uint64(k[i+3]) //nolint:mnd
			i += 4
		}
	}

	switch l - i {
	case 1:
	case 2: //nolint:mnd
		d = (d * 33) ^ uint64(k[i]) //nolint:mnd
	case 3: //nolint:mnd
		d = (d * 33) ^ uint64(k[i])   //nolint:mnd
		d = (d * 33) ^ uint64(k[i+1]) //nolint:mnd
	case 4: //nolint:mnd
		d = (d * 33) ^ uint64(k[i])   //nolint:mnd
		d = (d * 33) ^ uint64(k[i+1]) //nolint:mnd
		d = (d * 33) ^ uint64(k[i+2]) //nolint:mnd
	}

	return d ^ (d >> 16) //nolint:mnd
}

func Murmur3(seed uint64, s string) uint64 {
	return murmur3.SeedStringSum64(seed, s)
}
