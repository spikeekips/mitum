package util

import (
	"crypto/rand"
	"math"
	"math/big"
	mathrand "math/rand"
)

func NewDjb2Seed() uint64 {
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

const (
	murmur2M     = 0x5bd1e995
	murmur2BIG_M = 0xc6a4a7935bd1e995
	murmur2R     = 24
	murmur2BIG_R = 47
)

// 32-bit mixing function.
func murmurMix(h, k uint64) (uint64, uint64) {
	k *= murmur2M
	k ^= k >> murmur2R
	k *= murmur2M
	h *= murmur2M
	h ^= k
	return h, k
}

// Murmur2 is derived from
// https://github.com/aviddiviner/go-murmur/blob/master/murmur2.go .
func Murmur2(seed uint64, data []byte) (h uint64) {
	var k uint64

	h = seed ^ uint64(len(data))

	for l := len(data); l >= 4; l -= 4 {
		k = uint64(data[0]) | uint64(data[1])<<8 | uint64(data[2])<<16 | uint64(data[3])<<24
		h, k = murmurMix(h, k)
		data = data[4:]
	}

	switch len(data) {
	case 3:
		h ^= uint64(data[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(data[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(data[0])
		h *= murmur2M
	}

	h ^= h >> 13
	h *= murmur2M
	h ^= h >> 15

	return
}
