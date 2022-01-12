//go:build test
// +build test

package base

func MustNewStringAddress(s string) StringAddress {
	ad := NewStringAddress(s)

	if err := ad.IsValid(nil); err != nil {
		panic(err)
	}

	return ad
}
