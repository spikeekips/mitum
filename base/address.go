package base

import (
	"fmt"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

const AddressTypeSize = 3

// Address represents the address of account.
type Address interface {
	fmt.Stringer // NOTE String() should be typed string
	util.Byter
	util.IsValider
	Equal(Address) bool
}

type Addresses []Address

func (as Addresses) Len() int {
	return len(as)
}

func (as Addresses) Less(i, j int) bool {
	return as[i].String() < as[j].String()
}

func (as Addresses) Swap(i, j int) {
	as[i], as[j] = as[j], as[i]
}

// DecodeAddress decodes Address from string.
func DecodeAddress(s string, enc encoder.Encoder) (Address, error) {
	if len(s) < 1 {
		return nil, nil
	}

	switch i, found := objcache.Get(s); {
	case !found:
	case i == nil:
		return nil, nil
	default:
		if err, ok := i.(error); ok {
			return nil, err
		}

		return i.(Address), nil //nolint:forcetypeassert //...
	}

	ad, err := decodeAddress(s, enc)
	if err != nil {
		objcache.Set(s, err, nil)

		return nil, err
	}

	objcache.Set(s, ad, nil)

	return ad, nil
}

func decodeAddress(s string, enc encoder.Encoder) (Address, error) {
	e := util.StringErrorFunc("failed to parse address")

	i, err := enc.DecodeWithFixedHintType(s, AddressTypeSize)

	switch {
	case err != nil:
		return nil, e(err, "failed to decode address")
	case i == nil:
		return nil, nil
	default:
		ad, ok := i.(Address)
		if !ok {
			return nil, e(nil, "failed to decode address; not Address, %T", i)
		}

		return ad, nil
	}
}
