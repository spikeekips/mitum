package base

import (
	"fmt"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

const AddressTypeSize = 3

// Address represents the address of account.
type Address interface {
	fmt.Stringer // NOTE String() should be typed string
	hint.Hinter
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

	e := util.StringErrorFunc("failed to parse address")

	i, err := enc.DecodeWithFixedHintType(s, AddressTypeSize)
	switch {
	case err != nil:
		return nil, e(err, "failed to decode address")
	case i == nil:
		return nil, nil
	}

	ad, ok := i.(Address)
	if !ok {
		return nil, e(nil, "failed to decode address; not Address, %T", i)
	}

	return ad, nil
}
