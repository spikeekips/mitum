package base

import (
	"fmt"

	"github.com/spikeekips/mitum/util"
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
