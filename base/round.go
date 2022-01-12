package base

import (
	"github.com/spikeekips/mitum/util"
)

type Round uint64

func (r Round) Uint64() uint64 {
	return uint64(r)
}

func (r Round) Bytes() []byte {
	return util.Uint64ToBytes(uint64(r))
}
