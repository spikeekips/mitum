package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type VoteResultType uint8

const (
	VoteResultNotYet VoteResultType = iota
	VoteResultDraw
	VoteResultMajority
)

func (vrt VoteResultType) Bytes() []byte {
	return util.Uint8ToBytes(uint8(vrt))
}

func (vrt VoteResultType) String() string {
	switch vrt {
	case VoteResultNotYet:
		return "NOT-YET"
	case VoteResultDraw:
		return "DRAW"
	case VoteResultMajority:
		return "MAJORITY"
	default:
		return "<Unknown VoteResult>"
	}
}

func (vrt VoteResultType) IsValid([]byte) error {
	switch vrt {
	case VoteResultNotYet, VoteResultDraw, VoteResultMajority:
		return nil
	}

	return util.InvalidError.Errorf("unknown vote result, %d", vrt)
}

func (vrt VoteResultType) MarshalText() ([]byte, error) {
	return []byte(vrt.String()), nil
}

func (vrt *VoteResultType) UnmarshalText(b []byte) error {
	var v VoteResultType
	switch t := string(b); t {
	case "NOT-YET":
		v = VoteResultNotYet
	case "DRAW":
		v = VoteResultDraw
	case "MAJORITY":
		v = VoteResultMajority
	default:
		return errors.Errorf("unknown vote result, %q>", t)
	}

	*vrt = v

	return nil
}
