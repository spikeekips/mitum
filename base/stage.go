package base

import (
	"github.com/spikeekips/mitum/util"
)

type Stage string

const (
	StageUnknown  = Stage("UNKNOWN")
	StageINIT     = Stage("INIT")
	StageProposal = Stage("PROPOSAL")
	StageACCEPT   = Stage("ACCEPT")
)

var statesmap = map[Stage]int{
	StageUnknown:  0,
	StageINIT:     1,
	StageProposal: 2,
	StageACCEPT:   3,
}

func (st Stage) Bytes() []byte {
	return []byte(st)
}

func (st Stage) IsValid([]byte) error {
	switch st {
	case StageINIT, StageACCEPT, StageProposal:
		return nil
	}

	return util.InvalidError.Errorf("unknown stage, %q", st)
}

func (st Stage) Compare(b Stage) int {
	as, found := statesmap[st]
	if !found {
		return 1
	}

	bs, found := statesmap[b]
	if !found {
		return 1
	}

	switch c := as - bs; {
	case c > 0:
		return 1
	case c < 0:
		return -1
	default:
		return 0
	}
}

func (st Stage) CanVote() bool {
	switch st {
	case StageINIT, StageACCEPT:
		return true
	default:
		return false
	}
}

func (st Stage) MarshalText() ([]byte, error) {
	return []byte(st), nil
}

func (st *Stage) UnmarshalText(b []byte) error {
	*st = Stage(string(b))

	return nil
}
