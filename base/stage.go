package base

import (
	"github.com/spikeekips/mitum/util"
)

type Stage string

const (
	StageUnknown = Stage("UNKNOWN")
	StageINIT    = Stage("INIT")
	StageACCEPT  = Stage("ACCEPT")
)

var statesmap = map[Stage]int{
	StageUnknown: 0,
	StageINIT:    1,
	StageACCEPT:  3, //nolint:gomnd // for Compare()
}

func (st Stage) Bytes() []byte {
	return []byte(st)
}

func (st Stage) String() string {
	return string(st)
}

func (st Stage) IsValid([]byte) error {
	switch st {
	case StageINIT, StageACCEPT:
		return nil
	default:
		return util.ErrInvalid.Errorf("unknown stage, %q", st)
	}
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
