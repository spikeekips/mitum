package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type Stage string

const (
	StageUnknown  = Stage("UNKNOWN")
	StageINIT     = Stage("INIT")
	StageProposal = Stage("PROPOSAL")
	StageACCEPT   = Stage("ACCEPT")
)

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

func (st Stage) MarshalText() ([]byte, error) {
	return []byte(st), nil
}

func (st *Stage) UnmarshalText(b []byte) error {
	var t Stage
	switch s := string(b); s {
	case "INIT":
		t = StageINIT
	case "PROPOSAL":
		t = StageProposal
	case "ACCEPT":
		t = StageACCEPT
	default:
		return errors.Errorf("unknown stage, %q", s)
	}

	*st = t

	return nil
}

func (st Stage) CanVote() bool {
	switch st {
	case StageINIT, StageACCEPT:
		return true
	default:
		return false
	}
}
