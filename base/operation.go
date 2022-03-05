package base

import (
	"github.com/spikeekips/mitum/util/hint"
)

type Operation interface {
	hint.Hinter
	SealBody
	SignedFact
}

type OperationProcessor interface {
	PreProcess(StatePool) (bool, error)
	Process(StatePool) ([]State, error)
}

type StatePool interface {
	GetState(key string) (State, bool, error)
	SetStates([]State) error
}
