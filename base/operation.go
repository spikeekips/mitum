package base

import (
	"github.com/spikeekips/mitum/util/hint"
)

type Operation interface {
	SealBody
	hint.Hinter
	Process() ([]State, error)
	SignedFact() SignedFact
}
