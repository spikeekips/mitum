package encoder

import (
	"github.com/spikeekips/mitum/util/hint"
)

type Encoders struct {
	*hint.CompatibleSet
}

func NewEncoders() *Encoders {
	return &Encoders{
		CompatibleSet: hint.NewCompatibleSet(),
	}
}
