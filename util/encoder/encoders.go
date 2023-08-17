package encoder

import (
	"github.com/spikeekips/mitum/util/hint"
)

type Encoders struct {
	*hint.CompatibleSet[Encoder]
}

func NewEncoders() *Encoders {
	return &Encoders{
		CompatibleSet: hint.NewCompatibleSet[Encoder](3), //nolint:gomnd // small number of encoders
	}
}
