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

func (encs *Encoders) Find(ht hint.Hint) Encoder {
	hinter := encs.CompatibleSet.Find(ht)
	if hinter == nil {
		return nil
	}

	r, ok := hinter.(Encoder)
	if !ok {
		return nil
	}

	return r
}
