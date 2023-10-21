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

func (encs *Encoders) AddEncoder(enc Encoder) error {
	return encs.CompatibleSet.AddHinter(enc)
}

func (encs *Encoders) AddDetail(d DecodeDetail) error {
	var gerr error

	encs.Traverse(func(_ hint.Hint, enc Encoder) bool {
		if err := enc.Add(d); err != nil {
			gerr = err

			return false
		}

		return true
	})

	return gerr
}

func (encs *Encoders) AddHinter(hr hint.Hinter) error {
	var gerr error

	encs.Traverse(func(_ hint.Hint, enc Encoder) bool {
		if err := enc.AddHinter(hr); err != nil {
			gerr = err

			return false
		}

		return true
	})

	return gerr
}
