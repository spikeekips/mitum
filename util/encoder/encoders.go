package encoder

import (
	"github.com/spikeekips/mitum/util/hint"
)

type Encoders struct {
	*hint.CompatibleSet[Encoder]
	defaultenc Encoder
	jenc       Encoder
}

func NewEncoders(defaultenc, jenc Encoder) *Encoders {
	encs := &Encoders{
		CompatibleSet: hint.NewCompatibleSet[Encoder](3), //nolint:mnd // small number of encoders
		defaultenc:    defaultenc,
		jenc:          jenc,
	}

	_ = encs.AddEncoder(defaultenc)

	if !jenc.Hint().Equal(defaultenc.Hint()) {
		_ = encs.AddEncoder(jenc)
	}

	return encs
}

func (encs *Encoders) Default() Encoder {
	return encs.defaultenc
}

func (encs *Encoders) JSON() Encoder {
	return encs.jenc
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
