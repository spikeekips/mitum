package launch

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
)

func PrepareEncoders() (*encoder.Encoders, *jsonenc.Encoder, error) {
	e := util.StringErrorFunc("failed to prepare encoders")

	encs := encoder.NewEncoders()
	enc := jsonenc.NewEncoder()

	if err := encs.AddHinter(enc); err != nil {
		return nil, nil, e(err, "")
	}

	if err := LoadHinters(enc); err != nil {
		return nil, nil, e(err, "")
	}

	return encs, enc, nil
}
