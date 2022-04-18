package isaacoperation

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
)

type SuffrageUpdateFactJSONMarshaler struct {
	base.BaseFactJSONMarshaler
	News []base.Node    `json:"new_members"`
	Outs []base.Address `json:"out_members"`
}

func (fact SuffrageUpdateFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(SuffrageUpdateFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.JSONMarshaler(),
		News:                  fact.news,
		Outs:                  fact.outs,
	})
}

type SuffrageUpdateFactJSONUnmarshaler struct {
	base.BaseFactJSONUnmarshaler
	News json.RawMessage `json:"new_members"`
	Outs []string        `json:"out_members"`
}

func (fact *SuffrageUpdateFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageUpdateFact")

	var u SuffrageUpdateFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	fact.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)

	switch hinters, err := enc.DecodeSlice(u.News); {
	case err != nil:
		return e(err, "")
	default:
		fact.news = make([]base.Node, len(hinters))
		for i := range hinters {
			j, ok := hinters[i].(base.Node)
			if !ok {
				return e(nil, "not Node, %T", hinters[i])
			}

			fact.news[i] = j
		}
	}

	fact.outs = make([]base.Address, len(u.Outs))
	for i := range u.Outs {
		address, err := base.DecodeAddress(u.Outs[i], enc)
		if err != nil {
			return e(err, "")
		}

		fact.outs[i] = address
	}

	return nil
}
