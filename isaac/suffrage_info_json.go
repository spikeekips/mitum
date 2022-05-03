package isaac

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

type SuffrageInfoJSONMarshaler struct {
	hint.BaseHinter
	State      util.Hash                `json:"state"`
	Height     base.Height              `json:"height"`
	Suffrage   []base.Node              `json:"suffrage"`
	Candidates []base.SuffrageCandidate `json:"candidates"`
}

func (info SuffrageInfo) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(SuffrageInfoJSONMarshaler{
		BaseHinter: info.BaseHinter,
		State:      info.state,
		Height:     info.height,
		Suffrage:   info.suffrage,
		Candidates: info.candidates,
	})
}

type SuffrageInfoJSONUnmarshaler struct {
	State      valuehash.HashDecoder `json:"state"`
	Height     base.HeightDecoder    `json:"height"`
	Suffrage   []json.RawMessage     `json:"suffrage"`
	Candidates []json.RawMessage     `json:"candidates"`
}

func (info *SuffrageInfo) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageInfo")

	var u SuffrageInfoJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	info.suffrage = make([]base.Node, len(u.Suffrage))
	for i := range u.Suffrage {
		node, err := base.DecodeNode(u.Suffrage[i], enc)
		if err != nil {
			return e(err, "")
		}

		info.suffrage[i] = node
	}

	info.candidates = make([]base.SuffrageCandidate, len(u.Candidates))
	for i := range u.Candidates {
		switch hinter, err := enc.Decode(u.Candidates[i]); {
		case err != nil:
			return e(err, "failed to decode SuffrageCandidate")
		case hinter == nil:
			continue
		default:
			j, ok := hinter.(base.SuffrageCandidate)
			if !ok {
				return e(nil, "expected base.SuffrageCandidate, but %T", hinter)
			}

			info.candidates[i] = j
		}
	}

	info.state = u.State.Hash()
	info.height = u.Height.Height()

	return nil
}
