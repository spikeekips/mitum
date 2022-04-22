package isaac

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

type suffrageStateValueJSONMarshaler struct {
	hint.BaseHinter
	Height   base.Height `json:"height"`
	Previous util.Hash   `json:"previous"`
	Nodes    []base.Node `json:"nodes"`
}

func (s SuffrageStateValue) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(suffrageStateValueJSONMarshaler{
		BaseHinter: s.BaseHinter,
		Height:     s.height,
		Previous:   s.previous,
		Nodes:      s.nodes,
	})
}

type suffrageStateValueJSONUnmarshaler struct {
	Height   base.HeightDecoder    `json:"height"`
	Previous valuehash.HashDecoder `json:"previous"`
	Nodes    []json.RawMessage     `json:"nodes"`
}

func (s *SuffrageStateValue) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageStateValue")

	var u suffrageStateValueJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	nodes := make([]base.Node, len(u.Nodes))
	for i := range u.Nodes {
		j, err := base.DecodeNode(u.Nodes[i], enc)
		if err != nil {
			return e(err, "")
		}

		nodes[i] = j
	}

	s.height = u.Height.Height()
	s.previous = u.Previous.Hash()
	s.nodes = nodes

	return nil
}
