package isaacblock

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
)

type SuffrageProofJSONMarshaler struct {
	Map   base.BlockMap   `json:"map"`
	State base.State      `json:"state"`
	Proof fixedtree.Proof `json:"proof"`
	hint.BaseHinter
}

func (s SuffrageProof) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(SuffrageProofJSONMarshaler{
		BaseHinter: s.BaseHinter,
		Map:        s.m,
		State:      s.st,
		Proof:      s.proof,
	})
}

type SuffrageProofJSONUnmarshaler struct {
	Map   json.RawMessage `json:"map"`
	State json.RawMessage `json:"state"`
	Proof fixedtree.Proof `json:"proof"`
}

func (s *SuffrageProof) DecodeJSON(b []byte, enc encoder.Encoder) error {
	e := util.StringError("decode SuffrageProof")

	var u SuffrageProofJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := encoder.Decode(enc, u.Map, &s.m); err != nil {
		return e.Wrap(err)
	}

	if err := encoder.Decode(enc, u.State, &s.st); err != nil {
		return e.Wrap(err)
	}

	s.proof = u.Proof

	return nil
}
