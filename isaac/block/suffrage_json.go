package isaacblock

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
)

type SuffrageProofJSONMarshaler struct {
	hint.BaseHinter
	M         base.BlockMap        `json:"map"`
	St        base.State           `json:"state"`
	Proof     fixedtree.Proof      `json:"proof"`
	Voteproof base.ACCEPTVoteproof `json:"voteproof"`
}

func (s SuffrageProof) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(SuffrageProofJSONMarshaler{
		BaseHinter: s.BaseHinter,
		M:          s.m,
		St:         s.st,
		Proof:      s.proof,
		Voteproof:  s.voteproof,
	})
}

type SuffrageProofJSONUnmarshaler struct {
	M         json.RawMessage `json:"map"`
	St        json.RawMessage `json:"state"`
	Proof     fixedtree.Proof `json:"proof"`
	Voteproof json.RawMessage `json:"voteproof"`
}

func (s *SuffrageProof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageProof")

	var u SuffrageProofJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch hinter, err := enc.Decode(u.M); {
	case err != nil:
		return e(err, "")
	case hinter == nil:
		return e(nil, "empty manifest")
	default:
		i, ok := hinter.(base.BlockMap)
		if !ok {
			return e(nil, "expected BlockMap, but %T", hinter)
		}

		s.m = i
	}

	switch hinter, err := enc.Decode(u.St); {
	case err != nil:
		return e(err, "")
	case hinter == nil:
		return e(nil, "empty state")
	default:
		i, ok := hinter.(base.State)
		if !ok {
			return e(nil, "expected State, but %T", hinter)
		}

		s.st = i
	}

	switch hinter, err := enc.Decode(u.Voteproof); {
	case err != nil:
		return e(err, "")
	case hinter == nil:
		return e(nil, "empty accept voteproof")
	default:
		i, ok := hinter.(base.ACCEPTVoteproof)
		if !ok {
			return e(nil, "expected ACCEPTVoteproof, but %T", hinter)
		}

		s.voteproof = i
	}

	s.proof = u.Proof

	return nil
}
