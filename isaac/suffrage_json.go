package isaac

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

type suffrageNodesStateValueJSONMarshaler struct {
	Nodes []base.Node `json:"nodes"`
	hint.BaseHinter
	Height base.Height `json:"height"`
}

func (s SuffrageNodesStateValue) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(suffrageNodesStateValueJSONMarshaler{
		BaseHinter: s.BaseHinter,
		Height:     s.height,
		Nodes:      s.nodes,
	})
}

type suffrageNodesStateValueJSONUnmarshaler struct {
	Nodes  []json.RawMessage  `json:"nodes"`
	Height base.HeightDecoder `json:"height"`
}

func (s *SuffrageNodesStateValue) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageNodesStateValue")

	var u suffrageNodesStateValueJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	s.nodes = make([]base.Node, len(u.Nodes))

	for i := range u.Nodes {
		if err := encoder.Decode(enc, u.Nodes[i], &s.nodes[i]); err != nil {
			return e(err, "")
		}
	}

	s.height = u.Height.Height()

	return nil
}

type SuffrageCandidateJSONMarshaler struct {
	Node base.Node `json:"node"`
	hint.BaseHinter
	Start    base.Height `json:"start"`
	Deadline base.Height `json:"deadline"`
}

func (suf SuffrageCandidateStateValue) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(SuffrageCandidateJSONMarshaler{
		BaseHinter: suf.BaseHinter,
		Node:       suf.Node,
		Start:      suf.start,
		Deadline:   suf.deadline,
	})
}

type SuffrageCandidateJSONUnmarshaler struct {
	Node     json.RawMessage    `json:"node"`
	Start    base.HeightDecoder `json:"start"`
	Deadline base.HeightDecoder `json:"deadline"`
}

func (suf *SuffrageCandidateStateValue) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageCandidateStateValue")

	var u SuffrageCandidateJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	if err := encoder.Decode(enc, u.Node, &suf.Node); err != nil {
		return e(err, "")
	}

	suf.start = u.Start.Height()
	suf.deadline = u.Deadline.Height()

	return nil
}

type suffrageCandidatesStateValueJSONMarshaler struct {
	Nodes []base.SuffrageCandidateStateValue `json:"nodes"`
	hint.BaseHinter
}

func (s SuffrageCandidatesStateValue) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(suffrageCandidatesStateValueJSONMarshaler{
		BaseHinter: s.BaseHinter,
		Nodes:      s.nodes,
	})
}

type suffrageCandidatesStateValueJSONUnmarshaler struct {
	Nodes []json.RawMessage `json:"nodes"`
}

func (s *SuffrageCandidatesStateValue) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageCandidatesStateValue")

	var u suffrageCandidatesStateValueJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	s.nodes = make([]base.SuffrageCandidateStateValue, len(u.Nodes))

	for i := range u.Nodes {
		if err := encoder.Decode(enc, u.Nodes[i], &s.nodes[i]); err != nil {
			return e(err, "")
		}
	}

	return nil
}
