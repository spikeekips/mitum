package isaac

import (
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

type ManifestJSONMarshaler struct {
	hint.BaseHinter
	H              util.Hash   `json:"hash"`
	Height         base.Height `json:"height"`
	Previous       util.Hash   `json:"previous"`
	Proposal       util.Hash   `json:"proposal"`
	OperationsTree util.Hash   `json:"operations_tree"`
	StatesTree     util.Hash   `json:"states_tree"`
	Suffrage       util.Hash   `json:"suffrage"`
	ProposedAt     time.Time   `json:"proposed_at"`
	NodeCreatedAt  time.Time   `json:"node_created_at"`
}

func (m Manifest) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(ManifestJSONMarshaler{
		BaseHinter:     m.BaseHinter,
		H:              m.h,
		Height:         m.height,
		Previous:       m.previous,
		Proposal:       m.proposal,
		OperationsTree: m.operationsTree,
		StatesTree:     m.statesTree,
		Suffrage:       m.suffrage,
		ProposedAt:     m.proposedAt,
		NodeCreatedAt:  m.nodeCreatedAt,
	})
}

type ManifestJSONUnmarshaler struct {
	H              valuehash.HashDecoder `json:"hash"`
	Height         base.HeightDecoder    `json:"height"`
	Previous       valuehash.HashDecoder `json:"previous"`
	Proposal       valuehash.HashDecoder `json:"proposal"`
	OperationsTree valuehash.HashDecoder `json:"operations_tree"`
	StatesTree     valuehash.HashDecoder `json:"states_tree"`
	Suffrage       valuehash.HashDecoder `json:"suffrage"`
	ProposedAt     localtime.Time        `json:"proposed_at"`
	NodeCreatedAt  localtime.Time        `json:"node_created_at"`
}

func (m *Manifest) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal manifest")

	var u ManifestJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	m.h = u.H.Hash()
	m.height = u.Height.Height()
	m.previous = u.Previous.Hash()
	m.proposal = u.Proposal.Hash()
	m.operationsTree = u.OperationsTree.Hash()
	m.statesTree = u.StatesTree.Hash()
	m.suffrage = u.Suffrage.Hash()
	m.proposedAt = u.ProposedAt.Time
	m.nodeCreatedAt = u.NodeCreatedAt.Time

	return nil
}
