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
	ProposedAt     time.Time `json:"proposed_at"`
	StatesTree     util.Hash `json:"states_tree"`
	Hash           util.Hash `json:"hash"`
	Previous       util.Hash `json:"previous"`
	Proposal       util.Hash `json:"proposal"`
	OperationsTree util.Hash `json:"operations_tree"`
	Suffrage       util.Hash `json:"suffrage"`
	hint.BaseHinter
	Height base.Height `json:"height"`
}

func (m Manifest) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(ManifestJSONMarshaler{
		BaseHinter:     m.BaseHinter,
		Hash:           m.h,
		Height:         m.height,
		Previous:       m.previous,
		Proposal:       m.proposal,
		OperationsTree: m.operationsTree,
		StatesTree:     m.statesTree,
		Suffrage:       m.suffrage,
		ProposedAt:     m.proposedAt,
	})
}

type ManifestJSONUnmarshaler struct {
	ProposedAt     localtime.Time        `json:"proposed_at"`
	Hash           valuehash.HashDecoder `json:"hash"`
	Previous       valuehash.HashDecoder `json:"previous"`
	Proposal       valuehash.HashDecoder `json:"proposal"`
	OperationsTree valuehash.HashDecoder `json:"operations_tree"`
	StatesTree     valuehash.HashDecoder `json:"states_tree"`
	Suffrage       valuehash.HashDecoder `json:"suffrage"`
	Height         base.HeightDecoder    `json:"height"`
}

func (m *Manifest) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal manifest")

	var u ManifestJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	m.h = u.Hash.Hash()
	m.height = u.Height.Height()
	m.previous = u.Previous.Hash()
	m.proposal = u.Proposal.Hash()
	m.operationsTree = u.OperationsTree.Hash()
	m.statesTree = u.StatesTree.Hash()
	m.suffrage = u.Suffrage.Hash()
	m.proposedAt = u.ProposedAt.Time

	return nil
}
