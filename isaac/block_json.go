package isaac

import (
	"encoding/json"
	"net/url"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
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
	e := util.StringError("unmarshal manifest")

	var u ManifestJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
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

type BlockItemFileJSONMarshaler struct {
	URI            string `json:"uri,omitempty"`
	CompressFormat string `json:"compress_format,omitempty"`
	hint.BaseHinter
}

func (f BlockItemFile) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(BlockItemFileJSONMarshaler{
		BaseHinter:     f.BaseHinter,
		URI:            f.uri.String(),
		CompressFormat: f.compressFormat,
	})
}

func (f *BlockItemFile) UnmarshalJSON(b []byte) error {
	var u BlockItemFileJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	switch i, err := url.Parse(u.URI); {
	case err != nil:
		return util.ErrInvalid.Wrap(err)
	default:
		f.uri = *i
	}

	f.compressFormat = u.CompressFormat

	return nil
}

type BlockItemFilesJSONMarshaler struct {
	Items map[base.BlockItemType]base.BlockItemFile `json:"items"`
	hint.BaseHinter
}

func (f BlockItemFiles) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(BlockItemFilesJSONMarshaler{
		BaseHinter: f.BaseHinter,
		Items:      f.items,
	})
}

type BlockItemFilesJSONUnmarshaler struct {
	Items map[base.BlockItemType]json.RawMessage `json:"items"`
	hint.BaseHinter
}

func (f *BlockItemFiles) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u BlockItemFilesJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	f.items = map[base.BlockItemType]base.BlockItemFile{}

	for i := range u.Items {
		var j base.BlockItemFile

		if err := encoder.Decode(enc, u.Items[i], &j); err != nil {
			return err
		}

		f.items[i] = j
	}

	return nil
}
