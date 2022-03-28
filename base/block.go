package base

import (
	"io"
	"net/url"
	"time"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type Manifest interface {
	hint.Hinter
	util.Hasher
	util.IsValider
	Height() Height
	Previous() util.Hash
	Proposal() util.Hash       // NOTE proposal fact hash
	OperationsTree() util.Hash // NOTE operations tree root hash
	StatesTree() util.Hash     // NOTE states tree root hash
	Suffrage() util.Hash       // NOTE state hash of SuffrageStateValue
	CreatedAt() time.Time      // NOTE Proposal proposed time
	NodeCreatedAt() time.Time  // NOTE created time in local node
}

type BlockDataMap interface {
	hint.Hinter
	util.IsValider
	Height() Height
	Manifest() BlockDataMapItem
	Proposal() BlockDataMapItem
	Operations() BlockDataMapItem
	OperationsTree() BlockDataMapItem
	States() BlockDataMapItem
	StatesTree() BlockDataMapItem
	INITVoteproof() BlockDataMapItem
	ACCEPTVoteproof() BlockDataMapItem
}

type BlockDataMapItem interface {
	hint.Hinter
	util.IsValider
	Type() BlockDataType
	URL() *url.URL
	Checksum() string
}

type BlockDataType string

var (
	BlockDataTypeManifest        BlockDataType = "block_data_manifest"
	BlockDataTypeProposal        BlockDataType = "block_data_proposal"
	BlockDataTypeOperations      BlockDataType = "block_data_operations"
	BlockDataTypeOperationsTree  BlockDataType = "block_data_operations_tree"
	BlockDataTypeStates          BlockDataType = "block_data_states"
	BlockDataTypeStatesTree      BlockDataType = "block_data_states_tree"
	BlockDataTypeINITVoteproof   BlockDataType = "block_data_init_voteproof"
	BlockDataTypeACCEPTVoteproof BlockDataType = "block_data_accept_voteproof"
)

func (t BlockDataType) IsValid([]byte) error {
	switch t {
	case BlockDataTypeManifest,
		BlockDataTypeProposal,
		BlockDataTypeOperations,
		BlockDataTypeOperationsTree,
		BlockDataTypeStates,
		BlockDataTypeStatesTree,
		BlockDataTypeINITVoteproof,
		BlockDataTypeACCEPTVoteproof:
		return nil
	default:
		return util.InvalidError.Errorf("unknown block data type, %q", t)
	}
}

type BlockData interface {
	hint.Hinter
	util.IsValider
	Height() Height
	Type() BlockDataType
	io.ReadCloser
}
