package base

import (
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
	NodeSigned
	Manifest() Manifest
	Item(BlockDataType) (BlockDataMapItem, bool)
	All() map[BlockDataType]BlockDataMapItem
}

type BlockDataMapItem interface {
	hint.Hinter
	util.IsValider
	Type() BlockDataType
	URL() *url.URL
	Checksum() string
	Num() int64
}

type BlockDataType string

var (
	BlockDataTypeProposal       BlockDataType = "block_data_proposal"
	BlockDataTypeOperations     BlockDataType = "block_data_operations"
	BlockDataTypeOperationsTree BlockDataType = "block_data_operations_tree"
	BlockDataTypeStates         BlockDataType = "block_data_states"
	BlockDataTypeStatesTree     BlockDataType = "block_data_states_tree"
	BlockDataTypeVoteproofs     BlockDataType = "block_data_voteproofs"
)

func (t BlockDataType) IsValid([]byte) error {
	switch t {
	case BlockDataTypeProposal,
		BlockDataTypeOperations,
		BlockDataTypeOperationsTree,
		BlockDataTypeStates,
		BlockDataTypeStatesTree,
		BlockDataTypeVoteproofs:
		return nil
	default:
		return util.InvalidError.Errorf("unknown block data type, %q", t)
	}
}

func (t BlockDataType) String() string {
	return string(t)
}
