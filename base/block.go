package base

import (
	"net/url"
	"time"

	"github.com/spikeekips/mitum/util"
)

type Manifest interface {
	util.Hasher
	util.IsValider
	Height() Height
	Previous() util.Hash
	Proposal() util.Hash       // NOTE proposal fact hash
	OperationsTree() util.Hash // NOTE operations tree root hash
	StatesTree() util.Hash     // NOTE states tree root hash
	Suffrage() util.Hash       // NOTE state hash of newly updated SuffrageStateValue
	ProposedAt() time.Time     // NOTE Proposal proposed time
}

type BlockdataMap interface {
	NodeSigned
	Manifest() Manifest
	Item(BlockdataType) (BlockdataMapItem, bool)
	Items(func(BlockdataMapItem) bool)
}

type BlockdataMapItem interface {
	util.IsValider
	Type() BlockdataType
	URL() *url.URL
	Checksum() string
	Num() uint64
}

type BlockdataType string

var (
	BlockdataTypeProposal       BlockdataType = "block_data_proposal"
	BlockdataTypeOperations     BlockdataType = "block_data_operations"
	BlockdataTypeOperationsTree BlockdataType = "block_data_operations_tree"
	BlockdataTypeStates         BlockdataType = "block_data_states"
	BlockdataTypeStatesTree     BlockdataType = "block_data_states_tree"
	BlockdataTypeVoteproofs     BlockdataType = "block_data_voteproofs"
)

func (t BlockdataType) IsValid([]byte) error {
	switch t {
	case BlockdataTypeProposal,
		BlockdataTypeOperations,
		BlockdataTypeOperationsTree,
		BlockdataTypeStates,
		BlockdataTypeStatesTree,
		BlockdataTypeVoteproofs:
		return nil
	default:
		return util.InvalidError.Errorf("unknown block data type, %q", t)
	}
}

func (t BlockdataType) String() string {
	return string(t)
}
