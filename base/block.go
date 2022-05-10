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

type BlockMap interface {
	NodeSigned
	Manifest() Manifest
	Item(BlockMapItemType) (BlockMapItem, bool)
	Items(func(BlockMapItem) bool)
}

type BlockMapItem interface {
	util.IsValider
	Type() BlockMapItemType
	URL() *url.URL
	Checksum() string
	Num() uint64
}

type BlockMapItemType string

var (
	BlockMapItemTypeProposal       BlockMapItemType = "blockmapitem_proposal"
	BlockMapItemTypeOperations     BlockMapItemType = "blockmapitem_operations"
	BlockMapItemTypeOperationsTree BlockMapItemType = "blockmapitem_operations_tree"
	BlockMapItemTypeStates         BlockMapItemType = "blockmapitem_states"
	BlockMapItemTypeStatesTree     BlockMapItemType = "blockmapitem_states_tree"
	BlockMapItemTypeVoteproofs     BlockMapItemType = "blockmapitem_voteproofs"
)

func (t BlockMapItemType) IsValid([]byte) error {
	switch t {
	case BlockMapItemTypeProposal,
		BlockMapItemTypeOperations,
		BlockMapItemTypeOperationsTree,
		BlockMapItemTypeStates,
		BlockMapItemTypeStatesTree,
		BlockMapItemTypeVoteproofs:
		return nil
	default:
		return util.ErrInvalid.Errorf("unknown block map item type, %q", t)
	}
}

func (t BlockMapItemType) String() string {
	return string(t)
}
