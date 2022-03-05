package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type Database interface {
	Close() error
	Manifest(height base.Height) (base.Manifest, bool, error)
	LastManifest() (base.Manifest, bool, error)
	Suffrage(suffrageHeight base.Height) (base.State, bool, error)
	LastSuffrage() (base.State, bool, error)
	State(stateHash util.Hash) (base.State, bool, error)
	ExistsOperation(operationFactHash util.Hash) (bool, error)
	NewWBlockWriteDatabase(height base.Height, proposal util.Hash) BlockWriteDatabase
	MergeBlockWriteDatabase(BlockWriteDatabase) error
}

// TempDatabase is the temporary database; it contains only manifest and
// others of one block.
type TempDatabase interface {
	Close() error
	Remove() error
	Height() (manifestHeight base.Height, suffrageHeight base.Height)
	Manifest() (base.Manifest, error)
	Suffrage() (base.State, bool, error)
	State(stateHash util.Hash) (base.State, bool, error)
	ExistsOperation(operationFactHash util.Hash) (bool, error)
}

type BlockWriteDatabase interface {
	Close() error
	Cancel() error
	Manifest() (base.Manifest, error)
	SetManifest(m base.Manifest) error
	SetStates(sts []base.State) error
	SetOperations(ops []util.Hash) error
	Write() error
}
