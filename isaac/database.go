package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

var RetryMergeToPermanentDatabaseError = util.NewError("failed to merge to permanent database; retry")

// Database serves block data like manifest, states and operations from
// TempDatabases and PermanentDatabase. It has several TempDatabases and one
// PermanentDatabase.
type Database interface {
	util.Daemon
	Close() error
	Manifest(height base.Height) (base.Manifest, bool, error)
	LastManifest() (base.Manifest, bool, error)
	Suffrage(blockheight base.Height) (base.State, bool, error)
	SuffrageByHeight(suffrageHeight base.Height) (base.State, bool, error)
	LastSuffrage() (base.State, bool, error)
	State(key string) (base.State, bool, error)
	ExistsOperation(operationFactHash util.Hash) (bool, error)
	NewBlockWriteDatabase(height base.Height) (BlockWriteDatabase, error)
	MergeBlockWriteDatabase(BlockWriteDatabase) error
}

type PartialDatabase interface {
	State(key string) (base.State, bool, error)
	ExistsOperation(operationFactHash util.Hash) (bool, error)
}

// TempDatabase is the temporary database; it contains only manifest and
// others of one block for storing block data fast.
type TempDatabase interface {
	PartialDatabase
	Close() error
	Remove() error
	Height() base.Height
	SuffrageHeight() base.Height
	Manifest() (base.Manifest, error)
	Suffrage() (base.State, bool, error)
}

type BlockWriteDatabase interface {
	Close() error
	Cancel() error
	Manifest() (base.Manifest, error)
	SetManifest(m base.Manifest) error
	SetStates(sts []base.State) error
	SetOperations(ops []util.Hash) error
	Write() error
	TempDatabase() (TempDatabase, error)
}

// PermanentDatabase stores block data permanently.
type PermanentDatabase interface {
	PartialDatabase
	Close() error
	LastManifest() (base.Manifest, bool, error)
	Manifest(base.Height) (base.Manifest, bool, error)
	LastSuffrage() (base.State, bool, error)
	Suffrage(blockheight base.Height) (base.State, bool, error)
	SuffrageByHeight(suffrageHeight base.Height) (base.State, bool, error)
	MergeTempDatabase(TempDatabase) error
}
