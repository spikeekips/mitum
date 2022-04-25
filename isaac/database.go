package isaac

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

var RetryMergeToPermanentDatabaseError = util.NewError("failed to merge to permanent database; retry")

// Database serves block data like blockdatamap, states and operations from
// TempDatabases and PermanentDatabase. It has several TempDatabases and one
// PermanentDatabase.
type Database interface {
	util.Daemon
	Close() error
	Map(height base.Height) (base.BlockDataMap, bool, error)
	LastMap() (base.BlockDataMap, bool, error)
	Suffrage(blockheight base.Height) (base.State, bool, error)
	SuffrageByHeight(suffrageHeight base.Height) (base.State, bool, error)
	LastSuffrage() (base.State, bool, error)
	LastNetworkPolicy() base.NetworkPolicy
	State(key string) (base.State, bool, error)
	// ExistsInStateOperation has only operation facts, which is in state
	ExistsInStateOperation(operationFactHash util.Hash) (bool, error)
	// NOTE ExistsKnownOperation has the known operation hashes
	ExistsKnownOperation(operationHash util.Hash) (bool, error)
	NewBlockWriteDatabase(height base.Height) (BlockWriteDatabase, error)
	MergeBlockWriteDatabase(BlockWriteDatabase) error
	// BLOCK get last init and accept voteproof
}

type PartialDatabase interface {
	State(key string) (base.State, bool, error)
	ExistsInStateOperation(operationFactHash util.Hash) (bool, error)
	ExistsKnownOperation(operationFactHash util.Hash) (bool, error)
}

// TempDatabase is the temporary database; it contains only blockdatamap and
// others of one block for storing block data fast.
type TempDatabase interface {
	PartialDatabase
	Close() error
	Remove() error
	Height() base.Height
	Map() (base.BlockDataMap, error)
	SuffrageHeight() base.Height
	Suffrage() (base.State, bool, error)
	NetworkPolicy() base.NetworkPolicy
}

type BlockWriteDatabase interface {
	Close() error
	Cancel() error
	Map() (base.BlockDataMap, error)
	SetMap(base.BlockDataMap) error
	SetStates(sts []base.State) error
	SetOperations(ops []util.Hash) error // NOTE operation hash, not operation fact hash
	SuffrageState() base.State
	NetworkPolicy() base.NetworkPolicy
	Write() error
	TempDatabase() (TempDatabase, error)
}

// PermanentDatabase stores block data permanently.
type PermanentDatabase interface {
	PartialDatabase
	Close() error
	LastMap() (base.BlockDataMap, bool, error)
	LastSuffrage() (base.State, bool, error)
	Suffrage(blockheight base.Height) (base.State, bool, error)
	SuffrageByHeight(suffrageHeight base.Height) (base.State, bool, error)
	Map(base.Height) (base.BlockDataMap, bool, error)
	LastNetworkPolicy() base.NetworkPolicy
	MergeTempDatabase(context.Context, TempDatabase) error
}

type TempPoolDatabase interface {
	Proposal(util.Hash) (base.ProposalSignedFact, bool, error)
	ProposalByPoint(base.Point, base.Address) (base.ProposalSignedFact, bool, error)
	SetProposal(pr base.ProposalSignedFact) (bool, error)
}

type NewOperationPool interface {
	NewOperation(_ context.Context, facthash util.Hash) (base.Operation, bool, error)
	NewOperationHashes(
		_ context.Context,
		limit uint64,
		filter func(facthash util.Hash) (ok bool, err error),
	) ([]util.Hash, error)
	SetNewOperation(context.Context, base.Operation) (bool, error)
	RemoveNewOperations(context.Context, []util.Hash) error
}
