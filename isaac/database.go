package isaac

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

// Database serves block map item like blockmap, states and operations from
// TempDatabases and PermanentDatabase. It has several TempDatabases and one
// PermanentDatabase.
type Database interface {
	util.Daemon
	Close() error
	Map(height base.Height) (base.BlockMap, bool, error)
	LastMap() (base.BlockMap, bool, error)
	Suffrage(blockheight base.Height) (base.State, bool, error) // BLOCK should returns SuffrageProof
	SuffrageByHeight(suffrageHeight base.Height) (base.State, bool, error)
	LastSuffrage() (base.State, bool, error)
	LastSuffrageProof() (base.SuffrageProof, bool, error)
	SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error)
	LastNetworkPolicy() base.NetworkPolicy
	State(key string) (base.State, bool, error)
	// ExistsInStateOperation has only operation facts, which is in state
	ExistsInStateOperation(operationFactHash util.Hash) (bool, error)
	// NOTE ExistsKnownOperation has the known operation hashes
	ExistsKnownOperation(operationHash util.Hash) (bool, error)
	NewBlockWriteDatabase(height base.Height) (BlockWriteDatabase, error)
	MergeBlockWriteDatabase(BlockWriteDatabase) error
	MergeAllPermanent() error
}

type PartialDatabase interface {
	State(key string) (base.State, bool, error)
	ExistsInStateOperation(operationFactHash util.Hash) (bool, error)
	ExistsKnownOperation(operationHash util.Hash) (bool, error)
}

// TempDatabase is the temporary database; it contains only blockmap and
// others of one block for storing block map item fast.
type TempDatabase interface {
	PartialDatabase
	Close() error
	Remove() error
	Height() base.Height
	Map() (base.BlockMap, error)
	SuffrageHeight() base.Height
	Suffrage() (base.State, bool, error)
	SuffrageProof() (base.SuffrageProof, bool, error)
	NetworkPolicy() base.NetworkPolicy
}

type BlockWriteDatabase interface {
	Close() error
	Cancel() error
	Map() (base.BlockMap, error)
	SetMap(base.BlockMap) error
	SetStates(sts []base.State) error
	SetOperations(ops []util.Hash) error // NOTE operation hash, not operation fact hash
	SetSuffrageProof(base.SuffrageProof) error
	SuffrageState() base.State
	NetworkPolicy() base.NetworkPolicy
	Write() error
	TempDatabase() (TempDatabase, error)
}

// PermanentDatabase stores block map item permanently.
type PermanentDatabase interface {
	PartialDatabase
	Close() error
	Clean() error
	LastMap() (base.BlockMap, bool, error)
	LastSuffrage() (base.State, bool, error)
	LastSuffrageProof() (base.SuffrageProof, bool, error)
	Suffrage(blockheight base.Height) (base.State, bool, error)
	SuffrageByHeight(suffrageHeight base.Height) (base.State, bool, error)
	SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error)
	Map(base.Height) (base.BlockMap, bool, error)
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

type VoteproofsPool interface {
	LastVoteproofs() (base.INITVoteproof, base.ACCEPTVoteproof, bool, error)
	SetLastVoteproofs(base.INITVoteproof, base.ACCEPTVoteproof) error
}

type TempSyncPool interface {
	Map(base.Height) (base.BlockMap, bool, error)
	SetMap(base.BlockMap) error
	Cancel() error
	Close() error
}
