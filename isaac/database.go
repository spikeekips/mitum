package isaac

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

// Database serves BlockMapItem like blockmap, states and operations from
// TempDatabases and PermanentDatabase. It has several TempDatabases and one
// PermanentDatabase.
type Database interface {
	util.Daemon
	Close() error
	BlockMap(height base.Height) (base.BlockMap, bool, error)
	LastBlockMap() (base.BlockMap, bool, error)
	LastSuffrageProof() (base.SuffrageProof, bool, error)
	SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error)
	SuffrageProofByBlockHeight(blockheight base.Height) (base.SuffrageProof, bool, error)
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
// others of one block for storing BlockMapItem fast.
type TempDatabase interface {
	PartialDatabase
	Close() error
	Remove() error
	Height() base.Height
	BlockMap() (base.BlockMap, error)
	SuffrageHeight() base.Height
	SuffrageProof() (base.SuffrageProof, bool, error)
	NetworkPolicy() base.NetworkPolicy
}

type BlockWriteDatabase interface {
	Close() error
	Cancel() error
	BlockMap() (base.BlockMap, error)
	SetBlockMap(base.BlockMap) error
	SetStates(sts []base.State) error
	SetOperations(ops []util.Hash) error // NOTE operation hash, not operation fact hash
	SetSuffrageProof(base.SuffrageProof) error
	SuffrageState() base.State
	NetworkPolicy() base.NetworkPolicy
	Write() error
	TempDatabase() (TempDatabase, error)
}

// PermanentDatabase stores BlockMapItem permanently.
type PermanentDatabase interface {
	PartialDatabase
	Close() error
	Clean() error
	LastBlockMap() (base.BlockMap, bool, error)
	LastSuffrageProof() (base.SuffrageProof, bool, error)
	SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error)
	SuffrageProofByBlockHeight(blockheight base.Height) (base.SuffrageProof, bool, error)
	BlockMap(base.Height) (base.BlockMap, bool, error)
	LastNetworkPolicy() base.NetworkPolicy
	MergeTempDatabase(context.Context, TempDatabase) error
}

type ProposalPool interface {
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
	BlockMap(base.Height) (base.BlockMap, bool, error)
	SetBlockMap(base.BlockMap) error
	Cancel() error
	Close() error
}
