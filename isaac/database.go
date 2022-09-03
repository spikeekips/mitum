package isaac

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

// Database serves BlockMapItem like blockmap, states and operations from
// TempDatabases and PermanentDatabase. It has several TempDatabases and one
// PermanentDatabase.
type Database interface {
	util.Daemon
	Close() error
	BlockMap(height base.Height) (base.BlockMap, bool, error)
	BlockMapBytes(base.Height) (hint.Hint, []byte, []byte, bool, error)
	LastBlockMap() (base.BlockMap, bool, error)
	LastBlockMapBytes() (hint.Hint, []byte, []byte, bool, error)
	LastSuffrageProof() (base.SuffrageProof, bool, error)
	LastSuffrageProofBytes() (hint.Hint, []byte, []byte, bool, error)
	SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error)
	SuffrageProofBytes(suffrageHeight base.Height) (hint.Hint, []byte, []byte, bool, error)
	SuffrageProofByBlockHeight(blockheight base.Height) (base.SuffrageProof, bool, error)
	LastNetworkPolicy() base.NetworkPolicy
	State(key string) (base.State, bool, error)
	StateBytes(key string) (hint.Hint, []byte, []byte, bool, error)
	// ExistsInStateOperation has only operation facts, which is in state
	ExistsInStateOperation(operationFactHash util.Hash) (bool, error)
	// NOTE ExistsKnownOperation has the known operation hashes
	ExistsKnownOperation(operationHash util.Hash) (bool, error)
	NewBlockWriteDatabase(height base.Height) (BlockWriteDatabase, error)
	MergeBlockWriteDatabase(BlockWriteDatabase) error
	MergeAllPermanent() error
}

type BaseDatabase interface {
	State(key string) (base.State, bool, error)
	StateBytes(key string) (hint.Hint, []byte, []byte, bool, error)
	ExistsInStateOperation(operationFactHash util.Hash) (bool, error)
	ExistsKnownOperation(operationHash util.Hash) (bool, error)
}

// TempDatabase is the temporary database; it contains only blockmap and
// others of one block for storing BlockMapItem fast.
type TempDatabase interface {
	BaseDatabase
	Height() base.Height
	Close() error
	Remove() error
	BlockMap() (base.BlockMap, error)
	BlockMapBytes() (hint.Hint, []byte, []byte, error)
	SuffrageHeight() base.Height
	SuffrageProof() (base.SuffrageProof, bool, error)
	SuffrageProofBytes() (hint.Hint, []byte, []byte, bool, error)
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
	BaseDatabase
	Close() error
	Clean() error
	LastBlockMap() (base.BlockMap, bool, error)
	LastBlockMapBytes() (hint.Hint, []byte, []byte, bool, error)
	LastSuffrageProof() (base.SuffrageProof, bool, error)
	LastSuffrageProofBytes() (hint.Hint, []byte, []byte, bool, error)
	SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error)
	SuffrageProofBytes(suffrageHeight base.Height) (hint.Hint, []byte, []byte, bool, error)
	SuffrageProofByBlockHeight(blockheight base.Height) (base.SuffrageProof, bool, error)
	BlockMap(base.Height) (base.BlockMap, bool, error)
	BlockMapBytes(base.Height) (hint.Hint, []byte, []byte, bool, error)
	LastNetworkPolicy() base.NetworkPolicy
	MergeTempDatabase(context.Context, TempDatabase) error
}

type ProposalPool interface {
	Proposal(util.Hash) (base.ProposalSignedFact, bool, error)
	ProposalBytes(util.Hash) (hint.Hint, []byte, []byte, bool, error)
	ProposalByPoint(base.Point, base.Address) (base.ProposalSignedFact, bool, error)
	SetProposal(pr base.ProposalSignedFact) (bool, error)
}

type PoolOperationHeader interface {
	Version() byte
	AddedAt() []byte
	HintBytes() []byte
}

type NewOperationPool interface {
	NewOperation(_ context.Context, operationhash util.Hash) (base.Operation, bool, error)
	NewOperationBytes(_ context.Context, operationhash util.Hash) (hint.Hint, []byte, []byte, bool, error)
	NewOperationHashes(
		_ context.Context,
		_ base.Height,
		limit uint64,
		filter func(operationhash, facthash util.Hash, _ PoolOperationHeader) (ok bool, err error),
	) ([]util.Hash, error)
	SetNewOperation(context.Context, base.Operation) (bool, error)
}

type TempSyncPool interface {
	BlockMap(base.Height) (base.BlockMap, bool, error)
	SetBlockMap(base.BlockMap) error
	Cancel() error
	Close() error
}
