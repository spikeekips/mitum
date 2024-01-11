package isaac

import (
	"context"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

// Database serves some of block items like blockmap, states and operations from
// TempDatabases and PermanentDatabase. It has several TempDatabases and one
// PermanentDatabase.
type Database interface { //nolint:interfacebloat //..
	util.Daemon
	Close() error
	BlockMap(height base.Height) (base.BlockMap, bool, error)
	BlockMapBytes(base.Height) (string, []byte, []byte, bool, error)
	LastBlockMap() (base.BlockMap, bool, error)
	LastBlockMapBytes() (string, []byte, []byte, bool, error)
	LastSuffrageProof() (base.SuffrageProof, bool, error)
	LastSuffrageProofBytes() (string, []byte, []byte, bool, base.Height, error)
	SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error)
	SuffrageProofBytes(suffrageHeight base.Height) (string, []byte, []byte, bool, error)
	SuffrageProofByBlockHeight(blockheight base.Height) (base.SuffrageProof, bool, error)
	LastNetworkPolicy() base.NetworkPolicy
	State(key string) (base.State, bool, error)
	StateBytes(key string) (string, []byte, []byte, bool, error)
	// NOTE ExistsInStateOperation has only operation facts, which is in state
	ExistsInStateOperation(operationFactHash util.Hash) (bool, error)
	// NOTE ExistsKnownOperation has the known operation hashes
	ExistsKnownOperation(operationHash util.Hash) (bool, error)
	NewBlockWriteDatabase(height base.Height) (BlockWriteDatabase, error)
	MergeBlockWriteDatabase(BlockWriteDatabase) error
	MergeAllPermanent() error
	RemoveBlocks(base.Height) (bool, error)
}

type BaseDatabase interface {
	State(key string) (base.State, bool, error)
	StateBytes(key string) (string, []byte, []byte, bool, error)
	ExistsInStateOperation(operationFactHash util.Hash) (bool, error)
	ExistsKnownOperation(operationHash util.Hash) (bool, error)
}

// TempDatabase is the temporary database; it contains only blockmap and
// others of one block for storing some of block items fast.
type TempDatabase interface {
	BaseDatabase
	Height() base.Height
	Close() error
	Remove() error
	Merge() error
	LastBlockMap() (base.BlockMap, bool, error)
	BlockMapBytes() (string, []byte, []byte, error)
	SuffrageHeight() base.Height
	SuffrageProof() (base.SuffrageProof, bool, error)
	LastSuffrageProofBytes() (string, []byte, []byte, bool, error)
	NetworkPolicy() base.NetworkPolicy
}

type BlockWriteDatabase interface { //nolint:interfacebloat //..
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

// PermanentDatabase stores some of block items permanently.
type PermanentDatabase interface { //nolint:interfacebloat //..
	BaseDatabase
	Close() error
	Clean() error
	LastBlockMap() (base.BlockMap, bool, error)
	LastBlockMapBytes() (string, []byte, []byte, bool, error)
	LastSuffrageProof() (base.SuffrageProof, bool, error)
	LastSuffrageProofBytes() (string, []byte, []byte, bool, error)
	SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error)
	SuffrageProofBytes(suffrageHeight base.Height) (string, []byte, []byte, bool, error)
	SuffrageProofByBlockHeight(blockheight base.Height) (base.SuffrageProof, bool, error)
	BlockMap(base.Height) (base.BlockMap, bool, error)
	BlockMapBytes(base.Height) (string, []byte, []byte, bool, error)
	LastNetworkPolicy() base.NetworkPolicy
	MergeTempDatabase(context.Context, TempDatabase) error
}

type ProposalPool interface {
	Proposal(util.Hash) (base.ProposalSignFact, bool, error)
	ProposalBytes(util.Hash) (string, []byte, []byte, bool, error)
	ProposalByPoint(base.Point, base.Address, util.Hash) (base.ProposalSignFact, bool, error)
	SetProposal(pr base.ProposalSignFact) (bool, error)
}

type PoolOperationRecordMeta interface {
	Version() [2]byte
	AddedAt() time.Time
	Hint() hint.Hint
	Operation() util.Hash
	Fact() util.Hash
}

type NewOperationPool interface {
	Operation(_ context.Context, operationhash util.Hash) (base.Operation, bool, error)
	OperationBytes(_ context.Context, operationhash util.Hash) (string, []byte, []byte, bool, error)
	OperationHashes(
		_ context.Context,
		_ base.Height,
		limit uint64,
		filter func(PoolOperationRecordMeta) (ok bool, err error),
	) ([][2]util.Hash, error)
	SetOperation(context.Context, base.Operation) (bool, error)
}

type TempSyncPool interface {
	BlockMap(base.Height) (base.BlockMap, bool, error)
	SetBlockMap(base.BlockMap) error
	Cancel() error
	Close() error
}

type SuffrageExpelPool interface {
	SuffrageExpelOperation(base.Height, base.Address) (base.SuffrageExpelOperation, bool, error)
	SetSuffrageExpelOperation(base.SuffrageExpelOperation) error
	TraverseSuffrageExpelOperations(
		context.Context,
		base.Height,
		SuffrageVoteFunc,
	) error
	RemoveSuffrageExpelOperationsByFact([]base.SuffrageExpelFact) error
	RemoveSuffrageExpelOperationsByHeight(base.Height) error
}

// BallotPool stores latest ballots of local
type BallotPool interface {
	Ballot(_ base.Point, _ base.Stage, isSuffrageConfirm bool) (base.Ballot, bool, error)
	SetBallot(base.Ballot) (bool, error)
}

type StateCacheSetter interface {
	SetStateCache(util.GCache[string, [2]interface{}])
}
