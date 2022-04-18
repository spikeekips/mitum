package isaac

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type BlockDataWriter interface {
	SetOperationsSize(uint64)
	SetProcessResult(
		_ context.Context,
		index int,
		facthash util.Hash,
		instate bool,
		errorreason base.OperationProcessReasonError,
	) error
	SetStates(_ context.Context, index int, values []base.StateMergeValue, operation base.Operation) error
	Manifest(_ context.Context, previous base.Manifest) (base.Manifest, error)
	SetINITVoteproof(context.Context, base.INITVoteproof) error
	SetACCEPTVoteproof(context.Context, base.ACCEPTVoteproof) error
	Save(context.Context) error
	Cancel() error
}

type BlockDataReader interface {
	Map() (base.BlockDataMap, bool, error)
	Reader(base.BlockDataType) (util.ChecksumReader, bool, error)
	Item(base.BlockDataType) (interface{}, bool, error)
}
