package isaac

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type BlockdataWriter interface {
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
	Save(context.Context) (base.BlockdataMap, error)
	Cancel() error
}

type BlockdataReader interface {
	Map() (base.BlockdataMap, bool, error)
	Reader(base.BlockdataType) (util.ChecksumReader, bool, error)
	Item(base.BlockdataType) (interface{}, bool, error)
}
