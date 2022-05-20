package isaac

import (
	"context"
	"io"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type BlockWriter interface {
	SetOperationsSize(uint64)
	SetProcessResult(
		_ context.Context,
		index uint64,
		ophash, facthash util.Hash,
		instate bool,
		errorreason base.OperationProcessReasonError,
	) error
	SetStates(_ context.Context, index uint64, values []base.StateMergeValue, operation base.Operation) error
	Manifest(_ context.Context, previous base.Manifest) (base.Manifest, error)
	SetINITVoteproof(context.Context, base.INITVoteproof) error
	SetACCEPTVoteproof(context.Context, base.ACCEPTVoteproof) error
	Save(context.Context) (base.BlockMap, error)
	Cancel() error
}

type BlockReader interface {
	Map() (base.BlockMap, bool, error)
	Reader(base.BlockMapItemType) (io.ReadCloser, bool, error)
	ChecksumReader(base.BlockMapItemType) (util.ChecksumReader, bool, error)
	Item(base.BlockMapItemType) (interface{}, bool, error)
}

type BlockImporter interface {
	WriteMap(base.BlockMap) error
	WriteItem(base.BlockMapItemType, io.Reader) error
	Save(context.Context) error
}
