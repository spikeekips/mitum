package isaac

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/tree"
)

type BlockDataWriter interface {
	SetProposal(context.Context, base.ProposalSignedFact) error
	SetOperationsSize(uint64)
	SetProcessResult(
		_ context.Context,
		index int,
		facthash util.Hash,
		instate bool,
		errorreason base.OperationProcessReasonError,
	) error
	SetStates(_ context.Context, index int, states []base.State, operation base.Operation) error
	Manifest(_ context.Context, previous base.Manifest) (base.Manifest, error)
	SetINITVoteproof(context.Context, base.INITVoteproof) error
	SetACCEPTVoteproof(context.Context, base.ACCEPTVoteproof) error
	Save(context.Context) error
	Cancel() error
}

type BlockDataFSWriter interface {
	SetProposal(context.Context, base.ProposalSignedFact) error
	SetOperation(context.Context, int, base.Operation) error
	SetOperationsTree(context.Context, tree.FixedTree) error
	SetState(context.Context, int, base.State) error
	SetStatesTree(context.Context, tree.FixedTree) error
	SetManifest(context.Context, base.Manifest) error
	SetINITVoteproof(context.Context, base.INITVoteproof) error
	SetACCEPTVoteproof(context.Context, base.ACCEPTVoteproof) error
	Save(context.Context) (base.BlockDataMap, error)
	Cancel() error
}

type BlockDataReader interface {
	Map() (base.BlockDataMap, bool, error)
	Reader(base.BlockDataType) (util.ChecksumReader, bool, error)
	Item(base.BlockDataType) (interface{}, bool, error)
}

type NewBlockDataReaderFunc func(base.Height, encoder.Encoder) (BlockDataReader, error)

type BlockDataReaders struct {
	*hint.CompatibleSet // NOTE handles NewBlockDataReaderFunc
}

func NewBlockDataReaders() *BlockDataReaders {
	return &BlockDataReaders{
		CompatibleSet: hint.NewCompatibleSet(),
	}
}

func (rs *BlockDataReaders) Add(ht hint.Hint, v interface{}) error {
	r, ok := v.(NewBlockDataReaderFunc)
	if !ok {
		f, ok := v.(func(base.Height, encoder.Encoder) (BlockDataReader, error))
		if !ok {
			return errors.Errorf("not valid NewBlockDataReaderFunc")
		}

		r = NewBlockDataReaderFunc(f)
	}

	return rs.CompatibleSet.Add(ht, r)
}

func (rs *BlockDataReaders) Find(writerhint hint.Hint) NewBlockDataReaderFunc {
	i := rs.CompatibleSet.Find(writerhint)
	if i == nil {
		return nil
	}

	r, ok := i.(NewBlockDataReaderFunc)
	if !ok {
		return nil
	}

	return r
}

func LoadBlockDataReader(
	readers *BlockDataReaders,
	encs *encoder.Encoders,
	writerhint, enchint hint.Hint,
	height base.Height,
) (BlockDataReader, error) {
	e := util.StringErrorFunc("failed to load BlockDataReader")

	f := readers.Find(writerhint)
	if f == nil {
		return nil, e(nil, "unknown writer hint, %q", writerhint)
	}

	enc := encs.Find(enchint)
	if enc == nil {
		return nil, e(nil, "unknown encoder hint, %q", enchint)
	}

	return f(height, enc)
}
