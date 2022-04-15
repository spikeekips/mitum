package blockdata

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type NewBlockDataReaderFunc func(base.Height, encoder.Encoder) (isaac.BlockDataReader, error)

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
		f, ok := v.(func(base.Height, encoder.Encoder) (isaac.BlockDataReader, error))
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
) (isaac.BlockDataReader, error) {
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
