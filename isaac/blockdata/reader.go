package isaacblockdata

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type NewBlockdataReaderFunc func(base.Height, encoder.Encoder) (isaac.BlockdataReader, error)

type BlockdataReaders struct {
	*hint.CompatibleSet // NOTE handles NewBlockdataReaderFunc
}

func NewBlockdataReaders() *BlockdataReaders {
	return &BlockdataReaders{
		CompatibleSet: hint.NewCompatibleSet(),
	}
}

func (rs *BlockdataReaders) Add(ht hint.Hint, v interface{}) error {
	r, ok := v.(NewBlockdataReaderFunc)
	if !ok {
		f, ok := v.(func(base.Height, encoder.Encoder) (isaac.BlockdataReader, error))
		if !ok {
			return errors.Errorf("not valid NewBlockdataReaderFunc")
		}

		r = NewBlockdataReaderFunc(f)
	}

	return rs.CompatibleSet.Add(ht, r)
}

func (rs *BlockdataReaders) Find(writerhint hint.Hint) NewBlockdataReaderFunc {
	i := rs.CompatibleSet.Find(writerhint)
	if i == nil {
		return nil
	}

	r, ok := i.(NewBlockdataReaderFunc)
	if !ok {
		return nil
	}

	return r
}

func LoadBlockdataReader(
	readers *BlockdataReaders,
	encs *encoder.Encoders,
	writerhint, enchint hint.Hint,
	height base.Height,
) (isaac.BlockdataReader, error) {
	e := util.StringErrorFunc("failed to load BlockdataReader")

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
