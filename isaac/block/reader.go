package isaacblock

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type NewBlockReaderFunc func(base.Height, encoder.Encoder) (isaac.BlockReader, error)

type BlockReaders struct {
	*hint.CompatibleSet // NOTE handles NewBlockReaderFunc
}

func NewBlockReaders() *BlockReaders {
	return &BlockReaders{
		CompatibleSet: hint.NewCompatibleSet(),
	}
}

func (rs *BlockReaders) Add(ht hint.Hint, v interface{}) error {
	r, ok := v.(NewBlockReaderFunc)
	if !ok {
		f, ok := v.(func(base.Height, encoder.Encoder) (isaac.BlockReader, error))
		if !ok {
			return errors.Errorf("not valid NewBlockReaderFunc")
		}

		r = NewBlockReaderFunc(f)
	}

	return rs.CompatibleSet.Add(ht, r)
}

func (rs *BlockReaders) Find(writerhint hint.Hint) NewBlockReaderFunc {
	i := rs.CompatibleSet.Find(writerhint)
	if i == nil {
		return nil
	}

	r, ok := i.(NewBlockReaderFunc)
	if !ok {
		return nil
	}

	return r
}

func LoadBlockReader(
	readers *BlockReaders,
	encs *encoder.Encoders,
	writerhint, enchint hint.Hint,
	height base.Height,
) (isaac.BlockReader, error) {
	e := util.StringErrorFunc("failed to load BlockReader")

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
