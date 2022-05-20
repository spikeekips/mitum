package isaacblock

import (
	"bufio"
	"context"
	"io"
	"math"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
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

func LoadTree(
	enc encoder.Encoder,
	item base.BlockMapItem,
	f io.Reader,
	callback func(interface{}) (fixedtree.Node, error),
) (tr fixedtree.Tree, err error) {
	if item.Num() < 1 {
		return tr, nil
	}

	e := util.StringErrorFunc("failed to load tree")

	br := bufio.NewReader(f)

	ht, err := LoadTreeHint(br)
	if err != nil {
		return tr, e(err, "")
	}

	nodes := make([]fixedtree.Node, item.Num())
	if tr, err = fixedtree.NewTree(ht, nodes); err != nil {
		return tr, e(err, "")
	}

	if err := LoadRawItemsWithWorker(
		br,
		func(b []byte) (interface{}, error) {
			return unmarshalIndexedTreeNode(enc, b, ht)
		},
		func(_ uint64, v interface{}) error {
			in := v.(indexedTreeNode) //nolint:forcetypeassert //...
			n, err := callback(in.Node)
			if err != nil {
				return errors.Wrap(err, "")
			}

			if err := tr.Set(in.Index, n); err != nil {
				return errors.Wrap(err, "")
			}

			return nil
		},
	); err != nil {
		return tr, e(err, "")
	}

	return tr, nil
}

func LoadRawItems(
	f io.Reader,
	decode func([]byte) (interface{}, error),
	callback func(uint64, interface{}) error,
) error {
	var br *bufio.Reader
	if i, ok := f.(*bufio.Reader); ok {
		br = i
	} else {
		br = bufio.NewReader(f)
	}

	var index uint64
end:
	for {
		b, err := br.ReadBytes('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return errors.Wrap(err, "")
		}

		if len(b) > 0 {
			v, eerr := decode(b)
			if eerr != nil {
				return errors.Wrap(eerr, "")
			}

			if eerr := callback(index, v); eerr != nil {
				return errors.Wrap(eerr, "")
			}

			index++
		}

		switch {
		case err == nil:
		case errors.Is(err, io.EOF):
			break end
		default:
			return errors.Wrap(err, "")
		}
	}

	return nil
}

func LoadRawItemsWithWorker(
	f io.Reader,
	decode func([]byte) (interface{}, error),
	callback func(uint64, interface{}) error,
) error {
	worker := util.NewErrgroupWorker(context.Background(), math.MaxInt32)
	defer worker.Close()

	if err := LoadRawItems(f, decode, func(index uint64, v interface{}) error {
		return worker.NewJob(func(ctx context.Context, _ uint64) error {
			if err := callback(index, v); err != nil {
				return errors.Wrap(err, "")
			}

			return nil
		})
	}); err != nil {
		return errors.Wrap(err, "")
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func LoadTreeHint(br *bufio.Reader) (hint.Hint, error) {
end:
	for {
		s, err := br.ReadString('\n')

		switch {
		case err != nil:
			return hint.Hint{}, errors.Wrap(err, "")
		case len(s) < 1:
			continue end
		}

		ht, err := hint.ParseHint(s)
		if err != nil {
			return hint.Hint{}, errors.Wrap(err, "failed to load tree hint")
		}

		return ht, nil
	}
}
