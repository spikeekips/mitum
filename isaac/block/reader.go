package isaacblock

import (
	"bufio"
	"context"
	"io"

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
	*hint.CompatibleSet[NewBlockReaderFunc]
}

func NewBlockReaders() *BlockReaders {
	return &BlockReaders{
		CompatibleSet: hint.NewCompatibleSet[NewBlockReaderFunc](8), //nolint:gomnd //...
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

func LoadBlockReader(
	readers *BlockReaders,
	encs *encoder.Encoders,
	writerhint, enchint hint.Hint,
	height base.Height,
) (isaac.BlockReader, error) {
	e := util.StringError("load BlockReader")

	f, found := readers.Find(writerhint)
	if !found {
		return nil, e.Errorf("unknown writer hint, %q", writerhint)
	}

	enc, found := encs.Find(enchint)
	if !found {
		return nil, e.Errorf("unknown encoder hint, %q", enchint)
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

	e := util.StringError("load tree")

	br := bufio.NewReader(f)

	ht, err := LoadTreeHint(br)
	if err != nil {
		return tr, e.Wrap(err)
	}

	nodes := make([]fixedtree.Node, item.Num())
	if tr, err = fixedtree.NewTree(ht, nodes); err != nil {
		return tr, e.Wrap(err)
	}

	if err := LoadRawItemsWithWorker(
		br,
		item.Num(),
		func(b []byte) (interface{}, error) {
			return unmarshalIndexedTreeNode(enc, b, ht)
		},
		func(_ uint64, v interface{}) error {
			in := v.(indexedTreeNode) //nolint:forcetypeassert //...
			n, err := callback(in.Node)
			if err != nil {
				return err
			}

			return tr.Set(in.Index, n)
		},
	); err != nil {
		return tr, e.Wrap(err)
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
			return errors.WithStack(err)
		}

		if len(b) > 0 {
			v, eerr := decode(b)
			if eerr != nil {
				return errors.WithStack(eerr)
			}

			if eerr := callback(index, v); eerr != nil {
				return eerr
			}

			index++
		}

		switch {
		case err == nil:
		case errors.Is(err, io.EOF):
			break end
		default:
			return errors.WithStack(err)
		}
	}

	return nil
}

func LoadRawItemsWithWorker(
	f io.Reader,
	num uint64,
	decode func([]byte) (interface{}, error),
	callback func(uint64, interface{}) error,
) error {
	worker, err := util.NewErrgroupWorker(context.Background(), int64(num))
	if err != nil {
		return err
	}

	defer worker.Close()

	if err := LoadRawItems(f, decode, func(index uint64, v interface{}) error {
		return worker.NewJob(func(ctx context.Context, _ uint64) error {
			return callback(index, v)
		})
	}); err != nil {
		return err
	}

	worker.Done()

	return worker.Wait()
}

func LoadTreeHint(br *bufio.Reader) (ht hint.Hint, _ error) {
end:
	for {
		s, err := br.ReadString('\n')

		switch {
		case err != nil:
			return ht, errors.WithStack(err)
		case len(s) < 1:
			continue end
		}

		ht, err = hint.ParseHint(s)
		if err != nil {
			return ht, errors.Wrap(err, "load tree hint")
		}

		if err := ht.IsValid(nil); err != nil {
			return ht, errors.Wrap(err, "load tree hint")
		}

		return ht, nil
	}
}
