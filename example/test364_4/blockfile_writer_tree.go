package main

import (
	"bytes"
	"context"
	"strconv"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type blockFileWriterTreeFunc func(_ context.Context, createID string, _ fixedtree.Tree) ([]blockFileChunk, error)

func newFuncBlockFileTreeWriter(
	it base.BlockItemType,
	enc encoder.Encoder,
	createWriter blockFileWriterCreateWriterFunc,
	chunkItemsf,
	maxSemSizef func() uint64,
) func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
	encode := encodeWithTreeIndex[fixedtree.Node]
	w := newFuncBaseBlockFileChunkWriter(it, createWriter)

	if maxSemSizef == nil {
		maxSemSizef = func() uint64 { return 1 << 9 }
	}

	return func(ctx context.Context, createID string, tr fixedtree.Tree) ([]blockFileChunk, error) {
		total := uint64(tr.Len())

		if total < 1 {
			return nil, nil
		}

		var chunkItems uint64

		switch {
		case chunkItemsf == nil:
			chunkItems = total
		default:
			chunkItems = chunkItemsf()
		}

		maxSemSize := maxSemSizef()

		return newBlockFileMultiChunksWriter(
			ctx,
			chunkItems,
			maxSemSize,
			total,
			newTreeItemsHeader(enc.Hint(), total, tr.Hint()),
			func() (blockFileChunkWriterBaseNewWriteFunc, error) {
				return w(createID)
			},
			func(f func(func(ioWriteFunc) error) error) error {
				if f == nil {
					return nil
				}

				if total < 1 {
					return util.ErrFound.Errorf("zero total, but tried to write")
				}

				return tr.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
					return true, f(func(write ioWriteFunc) error {
						switch b, err := encode(enc, index, n); {
						case err != nil:
							return err
						default:
							_, err := write(b)

							return err
						}
					})
				})
			},
		)
	}
}

func encodeWithIndex[T any](enc encoder.Encoder, index uint64, indexName string, i T) ([]byte, error) {
	switch b, err := enc.Marshal(i); {
	case err != nil:
		return nil, err
	default:
		buf := bytes.NewBuffer(nil)
		defer buf.Reset()

		_, _ = buf.Write([]byte("# "))
		_, _ = buf.Write([]byte(indexName))
		_, _ = buf.Write([]byte("="))
		_, _ = buf.Write([]byte(strconv.FormatUint(index, 10)))
		_, _ = buf.Write([]byte("\n"))
		_, _ = buf.Write(b)
		_, _ = buf.Write([]byte("\n"))

		return buf.Bytes(), nil
	}
}

func encodeWithTreeIndex[T any](enc encoder.Encoder, index uint64, i T) ([]byte, error) {
	return encodeWithIndex(enc, index, "index", i)
}
