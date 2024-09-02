package main

import (
	"bytes"
	"context"
	"strconv"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type blockMapItemWriterNewWriteTreeFunc func(_ context.Context, createID string, _ fixedtree.Tree) (base.BlockMapItem, error)

func newFuncBlockMapItemTreeWriter(
	it base.BlockItemType,
	enc encoder.Encoder,
	createWriter blockMapItemWriterCreateWriterFunc,
) func(context.Context, string, fixedtree.Tree) (base.BlockMapItem, error) {
	encode := encodeWithTreeIndex[fixedtree.Node]

	return func(ctx context.Context, createID string, tr fixedtree.Tree) (base.BlockMapItem, error) {
		if tr.Len() < 1 {
			return nil, nil
		}

		var c blockMapItemWriterBaseNewWriteFunc

		switch i, err := newFuncBaseBlockMapItemWriter(it, createWriter)(createID); {
		case err != nil:
			return nil, err
		default:
			c = i
		}

		return c(newTreeItemsHeader(enc.Hint(), uint64(tr.Len()), tr.Hint()), func(write ioWriteFunc) error {
			var jw JobWorker

			switch i, err := NewBaseJobWorker(ctx, int64(tr.Len())); {
			case err != nil:
				return err
			default:
				defer i.Close()

				jw = i
			}

			if err := tr.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
				return true, jw.NewJob(func(context.Context) error {
					switch b, err := encode(enc, index, n); {
					case err != nil:
						return err
					default:
						_, err := write(b)

						return err
					}
				})
			}); err != nil {
				return err
			}

			jw.JobDone()

			return jw.Wait()
		})
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
