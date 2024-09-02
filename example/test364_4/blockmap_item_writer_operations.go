package main

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type blockMapItemWriterNewWriteOperationsFunc func(_ context.Context, createID string, total uint64, _ func(operationsWorkerSaveOperationFunc) error) (base.BlockMapItem, error)

func newFuncBlockMapItemOperationsWriter(
	enc encoder.Encoder,
	createWriter blockMapItemWriterCreateWriterFunc,
) blockMapItemWriterNewWriteOperationsFunc {
	w := newFuncBaseBlockMapItemWriter(base.BlockItemOperations, createWriter)

	return func(ctx context.Context, createID string, total uint64, f func(operationsWorkerSaveOperationFunc) error) (base.BlockMapItem, error) {
		var c blockMapItemWriterBaseNewWriteFunc

		switch i, err := w(createID); {
		case err != nil:
			return nil, err
		default:
			c = i
		}

		return c(newTotalItemsHeader(enc.Hint(), total), func(write ioWriteFunc) error {
			return f(func(opindex uint64, op Operation) error {
				if total < 1 {
					return util.ErrFound.Errorf("zero total, but tried to write")
				}

				switch b, err := encodeWithIndex(enc, opindex, "opindex", op); {
				case err != nil:
					return err
				default:
					_, err := write(b)

					return err
				}
			})
		})
	}
}
