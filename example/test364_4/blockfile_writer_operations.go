package main

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type blockFileWriterOperationsFunc func(_ context.Context, workID string, total uint64, _ func(operationsWorkerSaveOperationFunc) error) ([]blockFileChunk, error)

func newFuncBlockFileOperationsWriter(
	enc encoder.Encoder,
	createWriter blockFileWriterCreateWriterFunc,
	chunkItemsf,
	maxSemSizef func() uint64,
) blockFileWriterOperationsFunc {
	w := newFuncBaseBlockFileChunkWriter(base.BlockItemOperations, createWriter)

	if maxSemSizef == nil {
		maxSemSizef = func() uint64 { return 1 << 9 }
	}

	return func(ctx context.Context, workID string, total uint64, f func(operationsWorkerSaveOperationFunc) error) ([]blockFileChunk, error) {
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
			newTotalItemsHeader(enc.Hint(), total),
			func() (blockFileChunkWriterBaseNewWriteFunc, error) {
				return w(workID)
			},
			func(w func(func(ioWriteFunc) error) error) error {
				if f == nil {
					return nil
				}

				return f(func(opindex uint64, op Operation) error {
					if total < 1 {
						return util.ErrFound.Errorf("zero total, but tried to write")
					}

					return w(func(write ioWriteFunc) error {
						switch b, err := encodeWithIndex(enc, opindex, "opindex", op); {
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
