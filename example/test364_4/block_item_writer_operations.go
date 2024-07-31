package main

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type (
	blockItemWriterNewWriteOperationsCallbackFunc func(func(operationsWorkerSaveOperationFunc) error) (base.BlockMapItem, error)
	blockItemWriterNewWriteOperationsFunc         func(total uint64) blockItemWriterNewWriteOperationsCallbackFunc
)

func newBlockItemOperationsWriterFunc(
	enc encoder.Encoder,
	createWriter blockItemWriterCreateWriterFunc,
) func(string) (blockItemWriterNewWriteOperationsFunc, error) {
	w := newBaseBlockItemWriterFunc(base.BlockItemOperations, createWriter)

	return func(createID string) (blockItemWriterNewWriteOperationsFunc, error) {
		var c blockItemWriterBaseNewWriteFunc

		switch i, err := w(createID); {
		case err != nil:
			return nil, err
		default:
			c = i
		}

		return func(total uint64) blockItemWriterNewWriteOperationsCallbackFunc {
				return func(f func(operationsWorkerSaveOperationFunc) error) (base.BlockMapItem, error) {
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
			},
			nil
	}
}
