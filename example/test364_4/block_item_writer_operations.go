package main

import (
	"bytes"
	"strconv"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type (
	blockItemWriterNewWriteOperationsCallbackFunc func(func(operationsWorkerSaveOperationFunc) error) (base.BlockMapItem, error)
	blockItemWriterNewWriteOperationsFunc         func(total uint64) blockItemWriterNewWriteOperationsCallbackFunc
)

func newBlockItemOperationsWriterNewWriteFunc(
	enc encoder.Encoder,
	createWriter blockItemWriterCreateWriterFunc,
) func(string) (blockItemWriterNewWriteOperationsFunc, error) {
	w := newBaseBlockItemWriterNewWriteFunc(base.BlockItemOperations, createWriter)

	return func(createID string) (blockItemWriterNewWriteOperationsFunc, error) {
		var c blockItemWriterBaseNewWriteFunc

		switch i, err := w(createID); {
		case err != nil:
			return nil, err
		default:
			c = i
		}

		encode := func(w ioWriteFunc, opindex uint64, op Operation) error {
			switch b, err := enc.Marshal(op); {
			case err != nil:
				return err
			default:
				buf := bytes.NewBuffer(nil)
				defer buf.Reset()

				_, _ = buf.Write([]byte("# opindex="))
				_, _ = buf.Write([]byte(strconv.FormatUint(opindex, 10)))
				_, _ = buf.Write([]byte("\n"))
				_, _ = buf.Write(b)
				_, _ = buf.Write([]byte("\n"))

				if _, err := w(buf.Bytes()); err != nil {
					return errors.WithStack(err)
				}

				return nil
			}
		}

		return func(total uint64) blockItemWriterNewWriteOperationsCallbackFunc {
				header := totalItemsHeader{
					BlockItemFileBaseItemsHeader: isaac.BlockItemFileBaseItemsHeader{Writer: LocalFSWriterHint, Encoder: enc.Hint()},
					Total:                        total,
				}

				return func(f func(operationsWorkerSaveOperationFunc) error) (base.BlockMapItem, error) {
					return c(header, func(write ioWriteFunc) error {
						return f(func(opindex uint64, op Operation) error {
							if total < 1 {
								return util.ErrFound.Errorf("zero total, but tried to write")
							}

							return encode(write, opindex, op)
						})
					})
				}
			},
			nil
	}
}
