package main

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type (
	blockItemWriterNewWriteStatesCallbackFunc func(func(saveWorkerMergerSaveStateFunc) error) (base.BlockMapItem, error)
	blockItemWriterNewWriteStatesFunc         func(total uint64) blockItemWriterNewWriteStatesCallbackFunc
)

func newBlockItemStatesWriterNewWriteFunc(
	enc encoder.Encoder,
	createWriter blockItemWriterCreateWriterFunc,
) func(string) (blockItemWriterNewWriteStatesFunc, error) {
	w := newBaseBlockItemWriterNewWriteFunc(base.BlockItemStates, createWriter)

	return func(createID string) (blockItemWriterNewWriteStatesFunc, error) {
		var c blockItemWriterBaseNewWriteFunc

		switch i, err := w(createID); {
		case err != nil:
			return nil, err
		default:
			c = i
		}

		return func(total uint64) blockItemWriterNewWriteStatesCallbackFunc {
				header := totalItemsHeader{
					BlockItemFileBaseItemsHeader: isaac.BlockItemFileBaseItemsHeader{Writer: LocalFSWriterHint, Encoder: enc.Hint()},
					Total:                        total,
				}

				return func(f func(saveWorkerMergerSaveStateFunc) error) (base.BlockMapItem, error) {
					return c(header, func(write ioWriteFunc) error {
						encw := safeStreamerEncode(enc.StreamEncoder(writeFunc(write)))

						return f(func(st base.State) error {
							if total < 1 {
								return util.ErrFound.Errorf("zero total, but tried to write")
							}

							return encw(st)
						})
					})
				}
			},
			nil
	}
}
