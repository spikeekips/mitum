package main

import (
	"bytes"

	"github.com/spikeekips/mitum/base"
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
				return func(f func(saveWorkerMergerSaveStateFunc) error) (base.BlockMapItem, error) {
					return c(newTotalItemsHeader(enc.Hint(), total), func(write ioWriteFunc) error {
						return f(func(st base.State) error {
							if total < 1 {
								return util.ErrFound.Errorf("zero total, but tried to write")
							}

							switch b, err := encodeStateItem(enc, st); {
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

func encodeStateItem(enc encoder.Encoder, st base.State) ([]byte, error) {
	switch b, err := enc.Marshal(st); {
	case err != nil:
		return nil, err
	default:
		buf := bytes.NewBuffer(nil)
		defer buf.Reset()

		_, _ = buf.Write([]byte("# state_key="))
		_, _ = buf.Write([]byte(st.Key()))
		_, _ = buf.Write([]byte("\n"))
		_, _ = buf.Write(b)
		_, _ = buf.Write([]byte("\n"))

		return buf.Bytes(), nil
	}
}
