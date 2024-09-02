package main

import (
	"bytes"
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type blockMapItemWriterNewWriteStatesFunc func(_ context.Context, createID string, total uint64, _ func(workerSaveStateFunc) error) (blockMapItemChunk, error)

func newFuncBlockMapItemStatesWriter(
	enc encoder.Encoder,
	createWriter blockMapItemWriterCreateWriterFunc,
) blockMapItemWriterNewWriteStatesFunc {
	w := newFuncBaseBlockMapItemChunkWriter(base.BlockItemStates, createWriter)

	return func(ctx context.Context, createID string, total uint64, f func(workerSaveStateFunc) error) (blockMapItemChunk, error) {
		var c blockMapItemChunkWriterBaseNewWriteFunc

		switch i, err := w(createID); {
		case err != nil:
			return nil, err
		default:
			c = i
		}

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
