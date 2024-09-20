package main

import (
	"bytes"
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type blockFileWriterStatesFunc func(_ context.Context, workID string, total uint64, _ func(workerSaveStateFunc) error) ([]blockFileChunk, error)

func newFuncBlockFileStatesWriter(
	enc encoder.Encoder,
	createWriter blockFileWriterCreateWriterFunc,
	chunkItemsf,
	maxSemSizef func() uint64,
) blockFileWriterStatesFunc {
	w := newFuncBaseBlockFileChunkWriter(base.BlockItemStates, createWriter)

	if maxSemSizef == nil {
		maxSemSizef = func() uint64 { return 1 << 9 }
	}

	return func(ctx context.Context, workID string, total uint64, f func(workerSaveStateFunc) error) ([]blockFileChunk, error) {
		if chunkItemsf == nil {
			chunkItemsf = func() uint64 { return total }
		}

		chunkItems := chunkItemsf()
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

				return f(func(st base.State) error {
					if total < 1 {
						return util.ErrFound.Errorf("zero total, but tried to write")
					}

					return w(func(write ioWriteFunc) error {
						switch b, err := encodeStateItem(enc, st); {
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
