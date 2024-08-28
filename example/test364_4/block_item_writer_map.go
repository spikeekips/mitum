package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/encoder"
)

type blockItemWriterNewWriteMapFunc func(_ context.Context, createID string, _ base.BlockMap) error

func newFuncBlockItemMapWriter(
	enc encoder.Encoder,
	createWriter blockItemWriterCreateWriterFunc,
) blockItemWriterNewWriteMapFunc {
	return func(ctx context.Context, createID string, bm base.BlockMap) error {
		if len(createID) < 1 {
			return errors.Errorf("empty create id")
		}

		var write ioWriteFunc
		var done func(error) error

		switch w, d, err := createWriter(base.BlockItemMap, createID); {
		case err != nil:
			return err
		default:
			write = w
			done = d
		}

		if err := writeBlockItemFileHeader(write, newItemsHeader(enc.Hint())); err != nil {
			return err
		}

		switch b, err := enc.Marshal(bm); {
		case err != nil:
			return done(err)
		default:
			if _, err := write(b); err != nil {
				return done(err)
			}

			return done(nil)
		}
	}
}
