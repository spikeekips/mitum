package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/encoder"
)

type blockFileWriterMapFunc func(_ context.Context, workID string, _ BlockMap) error

func newFuncBlockFileMapWriter(
	enc encoder.Encoder,
	createWriter blockFileWriterCreateWriterFunc,
) blockFileWriterMapFunc {
	return func(_ context.Context, workID string, bm BlockMap) error {
		if len(workID) < 1 {
			return errors.Errorf("empty create id")
		}

		var write ioWriteFunc
		var done blockFileDoneFileFunc

		switch _, w, d, err := createWriter(base.BlockItemMap, workID); {
		case err != nil:
			return err
		default:
			write = w
			done = d
		}

		if err := writeBlockFileFileHeader(write, newItemsHeader(enc.Hint())); err != nil {
			return err
		}

		switch b, err := enc.Marshal(bm); {
		case err != nil:
			_, err = done(err, "")

			return err
		default:
			if _, err := write(b); err != nil {
				_, err = done(err, "")

				return err
			}

			_, err := done(nil, blockFileFilenameBody(base.BlockItemMap, ""))

			return err
		}
	}
}
