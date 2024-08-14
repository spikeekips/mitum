package main

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/encoder"
)

type blockItemWriterNewWriteMapFunc func(base.BlockMap) error

func newBlockItemMapWriterFunc(
	enc encoder.Encoder,
	createWriter blockItemWriterCreateWriterFunc,
) func(string) (blockItemWriterNewWriteMapFunc, error) {
	return func(createID string) (blockItemWriterNewWriteMapFunc, error) {
		if len(createID) < 1 {
			return nil, errors.Errorf("empty create id")
		}

		var write ioWriteFunc
		done := func(err error) error { return err }

		once := OnceFunc(func() error {
			switch w, d, err := createWriter(base.BlockItemMap, createID); {
			case err != nil:
				return err
			default:
				write = w
				done = d

				return writeBlockItemFileHeader(write, newItemsHeader(enc.Hint()))
			}
		})

		return func(bm base.BlockMap) error {
				if err := once(); err != nil {
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
			},
			nil
	}
}
