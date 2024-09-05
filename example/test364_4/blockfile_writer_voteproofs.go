package main

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type blockFileWriterVoteproofsFunc func(_ context.Context, createID string) (
	func(base.INITVoteproof) error,
	func(base.ACCEPTVoteproof) (blockFileChunk, error),
	error,
)

func newFuncBlockFileVoteproofsWriter(
	enc encoder.Encoder,
	createWriter blockFileWriterCreateWriterFunc,
) blockFileWriterVoteproofsFunc {
	w := newFuncBaseBlockFileChunkWriter(base.BlockItemVoteproofs, createWriter)

	return func(ctx context.Context, createID string) (
		func(base.INITVoteproof) error,
		func(base.ACCEPTVoteproof) (blockFileChunk, error),
		error,
	) {
		var c blockFileChunkWriterBaseNewWriteFunc

		switch i, err := w(createID); {
		case err != nil:
			return nil, nil, err
		default:
			c = i
		}

		var ivpb []byte

		return func(ivp base.INITVoteproof) error {
				switch b, err := enc.Marshal(ivp); {
				case err != nil:
					return err
				default:
					ivpb = b

					return nil
				}
			},
			func(avp base.ACCEPTVoteproof) (blockFileChunk, error) {
				if len(ivpb) < 1 {
					return nil, util.ErrNotFound.Errorf("init voteproof")
				}

				return c(newItemsHeader(enc.Hint()), func(write ioWriteFunc) error {
					if _, err := write(ivpb); err != nil {
						return err
					}

					switch b, err := enc.Marshal(avp); {
					case err != nil:
						return err
					default:
						_, err := write(b)

						return err
					}
				})
			},
			nil
	}
}
