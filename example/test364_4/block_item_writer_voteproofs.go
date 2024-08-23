package main

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/encoder"
)

type blockItemWriterNewWriteVoteproofsFunc func(_ context.Context, createID string, _ base.INITVoteproof, _ base.ACCEPTVoteproof) (base.BlockMapItem, error)

func newBlockItemVoteproofsWriterFunc(
	enc encoder.Encoder,
	createWriter blockItemWriterCreateWriterFunc,
) blockItemWriterNewWriteVoteproofsFunc {
	w := newBaseBlockItemWriterFunc(base.BlockItemVoteproofs, createWriter)

	return func(ctx context.Context, createID string, ivp base.INITVoteproof, avp base.ACCEPTVoteproof) (base.BlockMapItem, error) {
		var c blockItemWriterBaseNewWriteFunc

		switch i, err := w(createID); {
		case err != nil:
			return nil, err
		default:
			c = i
		}

		return c(newItemsHeader(enc.Hint()), func(write ioWriteFunc) error {
			switch b, err := enc.Marshal(ivp); {
			case err != nil:
				return err
			default:
				if _, err := write(b); err != nil {
					return err
				}
			}

			switch b, err := enc.Marshal(avp); {
			case err != nil:
				return err
			default:
				_, err := write(b)

				return err
			}
		})
	}
}
