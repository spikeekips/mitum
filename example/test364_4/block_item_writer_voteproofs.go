package main

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/encoder"
)

type blockItemWriterNewWriteVoteproofsFunc func(base.INITVoteproof, base.ACCEPTVoteproof) (base.BlockMapItem, error)

func newBlockItemVoteproofsWriterFunc(
	enc encoder.Encoder,
	createWriter blockItemWriterCreateWriterFunc,
) func(string) (
	blockItemWriterNewWriteVoteproofsFunc,
	error,
) {
	w := newBaseBlockItemWriterFunc(base.BlockItemVoteproofs, createWriter)

	return func(createID string) (blockItemWriterNewWriteVoteproofsFunc, error) {
		var c blockItemWriterBaseNewWriteFunc

		switch i, err := w(createID); {
		case err != nil:
			return nil, err
		default:
			c = i
		}

		return func(ivp base.INITVoteproof, avp base.ACCEPTVoteproof) (base.BlockMapItem, error) {
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
			},
			nil
	}
}
