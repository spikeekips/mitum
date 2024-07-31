package main

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/encoder"
)

type blockItemWriterNewWriteProposalFunc func(base.ProposalSignFact) (base.BlockMapItem, error)

func newBlockItemProposalWriterNewWriteFunc(
	enc encoder.Encoder,
	createWriter blockItemWriterCreateWriterFunc,
) func(string) (blockItemWriterNewWriteProposalFunc, error) {
	w := newBaseBlockItemWriterFunc(base.BlockItemProposal, createWriter)

	return func(createID string) (blockItemWriterNewWriteProposalFunc, error) {
		var c blockItemWriterBaseNewWriteFunc

		switch i, err := w(createID); {
		case err != nil:
			return nil, err
		default:
			c = i
		}

		return func(pr base.ProposalSignFact) (base.BlockMapItem, error) {
				return c(newItemsHeader(enc.Hint()), func(write ioWriteFunc) error {
					switch b, err := enc.Marshal(pr); {
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
