package main

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/encoder"
)

type blockMapItemWriterNewWriteProposalFunc func(_ context.Context, createID string, _ base.ProposalSignFact) (base.BlockMapItem, error)

func newFuncBlockMapItemProposalWriterNewWrite(
	enc encoder.Encoder,
	createWriter blockMapItemWriterCreateWriterFunc,
) blockMapItemWriterNewWriteProposalFunc {
	w := newFuncBaseBlockMapItemWriter(base.BlockItemProposal, createWriter)

	return func(ctx context.Context, createID string, pr base.ProposalSignFact) (base.BlockMapItem, error) {
		var c blockMapItemWriterBaseNewWriteFunc

		switch i, err := w(createID); {
		case err != nil:
			return nil, err
		default:
			c = i
		}

		return c(newItemsHeader(enc.Hint()), func(write ioWriteFunc) error {
			switch b, err := enc.Marshal(pr); {
			case err != nil:
				return err
			default:
				_, err := write(b)

				return err
			}
		})
	}
}
