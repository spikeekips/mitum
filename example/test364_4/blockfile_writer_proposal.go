package main

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/encoder"
)

type blockFileWriterProposalFunc func(_ context.Context, workID string, _ base.ProposalSignFact) (BlockMapItem, error)

func newFuncBlockFileProposalWriter(
	enc encoder.Encoder,
	createWriter blockFileWriterCreateWriterFunc,
) blockFileWriterProposalFunc {
	w := newFuncBaseBlockFileChunkWriter(base.BlockItemProposal, createWriter)

	return func(_ context.Context, workID string, pr base.ProposalSignFact) (BlockMapItem, error) {
		var c blockFileChunkWriterBaseNewWriteFunc

		switch i, err := w(workID); {
		case err != nil:
			return nil, err
		default:
			c = i
		}

		switch b, err := c(newItemsHeader(enc.Hint()), func(write ioWriteFunc) error {
			switch b, err := enc.Marshal(pr); {
			case err != nil:
				return err
			default:
				_, err := write(b)

				return err
			}
		}); {
		case err != nil:
			return nil, err
		default:
			return newBaseBlockMapItem(base.BlockItemProposal, b.Checksum(), false), nil
		}
	}
}
