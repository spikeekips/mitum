package main

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/encoder"
)

type blockFileWriterProposalFunc func(_ context.Context, createID string, _ base.ProposalSignFact) (blockFileChunk, error)

func newFuncBlockFileProposalWriterNewWrite(
	enc encoder.Encoder,
	createWriter blockFileWriterCreateWriterFunc,
) blockFileWriterProposalFunc {
	w := newFuncBaseBlockFileChunkWriter(base.BlockItemProposal, createWriter)

	return func(ctx context.Context, createID string, pr base.ProposalSignFact) (blockFileChunk, error) {
		var c blockFileChunkWriterBaseNewWriteFunc

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
