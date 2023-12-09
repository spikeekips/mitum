package launch

import (
	"context"
	"io"
	"net/url"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

func ImportBlocks(
	fromreaders *isaac.BlockItemReaders,
	fromremotes isaac.RemotesBlockItemReadFunc,
	toreaders *isaac.BlockItemReaders,
	fromHeight, toHeight base.Height,
	encs *encoder.Encoders,
	db isaac.Database,
	params *isaac.Params,
) error {
	if err := isaacblock.ImportBlocks(
		context.Background(),
		fromHeight, toHeight,
		333, //nolint:gomnd //...
		toreaders,
		func(ctx context.Context, height base.Height) (base.BlockMap, bool, error) {
			var uri url.URL
			var compressFormat string

			switch itemfile, found, err := fromreaders.ItemFile(height, base.BlockItemMap); {
			case err != nil, !found:
				return nil, found, err
			default:
				uri = itemfile.URI()
				compressFormat = itemfile.CompressFormat()
			}

			if isaac.IsInLocalBlockItemFile(uri) {
				return isaac.BlockItemReadersDecode[base.BlockMap](fromreaders, height, base.BlockItemMap, nil)
			}

			var bm base.BlockMap

			switch known, found, err := fromremotes(ctx, uri, compressFormat,
				func(r io.Reader, compressFormat string) error {
					i, err := isaac.BlockItemReadersDecodeFromReader[base.BlockMap](
						fromreaders,
						base.BlockItemMap,
						r,
						compressFormat,
						nil,
					)

					bm = i

					return err
				},
			); {
			case err != nil:
				return nil, false, err
			case !known:
				return nil, false, errors.Errorf("unknown remote, %v", uri)
			case !found:
				return nil, false, nil
			default:
				return bm, true, nil
			}
		},
		func(
			ctx context.Context, height base.Height, item base.BlockItemType,
			f func(r io.Reader, found bool, compressFormat string) error,
		) error {
			var uri url.URL
			var compressFormat string

			switch itemfile, found, err := fromreaders.Item(height, item, func(ir isaac.BlockItemReader) error {
				return f(ir.Reader(), true, ir.Reader().Format)
			}); {
			case err != nil:
				return err
			case !found:
				if itemfile == nil {
					return f(nil, false, "")
				}

				uri = itemfile.URI()
				compressFormat = itemfile.CompressFormat()
			default:
				return nil
			}

			switch known, found, err := fromremotes(ctx, uri, compressFormat,
				func(r io.Reader, compressFormat string) error {
					return f(r, true, compressFormat)
				},
			); {
			case err != nil:
				return err
			case !known:
				return errors.Errorf("unknown remote, %v", uri)
			case !found:
				return f(nil, false, "")
			default:
				return nil
			}
		},
		func(m base.BlockMap) (isaac.BlockImporter, error) {
			bwdb, err := db.NewBlockWriteDatabase(m.Manifest().Height())
			if err != nil {
				return nil, err
			}

			return isaacblock.NewBlockImporter(
				toreaders.Root(),
				encs,
				m,
				bwdb,
				func(context.Context) error {
					return db.MergeBlockWriteDatabase(bwdb)
				},
				params.NetworkID(),
			)
		},
		nil,
		func(context.Context) error {
			return db.MergeAllPermanent()
		},
	); err != nil {
		return errors.WithMessagef(err, "import blocks")
	}

	return nil
}

func NewBlockWriterFunc(
	local base.LocalNode,
	networkID base.NetworkID,
	dataroot string,
	jsonenc, enc encoder.Encoder,
	db isaac.Database,
	workersize int64,
) isaac.NewBlockWriterFunc {
	return func(proposal base.ProposalSignFact, getStateFunc base.GetStateFunc) (isaac.BlockWriter, error) {
		e := util.StringError("create BlockWriter")

		dbw, err := db.NewBlockWriteDatabase(proposal.Point().Height())
		if err != nil {
			return nil, e.Wrap(err)
		}

		fswriter, err := isaacblock.NewLocalFSWriter(
			dataroot,
			proposal.Point().Height(),
			jsonenc, enc,
			local,
			networkID,
		)
		if err != nil {
			return nil, e.Wrap(err)
		}

		return isaacblock.NewWriter(
			proposal,
			getStateFunc,
			dbw,
			db.MergeBlockWriteDatabase,
			fswriter,
			workersize,
		), nil
	}
}
