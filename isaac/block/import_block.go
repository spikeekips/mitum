package isaacblock

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

func ImportBlocks(
	ctx context.Context,
	from, to base.Height,
	batchlimit int64,
	blockMapf ImportBlocksBlockMapFunc,
	blockMapItemf ImportBlocksBlockMapItemFunc,
	newBlockImporter func(base.BlockMap) (isaac.BlockImporter, error),
	setLastVoteproofsFunc func(isaac.BlockReader) error,
	mergeBlockWriterDatabasesf func(context.Context) error,
) error {
	e := util.StringError("import blocks; %d - %d", from, to)

	var lastim isaac.BlockImporter
	var ims []isaac.BlockImporter

	if err := util.BatchWork(
		ctx,
		(to - from + 1).Int64(),
		batchlimit,
		func(ctx context.Context, last uint64) error {
			if ims != nil {
				if err := saveImporters(ctx, ims, mergeBlockWriterDatabasesf); err != nil {
					return err
				}
			}

			switch r := (last + 1) % uint64(batchlimit); {
			case r == 0:
				ims = make([]isaac.BlockImporter, batchlimit)
			default:
				ims = make([]isaac.BlockImporter, r)
			}

			return nil
		},
		func(ctx context.Context, i, end uint64) error {
			height := from + base.Height(int64(i))

			m, found, err := blockMapf(ctx, height)
			switch {
			case err != nil:
				return err
			case !found:
				return util.ErrNotFound.Errorf("BlockMap not found")
			}

			im, err := newBlockImporter(m)
			if err != nil {
				return err
			}

			if err := importBlock(ctx, height, m, im, blockMapItemf); err != nil {
				return err
			}

			ims[(height-from).Int64()%batchlimit] = im

			if height == to {
				lastim = im
			}

			return nil
		},
	); err != nil {
		return e.Wrap(err)
	}

	if int64(len(ims)) < batchlimit {
		if err := saveImporters(ctx, ims, mergeBlockWriterDatabasesf); err != nil {
			return e.Wrap(err)
		}
	}

	switch reader, err := lastim.Reader(); {
	case err != nil:
		return e.Wrap(err)
	case setLastVoteproofsFunc != nil:
		if err := setLastVoteproofsFunc(reader); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func importBlock(
	ctx context.Context,
	height base.Height,
	m base.BlockMap,
	im isaac.BlockImporter,
	blockMapItemf ImportBlocksBlockMapItemFunc,
) error {
	e := util.StringError("import block, %d", height)

	var num int64
	m.Items(func(base.BlockMapItem) bool {
		num++

		return true
	})

	if num < 1 {
		return nil
	}

	worker, err := util.NewErrgroupWorker(ctx, num)
	if err != nil {
		return e.Wrap(err)
	}

	defer worker.Close()

	m.Items(func(item base.BlockMapItem) bool {
		if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
			return blockMapItemf(ctx, height, item.Type(), func(r io.Reader, found bool) error {
				switch {
				case !found:
					return e.Wrap(util.ErrNotFound.Errorf("blockMapItem not found"))
				default:
					return im.WriteItem(item.Type(), r)
				}
			})
		}); err != nil {
			return false
		}

		return true
	})

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func saveImporters(
	ctx context.Context,
	ims []isaac.BlockImporter,
	mergeBlockWriterDatabasesf func(context.Context) error,
) error {
	e := util.StringError("save importers")

	switch {
	case len(ims) < 1:
		return errors.Errorf("empty BlockImporters")
	case len(ims) < 2:
		deferred, err := ims[0].Save(ctx)
		if err != nil {
			_ = cancelImporters(ctx, ims)

			return e.Wrap(err)
		}

		if err := deferred(ctx); err != nil {
			_ = cancelImporters(ctx, ims)

			return e.Wrap(err)
		}
	default:
		deferreds := make([]func(context.Context) error, len(ims))

		n := int64(len(ims))

		if err := util.RunErrgroupWorker(ctx, n, n, func(ctx context.Context, i, _ uint64) error {
			deferred, err := ims[i].Save(ctx)
			if err != nil {
				return err
			}

			deferreds[i] = deferred

			return nil
		}); err != nil {
			_ = cancelImporters(ctx, ims)

			return e.Wrap(err)
		}

		for i := range deferreds {
			if err := deferreds[i](ctx); err != nil {
				_ = cancelImporters(ctx, ims)

				return e.Wrap(err)
			}
		}
	}

	if mergeBlockWriterDatabasesf != nil {
		if err := mergeBlockWriterDatabasesf(ctx); err != nil {
			_ = cancelImporters(ctx, ims)

			return e.Wrap(err)
		}
	}

	return nil
}

func cancelImporters(ctx context.Context, ims []isaac.BlockImporter) error {
	e := util.StringError("cancel importers")

	switch {
	case len(ims) < 1:
		return nil
	case len(ims) < 2:
		if err := ims[0].CancelImport(ctx); err != nil {
			return e.Wrap(err)
		}

		return nil
	}

	n := int64(len(ims))

	if err := util.RunErrgroupWorker(ctx, n, n, func(ctx context.Context, i, _ uint64) error {
		return ims[i].CancelImport(ctx)
	}); err != nil {
		return e.Wrap(err)
	}

	return nil
}
