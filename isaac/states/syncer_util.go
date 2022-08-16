package isaacstates

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
	blockMapf func(base.Height) (base.BlockMap, bool, error),
	blockMapItemf func(context.Context, base.Height, base.BlockMapItemType) (io.ReadCloser, func() error, bool, error),
	newBlockWriteDatabase newBlockWriteDatabaseFunc,
	newBlockImporter func(base.BlockMap, isaac.BlockWriteDatabase) (isaac.BlockImporter, error),
	setLastVoteproofsFunc func(isaac.BlockReader) error,
) error {
	e := util.StringErrorFunc("failed to import blocks; %d - %d", from, to)

	bwdb, merge, err := newBlockWriteDatabase(from)
	if err != nil {
		return e(err, "")
	}

	var lastim isaac.BlockImporter
	var ims []isaac.BlockImporter

	if err := util.BatchWork(
		ctx,
		uint64((to - from + 1).Int64()),
		uint64(batchlimit),
		func(ctx context.Context, last uint64) error {
			if ims != nil {
				if err := saveImporters(ctx, ims, merge); err != nil {
					return err
				}
			}

			switch i, j, err := newBlockWriteDatabase(from); {
			case err != nil:
				return err
			default:
				bwdb = i
				merge = j
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

			m, found, err := blockMapf(height)
			switch {
			case err != nil:
				return err
			case !found:
				return util.ErrNotFound.Errorf("BlockMap not found")
			}

			im, err := newBlockImporter(m, bwdb)
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
		return e(err, "")
	}

	if int64(len(ims)) < batchlimit {
		if err := saveImporters(ctx, ims, merge); err != nil {
			return e(err, "")
		}
	}

	switch reader, err := lastim.Reader(); {
	case err != nil:
		return e(err, "")
	default:
		if err := setLastVoteproofsFunc(reader); err != nil {
			return e(err, "")
		}
	}

	return nil
}

func importBlock(
	ctx context.Context,
	height base.Height,
	m base.BlockMap,
	im isaac.BlockImporter,
	blockMapItemf func(context.Context, base.Height, base.BlockMapItemType) (io.ReadCloser, func() error, bool, error),
) error {
	e := util.StringErrorFunc("failed to import block, %d", height)

	var num int64
	m.Items(func(base.BlockMapItem) bool {
		num++

		return true
	})

	worker := util.NewErrgroupWorker(ctx, num)
	defer worker.Close()

	m.Items(func(item base.BlockMapItem) bool {
		if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
			switch r, cancel, found, err := blockMapItemf(ctx, height, item.Type()); {
			case err != nil:
				return err
			case !found:
				_ = cancel()

				return e(util.ErrNotFound.Errorf("blockMapItem not found"), "")
			default:
				defer func() {
					_ = cancel()
					_ = r.Close()
				}()

				if err := im.WriteItem(item.Type(), r); err != nil {
					return err
				}

				return nil
			}
		}); err != nil {
			return false
		}

		return true
	})

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	return nil
}

func saveImporters(ctx context.Context, ims []isaac.BlockImporter, merge func(context.Context) error) error {
	e := util.StringErrorFunc("failed to save importers")

	switch {
	case len(ims) < 1:
		return errors.Errorf("empty BlockImporters")
	case len(ims) < 2: //nolint:gomnd //...
		if err := ims[0].Save(ctx); err != nil {
			_ = cancelImporters(ctx, ims)

			return e(err, "")
		}
	default:
		if err := util.RunErrgroupWorker(ctx, uint64(len(ims)), func(ctx context.Context, i, _ uint64) error {
			return ims[i].Save(ctx)
		}); err != nil {
			_ = cancelImporters(ctx, ims)

			return e(err, "")
		}
	}

	if err := merge(ctx); err != nil {
		return e(err, "")
	}

	return nil
}

func cancelImporters(ctx context.Context, ims []isaac.BlockImporter) error {
	e := util.StringErrorFunc("failed to cancel importers")

	switch {
	case len(ims) < 1:
		return nil
	case len(ims) < 2: //nolint:gomnd //...
		if err := ims[0].CancelImport(ctx); err != nil {
			return e(err, "")
		}

		return nil
	}

	if err := util.RunErrgroupWorker(ctx, uint64(len(ims)), func(ctx context.Context, i, _ uint64) error {
		return ims[i].CancelImport(ctx)
	}); err != nil {
		return e(err, "")
	}

	return nil
}
