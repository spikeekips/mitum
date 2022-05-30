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
	blockMapItemf func(context.Context, base.Height, base.BlockMapItemType) (io.Reader, bool, error),
	newBlockImporter func(base.BlockMap) (isaac.BlockImporter, error),
	setLastVoteproofsFunc func(isaac.BlockReader) error,
) error {
	e := util.StringErrorFunc("failed to import blocks; %d - %d", from, to)

	var lastim isaac.BlockImporter
	var ims []isaac.BlockImporter

	if err := util.BatchWork(
		ctx,
		uint64((to - from + 1).Int64()),
		uint64(batchlimit),
		func(ctx context.Context, last uint64) error {
			if ims != nil {
				if err := SaveImporters(ctx, ims); err != nil {
					return errors.Wrap(err, "")
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
		func(ctx context.Context, i, _ uint64) error {
			height := from + base.Height(int64(i))

			m, found, err := blockMapf(height)
			switch {
			case err != nil:
				return errors.Wrap(err, "")
			case !found:
				return util.ErrNotFound.Errorf("BlockMap not found")
			}

			im, err := newBlockImporter(m)
			if err != nil {
				return errors.Wrap(err, "")
			}

			if err := ImportBlock(ctx, height, m, im, blockMapItemf); err != nil {
				return errors.Wrap(err, "")
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

	if err := SaveImporters(ctx, ims); err != nil {
		return e(err, "")
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

func ImportBlock(
	ctx context.Context,
	height base.Height,
	m base.BlockMap,
	im isaac.BlockImporter,
	blockMapItemf func(context.Context, base.Height, base.BlockMapItemType) (io.Reader, bool, error),
) error {
	e := util.StringErrorFunc("failed to import block, %d", height)

	workch := make(chan util.ContextWorkerCallback)

	go func() {
		defer close(workch)

		m.Items(func(item base.BlockMapItem) bool {
			workch <- func(ctx context.Context, _ uint64) error {
				switch r, found, err := blockMapItemf(ctx, height, item.Type()); {
				case err != nil:
					return errors.Wrap(err, "")
				case !found:
					return e(util.ErrNotFound.Errorf("blockMapItem not found"), "")
				default:
					if err := im.WriteItem(item.Type(), r); err != nil {
						return errors.Wrap(err, "")
					}

					return nil
				}
			}

			return true
		})
	}()

	if err := util.RunErrgroupWorkerByChan(ctx, workch); err != nil {
		return e(err, "")
	}

	return nil
}

func SaveImporters(ctx context.Context, ims []isaac.BlockImporter) error {
	e := util.StringErrorFunc("failed to save importers")

	switch {
	case len(ims) < 1:
		return errors.Errorf("empty BlockImporters")
	case len(ims) < 2: //nolint:gomnd //...
		if err := ims[0].Save(ctx); err != nil {
			return e(err, "")
		}

		if err := ims[0].Merge(ctx); err != nil {
			return e(err, "")
		}

		return nil
	}

	if err := util.RunErrgroupWorker(ctx, uint64(len(ims)), func(ctx context.Context, i, _ uint64) error {
		return errors.Wrap(ims[i].Save(ctx), "")
	}); err != nil {
		_ = CancelImporters(ctx, ims)

		return e(err, "")
	}

	if err := util.RunErrgroupWorker(ctx, uint64(len(ims)), func(ctx context.Context, i, _ uint64) error {
		return errors.Wrap(ims[i].Merge(ctx), "")
	}); err != nil {
		return e(err, "")
	}

	return nil
}

func CancelImporters(ctx context.Context, ims []isaac.BlockImporter) error {
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
		return errors.Wrap(ims[i].CancelImport(ctx), "")
	}); err != nil {
		return e(err, "")
	}

	return nil
}