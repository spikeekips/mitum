package launch

import (
	"context"
	"io"
	"math"
	"sync"

	"github.com/bluele/gcache"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

func ImportBlocks(
	from, root string,
	fromHeight, toHeight base.Height,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	db isaac.Database,
	perm isaac.PermanentDatabase,
	params base.LocalParams,
) error {
	e := util.StringErrorFunc("failed to import blocks")

	readercache := gcache.New(math.MaxInt).LRU().Build()
	var readerLock sync.Mutex
	getreader := func(height base.Height) (isaac.BlockReader, error) {
		readerLock.Lock()
		defer readerLock.Unlock()

		reader, err := readercache.Get(height)
		if err != nil {
			i, err := isaacblock.NewLocalFSReaderFromHeight(from, height, enc)
			if err != nil {
				return nil, err
			}

			_ = readercache.Set(height, i)

			reader = i
		}

		return reader.(isaac.BlockReader), nil //nolint:forcetypeassert //...
	}

	if err := isaacstates.ImportBlocks(
		context.Background(),
		fromHeight, toHeight,
		333, //nolint:gomnd //...
		func(height base.Height) (base.BlockMap, bool, error) {
			reader, err := getreader(height)
			if err != nil {
				return nil, false, err
			}

			m, found, err := reader.BlockMap()

			return m, found, err
		},
		func(
			_ context.Context, height base.Height, item base.BlockMapItemType,
		) (io.ReadCloser, func() error, bool, error) {
			reader, err := getreader(height)
			if err != nil {
				return nil, nil, false, err
			}

			r, found, err := reader.Reader(item)

			return r, func() error { return nil }, found, err
		},
		func(height base.Height) (isaac.BlockWriteDatabase, func(context.Context) error, error) {
			bwdb, err := db.NewBlockWriteDatabase(height)
			if err != nil {
				return nil, nil, err
			}

			return bwdb,
				func(ctx context.Context) error {
					return MergeBlockWriteToPermanentDatabase(ctx, bwdb, perm)
				},
				nil
		},
		func(m base.BlockMap, bwdb isaac.BlockWriteDatabase) (isaac.BlockImporter, error) {
			im, err := isaacblock.NewBlockImporter(
				LocalFSDataDirectory(root),
				encs,
				m,
				bwdb,
				params.NetworkID(),
			)
			if err != nil {
				return nil, err
			}

			return im, nil
		},
		nil,
	); err != nil {
		return e(err, "")
	}

	return nil
}

func MergeBlockWriteToPermanentDatabase(
	ctx context.Context, bwdb isaac.BlockWriteDatabase, perm isaac.PermanentDatabase,
) error {
	e := util.StringErrorFunc("failed to merge BlockWriter")

	temp, err := bwdb.TempDatabase()
	if err != nil {
		return e(err, "")
	}

	if err := perm.MergeTempDatabase(ctx, temp); err != nil {
		return e(err, "")
	}

	if err := temp.Remove(); err != nil {
		return e(err, "")
	}

	return nil
}

func NewBlockWriterFunc(
	local base.LocalNode,
	networkID base.NetworkID,
	dataroot string,
	enc encoder.Encoder,
	db isaac.Database,
) isaac.NewBlockWriterFunc {
	return func(proposal base.ProposalSignFact, getStateFunc base.GetStateFunc) (isaac.BlockWriter, error) {
		e := util.StringErrorFunc("failed to crete BlockWriter")

		dbw, err := db.NewBlockWriteDatabase(proposal.Point().Height())
		if err != nil {
			return nil, e(err, "")
		}

		fswriter, err := isaacblock.NewLocalFSWriter(
			dataroot,
			proposal.Point().Height(),
			enc,
			local,
			networkID,
		)
		if err != nil {
			return nil, e(err, "")
		}

		return isaacblock.NewWriter(proposal, getStateFunc, dbw, db.MergeBlockWriteDatabase, fswriter), nil
	}
}
