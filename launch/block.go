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
		func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
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
		func(m base.BlockMap) (isaac.BlockImporter, error) {
			bwdb, err := db.NewBlockWriteDatabase(m.Manifest().Height())
			if err != nil {
				return nil, err
			}

			return isaacblock.NewBlockImporter(
				LocalFSDataDirectory(root),
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
