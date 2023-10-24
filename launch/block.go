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
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

func ImportBlocks(
	from, root string,
	fromHeight, toHeight base.Height,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	db isaac.Database,
	params *isaac.Params,
) error {
	e := util.StringError("import blocks")

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

	if err := isaacblock.ImportBlocks(
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
			f func(r io.Reader, found bool) error,
		) error {
			reader, err := getreader(height)
			if err != nil {
				return err
			}

			switch r, found, err := reader.Reader(item); {
			case err != nil:
				return err
			default:
				return f(r, found)
			}
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
		return e.Wrap(err)
	}

	return nil
}

func NewBlockWriterFunc(
	local base.LocalNode,
	networkID base.NetworkID,
	dataroot string,
	enc encoder.Encoder,
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
			enc,
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
