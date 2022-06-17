package main

import (
	"context"
	"io"
	"math"
	"path/filepath"
	"sync"

	"github.com/bluele/gcache"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
)

type importCommand struct { //nolint:govet //...
	db   isaac.Database
	perm isaac.PermanentDatabase
	pool *isaacdatabase.TempPool
	baseNodeCommand
	From string `arg:"" name:"from directory" help:"block data directory to import" type:"existingdir"`
}

func (cmd *importCommand) Run() error {
	if err := cmd.prepareEncoder(); err != nil {
		return errors.Wrap(err, "")
	}

	if err := cmd.prepareDesigns(); err != nil {
		return errors.Wrap(err, "")
	}

	if err := cmd.prepareLocal(); err != nil {
		return errors.Wrap(err, "")
	}

	if err := cmd.prepareDatabase(); err != nil {
		return errors.Wrap(err, "")
	}

	last, err := cmd.checkLocalFS()
	if err != nil {
		return errors.Wrap(err, "")
	}

	if err := cmd.importBlocks(base.GenesisHeight, last); err != nil {
		return errors.Wrap(err, "")
	}

	if err := cmd.validateImported(last); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (cmd *importCommand) prepareDatabase() error {
	e := util.StringErrorFunc("failed to prepare database")

	if err := launch.CleanStorage(
		cmd.design.Storage.Database.String(),
		cmd.design.Storage.Base,
		cmd.encs, cmd.enc,
	); err != nil {
		return e(err, "")
	}

	nodeinfo, err := launch.CreateLocalFS(
		launch.CreateDefaultNodeInfo(networkID, version), cmd.design.Storage.Base, cmd.enc)
	if err != nil {
		return e(err, "")
	}

	db, perm, pool, err := launch.LoadDatabase(
		nodeinfo, cmd.design.Storage.Database.String(), cmd.design.Storage.Base, cmd.encs, cmd.enc)
	if err != nil {
		return e(err, "")
	}

	_ = db.SetLogging(logging)

	cmd.db = db
	cmd.perm = perm
	cmd.pool = pool

	return nil
}

func (cmd *importCommand) checkLocalFS() (last base.Height, _ error) {
	e := util.StringErrorFunc("failed to check localfs")

	last, err := launch.FindLastHeightFromLocalFS(cmd.From, cmd.enc, networkID)
	if err != nil {
		return last, e(err, "")
	}

	if err := launch.ValidateLocalFS(cmd.From, cmd.enc, last); err != nil {
		return last, e(err, "")
	}

	return last, nil
}

func (cmd *importCommand) importBlocks(from, to base.Height) error {
	e := util.StringErrorFunc("failed to import blocks")

	readercache := gcache.New(math.MaxInt).LRU().Build()
	var readerLock sync.Mutex
	getreader := func(height base.Height) (isaac.BlockReader, error) {
		readerLock.Lock()
		defer readerLock.Unlock()

		reader, err := readercache.Get(height)
		if err != nil {
			i, err := isaacblock.NewLocalFSReaderFromHeight(cmd.From, height, cmd.enc)
			if err != nil {
				return nil, errors.Wrap(err, "")
			}

			_ = readercache.Set(height, i)

			reader = i
		}

		return reader.(isaac.BlockReader), nil //nolint:forcetypeassert //...
	}

	if err := isaacstates.ImportBlocks(
		context.Background(),
		from, to,
		333, //nolint:gomnd //...
		func(height base.Height) (base.BlockMap, bool, error) {
			reader, err := getreader(height)
			if err != nil {
				return nil, false, errors.Wrap(err, "")
			}

			m, found, err := reader.BlockMap()

			return m, found, errors.Wrap(err, "")
		},
		func(
			_ context.Context, height base.Height, item base.BlockMapItemType,
		) (io.ReadCloser, func() error, bool, error) {
			reader, err := getreader(height)
			if err != nil {
				return nil, nil, false, errors.Wrap(err, "")
			}

			r, found, err := reader.Reader(item)

			return r, func() error { return nil }, found, errors.Wrap(err, "")
		},
		func(height base.Height) (isaac.BlockWriteDatabase, func(context.Context) error, error) {
			bwdb, err := cmd.db.NewBlockWriteDatabase(height)
			if err != nil {
				return nil, nil, errors.Wrap(err, "")
			}

			return bwdb,
				func(ctx context.Context) error {
					return launch.MergeBlockWriteToPermanentDatabase(ctx, bwdb, cmd.perm)
				},
				nil
		},
		func(m base.BlockMap, bwdb isaac.BlockWriteDatabase) (isaac.BlockImporter, error) {
			im, err := isaacblock.NewBlockImporter(
				launch.LocalFSDataDirectory(cmd.design.Storage.Base),
				cmd.encs,
				m,
				bwdb,
				networkID,
			)
			if err != nil {
				return nil, errors.Wrap(err, "")
			}

			return im, nil
		},
		func(reader isaac.BlockReader) error {
			switch v, found, err := reader.Item(base.BlockMapItemTypeVoteproofs); {
			case err != nil:
				return errors.Wrap(err, "")
			case !found:
				return errors.Errorf("voteproofs not found at last")
			default:
				vps := v.([]base.Voteproof)           //nolint:forcetypeassert //...
				if err := cmd.pool.SetLastVoteproofs( //nolint:forcetypeassert //...
					vps[0].(base.INITVoteproof),
					vps[1].(base.ACCEPTVoteproof),
				); err != nil {
					return errors.Wrap(err, "")
				}

				return nil
			}
		},
	); err != nil {
		return e(err, "")
	}

	return nil
}

func (cmd *importCommand) validateImported(last base.Height) error {
	e := util.StringErrorFunc("failed to validate imported")

	root := launch.LocalFSDataDirectory(cmd.design.Storage.Base)

	switch h, found, err := isaacblock.FindHighestDirectory(root); {
	case err != nil:
		return e(err, "")
	case !found:
		return util.ErrNotFound.Errorf("height directories not found")
	default:
		rel, err := filepath.Rel(root, h)
		if err != nil {
			return e(err, "")
		}

		switch found, err := isaacblock.HeightFromDirectory(rel); {
		case err != nil:
			return e(err, "")
		case found != last:
			return util.ErrNotFound.Errorf("last height not found; found=%d last=%d", found, last)
		}
	}

	if err := cmd.validateImportedBlockMaps(root, last); err != nil {
		return e(err, "")
	}

	if err := cmd.validateImportedBlocks(root, last); err != nil {
		return e(err, "")
	}

	return nil
}

func (cmd *importCommand) validateImportedBlockMaps(root string, last base.Height) error {
	e := util.StringErrorFunc("failed to validate imported BlockMaps")

	if err := base.BatchValidateMaps(
		context.Background(),
		nil,
		last,
		333, //nolint:gomnd //...
		func(_ context.Context, height base.Height) (base.BlockMap, error) {
			reader, err := isaacblock.NewLocalFSReaderFromHeight(root, height, cmd.enc)
			if err != nil {
				return nil, errors.Wrap(err, "")
			}

			switch m, found, err := reader.BlockMap(); {
			case err != nil:
				return nil, errors.Wrap(err, "")
			case !found:
				return nil, util.ErrNotFound.Call()
			default:
				if err := m.IsValid(networkID); err != nil {
					return nil, errors.Wrap(err, "")
				}

				return m, nil
			}
		},
		func(m base.BlockMap) error {
			return nil
		},
	); err != nil {
		return e(err, "")
	}

	log.Debug().Msg("imported BlockMaps validated")

	return nil
}

func (cmd *importCommand) validateImportedBlocks(root string, last base.Height) error {
	e := util.StringErrorFunc("failed to validate imported blocks")

	if err := util.BatchWork(
		context.Background(),
		uint64(last.Int64()),
		333, //nolint:gomnd //...
		func(context.Context, uint64) error {
			return nil
		},
		func(_ context.Context, i, _ uint64) error {
			return launch.ValidateBlockFromLocalFS(base.Height(int64(i)), root, cmd.enc, networkID, cmd.perm)
		},
	); err != nil {
		return e(err, "")
	}

	return nil
}

// FIXME check SuffrageProof
