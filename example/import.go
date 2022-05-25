package main

import (
	"context"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type importCommand struct {
	Address launch.AddressFlag `arg:"" name:"local address" help:"node address"`
	local   base.LocalNode
	enc     encoder.Encoder
	db      isaac.Database
	perm    isaac.PermanentDatabase
	pool    *isaacdatabase.TempPool
	encs    *encoder.Encoders
	From    string `arg:"" name:"from directory" help:" block data directory to import" type:"existingdir"`
}

func (cmd *importCommand) Run() error {
	encs, enc, err := launch.PrepareEncoders()
	if err != nil {
		return errors.Wrap(err, "")
	}

	cmd.encs = encs
	cmd.enc = enc

	local, err := prepareLocal(cmd.Address.Address())
	if err != nil {
		return errors.Wrap(err, "failed to prepare local")
	}

	cmd.local = local

	dbroot := defaultDBRoot(cmd.local.Address())

	log.Debug().Str("root", dbroot).Msg("block data directory")

	if err := cmd.prepareDatabase(dbroot); err != nil {
		return errors.Wrap(err, "")
	}

	last, err := cmd.checkLocalFS()
	if err != nil {
		return errors.Wrap(err, "")
	}

	if err := cmd.importBlocks(dbroot, last); err != nil {
		return errors.Wrap(err, "")
	}

	if err := cmd.validateImported(dbroot, last); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (cmd *importCommand) prepareDatabase(dbroot string) error {
	e := util.StringErrorFunc("failed to prepare database")

	if err := launch.CleanDatabase(dbroot); err != nil {
		return e(err, "")
	}

	if err := launch.InitializeDatabase(dbroot); err != nil {
		return e(err, "")
	}

	perm, err := loadPermanentDatabase(launch.DBRootPermDirectory(dbroot), cmd.encs, cmd.enc)
	if err != nil {
		return e(err, "")
	}

	if err := perm.Clean(); err != nil {
		return e(err, "")
	}

	db, pool, err := launch.PrepareDatabase(perm, dbroot, cmd.encs, cmd.enc)
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

	last, err := cmd.findLastFromLocalFS()
	if err != nil {
		return last, e(err, "")
	}

	if err := cmd.validateLocalFS(last); err != nil {
		return last, e(err, "")
	}

	return last, nil
}

func (cmd *importCommand) findLastFromLocalFS() (last base.Height, _ error) {
	e := util.StringErrorFunc("failed to find last height from localfs")

	last = base.NilHeight

	// NOTE check genesis first
	fsreader, err := isaacblock.NewLocalFSReaderFromHeight(cmd.From, base.GenesisHeight, cmd.enc)
	if err != nil {
		return last, e(err, "")
	}

	switch blockmap, found, err := fsreader.Map(); {
	case err != nil:
		return last, e(err, "")
	case !found:
		return last, util.ErrNotFound.Errorf("genesis not found")
	default:
		if err := blockmap.IsValid(networkID); err != nil {
			return last, e(err, "")
		}
	}

	switch h, found, err := isaacblock.FindHighestDirectory(cmd.From); {
	case err != nil:
		return last, e(err, "")
	case !found:
		return last, util.ErrNotFound.Errorf("height directories not found")
	default:
		rel, err := filepath.Rel(cmd.From, h)
		if err != nil {
			return last, e(err, "")
		}

		height, err := isaacblock.HeightFromDirectory(rel)
		if err != nil {
			return last, e(err, "")
		}

		last = height
	}

	return last, nil
}

func (cmd *importCommand) validateLocalFS(last base.Height) error {
	e := util.StringErrorFunc("failed to validate localfs")

	// NOTE check all blockmap items
	var validateLock sync.Mutex
	var lastprev, newprev base.BlockMap
	var maps []base.BlockMap

	batchlimit := uint64(333)

	if err := util.BatchWork(context.Background(), uint64(last.Int64())+1, batchlimit,
		func(_ context.Context, last uint64) error {
			lastprev = newprev

			switch r := (last + 1) % batchlimit; {
			case r == 0:
				maps = make([]base.BlockMap, batchlimit)
			default:
				maps = make([]base.BlockMap, r)
			}

			return nil
		},
		func(_ context.Context, i, last uint64) error {
			height := base.Height(int64(i))

			reader, err := isaacblock.NewLocalFSReaderFromHeight(cmd.From, height, cmd.enc)
			if err != nil {
				return errors.Wrap(err, "")
			}

			switch m, found, err := reader.Map(); {
			case err != nil:
				return errors.Wrap(err, "")
			case !found:
				return errors.Wrap(util.ErrNotFound.Errorf("BlockMap not found"), "")
			case m.Manifest().Height() != height:
				return errors.Wrap(util.ErrInvalid.Errorf(
					"invalid BlockMap; wrong height; directory=%d manifest=%d", height, m.Manifest().Height()), "")
			default:
				m := m
				if err = func() error {
					validateLock.Lock()
					defer validateLock.Unlock()

					if err = isaacstates.ValidateMaps(m, maps, lastprev); err != nil {
						return err
					}

					if m.Manifest().Height() == base.Height(int64(last)) {
						newprev = m
					}

					return nil
				}(); err != nil {
					return err
				}

				return nil
			}
		},
	); err != nil {
		return e(err, "")
	}

	maps = nil

	return nil
}

func (cmd *importCommand) importBlocks(dbroot string, last base.Height) error {
	e := util.StringErrorFunc("failed to import blocks")

	for i := base.GenesisHeight; i <= last; i++ {
		fsreader, err := isaacblock.NewLocalFSReaderFromHeight(cmd.From, i, cmd.enc)
		if err != nil {
			return e(err, "")
		}

		var m base.BlockMap
		var im isaac.BlockImporter
		switch j, found, err := fsreader.Map(); {
		case err != nil:
			return e(err, "")
		case !found:
			return errors.Errorf("BlockMap not found")
		default:
			m = j

			bwdb, err := cmd.db.NewBlockWriteDatabase(i)
			if err != nil {
				return e(err, "")
			}

			y, err := isaacblock.NewBlockImporter(
				launch.DBRootDataDirectory(dbroot),
				cmd.encs,
				m,
				bwdb,
				cmd.perm,
				networkID,
			)
			if err != nil {
				return e(err, "")
			}

			im = y
		}

		var itemserr error
		m.Items(func(item base.BlockMapItem) bool {
			r, found, err := fsreader.Reader(item.Type())
			switch {
			case err != nil:
				itemserr = err

				return false
			case !found:
				return false
			}

			if err := im.WriteItem(item.Type(), r); err != nil {
				itemserr = err

				return false
			}

			return true
		})

		if itemserr != nil {
			return e(itemserr, "")
		}

		if err := im.Save(context.Background()); err != nil {
			return e(err, "")
		}

		if err := im.Merge(context.Background()); err != nil {
			return e(err, "")
		}

		if i == last { // NOTE set last voteproof
			switch v, found, err := fsreader.Item(base.BlockMapItemTypeVoteproofs); {
			case err != nil:
				return e(err, "")
			case !found:
				return errors.Errorf("voteproofs not found at last")
			default:
				vps := v.([]base.Voteproof)
				if err := cmd.pool.SetLastVoteproofs(
					vps[0].(base.INITVoteproof),
					vps[1].(base.ACCEPTVoteproof),
				); err != nil {
					return e(err, "")
				}
			}
		}
	}

	log.Debug().Msg("blocks imported")

	return nil
}

func (cmd *importCommand) validateImported(dbroot string, last base.Height) error {
	e := util.StringErrorFunc("failed to validate imported")

	root := launch.DBRootDataDirectory(dbroot)
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
		333,
		func(_ context.Context, height base.Height) (base.BlockMap, error) {
			reader, err := isaacblock.NewLocalFSReaderFromHeight(root, height, cmd.enc)
			if err != nil {
				return nil, errors.Wrap(err, "")
			}

			switch m, found, err := reader.Map(); {
			case err != nil:
				return nil, errors.Wrap(err, "")
			case !found:
				return nil, util.ErrNotFound.Call()
			default:
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
		333,
		func(context.Context, uint64) error {
			return nil
		},
		func(_ context.Context, i, _ uint64) error {
			return cmd.validateImportedBlock(root, base.Height(int64(i)))
		},
	); err != nil {
		return e(err, "")
	}

	return nil
}

func (cmd *importCommand) validateImportedBlock(root string, height base.Height) error {
	e := util.StringErrorFunc("failed to validate imported block")

	reader, err := isaacblock.NewLocalFSReaderFromHeight(root, height, cmd.enc)
	if err != nil {
		return e(err, "")
	}

	var m base.BlockMap
	switch i, found, err := reader.Map(); {
	case err != nil:
		return e(err, "")
	case !found:
		return e(util.ErrNotFound.Errorf("BlockMap not found"), "")
	default:
		m = i
	}

	var ops []base.Operation
	var sts []base.State
	var opstree, ststree fixedtree.Tree

	var readererr error
	reader.Items(func(item base.BlockMapItem, i interface{}, found bool, err error) bool {
		switch {
		case err != nil:
			readererr = err
		case !found:
			readererr = util.ErrNotFound.Errorf("BlockMapItem not found, %q", item.Type())
		case i == nil:
			readererr = util.ErrNotFound.Errorf("empty item found, %q", item.Type())
		}

		if readererr != nil {
			return false
		}

		switch item.Type() {
		case base.BlockMapItemTypeProposal:
			readererr = base.ValidateProposalWithManifest(i.(base.ProposalSignedFact), m.Manifest())
		case base.BlockMapItemTypeOperations:
			ops = i.([]base.Operation)
		case base.BlockMapItemTypeOperationsTree:
			opstree = i.(fixedtree.Tree)
		case base.BlockMapItemTypeStates:
			sts = i.([]base.State)
		case base.BlockMapItemTypeStatesTree:
			ststree = i.(fixedtree.Tree)
		case base.BlockMapItemTypeVoteproofs:
			readererr = base.ValidateVoteproofsWithManifest(i.([]base.Voteproof), m.Manifest())
		}

		if readererr != nil {
			return false
		}

		return true
	})

	if readererr == nil {
		readererr = base.ValidateOperationsTreeWithManifest(opstree, ops, m.Manifest())
	}

	if readererr == nil {
		readererr = base.ValidateStatesTreeWithManifest(ststree, sts, m.Manifest())
	}

	return readererr
}
