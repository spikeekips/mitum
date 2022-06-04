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
	switch encs, enc, err := launch.PrepareEncoders(); {
	case err != nil:
		return errors.Wrap(err, "")
	default:
		cmd.encs = encs
		cmd.enc = enc
	}

	switch local, err := prepareLocal(cmd.Address.Address()); {
	case err != nil:
		return errors.Wrap(err, "failed to prepare local")
	default:
		cmd.local = local
	}

	var localfsroot string

	switch i, err := defaultLocalFSRoot(cmd.local.Address()); {
	case err != nil:
		return errors.Wrap(err, "")
	default:
		localfsroot = i

		log.Debug().Str("localfs_root", localfsroot).Msg("localfs root")
	}

	if err := cmd.prepareDatabase(localfsroot); err != nil {
		return errors.Wrap(err, "")
	}

	last, err := cmd.checkLocalFS()
	if err != nil {
		return errors.Wrap(err, "")
	}

	if err := cmd.importBlocks(localfsroot, base.GenesisHeight, last); err != nil {
		return errors.Wrap(err, "")
	}

	if err := cmd.validateImported(localfsroot, last); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (cmd *importCommand) prepareDatabase(localfsroot string) error {
	e := util.StringErrorFunc("failed to prepare database")

	permuri := defaultPermanentDatabaseURI()

	if err := launch.CleanStorage(
		permuri,
		localfsroot,
		cmd.encs, cmd.enc,
	); err != nil {
		return e(err, "")
	}

	nodeinfo, err := launch.CreateLocalFS(launch.CreateDefaultNodeInfo(networkID, version), localfsroot, cmd.enc)
	if err != nil {
		return e(err, "")
	}

	db, perm, pool, err := launch.LoadDatabase(nodeinfo, permuri, localfsroot, cmd.encs, cmd.enc)
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

	switch blockmap, found, err := fsreader.BlockMap(); {
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

	batchlimit := uint64(333) //nolint:gomnd //...

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

			switch m, found, err := reader.BlockMap(); {
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

					if err = base.ValidateMaps(m, maps, lastprev); err != nil {
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

func (cmd *importCommand) importBlocks(localfsroot string, from, to base.Height) error {
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
		func(_ context.Context, height base.Height, item base.BlockMapItemType) (io.ReadCloser, bool, error) {
			reader, err := getreader(height)
			if err != nil {
				return nil, false, errors.Wrap(err, "")
			}

			r, found, err := reader.Reader(item)

			return r, found, errors.Wrap(err, "")
		},
		func(m base.BlockMap) (isaac.BlockImporter, error) {
			bwdb, err := cmd.db.NewBlockWriteDatabase(m.Manifest().Height())
			if err != nil {
				return nil, errors.Wrap(err, "")
			}

			im, err := isaacblock.NewBlockImporter(
				launch.LocalFSDataDirectory(localfsroot),
				cmd.encs,
				m,
				bwdb,
				cmd.perm,
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

func (cmd *importCommand) validateImported(localfsroot string, last base.Height) error {
	e := util.StringErrorFunc("failed to validate imported")

	root := launch.LocalFSDataDirectory(localfsroot)

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

	switch i, found, err := reader.BlockMap(); {
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
	if err := reader.Items(func(item base.BlockMapItem, i interface{}, found bool, err error) bool {
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
			pr := i.(base.ProposalSignedFact) //nolint:forcetypeassert //...

			if err := pr.IsValid(networkID); err != nil {
				readererr = err
			}

			if readererr != nil {
				readererr = base.ValidateProposalWithManifest(pr, m.Manifest())
			}
		case base.BlockMapItemTypeOperations:
			ops = i.([]base.Operation) //nolint:forcetypeassert //...
		case base.BlockMapItemTypeOperationsTree:
			opstree = i.(fixedtree.Tree) //nolint:forcetypeassert //...
		case base.BlockMapItemTypeStates:
			sts = i.([]base.State) //nolint:forcetypeassert //...
		case base.BlockMapItemTypeStatesTree:
			ststree = i.(fixedtree.Tree) //nolint:forcetypeassert //...
		case base.BlockMapItemTypeVoteproofs:
			readererr = base.ValidateVoteproofsWithManifest( //nolint:forcetypeassert //...
				i.([]base.Voteproof), m.Manifest())
		}

		return readererr == nil
	}); err != nil {
		readererr = err
	}

	if readererr == nil {
		readererr = cmd.validateOperations(opstree, ops, m.Manifest())
	}

	if readererr == nil {
		readererr = cmd.validateStates(ststree, sts, m.Manifest())
	}

	return readererr
}

func (cmd *importCommand) validateOperations(
	opstree fixedtree.Tree, ops []base.Operation, manifest base.Manifest,
) error {
	e := util.StringErrorFunc("failed to validate imported operations")

	if err := opstree.IsValid(nil); err != nil {
		return e(err, "")
	}

	if err := base.ValidateOperationsTreeWithManifest(opstree, ops, manifest); err != nil {
		return e(err, "")
	}

	if len(ops) > 0 {
		if err := util.BatchWork(context.Background(), uint64(len(ops)), 333, //nolint:gomnd //...
			func(context.Context, uint64) error { return nil },
			func(_ context.Context, i, _ uint64) error {
				switch found, err := cmd.perm.ExistsKnownOperation(ops[i].Hash()); {
				case err != nil:
					return errors.Wrap(err, "")
				case !found:
					return util.ErrNotFound.Errorf("operation not found in ExistsKnownOperation; %q", ops[i].Hash())
				default:
					return nil
				}
			},
		); err != nil {
			return e(err, "")
		}
	}

	return nil
}

func (cmd *importCommand) validateStates(ststree fixedtree.Tree, sts []base.State, manifest base.Manifest) error {
	e := util.StringErrorFunc("failed to validate imported states")

	if err := ststree.IsValid(nil); err != nil {
		return e(err, "")
	}

	if err := base.ValidateStatesTreeWithManifest(ststree, sts, manifest); err != nil {
		return e(err, "")
	}

	if len(sts) > 0 {
		if err := util.BatchWork(context.Background(), uint64(len(sts)), 333, //nolint:gomnd //...
			func(context.Context, uint64) error { return nil },
			func(_ context.Context, i, _ uint64) error {
				st := sts[i]

				switch rst, found, err := cmd.perm.State(st.Key()); {
				case err != nil:
					return errors.Wrap(err, "")
				case !found:
					return util.ErrNotFound.Errorf("state not found in State")
				case !base.IsEqualState(st, rst):
					return errors.Errorf("states does not match")
				}

				ops := st.Operations()
				for j := range ops {
					switch found, err := cmd.perm.ExistsInStateOperation(ops[j]); {
					case err != nil:
						return errors.Wrap(err, "")
					case !found:
						return util.ErrNotFound.Errorf("operation of state not found in ExistsInStateOperation")
					}
				}

				return nil
			},
		); err != nil {
			return e(err, "")
		}
	}

	return nil
}

// FIXME check SuffrageProof
