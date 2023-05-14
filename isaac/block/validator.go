package isaacblock

import (
	"context"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

var (
	ErrLastBlockMapOnlyInDatabase = util.NewIDError("last blockmap found in database, but not in localfs")
	ErrLastBlockMapOnlyInLocalFS  = util.NewIDError("last blockmap found in localfs, but not in database")
)

type ErrValidatedDifferentHeightBlockMaps struct {
	*util.IDError
	db      base.Height
	localfs base.Height
}

func newErrValidatedDifferentHeightBlockMaps(db, localfs base.Height) *ErrValidatedDifferentHeightBlockMaps {
	return &ErrValidatedDifferentHeightBlockMaps{
		IDError: util.NewIDErrorWithID("dhb", "different height blockmaps"),
		db:      db,
		localfs: localfs,
	}
}

func (er *ErrValidatedDifferentHeightBlockMaps) Wrap(err error) error {
	return &ErrValidatedDifferentHeightBlockMaps{
		IDError: er.IDError.Wrap(err).(*util.IDError), //nolint:forcetypeassert //...
		db:      er.db,
		localfs: er.localfs,
	}
}

func (er *ErrValidatedDifferentHeightBlockMaps) WithMessage(err error, format string, args ...interface{}) error {
	return &ErrValidatedDifferentHeightBlockMaps{
		IDError: er.IDError.WithMessage(err, format, args...).(*util.IDError), //nolint:forcetypeassert //...
		db:      er.db,
		localfs: er.localfs,
	}
}

func (er *ErrValidatedDifferentHeightBlockMaps) Errorf(
	format string, args ...interface{},
) *ErrValidatedDifferentHeightBlockMaps {
	return &ErrValidatedDifferentHeightBlockMaps{
		IDError: er.IDError.Errorf(format, args...),
		db:      er.db,
		localfs: er.localfs,
	}
}

func (er *ErrValidatedDifferentHeightBlockMaps) WithStack() *ErrValidatedDifferentHeightBlockMaps {
	return &ErrValidatedDifferentHeightBlockMaps{
		IDError: er.IDError.WithStack(),
		db:      er.db,
		localfs: er.localfs,
	}
}

func (er *ErrValidatedDifferentHeightBlockMaps) DatabaseHeight() base.Height {
	return er.db
}

func (er *ErrValidatedDifferentHeightBlockMaps) LocalFSHeight() base.Height {
	return er.localfs
}

func ValidateLastBlocks(
	localfsroot string,
	localfsencs *encoder.Encoders,
	defaultLocalfsencs encoder.Encoder,
	db isaac.Database,
	networkID base.NetworkID,
) error {
	var lastmapdb, lastmaplocalfs base.BlockMap

	localfsenc := defaultLocalfsencs

	switch i, found, err := loadLastBlockMapFromDatabase(db, networkID); {
	case err != nil:
		return errors.WithMessage(err, "last BlockMap from database")
	case !found:
	default:
		localfsenc = localfsencs.Find(i.Encoder())
		if localfsenc == nil {
			return errors.Errorf("encoder of last blockmap not found")
		}

		lastmapdb = i
	}

	switch last, found, err := loadLastBlockMapFromLocalFS(localfsroot, localfsenc, networkID); {
	case err != nil:
		return errors.WithMessage(err, "find last height from localfs")
	case !found:
	default:
		lastmaplocalfs = last
	}

	switch {
	case lastmapdb == nil && lastmaplocalfs == nil:
		return nil
	case lastmapdb != nil && lastmaplocalfs == nil:
		return ErrLastBlockMapOnlyInDatabase.WithStack()
	case lastmapdb == nil && lastmaplocalfs != nil:
		return ErrLastBlockMapOnlyInLocalFS.WithStack()
	default:
		if err := base.IsEqualBlockMap(lastmapdb, lastmaplocalfs); err != nil {
			if lastmapdb.Manifest().Height() != lastmaplocalfs.Manifest().Height() {
				err = newErrValidatedDifferentHeightBlockMaps(
					lastmapdb.Manifest().Height(),
					lastmaplocalfs.Manifest().Height(),
				).WithStack()
			}

			return err
		}

		return nil
	}
}

func loadLastBlockMapFromDatabase(db isaac.Database, networkID base.NetworkID) (base.BlockMap, bool, error) {
	switch i, found, err := db.LastBlockMap(); {
	case err != nil:
		return nil, false, err
	case !found:
		return nil, false, nil
	default:
		if err := i.IsValid(networkID); err != nil {
			return nil, false, err
		}

		return i, true, nil
	}
}

func loadLastBlockMapFromLocalFS(
	localfsroot string,
	localfsenc encoder.Encoder,
	networkID base.NetworkID,
) (base.BlockMap, bool, error) {
	var last base.Height

	switch i, found, err := FindLastHeightFromLocalFS(localfsroot, localfsenc, networkID); {
	case err != nil:
		return nil, false, err
	case !found:
		return nil, false, nil
	default:
		last = i
	}

	reader, err := NewLocalFSReaderFromHeight(localfsroot, last, localfsenc)
	if err != nil {
		return nil, false, err
	}

	switch i, found, err := loadBlockMapFromReader(reader, networkID); {
	case err != nil:
		return nil, false, err
	case !found:
		return nil, false, nil
	default:
		return i, true, nil
	}
}

func loadBlockMapFromReader(reader *LocalFSReader, networkID base.NetworkID) (base.BlockMap, bool, error) {
	switch i, found, err := reader.BlockMap(); {
	case err != nil:
		return nil, false, err
	case !found:
		return nil, false, nil
	default:
		if err := i.IsValid(networkID); err != nil {
			return nil, false, err
		}

		return i, true, nil
	}
}

func ValidateAllBlockMapsFromLocalFS(
	dataroot string,
	enc encoder.Encoder,
	last base.Height,
	networkID base.NetworkID,
) error {
	e := util.StringError("validate localfs")

	switch fi, err := os.Stat(dataroot); {
	case err == nil:
		if !fi.IsDir() {
			return e.Errorf("not directory")
		}
	case os.IsNotExist(err):
		return e.Wrap(err)
	default:
		return e.Wrap(err)
	}

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

			var m base.BlockMap

			switch reader, err := NewLocalFSReaderFromHeight(dataroot, height, enc); {
			case err != nil:
				return err
			default:
				switch j, found, err := reader.BlockMap(); {
				case err != nil:
					return err
				case !found:
					return util.ErrNotFound.Errorf("BlockMap not found")
				default:
					if err := j.IsValid(networkID); err != nil {
						return err
					}

					m = j
				}
			}

			if m.Manifest().Height() != height {
				return newErrValidatedDifferentHeightBlockMaps(height, m.Manifest().Height()).WithStack()
			}

			return func() error {
				validateLock.Lock()
				defer validateLock.Unlock()

				if err := base.ValidateMaps(m, maps, lastprev); err != nil {
					return err
				}

				if m.Manifest().Height() == base.Height(int64(last)) {
					newprev = m
				}

				return nil
			}()
		},
	); err != nil {
		return e.Wrap(err)
	}

	maps = nil

	return nil
}

func ValidateBlockFromLocalFS(
	height base.Height,
	dataroot string,
	enc encoder.Encoder,
	networkID base.NetworkID,
	validateBlockMapf func(base.BlockMap) error,
	validateOperationf func(base.Operation) error,
	validateStatef func(base.State) error,
) error {
	e := util.StringError("validate imported block")

	var reader *LocalFSReader

	switch i, err := NewLocalFSReaderFromHeight(dataroot, height, enc); {
	case err != nil:
		return e.Wrap(err)
	default:
		reader = i
	}

	var m base.BlockMap

	switch i, found, err := loadBlockMapFromReader(reader, networkID); {
	case err != nil:
		return e.Wrap(err)
	case !found:
		return e.Wrap(util.ErrNotFound.Errorf("BlockMap not found"))
	default:
		if err := i.IsValid(networkID); err != nil {
			return err
		}

		if validateBlockMapf != nil {
			if err := validateBlockMapf(i); err != nil {
				return err
			}
		}

		m = i
	}

	pr, ops, sts, opstree, ststree, vps, err := loadBlockItemsFromReader(reader)
	if err != nil {
		return err
	}

	if err := pr.IsValid(networkID); err != nil {
		return err
	}

	if err := base.ValidateProposalWithManifest(pr, m.Manifest()); err != nil {
		return err
	}

	if err := ValidateOperationsOfBlock(opstree, ops, m.Manifest(), networkID, validateOperationf); err != nil {
		return err
	}

	if err := ValidateStatesOfBlock(ststree, sts, m.Manifest(), networkID, validateStatef); err != nil {
		return err
	}

	return validateVoteproofsFromLocalFS(networkID, vps, m.Manifest())
}

func loadBlockItemsFromReader(reader *LocalFSReader) ( //revive:disable-line:function-result-limit
	pr base.ProposalSignFact,
	ops []base.Operation,
	sts []base.State,
	opstree, ststree fixedtree.Tree,
	vps []base.Voteproof,
	rerr error,
) {
	//revive:disable:bare-return
	if err := reader.Items(func(item base.BlockMapItem, i interface{}, found bool, err error) bool {
		switch {
		case err != nil:
			rerr = err
		case !found:
			rerr = util.ErrNotFound.Errorf("BlockMapItem not found, %q", item.Type())
		case i == nil:
			rerr = util.ErrNotFound.Errorf("empty BlockMapItem found, %q", item.Type())
		}

		if rerr != nil {
			return false
		}

		switch item.Type() {
		case base.BlockMapItemTypeProposal:
			pr = i.(base.ProposalSignFact) //nolint:forcetypeassert //...
		case base.BlockMapItemTypeOperations:
			ops = i.([]base.Operation) //nolint:forcetypeassert //...
		case base.BlockMapItemTypeOperationsTree:
			opstree = i.(fixedtree.Tree) //nolint:forcetypeassert //...
		case base.BlockMapItemTypeStates:
			sts = i.([]base.State) //nolint:forcetypeassert //...
		case base.BlockMapItemTypeStatesTree:
			ststree = i.(fixedtree.Tree) //nolint:forcetypeassert //...
		case base.BlockMapItemTypeVoteproofs:
			vps = i.([]base.Voteproof) //nolint:forcetypeassert //...
		}

		return rerr == nil
	}); err != nil {
		rerr = err

		return
	}

	return //nolint:nakedret //...
	//revive:enable:bare-return
}

func ValidateOperationsOfBlock( //nolint:dupl //...
	opstree fixedtree.Tree,
	ops []base.Operation,
	manifest base.Manifest,
	networkID base.NetworkID,
	validateOperationf func(base.Operation) error,
) error {
	e := util.StringError("validate imported operations")

	if err := opstree.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := base.ValidateOperationsTreeWithManifest(opstree, ops, manifest); err != nil {
		return e.Wrap(err)
	}

	if len(ops) > 0 {
		if err := util.BatchWork(context.Background(), uint64(len(ops)), 333, //nolint:gomnd //...
			func(context.Context, uint64) error { return nil },
			func(_ context.Context, i, _ uint64) error {
				op := ops[i]

				if err := op.IsValid(networkID); err != nil {
					return err
				}

				if validateOperationf != nil {
					if err := validateOperationf(op); err != nil {
						return err
					}
				}

				return nil
			},
		); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func ValidateStatesOfBlock( //nolint:dupl //...
	ststree fixedtree.Tree,
	sts []base.State,
	manifest base.Manifest,
	networkID base.NetworkID,
	validateStatef func(base.State) error,
) error {
	e := util.StringError("validate imported states")

	if err := ststree.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := base.ValidateStatesTreeWithManifest(ststree, sts, manifest); err != nil {
		return e.Wrap(err)
	}

	if len(sts) > 0 {
		if err := util.BatchWork(context.Background(), uint64(len(sts)), 333, //nolint:gomnd //...
			func(context.Context, uint64) error { return nil },
			func(_ context.Context, i, _ uint64) error {
				st := sts[i]

				if err := st.IsValid(networkID); err != nil {
					return err
				}

				if validateStatef != nil {
					if err := validateStatef(st); err != nil {
						return err
					}
				}

				return nil
			},
		); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func validateVoteproofsFromLocalFS(networkID base.NetworkID, vps []base.Voteproof, m base.Manifest) error {
	for i := range vps {
		if vps[i] == nil {
			continue
		}

		if err := vps[i].IsValid(networkID); err != nil {
			return err
		}
	}

	return base.ValidateVoteproofsWithManifest(vps, m)
}

func ValidateBlocksFromStorage(
	root string,
	fromHeight, toHeight base.Height,
	enc encoder.Encoder,
	params *isaac.LocalParams,
	db isaac.Database,
	whenBlockDonef func(base.BlockMap, error) error,
) error {
	diff := toHeight - fromHeight

	if err := util.BatchWork(
		context.Background(),
		uint64(diff.Int64())+1,
		333, //nolint:gomnd //...
		func(context.Context, uint64) error {
			return nil
		},
		func(_ context.Context, i, _ uint64) error {
			height := base.Height(int64(i) + fromHeight.Int64())

			var mapdb base.BlockMap
			switch i, found, err := db.BlockMap(height); {
			case err != nil:
				return err
			case !found:
				return util.ErrNotFound.Errorf("blockmap not found in database; %d", height)
			default:
				mapdb = i
			}

			err := ValidateBlockFromLocalFS(height, root, enc, params.NetworkID(),
				func(m base.BlockMap) error {
					return base.IsEqualBlockMap(mapdb, m)
				},
				func(op base.Operation) error {
					switch found, err := db.ExistsKnownOperation(op.Hash()); {
					case err != nil:
						return err
					case !found:
						return util.ErrNotFound.Errorf("operation not found in database; %q", op.Hash())
					default:
						return nil
					}
				},
				func(st base.State) error {
					switch rst, found, err := db.State(st.Key()); {
					case err != nil:
						return err
					case !found:
						return util.ErrNotFound.Errorf("state not found in State")
					case !base.IsEqualState(st, rst):
						return errors.Errorf("states does not match")
					}

					ops := st.Operations()
					for j := range ops {
						switch found, err := db.ExistsInStateOperation(ops[j]); {
						case err != nil:
							return err
						case !found:
							return util.ErrNotFound.Errorf("operation of state not found in database")
						}
					}

					return nil
				},
			)

			if whenBlockDonef == nil {
				return err
			}

			return whenBlockDonef(mapdb, err)
		},
	); err != nil {
		return errors.WithMessage(err, "validate imported blocks")
	}

	return nil
}
