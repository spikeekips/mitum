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
	ErrLastBlockMapOnlyInDatabase = util.NewMError("last blockmap found in database, but not in localfs")
	ErrLastBlockMapOnlyInLocalFS  = util.NewMError("last blockmap found in localfs, but not in database")
)

type ErrorValidatedDifferentHeightBlockMaps struct {
	util.MError
	db      base.Height
	localfs base.Height
}

func newErrorValidatedDifferentHeightBlockMaps(db, localfs base.Height) ErrorValidatedDifferentHeightBlockMaps {
	return ErrorValidatedDifferentHeightBlockMaps{
		MError:  util.NewIDMError("dhb", "different height blockmaps"),
		db:      db,
		localfs: localfs,
	}
}

func (err ErrorValidatedDifferentHeightBlockMaps) DatabaseHeight() base.Height {
	return err.db
}

func (err ErrorValidatedDifferentHeightBlockMaps) LocalFSHeight() base.Height {
	return err.localfs
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
		return errors.WithMessage(err, "failed LastBlockMap from database")
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
		return errors.WithMessage(err, "failed to find last height from localfs")
	case !found:
	default:
		lastmaplocalfs = last
	}

	switch {
	case lastmapdb == nil && lastmaplocalfs == nil:
		return nil
	case lastmapdb != nil && lastmaplocalfs == nil:
		return ErrLastBlockMapOnlyInDatabase.Call()
	case lastmapdb == nil && lastmaplocalfs != nil:
		return ErrLastBlockMapOnlyInLocalFS.Call()
	default:
		if err := base.IsEqualBlockMap(lastmapdb, lastmaplocalfs); err != nil {
			if lastmapdb.Manifest().Height() != lastmaplocalfs.Manifest().Height() {
				err = newErrorValidatedDifferentHeightBlockMaps(
					lastmapdb.Manifest().Height(),
					lastmaplocalfs.Manifest().Height(),
				)
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
	e := util.StringErrorFunc("failed to validate localfs")

	switch fi, err := os.Stat(dataroot); {
	case err == nil:
		if !fi.IsDir() {
			return e(nil, "not directory")
		}
	case os.IsNotExist(err):
		return e(err, "")
	default:
		return e(err, "")
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
				return newErrorValidatedDifferentHeightBlockMaps(height, m.Manifest().Height())
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
		return e(err, "")
	}

	maps = nil

	return nil
}

func ValidateBlockFromLocalFS(
	height base.Height,
	dataroot string,
	enc encoder.Encoder,
	networkID base.NetworkID,
	db isaac.Database,
) error {
	e := util.StringErrorFunc("failed to validate imported block")

	reader, err := NewLocalFSReaderFromHeight(dataroot, height, enc)
	if err != nil {
		return e(err, "")
	}

	var m base.BlockMap

	switch i, found, err := loadBlockMapFromReader(reader, networkID); {
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
			readererr = util.ErrNotFound.Errorf("empty BlockMapItem found, %q", item.Type())
		}

		if readererr != nil {
			return false
		}

		switch item.Type() {
		case base.BlockMapItemTypeProposal:
			pr := i.(base.ProposalSignFact) //nolint:forcetypeassert //...

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
			readererr = validateVoteproofsFromLocalFS( //nolint:forcetypeassert //...
				networkID,
				i.([]base.Voteproof),
				m.Manifest(),
			)
		}

		return readererr == nil
	}); err != nil {
		readererr = err
	}

	if readererr == nil {
		readererr = ValidateOperationsOfBlock(opstree, ops, m.Manifest(), db, networkID)
	}

	if readererr == nil {
		readererr = ValidateStatesOfBlock(ststree, sts, m.Manifest(), db, networkID)
	}

	return readererr
}

func ValidateOperationsOfBlock(
	opstree fixedtree.Tree,
	ops []base.Operation,
	manifest base.Manifest,
	db isaac.Database,
	networkID base.NetworkID,
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
				op := ops[i]

				if err := op.IsValid(networkID); err != nil {
					return err
				}

				switch found, err := db.ExistsKnownOperation(op.Hash()); {
				case err != nil:
					return err
				case !found:
					return util.ErrNotFound.Errorf("operation not found in ExistsKnownOperation; %q", op.Hash())
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

func ValidateStatesOfBlock(
	ststree fixedtree.Tree,
	sts []base.State,
	manifest base.Manifest,
	db isaac.Database,
	networkID base.NetworkID,
) error {
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

				if err := st.IsValid(networkID); err != nil {
					return err
				}

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
