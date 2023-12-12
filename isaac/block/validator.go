package isaacblock

import (
	"context"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
)

var (
	ErrLastBlockMapOnlyInDatabase = util.NewIDError("last blockmap found in database, but not in local fs")
	ErrLastBlockMapOnlyInLocalFS  = util.NewIDError("last blockmap found in local fs, but not in database")
)

type ErrValidatedDifferentHeightBlockMaps struct {
	*util.IDError
	db      base.Height
	localfs base.Height
}

func newErrValidatedDifferentHeightBlockMaps(db, localfs base.Height) *ErrValidatedDifferentHeightBlockMaps {
	return &ErrValidatedDifferentHeightBlockMaps{
		IDError: util.NewBaseIDErrorWithID("dhb", "different height blockmaps"),
		db:      db,
		localfs: localfs,
	}
}

func (er *ErrValidatedDifferentHeightBlockMaps) Wrap(err error) error {
	ner := er.IDError.Wrap(err)
	if ner == nil {
		return nil
	}

	return &ErrValidatedDifferentHeightBlockMaps{
		IDError: ner.(*util.IDError), //nolint:forcetypeassert //...
		db:      er.db,
		localfs: er.localfs,
	}
}

func (er *ErrValidatedDifferentHeightBlockMaps) WithMessage(err error, format string, args ...interface{}) error {
	ner := er.IDError.WithMessage(err, format, args...)
	if ner == nil {
		return nil
	}

	return &ErrValidatedDifferentHeightBlockMaps{
		IDError: ner.(*util.IDError), //nolint:forcetypeassert //...
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
	readers *isaac.BlockItemReaders,
	remotes isaac.RemotesBlockItemReadFunc,
	db isaac.Database,
	networkID base.NetworkID,
) error {
	var lastmapdb, lastmaplocalfs base.BlockMap

	switch i, found, err := loadLastBlockMapFromDatabase(db, networkID); {
	case err != nil:
		return errors.WithMessage(err, "last BlockMap from database")
	case !found:
	default:
		lastmapdb = i
	}

	switch last, found, err := loadLastBlockMapFromLocalFS(readers, remotes); {
	case err != nil:
		return errors.WithMessage(err, "find last height from local fs")
	case !found:
	default:
		if err := last.IsValid(networkID); err != nil {
			return errors.WithMessage(err, "invalid last height from local fs")
		}

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
	readers *isaac.BlockItemReaders,
	remotes isaac.RemotesBlockItemReadFunc,
) (base.BlockMap, bool, error) {
	var last base.Height

	switch i, _, found, err := FindHighestDirectory(readers.Root()); {
	case err != nil:
		return nil, false, err
	case !found:
		return nil, false, nil
	default:
		last = i
	}

	return isaac.BlockItemReadersDecode[base.BlockMap](
		isaac.BlockItemReadersItemFuncWithRemote(readers, remotes, nil)(context.Background()),
		last,
		base.BlockItemMap,
		nil,
	)
}

func ValidateAllBlockMapsFromLocalFS(
	readers *isaac.BlockItemReaders,
	last base.Height,
	networkID base.NetworkID,
) error {
	e := util.StringError("validate local fs")

	switch fi, err := os.Stat(readers.Root()); {
	case err != nil:
		return e.Wrap(err)
	case !fi.IsDir():
		return e.Errorf("not directory")
	}

	// NOTE check all block items
	var validateLock sync.Mutex
	var lastprev, newprev base.BlockMap
	var maps []base.BlockMap

	var batchlimit int64 = 333 //nolint:gomnd //...

	if err := util.BatchWork(context.Background(), last.Int64()+1, batchlimit,
		func(_ context.Context, last uint64) error {
			lastprev = newprev

			switch r := (last + 1) % uint64(batchlimit); {
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

			switch i, found, err := isaac.BlockItemReadersDecode[base.BlockMap](
				readers.Item, height, base.BlockItemMap, nil); {
			case err != nil:
				return err
			case !found:
				return util.ErrNotFound.Errorf("blockmap")
			default:
				if err := i.IsValid(networkID); err != nil {
					return err
				}

				m = i
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
	itemf isaac.BlockItemReadersItemFunc,
	height base.Height,
	networkID base.NetworkID,
	validateBlockMapf func(base.BlockMap) error,
	validateOperationf func(base.Operation) error,
	validateStatef func(base.State) error,
) error {
	e := util.StringError("validate imported block")

	var bm base.BlockMap

	switch i, found, err := isaac.BlockItemReadersDecode[base.BlockMap](itemf, height, base.BlockItemMap, nil); {
	case err != nil:
		return e.Wrap(err)
	case !found:
		return e.Wrap(util.ErrNotFound.Errorf("blockmap"))
	default:
		if err := i.IsValid(networkID); err != nil {
			return err
		}

		if validateBlockMapf != nil {
			if err := validateBlockMapf(i); err != nil {
				return err
			}
		}

		bm = i
	}

	pr, ops, sts, opstree, ststree, vps, err := loadBlockItemsFromReader(bm, itemf, height)
	if err != nil {
		return err
	}

	if err := pr.IsValid(networkID); err != nil {
		return err
	}

	if err := base.ValidateProposalWithManifest(pr, bm.Manifest()); err != nil {
		return err
	}

	if err := ValidateOperationsOfBlock(opstree, ops, bm.Manifest(), networkID, validateOperationf); err != nil {
		return err
	}

	if err := ValidateStatesOfBlock(ststree, sts, bm.Manifest(), networkID, validateStatef); err != nil {
		return err
	}

	return validateVoteproofsFromLocalFS(networkID, vps, bm.Manifest())
}

func loadBlockItemsFromReader( //revive:disable-line:function-result-limit
	bm base.BlockMap,
	itemf isaac.BlockItemReadersItemFunc,
	height base.Height,
) (
	pr base.ProposalSignFact,
	ops []base.Operation,
	sts []base.State,
	opstree, ststree fixedtree.Tree,
	vps [2]base.Voteproof,
	_ error,
) {
	load := func(item base.BlockItemType) error {
		switch item {
		case base.BlockItemProposal:
			return decodeBlockItemFromReader[base.ProposalSignFact](itemf, height, item, &pr)
		case base.BlockItemOperationsTree:
			return decodeBlockItemFromReader[fixedtree.Tree](itemf, height, item, &opstree)
		case base.BlockItemStatesTree:
			return decodeBlockItemFromReader[fixedtree.Tree](itemf, height, item, &ststree)
		case base.BlockItemVoteproofs:
			return decodeBlockItemFromReader[[2]base.Voteproof](itemf, height, item, &vps)
		case base.BlockItemOperations:
			return decodeBlockItemsFromReader[base.Operation](itemf, height, item, &ops)
		case base.BlockItemStates:
			return decodeBlockItemsFromReader[base.State](itemf, height, item, &sts)
		default:
			return errors.Errorf("unknown item, %q", item)
		}
	}

	var rerr error

	bm.Items(func(item base.BlockMapItem) bool {
		rerr = load(item.Type())

		return rerr == nil
	})

	return pr, ops, sts, opstree, ststree, vps, rerr
}

func decodeBlockItemFromReader[T any](
	itemf isaac.BlockItemReadersItemFunc,
	height base.Height,
	item base.BlockItemType,
	v *T,
) error {
	switch i, found, err := isaac.BlockItemReadersDecode[T](itemf, height, item, nil); {
	case err != nil:
		return err
	case !found:
		return util.ErrNotFound.Errorf("block item, %q", item)
	default:
		return util.SetInterfaceValue(i, v)
	}
}

func decodeBlockItemsFromReader[T any](
	itemf isaac.BlockItemReadersItemFunc,
	height base.Height,
	item base.BlockItemType,
	v *[]T,
) error {
	switch _, i, found, err := isaac.BlockItemReadersDecodeItems[T](itemf, height, item, nil, nil); {
	case err != nil:
		return err
	case !found:
		return util.ErrNotFound.Errorf("block item, %q", item)
	default:
		return util.SetInterfaceValue(i, v)
	}
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
		if err := util.BatchWork(context.Background(), int64(len(ops)), 333, //nolint:gomnd //...
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
		if err := util.BatchWork(context.Background(), int64(len(sts)), 333, //nolint:gomnd //...
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

func validateVoteproofsFromLocalFS(networkID base.NetworkID, vps [2]base.Voteproof, m base.Manifest) error {
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
	itemf isaac.BlockItemReadersItemFunc,
	fromHeight, toHeight base.Height,
	networkID base.NetworkID,
	db isaac.Database,
	whenBlockDonef func(base.BlockMap, error) error,
) error {
	diff := toHeight - fromHeight

	if err := util.BatchWork(
		context.Background(),
		diff.Int64()+1,
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

			err := ValidateBlockFromLocalFS(itemf, height, networkID,
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
