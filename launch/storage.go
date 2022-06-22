package launch

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	redisstorage "github.com/spikeekips/mitum/storage/redis"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

var (
	LocalFSPermDirectoryName = "perm"
	LocalFSTempDirectoryName = "temp"
	LocalFSDataDirectoryName = "data"
	LocalFSPoolDirectoryName = "pool"

	RedisPermanentDatabasePrefixFormat = "mitum-%s"
	LeveldbURIScheme                   = "leveldb"
)

func CleanStorage(permuri, root string, encs *encoder.Encoders, enc encoder.Encoder) error {
	e := util.StringErrorFunc("failed to clean storage")

	switch nodeinfo, found, err := LoadNodeInfo(root, enc); {
	case err != nil:
		return e(err, "")
	case !found:
	default:
		perm, err := LoadPermanentDatabase(permuri, nodeinfo.ID(), encs, enc)
		if err == nil {
			if err := perm.Clean(); err != nil {
				return e(err, "")
			}
		}
	}

	if err := RemoveLocalFS(root); err != nil {
		return e(err, "")
	}

	return nil
}

func CreateLocalFS(newinfo NodeInfo, root string, enc encoder.Encoder) (NodeInfo, error) {
	e := util.StringErrorFunc("failed to initialize localfs")

	switch fi, err := os.Stat(root); {
	case err == nil:
		if !fi.IsDir() {
			return nil, e(nil, "root is not directory")
		}
	case os.IsNotExist(err):
		if err = os.MkdirAll(root, 0o700); err != nil {
			return nil, e(err, "")
		}
	default:
		return nil, e(err, "")
	}

	temproot := LocalFSTempDirectory(root)
	poolroot := LocalFSPoolDirectory(root)
	dataroot := LocalFSDataDirectory(root)

	for _, i := range []string{temproot, poolroot, dataroot} {
		switch fi, err := os.Stat(i); {
		case err == nil:
			if !fi.IsDir() {
				return nil, e(nil, "root is not directory, %q", i)
			}

			return nil, e(nil, "directory already exists, %q", i)
		case os.IsNotExist(err):
			if err = os.MkdirAll(i, 0o700); err != nil {
				return nil, e(err, "failed to make directory, %i", i)
			}
		default:
			return nil, e(err, "")
		}
	}

	var nodeinfo NodeInfo

	switch i, found, err := LoadNodeInfo(root, enc); {
	case err != nil:
		return nil, e(err, "")
	case !found: // NOTE if not found, create new one
		nodeinfo = newinfo
	case !i.NetworkID().Equal(newinfo.NetworkID()):
		return nil, e(nil, "network id does not match")
	default:
		nodeinfo = i
	}

	if err := SaveNodeInfo(root, nodeinfo); err != nil {
		return nil, e(err, "")
	}

	return nodeinfo, nil
}

func CheckLocalFS(networkID base.NetworkID, root string, enc encoder.Encoder) (NodeInfo, error) {
	e := util.StringErrorFunc("failed to check localfs")

	switch fi, err := os.Stat(root); {
	case err == nil:
		if !fi.IsDir() {
			return nil, e(nil, "root is not directory")
		}
	default:
		return nil, e(err, "")
	}

	temproot := LocalFSTempDirectory(root)
	poolroot := LocalFSPoolDirectory(root)
	dataroot := LocalFSDataDirectory(root)

	for _, i := range []string{temproot, poolroot, dataroot} {
		switch fi, err := os.Stat(i); {
		case err == nil:
			if !fi.IsDir() {
				return nil, e(nil, "root is not directory, %q", i)
			}
		default:
			return nil, e(err, "")
		}
	}

	switch info, found, err := LoadNodeInfo(root, enc); {
	case err != nil:
		return nil, e(err, "")
	case !found:
		return nil, e(util.ErrNotFound.Errorf("NodeInfo not found"), "")
	case !info.NetworkID().Equal(networkID):
		return nil, e(nil, "network id does not match")
	default:
		return info, nil
	}
}

// RemoveLocalFS removes basic directories and files except perm and nodeinfo
// file.
func RemoveLocalFS(root string) error {
	knowns := map[string]struct{}{
		LocalFSTempDirectoryName: {},
		LocalFSPoolDirectoryName: {},
		LocalFSDataDirectoryName: {},
	}

	if err := util.CleanDirectory(root, func(name string) bool {
		_, found := knowns[name]

		return found
	}); err != nil {
		return errors.Wrap(err, "failed to remove localfs")
	}

	return nil
}

func LoadDatabase(
	nodeinfo NodeInfo,
	permuri string,
	localfsroot string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*isaacdatabase.Default, isaac.PermanentDatabase, *isaacdatabase.TempPool, error) {
	e := util.StringErrorFunc("failed to prepare database")

	perm, err := LoadPermanentDatabase(permuri, nodeinfo.ID(), encs, enc)
	if err != nil {
		return nil, nil, nil, e(err, "")
	}

	temproot := LocalFSTempDirectory(localfsroot)
	poolroot := LocalFSPoolDirectory(localfsroot)

	db, err := isaacdatabase.NewDefault(
		temproot, encs, enc, perm, func(height base.Height) (isaac.BlockWriteDatabase, error) {
			newroot, eerr := isaacdatabase.NewTempDirectory(temproot, height)
			if eerr != nil {
				return nil, errors.Wrap(eerr, "")
			}

			return isaacdatabase.NewLeveldbBlockWrite(height, newroot, encs, enc)
		})
	if err != nil {
		return nil, nil, nil, e(err, "")
	}

	if err = CleanTempSyncPoolDatabase(localfsroot); err != nil {
		return nil, nil, nil, e(err, "")
	}

	if err = db.MergeAllPermanent(); err != nil {
		return nil, nil, nil, e(err, "")
	}

	pool, err := isaacdatabase.NewTempPool(poolroot, encs, enc)
	if err != nil {
		return nil, nil, nil, e(err, "")
	}

	return db, perm, pool, nil
}

func CleanTempSyncPoolDatabase(localfsroot string) error {
	e := util.StringErrorFunc("failed to clean syncpool database directories")

	temproot := LocalFSTempDirectory(localfsroot)

	matches, err := filepath.Glob(filepath.Join(filepath.Clean(temproot), "syncpool-*"))
	if err != nil {
		return e(err, "")
	}

	for i := range matches {
		if err := os.RemoveAll(matches[i]); err != nil {
			return e(err, "")
		}
	}

	return nil
}

func NewTempSyncPoolDatabase(
	localfsroot string, height base.Height, encs *encoder.Encoders, enc encoder.Encoder,
) (isaac.TempSyncPool, error) {
	temproot := LocalFSTempDirectory(localfsroot)

	root := isaacdatabase.NewSyncPoolDirectory(temproot, height)

	syncpool, err := isaacdatabase.NewLeveldbTempSyncPool(root, encs, enc)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new TempSyncPool")
	}

	return syncpool, nil
}

func LoadPermanentDatabase(
	uri, id string, encs *encoder.Encoders, enc encoder.Encoder,
) (isaac.PermanentDatabase, error) {
	e := util.StringErrorFunc("failed to load PermanentDatabase")

	u, err := url.Parse(uri)

	var dbtype, network string

	switch {
	case err != nil:
		return nil, e(err, "")
	case len(u.Scheme) < 1, strings.EqualFold(u.Scheme, LeveldbURIScheme):
		dbtype = LeveldbURIScheme
	default:
		u.Scheme = strings.ToLower(u.Scheme)

		l := strings.SplitN(u.Scheme, "+", 2)
		dbtype = l[0]

		if len(l) > 1 {
			network = l[1]
		}
	}

	switch {
	case dbtype == LeveldbURIScheme:
		if len(u.Path) < 1 {
			return nil, e(nil, "empty path")
		}

		perm, err := isaacdatabase.NewLeveldbPermanent(u.Path, encs, enc)
		if err != nil {
			return nil, e(err, "")
		}

		return perm, nil
	case dbtype == "redis":
		if strings.Contains(u.Scheme, "+") {
			u.Scheme = network
		}

		if len(u.Scheme) < 1 {
			u.Scheme = "redis"
		}

		perm, err := loadRedisPermanentDatabase(u.String(), id, encs, enc)
		if err != nil {
			return nil, e(err, "failed to create redis PermanentDatabase")
		}

		return perm, nil
	default:
		return nil, e(nil, "unsupported database type, %q", dbtype)
	}
}

func loadRedisPermanentDatabase(uri, id string, encs *encoder.Encoders, enc encoder.Encoder) (
	*isaacdatabase.RedisPermanent, error,
) {
	e := util.StringErrorFunc("failed to load redis PermanentDatabase")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2) //nolint:gomnd //...
	defer cancel()

	option, err := redis.ParseURL(uri)
	if err != nil {
		return nil, e(err, "invalid redis url")
	}

	st, err := redisstorage.NewStorage(ctx, option, fmt.Sprintf(RedisPermanentDatabasePrefixFormat, id))
	if err != nil {
		return nil, e(err, "failed to create redis storage")
	}

	perm, err := isaacdatabase.NewRedisPermanent(st, encs, enc)
	if err != nil {
		return nil, e(err, "")
	}

	return perm, nil
}

func LocalFSPermDatabaseURI(root string) string {
	return LeveldbURIScheme + "//" + filepath.Join(root, LocalFSPermDirectoryName)
}

func LocalFSTempDirectory(root string) string {
	return filepath.Join(root, LocalFSTempDirectoryName)
}

func LocalFSDataDirectory(root string) string {
	return filepath.Join(root, LocalFSDataDirectoryName)
}

func LocalFSPoolDirectory(root string) string {
	return filepath.Join(root, LocalFSPoolDirectoryName)
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

func FindLastHeightFromLocalFS(
	dataroot string, enc encoder.Encoder, networkID base.NetworkID,
) (last base.Height, _ error) {
	e := util.StringErrorFunc("failed to find last height from localfs")

	last = base.NilHeight

	// NOTE check genesis first
	fsreader, err := isaacblock.NewLocalFSReaderFromHeight(dataroot, base.GenesisHeight, enc)
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

	switch h, found, err := isaacblock.FindHighestDirectory(dataroot); {
	case err != nil:
		return last, e(err, "")
	case !found:
		return last, util.ErrNotFound.Errorf("height directories not found")
	default:
		rel, err := filepath.Rel(dataroot, h)
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

func ValidateLocalFS(
	dataroot string,
	enc encoder.Encoder,
	last base.Height,
) error {
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

			reader, err := isaacblock.NewLocalFSReaderFromHeight(dataroot, height, enc)
			if err != nil {
				return err
			}

			switch m, found, err := reader.BlockMap(); {
			case err != nil:
				return err
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

func ValidateBlockFromLocalFS(
	height base.Height,
	dataroot string,
	enc encoder.Encoder,
	networkID base.NetworkID,
	perm isaac.PermanentDatabase,
) error {
	e := util.StringErrorFunc("failed to validate imported block")

	reader, err := isaacblock.NewLocalFSReaderFromHeight(dataroot, height, enc)
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
		readererr = ValidateOperationsOfBlock(opstree, ops, m.Manifest(), perm)
	}

	if readererr == nil {
		readererr = ValidateStatesOfBlock(ststree, sts, m.Manifest(), perm)
	}

	return readererr
}

func ValidateOperationsOfBlock(
	opstree fixedtree.Tree,
	ops []base.Operation,
	manifest base.Manifest,
	perm isaac.PermanentDatabase,
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
				switch found, err := perm.ExistsKnownOperation(ops[i].Hash()); {
				case err != nil:
					return err
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

func ValidateStatesOfBlock(
	ststree fixedtree.Tree,
	sts []base.State,
	manifest base.Manifest,
	perm isaac.PermanentDatabase,
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

				switch rst, found, err := perm.State(st.Key()); {
				case err != nil:
					return err
				case !found:
					return util.ErrNotFound.Errorf("state not found in State")
				case !base.IsEqualState(st, rst):
					return errors.Errorf("states does not match")
				}

				ops := st.Operations()
				for j := range ops {
					switch found, err := perm.ExistsInStateOperation(ops[j]); {
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
