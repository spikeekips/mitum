package launch

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	redisstorage "github.com/spikeekips/mitum/storage/redis"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
	leveldbStorage "github.com/syndtr/goleveldb/leveldb/storage"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

var (
	PNameStorage                          = ps.Name("storage")
	PNameStartStorage                     = ps.Name("start-storage")
	PNameCheckLeveldbStorage              = ps.Name("check-leveldb-storage")
	PNameCheckLoadFromDatabase            = ps.Name("load-from-database")
	PNameCleanStorage                     = ps.Name("clean-storage")
	PNameCreateLocalFS                    = ps.Name("create-localfs")
	PNameCheckLocalFS                     = ps.Name("check-localfs")
	PNameLoadDatabase                     = ps.Name("load-database")
	PNameGetSuffrageFromDatabaseeFunc     = ps.Name("get-suffrage-from-database")
	FSNodeInfoContextKey                  = util.ContextKey("fs-node-info")
	LeveldbStorageContextKey              = util.ContextKey("leveldb-storage")
	CenterDatabaseContextKey              = util.ContextKey("center-database")
	PermanentDatabaseContextKey           = util.ContextKey("permanent-database")
	PoolDatabaseContextKey                = util.ContextKey("pool-database")
	LastVoteproofsHandlerContextKey       = util.ContextKey("last-voteproofs-handler")
	GetSuffrageFromDatabaseFuncContextKey = util.ContextKey("get-suffrage-from-database")
)

var (
	LocalFSDataDirectoryName           = "data"
	LocalFSDatabaseDirectoryName       = "db"
	LeveldbURIScheme                   = "leveldb"
	RedisPermanentDatabasePrefixFormat = "mitum-%s"
)

func PStorage(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func PStartStorage(ctx context.Context) (context.Context, error) {
	var log *logging.Logging
	if err := util.LoadFromContextOK(ctx, LoggingContextKey, &log); err != nil {
		return ctx, nil
	}

	var starters []func()

	load := func(name string, key util.ContextKey, v interface{}) bool {
		switch err := util.LoadFromContext(ctx, key, v); {
		case err != nil:
			return false
		case v == nil:
			return false
		}

		d, ok := reflect.ValueOf(v).Elem().Interface().(util.Daemon)
		if ok {
			starters = append(starters, func() {
				if err := d.Start(context.Background()); err != nil {
					log.Log().Error().Err(err).Msgf("failed to start %s", name)
				}
			})
		}

		return true
	}

	var st *leveldbstorage.Storage
	_ = load("leveldb storage", LeveldbStorageContextKey, &st)

	var pool *isaacdatabase.TempPool
	_ = load("pool database", PoolDatabaseContextKey, &pool)

	var perm isaac.PermanentDatabase
	_ = load("permanent database", PermanentDatabaseContextKey, &perm)

	var db isaac.Database
	_ = load("center database", CenterDatabaseContextKey, &db)

	for i := range starters {
		starters[i]()
	}

	return ctx, nil
}

func PCloseStorage(ctx context.Context) (context.Context, error) {
	var log *logging.Logging
	if err := util.LoadFromContextOK(ctx, LoggingContextKey, &log); err != nil {
		return ctx, nil
	}

	var closers []func()
	var stoppers []func()

	load := func(name string, key util.ContextKey, v interface{}) bool {
		switch err := util.LoadFromContext(ctx, key, v); {
		case err != nil:
			return false
		case v == nil:
			return false
		}

		closer, ok := reflect.ValueOf(v).Elem().Interface().(io.Closer)
		if ok {
			closers = append(closers, func() {
				err := closer.Close()
				if err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
					log.Log().Error().Err(err).Msgf("failed to close %s", name)
				}
			})
		}

		d, ok := reflect.ValueOf(v).Elem().Interface().(util.Daemon)
		if ok {
			stoppers = append(stoppers, func() {
				err := d.Stop()
				if err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
					log.Log().Error().Err(err).Msgf("failed to stop %s", name)
				}
			})
		}

		return true
	}

	var db isaac.Database
	_ = load("center database", CenterDatabaseContextKey, &db)

	var pool *isaacdatabase.TempPool
	_ = load("pool database", PoolDatabaseContextKey, &pool)

	var perm isaac.PermanentDatabase
	_ = load("permanent database", PermanentDatabaseContextKey, &perm)

	var st *leveldbstorage.Storage
	_ = load("leveldb storage", LeveldbStorageContextKey, &st)

	for i := range stoppers {
		stoppers[len(stoppers)-i-1]()
	}

	for i := range closers {
		closers[i]()
	}

	return ctx, nil
}

func PCheckLeveldbStorage(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to check leveldb storage")

	var log *logging.Logging
	if err := util.LoadFromContextOK(ctx, LoggingContextKey, &log); err != nil {
		return ctx, e(err, "")
	}

	var st *leveldbstorage.Storage
	if err := util.LoadFromContextOK(ctx, LeveldbStorageContextKey, &st); err != nil {
		return ctx, e(err, "")
	}

	if err := st.DB().CompactRange(leveldbutil.Range{}); err != nil {
		return ctx, e(err, "")
	}

	log.Log().Debug().Msg("leveldb storage compacted")

	return ctx, nil
}

func PLoadFromDatabase(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to load some stuffs from database")

	var design NodeDesign
	var encs *encoder.Encoders
	var center isaac.Database

	if err := util.LoadFromContextOK(ctx,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		CenterDatabaseContextKey, &center,
	); err != nil {
		return ctx, e(err, "")
	}

	// NOTE load from last voteproofs
	lvps := isaacstates.NewLastVoteproofsHandler()
	ctx = context.WithValue(ctx, LastVoteproofsHandlerContextKey, lvps) //revive:disable-line:modifies-parameter

	var manifest base.Manifest
	var enc encoder.Encoder

	switch m, found, err := center.LastBlockMap(); {
	case err != nil:
		return ctx, e(err, "")
	case !found:
		return ctx, nil
	default:
		enc = encs.Find(m.Encoder())
		if enc == nil {
			return ctx, e(nil, "encoder of last blockmap not found")
		}

		manifest = m.Manifest()
	}

	reader, err := isaacblock.NewLocalFSReaderFromHeight(
		LocalFSDataDirectory(design.Storage.Base), manifest.Height(), enc,
	)
	if err != nil {
		return ctx, e(err, "")
	}

	defer func() {
		_ = reader.Close()
	}()

	switch v, found, err := reader.Item(base.BlockMapItemTypeVoteproofs); {
	case err != nil:
		return ctx, e(err, "")
	case !found:
		return ctx, e(nil, "last voteproofs not found in localfs")
	default:
		vps := v.([]base.Voteproof) //nolint:forcetypeassert //...

		lvps.Set(vps[0].(base.INITVoteproof))   //nolint:forcetypeassert //...
		lvps.Set(vps[1].(base.ACCEPTVoteproof)) //nolint:forcetypeassert //...
	}

	return ctx, nil
}

func PCleanStorage(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to clean storage")

	var design NodeDesign
	var encs *encoder.Encoders
	var enc encoder.Encoder

	if err := util.LoadFromContextOK(ctx,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
	); err != nil {
		return ctx, e(err, "")
	}

	if err := CleanStorage(design.Storage.Database.String(), design.Storage.Base, encs, enc); err != nil {
		return ctx, e(err, "")
	}

	return ctx, nil
}

func PCreateLocalFS(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to create localfs")

	var design NodeDesign
	var enc encoder.Encoder
	var params *isaac.LocalParams
	var version util.Version

	if err := util.LoadFromContextOK(ctx,
		DesignContextKey, &design,
		EncoderContextKey, &enc,
		LocalParamsContextKey, &params,
		VersionContextKey, &version,
	); err != nil {
		return ctx, e(err, "")
	}

	fsnodeinfo, err := CreateLocalFS(
		CreateDefaultNodeInfo(params.NetworkID(), version), design.Storage.Base, enc)
	if err != nil {
		return ctx, e(err, "")
	}

	ctx = context.WithValue(ctx, FSNodeInfoContextKey, fsnodeinfo) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func PCheckLocalFS(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to check localfs")

	var version util.Version
	var design NodeDesign
	var params *isaac.LocalParams
	var encs *encoder.Encoders
	var enc encoder.Encoder

	if err := util.LoadFromContextOK(ctx,
		VersionContextKey, &version,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
		LocalParamsContextKey, &params,
	); err != nil {
		return ctx, e(err, "")
	}

	fsnodeinfo, err := CheckLocalFS(params.NetworkID(), design.Storage.Base, enc)

	switch {
	case err == nil:
		if err = isaacblock.CleanBlockTempDirectory(LocalFSDataDirectory(design.Storage.Base)); err != nil {
			return ctx, e(err, "")
		}
	case errors.Is(err, os.ErrNotExist):
		if err = CleanStorage(
			design.Storage.Database.String(),
			design.Storage.Base,
			encs,
			enc,
		); err != nil {
			return ctx, e(err, "")
		}

		fsnodeinfo, err = CreateLocalFS(
			CreateDefaultNodeInfo(params.NetworkID(), version), design.Storage.Base, enc)
		if err != nil {
			return ctx, e(err, "")
		}
	default:
		return ctx, e(err, "")
	}

	ctx = context.WithValue(ctx, FSNodeInfoContextKey, fsnodeinfo) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func PLoadDatabase(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to load database")

	var log *logging.Logging
	var design NodeDesign
	var encs *encoder.Encoders
	var enc encoder.Encoder
	var fsnodeinfo NodeInfo

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
		FSNodeInfoContextKey, &fsnodeinfo,
	); err != nil {
		return ctx, e(err, "")
	}

	st, db, perm, pool, err := LoadDatabase(
		fsnodeinfo, design.Storage.Database.String(), design.Storage.Base, encs, enc)
	if err != nil {
		return ctx, e(err, "")
	}

	_ = db.SetLogging(log)
	//revive:disable:modifies-parameter
	ctx = context.WithValue(ctx, LeveldbStorageContextKey, st)
	ctx = context.WithValue(ctx, CenterDatabaseContextKey, db)
	ctx = context.WithValue(ctx, PermanentDatabaseContextKey, perm)
	ctx = context.WithValue(ctx, PoolDatabaseContextKey, pool)
	//revive:enable:modifies-parameter

	return ctx, nil
}

func LastHeightOfLocalFS(ctx context.Context, from string) (last base.Height, _ error) {
	e := util.StringErrorFunc("failed to find last height from localfs")

	var enc encoder.Encoder
	var params *isaac.LocalParams

	if err := util.LoadFromContextOK(ctx,
		EncoderContextKey, &enc,
		LocalParamsContextKey, &params,
	); err != nil {
		return last, e(err, "")
	}

	last, err := FindLastHeightFromLocalFS(from, enc, params.NetworkID())
	if err != nil {
		return last, e(err, "")
	}

	if err := ValidateLocalFS(from, enc, last); err != nil {
		return last, e(err, "")
	}

	return last, nil
}

func LoadPermanentDatabase(
	uri, id string, encs *encoder.Encoders, enc encoder.Encoder, root string,
) (*leveldbstorage.Storage, isaac.PermanentDatabase, error) {
	e := util.StringErrorFunc("failed to load PermanentDatabase")

	u, err := url.Parse(uri)

	var dbtype, network string

	switch {
	case err != nil:
		return nil, nil, e(err, "")
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
			u.Path = LocalFSDatabaseDirectory(root)
		}

		str, err := leveldbStorage.OpenFile(u.Path, false)
		if err != nil {
			return nil, nil, e(err, "")
		}

		st, err := leveldbstorage.NewStorage(str, nil)
		if err != nil {
			return nil, nil, e(err, "")
		}

		perm, err := isaacdatabase.NewLeveldbPermanent(st, encs, enc)
		if err != nil {
			return nil, nil, e(err, "")
		}

		return st, perm, nil
	case dbtype == "redis":
		if strings.Contains(u.Scheme, "+") {
			u.Scheme = network
		}

		if len(u.Scheme) < 1 {
			u.Scheme = "redis"
		}

		perm, err := loadRedisPermanentDatabase(u.String(), id, encs, enc)
		if err != nil {
			return nil, nil, e(err, "failed to create redis PermanentDatabase")
		}

		return nil, perm, nil
	default:
		return nil, nil, e(nil, "unsupported database type, %q", dbtype)
	}
}

func CleanStorage(
	permuri, root string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) error {
	e := util.StringErrorFunc("failed to clean storage")

	switch fsnodeinfo, found, err := LoadNodeInfo(root, enc); {
	case err != nil:
		return e(err, "")
	case !found:
	default:
		_, perm, err := LoadPermanentDatabase(permuri, fsnodeinfo.ID(), encs, enc, root)
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

func RemoveLocalFS(root string) error {
	knowns := map[string]struct{}{
		LocalFSDataDirectoryName:     {},
		LocalFSDatabaseDirectoryName: {},
	}

	if err := util.CleanDirectory(root, func(name string) bool {
		_, found := knowns[name]

		return found
	}); err != nil {
		return errors.Wrap(err, "failed to remove localfs")
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

	for _, i := range []string{
		LocalFSDataDirectory(root),
		LocalFSDatabaseDirectory(root),
	} {
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

	var fsnodeinfo NodeInfo

	switch i, found, err := LoadNodeInfo(root, enc); {
	case err != nil:
		return nil, e(err, "")
	case !found: // NOTE if not found, create new one
		fsnodeinfo = newinfo
	case !i.NetworkID().Equal(newinfo.NetworkID()):
		return nil, e(nil, "network id does not match")
	default:
		fsnodeinfo = i
	}

	if err := SaveNodeInfo(root, fsnodeinfo); err != nil {
		return nil, e(err, "")
	}

	return fsnodeinfo, nil
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

func LocalFSDataDirectory(root string) string {
	return filepath.Join(root, LocalFSDataDirectoryName)
}

func LocalFSDatabaseDirectory(root string) string {
	return filepath.Join(root, LocalFSDatabaseDirectoryName)
}

func LoadDatabase(
	fsnodeinfo NodeInfo,
	permuri string,
	root string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (
	*leveldbstorage.Storage,
	*isaacdatabase.Center,
	isaac.PermanentDatabase,
	*isaacdatabase.TempPool,
	error,
) {
	e := util.StringErrorFunc("failed to prepare database")

	st, perm, err := LoadPermanentDatabase(permuri, fsnodeinfo.ID(), encs, enc, root)

	switch {
	case err != nil:
		return nil, nil, nil, nil, e(err, "")
	case st == nil:
		var str leveldbStorage.Storage

		str, err = leveldbStorage.OpenFile(LocalFSDatabaseDirectory(root), false)
		if err != nil {
			return nil, nil, nil, nil, e(err, "")
		}

		st, err = leveldbstorage.NewStorage(str, nil)
		if err != nil {
			return nil, nil, nil, nil, e(err, "")
		}
	}

	db, err := isaacdatabase.NewCenter(
		st,
		encs,
		enc,
		perm,
		func(height base.Height) (isaac.BlockWriteDatabase, error) {
			return isaacdatabase.NewLeveldbBlockWrite(height, st, encs, enc), nil
		},
	)
	if err != nil {
		return nil, nil, nil, nil, e(err, "")
	}

	if err = isaacdatabase.CleanSyncPool(st); err != nil {
		return nil, nil, nil, nil, e(err, "")
	}

	if err = db.MergeAllPermanent(); err != nil {
		return nil, nil, nil, nil, e(err, "")
	}

	pool, err := isaacdatabase.NewTempPool(st, encs, enc)
	if err != nil {
		return nil, nil, nil, nil, e(err, "")
	}

	return st, db, perm, pool, nil
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

	for _, i := range []string{
		LocalFSDataDirectory(root),
		LocalFSDatabaseDirectory(root),
	} {
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
				return util.ErrNotFound.Errorf("BlockMap not found")
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

func PGetSuffrageFromDatabaseFunc(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to create GetSuffrageFromDatabaseFunc")

	var db isaac.Database

	if err := util.LoadFromContextOK(ctx, CenterDatabaseContextKey, &db); err != nil {
		return ctx, e(err, "")
	}

	sufcache := util.NewGCacheObjectPool(1 << 9) //nolint:gomnd //...

	var l sync.Mutex

	f := func(height base.Height) (base.Suffrage, bool, error) {
		if i, found := sufcache.Get(height.String()); found {
			return i.(base.Suffrage), true, nil //nolint:forcetypeassert //...
		}

		l.Lock()
		defer l.Unlock()

		i, found, err := isaac.GetSuffrageFromDatabase(db, height)
		if err != nil || !found {
			return nil, false, err
		}

		sufcache.Set(height.String(), i, nil)

		return i, true, nil
	}

	return context.WithValue(ctx, GetSuffrageFromDatabaseFuncContextKey, isaac.GetSuffrageByBlockHeight(f)), nil
}
