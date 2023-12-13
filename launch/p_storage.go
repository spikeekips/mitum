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
	"time"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	redisstorage "github.com/spikeekips/mitum/storage/redis"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
	leveldbStorage "github.com/syndtr/goleveldb/leveldb/storage"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

var (
	PNameStorage                    = ps.Name("storage")
	PNameStartStorage               = ps.Name("start-storage")
	PNameCheckLeveldbStorage        = ps.Name("check-leveldb-storage")
	PNameLoadFromDatabase           = ps.Name("load-from-database")
	PNameCleanStorage               = ps.Name("clean-storage")
	PNameCreateLocalFS              = ps.Name("create-localfs")
	PNameCheckLocalFS               = ps.Name("check-localfs")
	PNameLoadDatabase               = ps.Name("load-database")
	PNameCheckBlocksOfStorage       = ps.Name("check-blocks-of-storage")
	PNamePatchBlockItemReaders      = ps.Name("patch-block-item-readers")
	FSNodeInfoContextKey            = util.ContextKey("fs-node-info")
	LeveldbStorageContextKey        = util.ContextKey("leveldb-storage")
	CenterDatabaseContextKey        = util.ContextKey("center-database")
	PermanentDatabaseContextKey     = util.ContextKey("permanent-database")
	PoolDatabaseContextKey          = util.ContextKey("pool-database")
	LastVoteproofsHandlerContextKey = util.ContextKey("last-voteproofs-handler")
	EventLoggingContextKey          = util.ContextKey("event-log")
)

var (
	LocalFSDataDirectoryName           = "data"
	LocalFSDatabaseDirectoryName       = "db"
	LocalFSEventDatabaseDirectoryName  = "event"
	LeveldbURIScheme                   = "leveldb"
	RedisPermanentDatabasePrefixFormat = "mitum-%s"
)

func PStorage(pctx context.Context) (context.Context, error) {
	return pctx, nil
}

func PStartStorage(pctx context.Context) (context.Context, error) {
	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, LoggingContextKey, &log); err != nil {
		return pctx, nil
	}

	var starters []func()

	load := func(name string, key util.ContextKey, v interface{}) bool {
		switch err := util.LoadFromContext(pctx, key, v); {
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

	var readers *isaac.BlockItemReaders
	_ = load("block item readers", BlockItemReadersContextKey, &readers)

	for i := range starters {
		starters[i]()
	}

	return pctx, nil
}

func PCloseStorage(pctx context.Context) (context.Context, error) {
	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, LoggingContextKey, &log); err != nil {
		return pctx, nil
	}

	var closers []func()
	var stoppers []func()

	load := func(name string, key util.ContextKey, v interface{}) bool {
		switch err := util.LoadFromContextOK(pctx, key, v); {
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

	var eventlogging *EventLogging
	_ = load("leveldb storage", EventLoggingContextKey, &eventlogging)

	for i := range stoppers {
		stoppers[len(stoppers)-i-1]()
	}

	for i := range closers {
		closers[i]()
	}

	return pctx, nil
}

func PCheckLeveldbStorage(pctx context.Context) (context.Context, error) {
	e := util.StringError("check leveldb storage")

	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, LoggingContextKey, &log); err != nil {
		return pctx, e.Wrap(err)
	}

	var st *leveldbstorage.Storage
	if err := util.LoadFromContextOK(pctx, LeveldbStorageContextKey, &st); err != nil {
		return pctx, e.Wrap(err)
	}

	if err := st.DB().CompactRange(leveldbutil.Range{}); err != nil {
		return pctx, e.Wrap(err)
	}

	log.Log().Debug().Msg("leveldb storage compacted")

	return pctx, nil
}

func PLoadFromDatabase(pctx context.Context) (context.Context, error) {
	e := util.StringError("load some stuffs from database")

	var design NodeDesign
	var encs *encoder.Encoders
	var center isaac.Database
	var readers *isaac.BlockItemReaders
	var fromRemotes isaac.RemotesBlockItemReadFunc

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		CenterDatabaseContextKey, &center,
		BlockItemReadersContextKey, &readers,
		RemotesBlockItemReaderFuncContextKey, &fromRemotes,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	// NOTE load from last voteproofs
	lvps := isaac.NewLastVoteproofsHandler()
	nctx := context.WithValue(pctx, LastVoteproofsHandlerContextKey, lvps)

	var bm base.BlockMap

	switch m, found, err := center.LastBlockMap(); {
	case err != nil:
		return nctx, e.Wrap(err)
	case !found:
		return nctx, nil
	default:
		bm = m
	}

	switch i, found, err := isaac.BlockItemReadersDecode[base.BlockMap](
		isaac.BlockItemReadersItemFuncWithRemote(readers, fromRemotes, nil)(context.Background()),
		bm.Manifest().Height(),
		base.BlockItemMap,
		nil,
	); {
	case err != nil:
		return nctx, e.Wrap(err)
	case !found:
		return nctx, e.Wrap(util.ErrNotFound.Errorf("blockmap in local fs"))
	default:
		if err := base.IsEqualBlockMap(bm, i); err != nil {
			return nctx, e.WithMessage(err, "different blockmap in db and local fs")
		}
	}

	switch vps, found, err := isaac.BlockItemReadersDecode[[2]base.Voteproof](
		isaac.BlockItemReadersItemFuncWithRemote(readers, fromRemotes, nil)(context.Background()),
		bm.Manifest().Height(),
		base.BlockItemVoteproofs,
		nil,
	); {
	case err != nil:
		return nctx, e.Wrap(err)
	case !found:
		return nctx, e.Wrap(util.ErrNotFound.Errorf("last voteproofs not found in local fs"))
	default:
		lvps.Set(vps[0].(base.INITVoteproof))   //nolint:forcetypeassert //...
		lvps.Set(vps[1].(base.ACCEPTVoteproof)) //nolint:forcetypeassert //...
	}

	return nctx, nil
}

func PCleanStorage(pctx context.Context) (context.Context, error) {
	e := util.StringError("clean storage")

	var design NodeDesign
	var encs *encoder.Encoders

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	if err := CleanStorage(
		design.Storage.Database.String(),
		design.Storage.Base,
		encs,
		encs.Default(),
	); err != nil {
		return pctx, e.Wrap(err)
	}

	return pctx, nil
}

func PCreateLocalFS(pctx context.Context) (context.Context, error) {
	e := util.StringError("create local fs")

	var design NodeDesign
	var encs *encoder.Encoders
	var isaacparams *isaac.Params
	var version util.Version

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		ISAACParamsContextKey, &isaacparams,
		VersionContextKey, &version,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	fsnodeinfo, err := CreateLocalFS(
		CreateDefaultNodeInfo(isaacparams.NetworkID(), version), design.Storage.Base, encs.Default())
	if err != nil {
		return pctx, e.Wrap(err)
	}

	return context.WithValue(pctx, FSNodeInfoContextKey, fsnodeinfo), nil
}

func PCheckLocalFS(pctx context.Context) (context.Context, error) {
	e := util.StringError("check local fs")

	var design NodeDesign
	var isaacparams *isaac.Params
	var encs *encoder.Encoders

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		ISAACParamsContextKey, &isaacparams,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	fsnodeinfo, err := CheckLocalFS(isaacparams.NetworkID(), design.Storage.Base, encs.Default())

	switch {
	case err == nil:
		if err = isaacblock.CleanBlockTempDirectory(LocalFSDataDirectory(design.Storage.Base)); err != nil {
			return pctx, e.Wrap(err)
		}
	case errors.Is(err, os.ErrNotExist):
		return pctx, e.Wrap(err)
	default:
		return pctx, e.Wrap(err)
	}

	return context.WithValue(pctx, FSNodeInfoContextKey, fsnodeinfo), nil
}

func PCheckAndCreateLocalFS(pctx context.Context) (context.Context, error) {
	e := util.StringError("check local fs")

	var version util.Version
	var design NodeDesign
	var isaacparams *isaac.Params
	var encs *encoder.Encoders

	if err := util.LoadFromContextOK(pctx,
		VersionContextKey, &version,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		ISAACParamsContextKey, &isaacparams,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	fsnodeinfo, err := CheckLocalFS(isaacparams.NetworkID(), design.Storage.Base, encs.Default())

	switch {
	case err == nil:
		if err = isaacblock.CleanBlockTempDirectory(LocalFSDataDirectory(design.Storage.Base)); err != nil {
			return pctx, e.Wrap(err)
		}
	case errors.Is(err, os.ErrNotExist):
		// NOTE database will be no cleaned.
		fsnodeinfo, err = CreateLocalFS(
			CreateDefaultNodeInfo(isaacparams.NetworkID(), version), design.Storage.Base, encs.Default())
		if err != nil {
			return pctx, e.Wrap(err)
		}
	default:
		return pctx, e.Wrap(err)
	}

	return context.WithValue(pctx, FSNodeInfoContextKey, fsnodeinfo), nil
}

func PLoadDatabase(pctx context.Context) (context.Context, error) {
	e := util.StringError("load database")

	var log *logging.Logging
	var design NodeDesign
	var isaacparams *isaac.Params
	var encs *encoder.Encoders
	var fsnodeinfo NodeInfo

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
		ISAACParamsContextKey, &isaacparams,
		EncodersContextKey, &encs,
		FSNodeInfoContextKey, &fsnodeinfo,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	st, db, perm, pool, err := LoadDatabase(
		fsnodeinfo,
		design.Storage.Database.String(),
		design.Storage.Base,
		encs,
		encs.Default(),
		isaacparams.StateCacheSize(),
		isaacparams.OperationPoolCacheSize(),
	)
	if err != nil {
		return pctx, e.Wrap(err)
	}

	_ = db.SetLogging(log)

	switch i, err := pLoadEventDatabase(pctx); {
	case err != nil:
		return pctx, err
	default:
		return util.ContextWithValues(i, map[util.ContextKey]interface{}{
			LeveldbStorageContextKey:    st,
			CenterDatabaseContextKey:    db,
			PermanentDatabaseContextKey: perm,
			PoolDatabaseContextKey:      pool,
		}), nil
	}
}

func PCheckBlocksOfStorage(pctx context.Context) (context.Context, error) {
	var log *logging.Logging
	var design NodeDesign
	var encs *encoder.Encoders
	var isaacparams *isaac.Params
	var db isaac.Database
	var readers *isaac.BlockItemReaders
	var fromRemotes isaac.RemotesBlockItemReadFunc

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		ISAACParamsContextKey, &isaacparams,
		CenterDatabaseContextKey, &db,
		BlockItemReadersContextKey, &readers,
		RemotesBlockItemReaderFuncContextKey, &fromRemotes,
	); err != nil {
		return pctx, err
	}

	if err := isaacblock.ValidateLastBlocks(readers, fromRemotes, db, isaacparams.NetworkID()); err != nil {
		var derr isaacblock.ErrValidatedDifferentHeightBlockMaps
		if errors.As(err, &derr) {
			l := log.Log().With().Err(err).
				Interface("database_height", derr.DatabaseHeight()).
				Interface("localfs_height", derr.LocalFSHeight()).
				Logger()

			switch {
			case derr.DatabaseHeight() > derr.LocalFSHeight():
				l.Error().Msg("last blocks is missing in local fs; fill the missing blocks into local fs")
			case derr.DatabaseHeight() < derr.LocalFSHeight():
				l.Error().Msg("last blocks is missing in database; import the missing blocks")
			}

			return pctx, err
		}
	}

	return pctx, nil
}

func PPatchBlockItemReaders(pctx context.Context) (context.Context, error) {
	var db isaac.Database
	var readers *isaac.BlockItemReaders

	if err := util.LoadFromContextOK(pctx,
		CenterDatabaseContextKey, &db,
		BlockItemReadersContextKey, &readers,
	); err != nil {
		return pctx, err
	}

	// FIXME

	return pctx, nil
}

func LoadPermanentDatabase(
	uri, id string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	root string,
	stcachesize int,
) (*leveldbstorage.Storage, isaac.PermanentDatabase, error) {
	e := util.StringError("load PermanentDatabase")

	u, err := url.Parse(uri)

	var dbtype, network string

	switch {
	case err != nil:
		return nil, nil, e.Wrap(err)
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
			return nil, nil, e.Wrap(err)
		}

		st, err := leveldbstorage.NewStorage(str, nil)
		if err != nil {
			return nil, nil, e.Wrap(err)
		}

		perm, err := isaacdatabase.NewLeveldbPermanent(st, encs, enc, stcachesize)
		if err != nil {
			return nil, nil, e.Wrap(err)
		}

		return st, perm, nil
	case dbtype == "redis":
		if strings.Contains(u.Scheme, "+") {
			u.Scheme = network
		}

		if len(u.Scheme) < 1 {
			u.Scheme = "redis"
		}

		perm, err := loadRedisPermanentDatabase(u.String(), id, encs, enc, stcachesize)
		if err != nil {
			return nil, nil, e.WithMessage(err, "create redis PermanentDatabase")
		}

		return nil, perm, nil
	default:
		return nil, nil, e.Errorf("unsupported database type, %q", dbtype)
	}
}

func CleanStorage(
	permuri, root string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) error {
	e := util.StringError("clean storage")

	switch fsnodeinfo, found, err := LoadNodeInfo(root, enc); {
	case err != nil:
		return e.Wrap(err)
	case !found:
	default:
		_, perm, err := LoadPermanentDatabase(permuri, fsnodeinfo.ID(), encs, enc, root, 0)
		if err == nil {
			if err := perm.Clean(); err != nil {
				return e.Wrap(err)
			}
		}
	}

	if err := RemoveLocalFS(root); err != nil {
		return e.Wrap(err)
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
		return errors.Wrap(err, "remove local fs")
	}

	return nil
}

func CreateLocalFS(newinfo NodeInfo, root string, enc encoder.Encoder) (NodeInfo, error) {
	e := util.StringError("initialize local fs")

	switch fi, err := os.Stat(root); {
	case os.IsNotExist(err):
		if err = os.MkdirAll(root, 0o700); err != nil {
			return nil, e.Wrap(err)
		}
	case err != nil:
		return nil, e.Wrap(err)
	case !fi.IsDir():
		return nil, e.Errorf("root is not directory")
	}

	for _, i := range []string{
		LocalFSDataDirectory(root),
		LocalFSDatabaseDirectory(root),
	} {
		switch fi, err := os.Stat(i); {
		case os.IsNotExist(err):
			if err = os.MkdirAll(i, 0o700); err != nil {
				return nil, e.WithMessage(err, "make directory, %q", i)
			}
		case err != nil:
			return nil, e.Wrap(err)
		case !fi.IsDir():
			return nil, e.Errorf("root is not directory, %q", i)
		default:
			return nil, e.Errorf("directory already exists, %q", i)
		}
	}

	var fsnodeinfo NodeInfo

	switch i, found, err := LoadNodeInfo(root, enc); {
	case err != nil:
		return nil, e.Wrap(err)
	case !found: // NOTE if not found, create new one
		fsnodeinfo = newinfo
	case !i.NetworkID().Equal(newinfo.NetworkID()):
		return nil, e.Errorf("network id does not match")
	default:
		fsnodeinfo = i
	}

	if err := SaveNodeInfo(root, fsnodeinfo); err != nil {
		return nil, e.Wrap(err)
	}

	return fsnodeinfo, nil
}

func loadRedisPermanentDatabase(uri, id string, encs *encoder.Encoders, enc encoder.Encoder, stcachesize int) (
	*isaacdatabase.RedisPermanent, error,
) {
	e := util.StringError("load redis PermanentDatabase")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2) //nolint:gomnd //...
	defer cancel()

	option, err := redis.ParseURL(uri)
	if err != nil {
		return nil, e.WithMessage(err, "invalid redis url")
	}

	st, err := redisstorage.NewStorage(ctx, option, fmt.Sprintf(RedisPermanentDatabasePrefixFormat, id))
	if err != nil {
		return nil, e.WithMessage(err, "create redis storage")
	}

	perm, err := isaacdatabase.NewRedisPermanent(st, encs, enc, stcachesize)
	if err != nil {
		return nil, e.Wrap(err)
	}

	return perm, nil
}

func LocalFSDataDirectory(root string) string {
	return filepath.Join(root, LocalFSDataDirectoryName)
}

func LocalFSDatabaseDirectory(root string) string {
	return filepath.Join(root, LocalFSDatabaseDirectoryName)
}

func LocalFSEventDatabaseDirectory(root string) string {
	return filepath.Join(root, LocalFSEventDatabaseDirectoryName)
}

func LoadDatabase(
	fsnodeinfo NodeInfo,
	permuri string,
	root string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	stcachesize,
	oppoolcachesize int,
) (
	*leveldbstorage.Storage,
	*isaacdatabase.Center,
	isaac.PermanentDatabase,
	*isaacdatabase.TempPool,
	error,
) {
	e := util.StringError("prepare database")

	st, perm, err := LoadPermanentDatabase(permuri, fsnodeinfo.ID(), encs, enc, root, stcachesize)

	switch {
	case err != nil:
		return nil, nil, nil, nil, e.Wrap(err)
	case st == nil:
		var str leveldbStorage.Storage

		str, err = leveldbStorage.OpenFile(LocalFSDatabaseDirectory(root), false)
		if err != nil {
			return nil, nil, nil, nil, e.Wrap(err)
		}

		st, err = leveldbstorage.NewStorage(str, nil)
		if err != nil {
			return nil, nil, nil, nil, e.Wrap(err)
		}
	}

	db, err := isaacdatabase.NewCenter(
		st,
		encs,
		enc,
		perm,
		func(height base.Height) (isaac.BlockWriteDatabase, error) {
			return isaacdatabase.NewLeveldbBlockWrite(height, st, encs, enc, stcachesize), nil
		},
	)
	if err != nil {
		return nil, nil, nil, nil, e.Wrap(err)
	}

	if err = db.MergeAllPermanent(); err != nil {
		return nil, nil, nil, nil, e.Wrap(err)
	}

	if err = isaacdatabase.CleanSyncPool(st); err != nil {
		return nil, nil, nil, nil, e.Wrap(err)
	}

	pool, err := isaacdatabase.NewTempPool(st, encs, enc, oppoolcachesize)
	if err != nil {
		return nil, nil, nil, nil, e.Wrap(err)
	}

	return st, db, perm, pool, nil
}

func CheckLocalFS(networkID base.NetworkID, root string, enc encoder.Encoder) (NodeInfo, error) {
	e := util.StringError("check local fs")

	switch fi, err := os.Stat(root); {
	case err != nil:
		return nil, e.Wrap(err)
	case !fi.IsDir():
		return nil, e.Errorf("root is not directory")
	}

	for _, i := range []string{
		LocalFSDataDirectory(root),
		LocalFSDatabaseDirectory(root),
	} {
		switch fi, err := os.Stat(i); {
		case err != nil:
			return nil, e.Wrap(err)
		case !fi.IsDir():
			return nil, e.Errorf("root is not directory, %q", i)
		}
	}

	switch info, found, err := LoadNodeInfo(root, enc); {
	case err != nil:
		return nil, e.Wrap(err)
	case !found:
		return nil, e.Wrap(util.ErrNotFound.Errorf("NodeInfo not found"))
	case !info.NetworkID().Equal(networkID):
		return nil, e.Errorf("network id does not match")
	default:
		return info, nil
	}
}

func pLoadEventDatabase(pctx context.Context) (context.Context, error) {
	var design NodeDesign
	var logout io.Writer

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		LogOutContextKey, &logout,
	); err != nil {
		return pctx, err
	}

	switch el, err := NewEventLogging(LocalFSEventDatabaseDirectory(design.Storage.Base), logout); {
	case err != nil:
		return pctx, err
	default:
		for i := range AllEventLoggerNames {
			switch n := AllEventLoggerNames[i]; n {
			case AllEventLogger, UnknownEventLogger:
				continue
			default:
				if _, err := el.Register(n); err != nil {
					return pctx, err
				}
			}
		}

		return context.WithValue(pctx, EventLoggingContextKey, el), nil
	}
}
