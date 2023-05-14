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
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
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
	FSNodeInfoContextKey            = util.ContextKey("fs-node-info")
	LeveldbStorageContextKey        = util.ContextKey("leveldb-storage")
	CenterDatabaseContextKey        = util.ContextKey("center-database")
	PermanentDatabaseContextKey     = util.ContextKey("permanent-database")
	PoolDatabaseContextKey          = util.ContextKey("pool-database")
	LastVoteproofsHandlerContextKey = util.ContextKey("last-voteproofs-handler")
)

var (
	LocalFSDataDirectoryName           = "data"
	LocalFSDatabaseDirectoryName       = "db"
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

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		CenterDatabaseContextKey, &center,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	// NOTE load from last voteproofs
	lvps := isaac.NewLastVoteproofsHandler()
	pctx = context.WithValue(pctx, LastVoteproofsHandlerContextKey, lvps) //revive:disable-line:modifies-parameter

	var manifest base.Manifest
	var enc encoder.Encoder

	switch m, found, err := center.LastBlockMap(); {
	case err != nil:
		return pctx, e.Wrap(err)
	case !found:
		return pctx, nil
	default:
		enc = encs.Find(m.Encoder())
		if enc == nil {
			return pctx, e.Errorf("encoder of last blockmap not found")
		}

		manifest = m.Manifest()
	}

	reader, err := isaacblock.NewLocalFSReaderFromHeight(
		LocalFSDataDirectory(design.Storage.Base), manifest.Height(), enc,
	)
	if err != nil {
		return pctx, e.Wrap(err)
	}

	defer func() {
		_ = reader.Close()
	}()

	switch v, found, err := reader.Item(base.BlockMapItemTypeVoteproofs); {
	case err != nil:
		return pctx, e.Wrap(err)
	case !found:
		return pctx, e.Errorf("last voteproofs not found in localfs")
	default:
		vps := v.([]base.Voteproof) //nolint:forcetypeassert //...

		lvps.Set(vps[0].(base.INITVoteproof))   //nolint:forcetypeassert //...
		lvps.Set(vps[1].(base.ACCEPTVoteproof)) //nolint:forcetypeassert //...
	}

	return pctx, nil
}

func PCleanStorage(pctx context.Context) (context.Context, error) {
	e := util.StringError("clean storage")

	var design NodeDesign
	var encs *encoder.Encoders
	var enc encoder.Encoder

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	if err := CleanStorage(design.Storage.Database.String(), design.Storage.Base, encs, enc); err != nil {
		return pctx, e.Wrap(err)
	}

	return pctx, nil
}

func PCreateLocalFS(pctx context.Context) (context.Context, error) {
	e := util.StringError("create localfs")

	var design NodeDesign
	var enc encoder.Encoder
	var params *isaac.LocalParams
	var version util.Version

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		EncoderContextKey, &enc,
		LocalParamsContextKey, &params,
		VersionContextKey, &version,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	fsnodeinfo, err := CreateLocalFS(
		CreateDefaultNodeInfo(params.NetworkID(), version), design.Storage.Base, enc)
	if err != nil {
		return pctx, e.Wrap(err)
	}

	return context.WithValue(pctx, FSNodeInfoContextKey, fsnodeinfo), nil
}

func PCheckLocalFS(pctx context.Context) (context.Context, error) {
	e := util.StringError("check localfs")

	var design NodeDesign
	var params *isaac.LocalParams
	var enc encoder.Encoder

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		EncoderContextKey, &enc,
		LocalParamsContextKey, &params,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	fsnodeinfo, err := CheckLocalFS(params.NetworkID(), design.Storage.Base, enc)

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
	e := util.StringError("check localfs")

	var version util.Version
	var design NodeDesign
	var params *isaac.LocalParams
	var encs *encoder.Encoders
	var enc encoder.Encoder

	if err := util.LoadFromContextOK(pctx,
		VersionContextKey, &version,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
		LocalParamsContextKey, &params,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	fsnodeinfo, err := CheckLocalFS(params.NetworkID(), design.Storage.Base, enc)

	switch {
	case err == nil:
		if err = isaacblock.CleanBlockTempDirectory(LocalFSDataDirectory(design.Storage.Base)); err != nil {
			return pctx, e.Wrap(err)
		}
	case errors.Is(err, os.ErrNotExist):
		// NOTE database will be no cleaned.
		fsnodeinfo, err = CreateLocalFS(
			CreateDefaultNodeInfo(params.NetworkID(), version), design.Storage.Base, enc)
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
	var encs *encoder.Encoders
	var enc encoder.Encoder
	var fsnodeinfo NodeInfo

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
		FSNodeInfoContextKey, &fsnodeinfo,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	st, db, perm, pool, err := LoadDatabase(
		fsnodeinfo, design.Storage.Database.String(), design.Storage.Base, encs, enc)
	if err != nil {
		return pctx, e.Wrap(err)
	}

	_ = db.SetLogging(log)
	//revive:disable:modifies-parameter
	pctx = context.WithValue(pctx, LeveldbStorageContextKey, st)
	pctx = context.WithValue(pctx, CenterDatabaseContextKey, db)
	pctx = context.WithValue(pctx, PermanentDatabaseContextKey, perm)
	pctx = context.WithValue(pctx, PoolDatabaseContextKey, pool)
	//revive:enable:modifies-parameter

	return pctx, nil
}

func PCheckBlocksOfStorage(pctx context.Context) (context.Context, error) {
	var log *logging.Logging
	var design NodeDesign
	var encs *encoder.Encoders
	var enc *jsonenc.Encoder
	var params *isaac.LocalParams
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
	); err != nil {
		return pctx, err
	}

	if err := isaacblock.ValidateLastBlocks(
		LocalFSDataDirectory(design.Storage.Base),
		encs,
		enc,
		db,
		params.NetworkID(),
	); err != nil {
		var derr isaacblock.ErrValidatedDifferentHeightBlockMaps
		if errors.As(err, &derr) {
			l := log.Log().With().Err(err).
				Interface("database_height", derr.DatabaseHeight()).
				Interface("localfs_height", derr.LocalFSHeight()).
				Logger()

			switch {
			case derr.DatabaseHeight() > derr.LocalFSHeight():
				l.Error().Msg("last blocks is missing in localfs; fill the missing blocks into localfs")
			case derr.DatabaseHeight() < derr.LocalFSHeight():
				l.Error().Msg("last blocks is missing in database; import the missing blocks")
			}

			return pctx, err
		}
	}

	return pctx, nil
}

func LoadPermanentDatabase(
	uri, id string, encs *encoder.Encoders, enc encoder.Encoder, root string,
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

		perm, err := isaacdatabase.NewLeveldbPermanent(st, encs, enc)
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

		perm, err := loadRedisPermanentDatabase(u.String(), id, encs, enc)
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
		_, perm, err := LoadPermanentDatabase(permuri, fsnodeinfo.ID(), encs, enc, root)
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
		return errors.Wrap(err, "remove localfs")
	}

	return nil
}

func CreateLocalFS(newinfo NodeInfo, root string, enc encoder.Encoder) (NodeInfo, error) {
	e := util.StringError("initialize localfs")

	switch fi, err := os.Stat(root); {
	case err == nil:
		if !fi.IsDir() {
			return nil, e.Errorf("root is not directory")
		}
	case os.IsNotExist(err):
		if err = os.MkdirAll(root, 0o700); err != nil {
			return nil, e.Wrap(err)
		}
	default:
		return nil, e.Wrap(err)
	}

	for _, i := range []string{
		LocalFSDataDirectory(root),
		LocalFSDatabaseDirectory(root),
	} {
		switch fi, err := os.Stat(i); {
		case err == nil:
			if !fi.IsDir() {
				return nil, e.Errorf("root is not directory, %q", i)
			}

			return nil, e.Errorf("directory already exists, %q", i)
		case os.IsNotExist(err):
			if err = os.MkdirAll(i, 0o700); err != nil {
				return nil, e.WithMessage(err, "make directory, %q", i)
			}
		default:
			return nil, e.Wrap(err)
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

func loadRedisPermanentDatabase(uri, id string, encs *encoder.Encoders, enc encoder.Encoder) (
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

	perm, err := isaacdatabase.NewRedisPermanent(st, encs, enc)
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
	e := util.StringError("prepare database")

	st, perm, err := LoadPermanentDatabase(permuri, fsnodeinfo.ID(), encs, enc, root)

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
			return isaacdatabase.NewLeveldbBlockWrite(height, st, encs, enc), nil
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

	pool, err := isaacdatabase.NewTempPool(st, encs, enc)
	if err != nil {
		return nil, nil, nil, nil, e.Wrap(err)
	}

	return st, db, perm, pool, nil
}

func CheckLocalFS(networkID base.NetworkID, root string, enc encoder.Encoder) (NodeInfo, error) {
	e := util.StringError("check localfs")

	switch fi, err := os.Stat(root); {
	case err == nil:
		if !fi.IsDir() {
			return nil, e.Errorf("root is not directory")
		}
	default:
		return nil, e.Wrap(err)
	}

	for _, i := range []string{
		LocalFSDataDirectory(root),
		LocalFSDatabaseDirectory(root),
	} {
		switch fi, err := os.Stat(i); {
		case err == nil:
			if !fi.IsDir() {
				return nil, e.Errorf("root is not directory, %q", i)
			}
		default:
			return nil, e.Wrap(err)
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
