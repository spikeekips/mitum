package launch

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	redisstorage "github.com/spikeekips/mitum/storage/redis"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

var (
	LocalFSPermDirectoryName = "perm"
	LocalFSTempDirectoryName = "temp"
	LocalFSDataDirectoryName = "data"
	LocalFSPoolDirectoryName = "pool"

	RedisPermanentDatabasePrefix = "mitum"
	LeveldbURIScheme             = "file+leveldb"
)

func CleanStorage(permuri, root string, encs *encoder.Encoders, enc encoder.Encoder) error {
	e := util.StringErrorFunc("failed to clean storage")

	if perm, err := LoadPermanentDatabase(permuri, encs, enc); err == nil {
		if err := perm.Clean(); err != nil {
			return e(err, "")
		}
	}

	if err := RemoveLocalFS(root); err != nil {
		return e(err, "")
	}

	return nil
}

func CreateLocalFS(root string) error {
	e := util.StringErrorFunc("failed to initialize localfs")

	switch fi, err := os.Stat(root); {
	case err == nil:
		if !fi.IsDir() {
			return e(nil, "root is not directory")
		}
	case os.IsNotExist(err):
		if err = os.MkdirAll(root, 0o700); err != nil {
			return e(err, "")
		}
	default:
		return e(err, "")
	}

	temproot := LocalFSTempDirectory(root)
	poolroot := LocalFSPoolDirectory(root)
	dataroot := LocalFSDataDirectory(root)

	for _, i := range []string{temproot, poolroot, dataroot} {
		switch fi, err := os.Stat(i); {
		case err == nil:
			if !fi.IsDir() {
				return e(nil, "root is not directory, %q", i)
			}

			return errors.Errorf("directory already exists, %q", i)
		case os.IsNotExist(err):
			if err = os.MkdirAll(i, 0o700); err != nil {
				return e(err, "failed to make directory, %i", i)
			}
		default:
			return e(err, "")
		}
	}

	return nil
}

func CheckLocalFS(root string) error {
	e := util.StringErrorFunc("failed to check localfs")

	switch fi, err := os.Stat(root); {
	case err == nil:
		if !fi.IsDir() {
			return e(nil, "root is not directory")
		}
	default:
		return e(err, "")
	}

	temproot := LocalFSTempDirectory(root)
	poolroot := LocalFSPoolDirectory(root)
	dataroot := LocalFSDataDirectory(root)

	for _, i := range []string{temproot, poolroot, dataroot} {
		switch fi, err := os.Stat(i); {
		case err == nil:
			if !fi.IsDir() {
				return e(nil, "root is not directory, %q", i)
			}
		default:
			return e(err, "")
		}
	}

	return nil
}

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
		return errors.Wrap(err, "failed to initialize localfs")
	}

	return nil
}

func LoadDatabase(
	permuri string,
	localfsroot string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*isaacdatabase.Default, isaac.PermanentDatabase, *isaacdatabase.TempPool, error) {
	e := util.StringErrorFunc("failed to prepare database")

	perm, err := LoadPermanentDatabase(permuri, encs, enc)
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

	if err = db.MergeAllPermanent(); err != nil {
		return nil, nil, nil, e(err, "")
	}

	pool, err := isaacdatabase.NewTempPool(poolroot, encs, enc)
	if err != nil {
		return nil, nil, nil, e(err, "")
	}

	return db, perm, pool, nil
}

func LoadPermanentDatabase(uri string, encs *encoder.Encoders, enc encoder.Encoder) (isaac.PermanentDatabase, error) {
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

		perm, err := loadRedisPermanentDatabase(u.String(), encs, enc)
		if err != nil {
			return nil, e(err, "failed to create redis PermanentDatabase")
		}

		return perm, nil
	default:
		return nil, e(nil, "unsupported database type, %q", dbtype)
	}
}

func loadRedisPermanentDatabase(uri string, encs *encoder.Encoders, enc encoder.Encoder) (
	*isaacdatabase.RedisPermanent, error,
) {
	e := util.StringErrorFunc("failed to load redis PermanentDatabase")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2) //nolint:gomnd //...
	defer cancel()

	option, err := redis.ParseURL(uri)
	if err != nil {
		return nil, e(err, "invalid redis url")
	}

	// BLOCK set local address in prefix
	st, err := redisstorage.NewStorage(ctx, option, RedisPermanentDatabasePrefix)
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
