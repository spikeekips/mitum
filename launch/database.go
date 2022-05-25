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
	DBRootPermDirectoryName = "perm"
	DBRootTempDirectoryName = "temp"
	DBRootDataDirectoryName = "data"
	DBRootPoolDirectoryName = "pool"

	RedisPermanentDatabasePrefix = "mitum"
	leveldbURIScheme             = "file"
)

func InitializeDatabase(root string) error {
	e := util.StringErrorFunc("failed to initialize database")

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

	temproot := DBRootTempDirectory(root)
	poolroot := DBRootPoolDirectory(root)
	dataroot := DBRootDataDirectory(root)

	for _, i := range []string{temproot, poolroot, dataroot} {
		switch fi, err := os.Stat(i); {
		case err == nil:
			if !fi.IsDir() {
				return e(nil, "root is not directory, %q", i)
			}
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

func CheckDatabase(root string) error {
	e := util.StringErrorFunc("failed to check database")

	switch fi, err := os.Stat(root); {
	case err == nil:
		if !fi.IsDir() {
			return e(nil, "root is not directory")
		}
	default:
		return e(err, "")
	}

	temproot := DBRootTempDirectory(root)
	poolroot := DBRootPoolDirectory(root)
	dataroot := DBRootDataDirectory(root)

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

func CleanDatabase(root string) error {
	e := util.StringErrorFunc("failed to initialize database")

	switch _, err := os.Stat(root); {
	case err == nil:
		if err = os.RemoveAll(root); err != nil {
			return e(err, "")
		}
	case os.IsNotExist(err):
	default:
		return e(err, "")
	}

	return nil
}

func PrepareDatabase(
	perm isaac.PermanentDatabase,
	root string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*isaacdatabase.Default, *isaacdatabase.TempPool, error) {
	e := util.StringErrorFunc("failed to prepare database")

	temproot := DBRootTempDirectory(root)
	poolroot := DBRootPoolDirectory(root)

	db, err := isaacdatabase.NewDefault(
		temproot, encs, enc, perm, func(height base.Height) (isaac.BlockWriteDatabase, error) {
			newroot, eerr := isaacdatabase.NewTempDirectory(temproot, height)
			if eerr != nil {
				return nil, errors.Wrap(eerr, "")
			}

			return isaacdatabase.NewLeveldbBlockWrite(height, newroot, encs, enc)
		})
	if err != nil {
		return nil, nil, e(err, "")
	}

	if err := db.MergeAllPermanent(); err != nil {
		return nil, nil, e(err, "")
	}

	pool, err := isaacdatabase.NewTempPool(poolroot, encs, enc)
	if err != nil {
		return nil, nil, e(err, "")
	}

	return db, pool, nil
}

func DBRootPermDirectory(root string) string {
	return filepath.Join(root, DBRootPermDirectoryName)
}

func DBRootTempDirectory(root string) string {
	return filepath.Join(root, DBRootTempDirectoryName)
}

func DBRootDataDirectory(root string) string {
	return filepath.Join(root, DBRootDataDirectoryName)
}

func DBRootPoolDirectory(root string) string {
	return filepath.Join(root, DBRootPoolDirectoryName)
}

func LoadPermanentDatabase(uri string, encs *encoder.Encoders, enc encoder.Encoder) (isaac.PermanentDatabase, error) {
	e := util.StringErrorFunc("failed to load PermanentDatabase")

	u, err := url.Parse(uri)

	var dbtype, network string

	switch {
	case err != nil:
		return nil, e(err, "")
	case len(u.Scheme) < 1, strings.EqualFold(u.Scheme, leveldbURIScheme):
		dbtype = leveldbURIScheme
	default:
		u.Scheme = strings.ToLower(u.Scheme)

		l := strings.SplitN(u.Scheme, "+", 2)
		dbtype = l[0]

		if len(l) > 1 {
			network = l[1]
		}
	}

	switch {
	case dbtype == leveldbURIScheme:
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
