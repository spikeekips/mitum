package launch

import (
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

var (
	FSRootPermDirectoryName = "perm"
	FSRootTempDirectoryName = "temp"
	FSRootDataDirectoryName = "data"
	FSRootPoolDirectoryName = "pool"
)

func PrepareDatabase(
	fsroot string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*database.Default, database.TempPool, error) {
	e := util.StringErrorFunc("failed to prepare database")

	switch _, err := os.Stat(fsroot); {
	case err == nil:
		if err = os.RemoveAll(fsroot); err != nil {
			return nil, nil, e(err, "")
		}
	case os.IsNotExist(err):
	default:
		return nil, nil, e(err, "")
	}

	if err := os.MkdirAll(fsroot, 0o700); err != nil {
		return nil, nil, e(err, "")
	}

	permroot := FSRootPermDirectory(fsroot)
	temproot := FSRootTempDirectory(fsroot)
	dataroot := FSRootDataDirectory(fsroot)
	poolroot := FSRootPoolDirectory(fsroot)

	if err := os.MkdirAll(dataroot, 0o700); err != nil {
		return nil, nil, e(err, "failed to make blockdata fsroot")
	}

	// NOTE db
	perm, err := database.NewLeveldbPermanent(permroot, encs, enc)
	if err != nil {
		return nil, nil, e(err, "")
	}

	db, err := database.NewDefault(temproot, encs, enc, perm, func(height base.Height) (isaac.BlockWriteDatabase, error) {
		newroot, eerr := database.NewTempDirectory(temproot, height)
		if eerr != nil {
			return nil, nil, errors.Wrap(eerr, "")
		}

		return database.NewLeveldbBlockWrite(height, newroot, encs, enc)
	})
	if err != nil {
		return nil, nil, e(err, "")
	}

	pool, err := database.NewTempPool(poolroot, encs, enc)
	if err != nil {
		return nil, nil, e(err, "")
	}

	return db, pool, nil
}

func FSRootPermDirectory(root string) string {
	return filepath.Join(root, FSRootPermDirectoryName)
}

func FSRootTempDirectory(root string) string {
	return filepath.Join(root, FSRootTempDirectoryName)
}

func FSRootDataDirectory(root string) string {
	return filepath.Join(root, FSRootDataDirectoryName)
}

func FSRootPoolDirectory(root string) string {
	return filepath.Join(root, FSRootPoolDirectoryName)
}
