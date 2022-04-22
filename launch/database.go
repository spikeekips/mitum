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
)

func PrepareDatabase(
	fsroot string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*database.Default, error) {
	e := util.StringErrorFunc("failed to prepare database")

	switch _, err := os.Stat(fsroot); {
	case err == nil:
		if err := os.RemoveAll(fsroot); err != nil {
			return nil, e(err, "")
		}
	case os.IsNotExist(err):
	default:
		return nil, e(err, "")
	}

	if err := os.MkdirAll(fsroot, 0o700); err != nil {
		return nil, e(err, "")
	}

	permroot := FSRootPermDirectory(fsroot)
	temproot := FSRootTempDirectory(fsroot)
	dataroot := FSRootDataDirectory(fsroot)

	if err := os.MkdirAll(dataroot, 0o700); err != nil {
		return nil, e(err, "failed to make blockdata fsroot")
	}

	// NOTE db
	perm, err := database.NewLeveldbPermanent(permroot, encs, enc)
	if err != nil {
		return nil, e(err, "")
	}

	db, err := database.NewDefault(temproot, encs, enc, perm, func(height base.Height) (isaac.BlockWriteDatabase, error) {
		newroot, err := database.NewTempDirectory(temproot, height)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}

		return database.NewLeveldbBlockWrite(height, newroot, encs, enc)
	})
	if err != nil {
		return nil, e(err, "")
	}

	return db, nil
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
