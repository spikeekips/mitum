package isaac

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type TempLeveldbDatabase struct {
	*baseLeveldbDatabase
	st     *leveldbstorage.ReadonlyStorage
	m      base.Manifest // NOTE last manifest
	sufstt base.State    // NOTE last suffrage state
}

func NewTempLeveldbDatabase(f string, encs *encoder.Encoders, enc encoder.Encoder) (*TempLeveldbDatabase, error) {
	e := util.StringErrorFunc("failed to open TempLeveldbDatabase")

	st, err := leveldbstorage.NewReadonlyStorage(f)
	if err != nil {
		return nil, e(err, "")
	}

	db, err := newTempLeveldbDatabase(st, encs, enc)
	if err != nil {
		return nil, e(err, "")
	}

	return db, nil
}

func newTempLeveldbDatabase(
	st *leveldbstorage.ReadonlyStorage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*TempLeveldbDatabase, error) {
	db := &TempLeveldbDatabase{
		baseLeveldbDatabase: newBaseLeveldbDatabase(st, encs, enc),
		st:                  st,
	}

	if err := db.loadLastManifest(); err != nil {
		return nil, err
	}

	if err := db.loadLastSuffrage(); err != nil {
		return nil, err
	}

	return db, nil
}

func newTempLeveldbDatabaseFromWOStorage(wst *LeveldbBlockWriteDatabase) (*TempLeveldbDatabase, error) {
	e := util.StringErrorFunc("failed new TempLeveldbDatabase from TempLeveldbDatabase")
	st, err := leveldbstorage.NewReadonlyStorageFromWrite(wst.st)
	if err != nil {
		return nil, e(err, "")
	}

	var m base.Manifest
	switch i, err := wst.Manifest(); {
	case err != nil:
		return nil, e(err, "")
	default:
		m = i
	}

	var sufstt base.State
	_ = wst.sufstt.Value(&sufstt)

	return &TempLeveldbDatabase{
		baseLeveldbDatabase: newBaseLeveldbDatabase(st, wst.encs, wst.enc),
		st:                  st,
		m:                   m,
		sufstt:              sufstt,
	}, nil
}

func (db *TempLeveldbDatabase) Height() base.Height {
	if db.m == nil {
		return base.NilHeight
	}

	return db.m.Height()
}

func (db *TempLeveldbDatabase) SuffrageHeight() base.Height {
	if db.sufstt == nil {
		return base.NilHeight
	}

	return db.sufstt.Value().(base.SuffrageStateValue).Height()
}

func (db *TempLeveldbDatabase) Manifest() (base.Manifest, error) {
	if db.m == nil {
		return nil, storage.NotFoundError.Errorf("manifest not found")
	}

	return db.m, nil
}

func (db *TempLeveldbDatabase) Suffrage() (base.State, bool, error) {
	if db.sufstt == nil {
		return nil, false, nil
	}

	return db.sufstt, true, nil
}

func (db *TempLeveldbDatabase) State(key string) (base.State, bool, error) {
	return db.state(key)
}

func (db *TempLeveldbDatabase) ExistsOperation(h util.Hash) (bool, error) {
	return db.existsOperation(h)
}

func (db *TempLeveldbDatabase) loadLastManifest() error {
	e := util.StringErrorFunc("failed to load manifest")

	switch b, found, err := db.st.Get(leveldbKeyPrefixManifest); {
	case err != nil:
		return e(err, "")
	case !found:
		return e(err, "manifest not found")
	default:
		m, err := db.decodeManifest(b)
		if err != nil {
			return e(err, "")
		}

		db.m = m

		return nil
	}
}

func (db *TempLeveldbDatabase) loadLastSuffrage() error {
	e := util.StringErrorFunc("failed to load suffrage state")

	var key string
	switch b, found, err := db.st.Get(leveldbKeyPrefixSuffrage); {
	case err != nil:
		return e(err, "")
	case !found:
		return nil
	default:
		key = string(b)
	}

	switch b, found, err := db.st.Get(leveldbStateKey(key)); {
	case err != nil:
		return e(err, "")
	case !found:
		return e(nil, "suffrage state not found")
	default:
		st, err := db.decodeSuffrage(b)
		if err != nil {
			return e(err, "")
		}

		db.sufstt = st

		return nil
	}
}

func newTempDatabaseDirectoryPrefix(root string) string {
	return filepath.Join(filepath.Clean(root), "temp")
}

func newTempDatabaseDirectoryPrefixWithHeight(root string, height base.Height) string {
	return filepath.Join(filepath.Clean(root), "temp"+height.String())
}

func newTempDatabaseDirectoryName(root string, height base.Height, suffix int64) string {
	return newTempDatabaseDirectoryPrefixWithHeight(root, height) + fmt.Sprintf("-%d", suffix)
}

func tempDatabaseDirectoryNameFormat() string {
	return "temp%d-%d"
}

func findSuffixFromTempDatabaseDirectoryName(d, f string) (height int64, suffix int64) {
	var h, s int64
	_, err := fmt.Sscanf(filepath.Base(d), f, &h, &s)
	if err != nil {
		return -1, -1
	}

	return h, s
}

func sortTempDatabaseDirectoryNames(matches []string) {
	f := tempDatabaseDirectoryNameFormat()
	sort.Slice(matches, func(i, j int) bool {
		hi, si := findSuffixFromTempDatabaseDirectoryName(matches[i], f)
		hj, sj := findSuffixFromTempDatabaseDirectoryName(matches[j], f)

		switch {
		case hi < 0 || hj < 0 || si < 0 || sj < 0:
			return true
		case hi > hj:
			return false
		case hi < hj:
			return true
		default:
			return si > sj
		}
	})
}

func newTempDatabaseDirectory(root string, height base.Height) (string, error) {
	e := util.StringErrorFunc("failed to get new TempDatabase directory")

	matches, err := loadTempDatabaseDirectoriesByHeight(root, height)
	zero := newTempDatabaseDirectoryName(root, height, 0)

	switch {
	case err != nil:
		return "", e(err, "")
	case len(matches) < 1:
		return zero, nil
	}

	sortTempDatabaseDirectoryNames(matches)

	var suffix int64 = -1

end:
	for i := range matches {
		h, s := findSuffixFromTempDatabaseDirectoryName(
			matches[i],
			tempDatabaseDirectoryNameFormat(),
		)
		switch {
		case h < 0 || s < 0:
			continue end
		case h != height.Int64():
			continue end
		}

		suffix = s

		break
	}

	if suffix < 0 {
		return zero, nil
	}

	return newTempDatabaseDirectoryName(root, height, suffix+1), nil
}

func loadTempDatabaseDirectoriesByHeight(root string, height base.Height) ([]string, error) {
	e := util.StringErrorFunc("failed to load TempDatabase directories of height")

	prefix := newTempDatabaseDirectoryPrefixWithHeight(root, height)
	switch matches, err := loadTempDatabaseDirectories(prefix + "*"); {
	case err != nil:
		return nil, e(err, "")
	default:
		return matches, nil
	}
}

func loadAllTempDatabaseDirectories(root string) ([]string, error) {
	prefix := newTempDatabaseDirectoryPrefix(root)
	switch matches, err := loadTempDatabaseDirectories(prefix + "*"); {
	case err != nil:
		return nil, errors.Wrap(err, "failed to load all TempDatabase directories")
	default:
		return matches, nil
	}
}

func loadTempDatabaseDirectories(prefix string) ([]string, error) {
	e := util.StringErrorFunc("failed to load TempDatabase directories")

	matches, err := filepath.Glob(prefix + "*")
	switch {
	case err != nil:
		return nil, e(err, "")
	default:
		sortTempDatabaseDirectoryNames(matches)

		return matches, nil
	}
}

func loadTempDatabase(f string, encs *encoder.Encoders, enc encoder.Encoder) (TempDatabase, error) {
	temp, err := NewTempLeveldbDatabase(f, encs, enc)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	return temp, nil
}

// loadTempDatabases loads all the TempDatabases from the given root directory.
// If clean is true, the useless directories will be removed.
func loadTempDatabases(
	root string,
	minHeight base.Height,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	clean bool,
) ([]TempDatabase, error) {
	e := util.StringErrorFunc("failed to load TempDatabase")

	matches, err := loadAllTempDatabaseDirectories(root)
	if err != nil {
		return nil, e(err, "")
	}

	height := minHeight.Int64()
	var temps []TempDatabase
	var removes []string

end:
	for i := range matches {
		f := matches[i]
		h, suffix := findSuffixFromTempDatabaseDirectoryName(
			f,
			tempDatabaseDirectoryNameFormat(),
		)
		switch {
		case h < 0 || suffix < 0:
			removes = append(removes, f)
			continue end
		case h != height+1:
			removes = append(removes, f)
			continue end
		}

		switch temp, err := loadTempDatabase(f, encs, enc); {
		case err != nil:
			removes = append(removes, f)
			continue end
		default:
			temps = append(temps, temp)

			height = h
		}
	}

	if clean {
		for i := range removes {
			f := removes[i]
			if err := os.RemoveAll(f); err != nil {
				return nil, e(err, "failed to remove useless directory, %q", f)
			}
		}
	}

	sort.Slice(temps, func(i, j int) bool {
		return temps[i].Height() < temps[j].Height()
	})

	return temps, nil
}
