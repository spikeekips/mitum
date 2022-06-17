package isaacdatabase

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type TempLeveldb struct {
	*baseLeveldb
	st     *leveldbstorage.ReadonlyStorage
	mp     base.BlockMap // NOTE last blockmap
	sufst  base.State    // NOTE last suffrage state
	policy base.NetworkPolicy
	proof  base.SuffrageProof
}

func NewTempLeveldb(f string, encs *encoder.Encoders, enc encoder.Encoder) (*TempLeveldb, error) {
	e := util.StringErrorFunc("failed to open TempLeveldbDatabase")

	st, err := leveldbstorage.NewReadonlyStorage(f)
	if err != nil {
		return nil, e(err, "")
	}

	db, err := newTempLeveldb(st, encs, enc)
	if err != nil {
		return nil, e(err, "")
	}

	return db, nil
}

func newTempLeveldb(
	st *leveldbstorage.ReadonlyStorage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*TempLeveldb, error) {
	db := &TempLeveldb{
		baseLeveldb: newBaseLeveldb(st, encs, enc),
		st:          st,
	}

	if err := db.loadLastBlockMap(); err != nil {
		return nil, err
	}

	if err := db.loadSuffrageState(); err != nil {
		return nil, err
	}

	if err := db.loadSuffrageProof(); err != nil {
		return nil, err
	}

	if err := db.loadNetworkPolicy(); err != nil {
		return nil, err
	}

	return db, nil
}

func newTempLeveldbFromBlockWriteStorage(wst *LeveldbBlockWrite) (*TempLeveldb, error) {
	e := util.StringErrorFunc("failed new TempLeveldbDatabase from TempLeveldbDatabase")

	st, err := leveldbstorage.NewReadonlyStorageFromWrite(wst.st)
	if err != nil {
		return nil, e(err, "")
	}

	var mp base.BlockMap

	switch i, err := wst.BlockMap(); {
	case err != nil:
		return nil, e(err, "")
	default:
		mp = i
	}

	sufst := wst.SuffrageState()
	policy := wst.NetworkPolicy()

	var proof base.SuffrageProof
	if i, _ := wst.proof.Value(); i != nil {
		proof = i.(base.SuffrageProof) //nolint:forcetypeassert //...
	}

	return &TempLeveldb{
		baseLeveldb: newBaseLeveldb(st, wst.encs, wst.enc),
		st:          st,
		mp:          mp,
		sufst:       sufst,
		policy:      policy,
		proof:       proof,
	}, nil
}

func (db *TempLeveldb) Height() base.Height {
	if db.mp == nil {
		return base.NilHeight
	}

	return db.mp.Manifest().Height()
}

func (db *TempLeveldb) SuffrageHeight() base.Height {
	if db.sufst == nil {
		return base.NilHeight
	}

	return db.sufst.Value().(base.SuffrageStateValue).Height() //nolint:forcetypeassert //...
}

func (db *TempLeveldb) BlockMap() (base.BlockMap, error) {
	if db.mp == nil {
		return nil, storage.NotFoundError.Errorf("blockmap not found")
	}

	return db.mp, nil
}

func (db *TempLeveldb) SuffrageProof() (base.SuffrageProof, bool, error) {
	if db.proof == nil {
		return nil, false, nil
	}

	return db.proof, true, nil
}

func (db *TempLeveldb) NetworkPolicy() base.NetworkPolicy {
	return db.policy
}

func (db *TempLeveldb) State(key string) (base.State, bool, error) {
	return db.state(key)
}

func (db *TempLeveldb) ExistsInStateOperation(h util.Hash) (bool, error) {
	return db.existsInStateOperation(h)
}

func (db *TempLeveldb) ExistsKnownOperation(h util.Hash) (bool, error) {
	return db.existsKnownOperation(h)
}

func (db *TempLeveldb) loadLastBlockMap() error {
	switch m, err := db.baseLeveldb.loadLastBlockMap(); {
	case err != nil:
		return err
	case m == nil:
		return util.ErrNotFound.Errorf("last BlockMap not found")
	default:
		db.mp = m

		return nil
	}
}

func (db *TempLeveldb) loadSuffrageState() error {
	e := util.StringErrorFunc("failed to load suffrage state")

	switch b, found, err := db.st.Get(leveldbStateKey(isaac.SuffrageStateKey)); {
	case err != nil:
		return e(err, "")
	case !found:
		return nil
	default:
		st, err := db.decodeSuffrage(b)
		if err != nil {
			return e(err, "")
		}

		db.sufst = st

		return nil
	}
}

func (db *TempLeveldb) loadSuffrageProof() error {
	e := util.StringErrorFunc("failed to load SuffrageProof")

	switch b, found, err := db.st.Get(leveldbKeySuffrageProof); {
	case err != nil:
		return e(err, "")
	case !found:
		return nil
	default:
		var proof base.SuffrageProof

		if err := db.readHinter(b, &proof); err != nil {
			return e(err, "")
		}

		db.proof = proof

		return nil
	}
}

func (db *TempLeveldb) loadNetworkPolicy() error {
	switch policy, found, err := db.baseLeveldb.loadNetworkPolicy(); {
	case err != nil:
		return errors.Wrap(err, "")
	case !found:
		return nil
	default:
		db.policy = policy

		return nil
	}
}

func newTempDirectoryPrefix(root string) string {
	return filepath.Join(filepath.Clean(root), "temp")
}

func newTempDirectoryPrefixWithHeight(root string, height base.Height) string {
	return filepath.Join(filepath.Clean(root), "temp"+height.String())
}

func newTempDirectoryName(root string, height base.Height, suffix int64) string {
	return newTempDirectoryPrefixWithHeight(root, height) + fmt.Sprintf("-%d", suffix)
}

func tempDirectoryNameFormat() string {
	return "temp%d-%d"
}

func findSuffixFromTempDirectoryName(d, f string) (height int64, suffix int64) {
	var h, s int64

	_, err := fmt.Sscanf(filepath.Base(d), f, &h, &s)
	if err != nil {
		return -1, -1
	}

	return h, s
}

func sortTempDirectoryNames(matches []string) {
	f := tempDirectoryNameFormat()

	sort.Slice(matches, func(i, j int) bool {
		hi, si := findSuffixFromTempDirectoryName(matches[i], f)
		hj, sj := findSuffixFromTempDirectoryName(matches[j], f)

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

func NewTempDirectory(temproot string, height base.Height) (string, error) {
	e := util.StringErrorFunc("failed to get new TempDatabase directory")

	matches, err := loadTempDirectoriesByHeight(temproot, height)
	zero := newTempDirectoryName(temproot, height, 0)

	switch {
	case err != nil:
		return "", e(err, "")
	case len(matches) < 1:
		return zero, nil
	}

	sortTempDirectoryNames(matches)

	var suffix int64 = -1

end:
	for i := range matches {
		h, s := findSuffixFromTempDirectoryName(
			matches[i],
			tempDirectoryNameFormat(),
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

	return newTempDirectoryName(temproot, height, suffix+1), nil
}

func NewSyncPoolDirectory(temproot string, height base.Height) string {
	return filepath.Join(filepath.Clean(temproot), "syncpool-"+height.String()+util.ULID().String())
}

func loadTempDirectoriesByHeight(root string, height base.Height) ([]string, error) {
	e := util.StringErrorFunc("failed to load TempDatabase directories of height")

	prefix := newTempDirectoryPrefixWithHeight(root, height)

	switch matches, err := loadTempDirectories(prefix + "*"); {
	case err != nil:
		return nil, e(err, "")
	default:
		return matches, nil
	}
}

func loadAllTempDirectories(root string) ([]string, error) {
	prefix := newTempDirectoryPrefix(root)

	switch matches, err := loadTempDirectories(prefix + "*"); {
	case err != nil:
		return nil, errors.Wrap(err, "failed to load all TempDatabase directories")
	default:
		return matches, nil
	}
}

func loadTempDirectories(prefix string) ([]string, error) {
	e := util.StringErrorFunc("failed to load TempDatabase directories")

	matches, err := filepath.Glob(prefix + "*")

	switch {
	case err != nil:
		return nil, e(err, "")
	default:
		sortTempDirectoryNames(matches)

		return matches, nil
	}
}

func loadTemp(f string, encs *encoder.Encoders, enc encoder.Encoder) (isaac.TempDatabase, error) {
	temp, err := NewTempLeveldb(f, encs, enc)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	return temp, nil
}

// loadTemps loads all the TempDatabases from the given root directory.
// If clean is true, the useless directories will be removed.
func loadTemps( // revive:disable-line:flag-parameter
	root string,
	minHeight base.Height,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	clean bool,
) ([]isaac.TempDatabase, error) {
	e := util.StringErrorFunc("failed to load TempDatabase")

	matches, err := loadAllTempDirectories(root)
	if err != nil {
		return nil, e(err, "")
	}

	height := minHeight.Int64()
	var temps []isaac.TempDatabase
	var removes []string

end:
	for i := range matches {
		f := matches[i]
		h, suffix := findSuffixFromTempDirectoryName(
			f,
			tempDirectoryNameFormat(),
		)

		switch {
		case h < 0 || suffix < 0:
			removes = append(removes, f)
			continue end
		case h != height+1:
			removes = append(removes, f)
			continue end
		}

		switch temp, err := loadTemp(f, encs, enc); {
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
		return temps[i].Height() > temps[j].Height()
	})

	return temps, nil
}
