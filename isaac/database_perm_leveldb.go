package isaac

import (
	"bytes"
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type LeveldbPermanentDatabase struct {
	*basePermanentDatabase
	*baseLeveldbDatabase
	st *leveldbstorage.WriteStorage
}

func NewLeveldbPermanentDatabase(
	f string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*LeveldbPermanentDatabase, error) {
	st, err := leveldbstorage.NewWriteStorage(f)
	if err != nil {
		return nil, errors.Wrap(err, "failed new LeveldbPermanentDatabase")
	}

	return newLeveldbPermanentDatabase(st, encs, enc)
}

func newLeveldbPermanentDatabase(
	st *leveldbstorage.WriteStorage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*LeveldbPermanentDatabase, error) {
	db := &LeveldbPermanentDatabase{
		basePermanentDatabase: newBasePermanentDatabase(),
		baseLeveldbDatabase:   newBaseLeveldbDatabase(st, encs, enc),
		st:                    st,
	}

	if err := db.loadLastManifest(); err != nil {
		return nil, err
	}

	if err := db.loadLastSuffrage(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *LeveldbPermanentDatabase) Manifest(height base.Height) (base.Manifest, bool, error) {
	e := util.StringErrorFunc("failed to load manifest")

	switch m, found, err := db.LastManifest(); {
	case err != nil:
		return nil, false, e(err, "")
	case found:
		return m, true, nil
	}

	switch b, found, err := db.st.Get(leveldbManifestKey(height)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		m, err := db.decodeManifest(b)
		if err != nil {
			return nil, false, e(err, "")
		}

		return m, true, nil
	}
}

func (db *LeveldbPermanentDatabase) Suffrage(height base.Height) (base.State, bool, error) {
	e := util.StringErrorFunc("failed to get suffrage by block height")

	switch m, found, err := db.LastManifest(); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	case height > m.Height():
		return nil, false, nil
	}

	switch st, found, err := db.LastSuffrage(); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	case height == st.Height():
		return st, true, nil
	}

	var st base.State
	if err := db.st.Iter(
		&leveldbutil.Range{Start: leveldbBeginSuffrageKey, Limit: leveldbSuffrageKey(height + 1)},
		func(_, b []byte) (bool, error) {
			i, err := db.decodeSuffrage(b)
			if err != nil {
				return false, errors.Wrap(err, "")
			}

			st = i

			return false, nil
		},
		false,
	); err != nil {
		return nil, false, errors.Wrap(err, "failed to get suffrage by block height")
	}

	return st, st != nil, nil
}

func (db *LeveldbPermanentDatabase) SuffrageByHeight(suffrageHeight base.Height) (base.State, bool, error) {
	e := util.StringErrorFunc("failed to get suffrage by height")

	switch st, found, err := db.LastSuffrage(); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	case suffrageHeight > st.Value().(base.SuffrageStateValue).Height():
		return nil, false, nil
	case suffrageHeight == st.Value().(base.SuffrageStateValue).Height():
		return st, true, nil
	}

	switch b, found, err := db.st.Get(leveldbSuffrageHeightKey(suffrageHeight)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		st, err := db.decodeSuffrage(b)
		if err != nil {
			return nil, false, e(err, "")
		}
		return st, true, nil
	}
}

func (db *LeveldbPermanentDatabase) State(key string) (base.State, bool, error) {
	return db.state(key)
}

func (db *LeveldbPermanentDatabase) ExistsOperation(h util.Hash) (bool, error) {
	return db.existsOperation(h)
}

func (db *LeveldbPermanentDatabase) MergeTempDatabase(_ context.Context, temp TempDatabase) error {
	db.Lock()
	defer db.Unlock()

	if !db.canMergeTempDatabase(temp) {
		return nil
	}

	e := util.StringErrorFunc("failed to merge TempDatabase")

	switch t := temp.(type) {
	case *TempLeveldbDatabase:
		m, sufstt, err := db.mergeTempDatabaseFromLeveldb(t)
		if err != nil {
			return e(err, "")
		}

		_ = db.m.SetValue(m)
		_ = db.sufstt.SetValue(sufstt)

		return nil
	default:
		return e(nil, "unknown temp database, %T", temp)
	}
}

func (db *LeveldbPermanentDatabase) mergeTempDatabaseFromLeveldb(temp *TempLeveldbDatabase) (
	base.Manifest, base.State, error,
) {
	e := util.StringErrorFunc("failed to merge LeveldbTempDatabase")

	var m base.Manifest
	switch i, err := temp.Manifest(); {
	case err != nil:
		return nil, nil, e(err, "")
	default:
		m = i
	}

	var sufstt base.State
	var sufsv base.SuffrageStateValue
	switch st, found, err := temp.Suffrage(); {
	case err != nil:
		return nil, nil, e(err, "")
	case found:
		sufstt = st
		sufsv = st.Value().(base.SuffrageStateValue)
	}

	// NOTE merge operations
	if err := temp.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixOperation),
		func(key, b []byte) (bool, error) {
			if err := db.st.Put(key, b, nil); err != nil {
				return false, err
			}

			return true, nil
		}, true); err != nil {
		return nil, nil, e(err, "failed to merge operations")
	}

	// NOTE merge states
	var bsufst []byte
	if err := temp.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixState),
		func(key, b []byte) (bool, error) {
			if err := db.st.Put(key, b, nil); err != nil {
				return false, err
			}

			if bytes.Equal(key, leveldbSuffrageStateKey) {
				bsufst = b
			}

			return true, nil
		}, true); err != nil {
		return nil, nil, e(err, "failed to merge states")
	}

	// NOTE merge suffrage state
	if sufsv != nil && len(bsufst) > 0 {
		if err := db.st.Put(leveldbSuffrageKey(temp.Height()), bsufst, nil); err != nil {
			return nil, nil, e(err, "failed to put suffrage by block height")
		}

		if err := db.st.Put(leveldbSuffrageHeightKey(sufsv.Height()), bsufst, nil); err != nil {
			return nil, nil, e(err, "failed to put suffrage by height")
		}
	}

	// NOTE merge manifest
	switch b, found, err := temp.st.Get(leveldbKeyPrefixManifest); {
	case err != nil || !found:
		return nil, nil, e(err, "failed to get manifest from TempDatabase")
	default:
		if err := db.st.Put(leveldbManifestKey(temp.Height()), b, nil); err != nil {
			return nil, nil, e(err, "failed to put manifest")
		}
	}

	return m, sufstt, nil
}

func (db *LeveldbPermanentDatabase) loadLastManifest() error {
	e := util.StringErrorFunc("failed to load last manifest")

	var m base.Manifest
	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixManifest),
		func(_, b []byte) (bool, error) {
			i, err := db.decodeManifest(b)
			if err != nil {
				return false, err
			}

			m = i

			return false, nil
		},
		false,
	); err != nil {
		return e(err, "")
	}

	if m == nil {
		return nil
	}

	_ = db.m.SetValue(m)

	return nil
}

func (db *LeveldbPermanentDatabase) loadLastSuffrage() error {
	e := util.StringErrorFunc("failed to load last suffrage state")

	var sufstt base.State
	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixSuffrageHeight),
		func(_, b []byte) (bool, error) {
			i, err := db.decodeSuffrage(b)
			if err != nil {
				return false, err
			}

			sufstt = i

			return false, nil
		},
		false,
	); err != nil {
		return e(err, "")
	}

	if sufstt == nil {
		return nil
	}

	_ = db.sufstt.SetValue(sufstt)

	return nil
}
