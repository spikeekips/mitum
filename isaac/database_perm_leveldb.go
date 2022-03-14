package isaac

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type LeveldbPermanentDatabase struct {
	*baseLeveldbDatabase
	st     *leveldbstorage.WriteStorage
	m      *util.Locked // NOTE last manifest
	sufstt *util.Locked // NOTE last suffrage state
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
		baseLeveldbDatabase: newBaseLeveldbDatabase(st, encs, enc),
		st:                  st,
		m:                   util.NewLocked(nil),
		sufstt:              util.NewLocked(nil),
	}

	return db, nil
}

func (db *LeveldbPermanentDatabase) LastManifest() (base.Manifest, bool, error) {
	var m base.Manifest
	switch _ = db.m.Value(&m); {
	case m == nil:
		return nil, false, nil
	default:
		return m, true, nil
	}
}

func (db *LeveldbPermanentDatabase) Manifest(height base.Height) (base.Manifest, bool, error) {
	e := util.StringErrorFunc("failed to load manifest")

	switch m, found, err := db.LastManifest(); {
	case err != nil:
		return nil, false, e(err, "")
	case found:
		return m, true, nil
	}

	switch b, found, err := db.st.Get(manifestDBKey(height)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, e(err, "manifest not found")
	default:
		m, err := db.decodeManifest(b)
		if err != nil {
			return nil, false, e(err, "")
		}

		return m, true, nil
	}
}

func (db *LeveldbPermanentDatabase) LastSuffrage() (base.State, bool, error) {
	var m base.State
	switch _ = db.sufstt.Value(&m); {
	case m == nil:
		return nil, false, nil
	default:
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
		&leveldbutil.Range{Start: beginSuffrageDBKey, Limit: suffrageDBKey(height + 1)},
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

	switch b, found, err := db.st.Get(suffrageHeightDBKey(suffrageHeight)); {
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

func (db *LeveldbPermanentDatabase) MergeTempDatabase(temp TempDatabase) error {
	e := util.StringErrorFunc("failed to merge TempDatabase")
	switch t := temp.(type) {
	case *TempLeveldbDatabase:
		if err := db.mergeTempDatabaseFromLeveldb(t); err != nil {
			return e(err, "")
		}

		return nil
	default:
		return e(nil, "unknown temp database, %T", temp)
	}
}

func (db *LeveldbPermanentDatabase) mergeTempDatabaseFromLeveldb(temp *TempLeveldbDatabase) error {
	// NOTE merge operations
	// NOTE merge states
	// NOTE merge suffrage
	// NOTE merge manifest
	return nil
}
