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

	return newLeveldbPermanentDatabase(st, encs, enc), nil
}

func newLeveldbPermanentDatabase(
	st *leveldbstorage.WriteStorage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *LeveldbPermanentDatabase {
	return &LeveldbPermanentDatabase{
		baseLeveldbDatabase: newBaseLeveldbDatabase(st, encs, enc),
		st:                  st,
		m:                   util.NewLocked(nil),
		sufstt:              util.NewLocked(nil),
	}
}

func (db *LeveldbPermanentDatabase) load() error {
	// BLOCK load last manifest and suffrage state
	return nil
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
	default:
		switch _, found, err := db.LastSuffrage(); {
		case err != nil:
			return nil, false, e(err, "")
		case !found:
			return nil, false, nil
		}
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
	}

	var st base.State
	if err := db.st.Iter(
		&leveldbutil.Range{Start: beginSuffrageHeightDBKey, Limit: suffrageHeightDBKey(suffrageHeight + 1)},
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
		return nil, false, errors.Wrap(err, "failed to get suffrage by height")
	}

	return st, st != nil, nil
}

func (db *LeveldbPermanentDatabase) State(key string) (base.State, bool, error) {
	return db.state(key)
}

func (db *LeveldbPermanentDatabase) ExistsOperation(h util.Hash) (bool, error) {
	return db.existsOperation(h)
}

func (db *LeveldbPermanentDatabase) MergeTempDatabase(TempDatabase) error {
	return nil
}
