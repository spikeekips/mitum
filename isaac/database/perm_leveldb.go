package database

import (
	"bytes"
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type LeveldbPermanent struct {
	*basePermanent
	*baseLeveldb
	st *leveldbstorage.WriteStorage
}

func NewLeveldbPermanent(
	f string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*LeveldbPermanent, error) {
	st, err := leveldbstorage.NewWriteStorage(f)
	if err != nil {
		return nil, errors.Wrap(err, "failed new LeveldbPermanentDatabase")
	}

	return newLeveldbPermanent(st, encs, enc)
}

func newLeveldbPermanent(
	st *leveldbstorage.WriteStorage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*LeveldbPermanent, error) {
	db := &LeveldbPermanent{
		basePermanent: newBasePermanent(),
		baseLeveldb:   newBaseLeveldb(st, encs, enc),
		st:            st,
	}

	if err := db.loadLastBlockDataMap(); err != nil {
		return nil, err
	}

	if err := db.loadLastSuffrage(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *LeveldbPermanent) Suffrage(height base.Height) (base.State, bool, error) {
	e := util.StringErrorFunc("failed to get suffrage by block height")

	switch m, found, err := db.LastMap(); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	case height > m.Manifest().Height():
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

func (db *LeveldbPermanent) SuffrageByHeight(suffrageHeight base.Height) (base.State, bool, error) {
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

func (db *LeveldbPermanent) State(key string) (base.State, bool, error) {
	return db.state(key)
}

func (db *LeveldbPermanent) ExistsOperation(h util.Hash) (bool, error) {
	return db.existsOperation(h)
}

func (db *LeveldbPermanent) Map(height base.Height) (base.BlockDataMap, bool, error) {
	e := util.StringErrorFunc("failed to load blockdatamap")

	switch m, found, err := db.LastMap(); {
	case err != nil:
		return nil, false, e(err, "")
	case found:
		return m, true, nil
	}

	switch b, found, err := db.st.Get(leveldbBlockDataMapKey(height)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		m, err := db.decodeBlockDataMap(b)
		if err != nil {
			return nil, false, e(err, "")
		}

		return m, true, nil
	}
}

func (db *LeveldbPermanent) MergeTempDatabase(_ context.Context, temp isaac.TempDatabase) error {
	db.Lock()
	defer db.Unlock()

	if !db.canMergeTempDatabase(temp) {
		return nil
	}

	e := util.StringErrorFunc("failed to merge TempDatabase")

	switch t := temp.(type) {
	case *TempLeveldb:
		mp, sufstt, err := db.mergeTempDatabaseFromLeveldb(t)
		if err != nil {
			return e(err, "")
		}

		_ = db.mp.SetValue(mp)
		_ = db.sufstt.SetValue(sufstt)

		return nil
	default:
		return e(nil, "unknown temp database, %T", temp)
	}
}

func (db *LeveldbPermanent) mergeTempDatabaseFromLeveldb(temp *TempLeveldb) (
	base.BlockDataMap, base.State, error,
) {
	e := util.StringErrorFunc("failed to merge LeveldbTempDatabase")

	var mp base.BlockDataMap
	switch i, err := temp.Map(); {
	case err != nil:
		return nil, nil, e(err, "")
	default:
		mp = i
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

	// NOTE merge blockdatamap
	switch b, found, err := temp.st.Get(leveldbKeyPrefixBlockDataMap); {
	case err != nil || !found:
		return nil, nil, e(err, "failed to get blockdatamap from TempDatabase")
	default:
		if err := db.st.Put(leveldbBlockDataMapKey(temp.Height()), b, nil); err != nil {
			return nil, nil, e(err, "failed to put blockdatamap")
		}
	}

	return mp, sufstt, nil
}

func (db *LeveldbPermanent) loadLastBlockDataMap() error {
	e := util.StringErrorFunc("failed to load last blockdatamap")

	var m base.BlockDataMap
	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixBlockDataMap),
		func(_, b []byte) (bool, error) {
			i, err := db.decodeBlockDataMap(b)
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

	_ = db.mp.SetValue(m)

	return nil
}

func (db *LeveldbPermanent) loadLastSuffrage() error {
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
