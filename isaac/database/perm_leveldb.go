package isaacdatabase

import (
	"bytes"
	"context"
	"math"

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

	if err := db.loadLastBlockdataMap(); err != nil {
		return nil, err
	}

	if err := db.loadLastSuffrage(); err != nil {
		return nil, err
	}

	if err := db.loadNetworkPolicy(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *LeveldbPermanent) Clean() error {
	if err := db.st.Remove(); err != nil {
		return errors.Wrap(err, "failed to clean leveldb PermanentDatabase")
	}

	return nil
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

func (db *LeveldbPermanent) ExistsInStateOperation(h util.Hash) (bool, error) {
	return db.existsInStateOperation(h)
}

func (db *LeveldbPermanent) ExistsKnownOperation(h util.Hash) (bool, error) {
	return db.existsKnownOperation(h)
}

func (db *LeveldbPermanent) Map(height base.Height) (base.BlockdataMap, bool, error) {
	e := util.StringErrorFunc("failed to load blockdatamap")

	switch m, found, err := db.LastMap(); {
	case err != nil:
		return nil, false, e(err, "")
	case found:
		return m, true, nil
	}

	switch b, found, err := db.st.Get(leveldbBlockdataMapKey(height)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		m, err := db.decodeBlockdataMap(b)
		if err != nil {
			return nil, false, e(err, "")
		}

		return m, true, nil
	}
}

func (db *LeveldbPermanent) MergeTempDatabase(ctx context.Context, temp isaac.TempDatabase) error {
	db.Lock()
	defer db.Unlock()

	if !db.canMergeTempDatabase(temp) {
		return nil
	}

	e := util.StringErrorFunc("failed to merge TempDatabase")

	switch t := temp.(type) {
	case *TempLeveldb:
		mp, sufstt, err := db.mergeTempDatabaseFromLeveldb(ctx, t)
		if err != nil {
			return e(err, "")
		}

		_ = db.mp.SetValue(mp)
		if sufstt != nil {
			_ = db.sufstt.SetValue(sufstt)
		}

		if t.policy != nil {
			_ = db.policy.SetValue(t.policy)
		}

		return nil
	default:
		return e(nil, "unknown temp database, %T", temp)
	}
}

func (db *LeveldbPermanent) mergeTempDatabaseFromLeveldb(ctx context.Context, temp *TempLeveldb) (
	base.BlockdataMap, base.State, error,
) {
	e := util.StringErrorFunc("failed to merge LeveldbTempDatabase")

	var mp base.BlockdataMap
	switch i, err := temp.Map(); {
	case err != nil:
		return nil, nil, e(err, "")
	default:
		mp = i
	}

	worker := util.NewErrgroupWorker(ctx, math.MaxInt32)
	defer worker.Close()

	// NOTE merge operations
	if err := worker.NewJob(func(ctx context.Context, jobid uint64) error {
		if err := db.mergeOperationsTempDatabaseFromLeveldb(temp); err != nil {
			return errors.Wrap(err, "failed to merge operations")
		}

		return nil
	}); err != nil {
		return nil, nil, e(err, "")
	}

	// NOTE merge states
	var sufstt base.State
	if err := worker.NewJob(func(ctx context.Context, jobid uint64) error {
		switch i, err := db.mergeStatesTempDatabaseFromLeveldb(temp); {
		case err != nil:
			return errors.Wrap(err, "failed to merge states")
		default:
			sufstt = i

			return nil
		}
	}); err != nil {
		return nil, nil, e(err, "")
	}

	// NOTE merge blockdatamap
	if err := worker.NewJob(func(ctx context.Context, jobid uint64) error {
		switch b, found, err := temp.st.Get(leveldbKeyPrefixBlockdataMap); {
		case err != nil || !found:
			return errors.Wrap(err, "failed to get blockdatamap from TempDatabase")
		default:
			if err := db.st.Put(leveldbBlockdataMapKey(temp.Height()), b, nil); err != nil {
				return errors.Wrap(err, "failed to put blockdatamap")
			}

			return nil
		}
	}); err != nil {
		return nil, nil, e(err, "")
	}

	worker.Done()
	if err := worker.Wait(); err != nil {
		return nil, nil, e(err, "")
	}

	return mp, sufstt, nil
}

func (db *LeveldbPermanent) loadLastBlockdataMap() error {
	e := util.StringErrorFunc("failed to load last blockdatamap")

	var m base.BlockdataMap
	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixBlockdataMap),
		func(_, b []byte) (bool, error) {
			i, err := db.decodeBlockdataMap(b)
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

func (db *LeveldbPermanent) loadNetworkPolicy() error {
	switch policy, found, err := db.baseLeveldb.loadNetworkPolicy(); {
	case err != nil:
		return errors.Wrap(err, "")
	case !found:
		return nil
	default:
		_ = db.policy.SetValue(policy)

		return nil
	}
}

func (db *LeveldbPermanent) mergeOperationsTempDatabaseFromLeveldb(temp *TempLeveldb) error {
	// NOTE merge operations
	if err := temp.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixInStateOperation),
		func(key, b []byte) (bool, error) {
			if err := db.st.Put(key, b, nil); err != nil {
				return false, err
			}

			return true, nil
		}, true); err != nil {
		return errors.Wrap(err, "failed to merge instate operations")
	}

	if err := temp.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixKnownOperation),
		func(key, b []byte) (bool, error) {
			if err := db.st.Put(key, b, nil); err != nil {
				return false, err
			}

			return true, nil
		}, true); err != nil {
		return errors.Wrap(err, "failed to merge known operations")
	}

	return nil
}

func (db *LeveldbPermanent) mergeStatesTempDatabaseFromLeveldb(temp *TempLeveldb) (base.State, error) {
	var sufstt base.State
	var sufsv base.SuffrageStateValue
	switch st, found, err := temp.Suffrage(); {
	case err != nil:
		return nil, errors.Wrap(err, "")
	case found:
		sufstt = st
		sufsv = st.Value().(base.SuffrageStateValue)
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
		return nil, errors.Wrap(err, "failed to merge states")
	}

	// NOTE merge suffrage state
	if sufsv != nil && len(bsufst) > 0 {
		if err := db.st.Put(leveldbSuffrageKey(temp.Height()), bsufst, nil); err != nil {
			return nil, errors.Wrap(err, "failed to put suffrage by block height")
		}

		if err := db.st.Put(leveldbSuffrageHeightKey(sufsv.Height()), bsufst, nil); err != nil {
			return nil, errors.Wrap(err, "failed to put suffrage by height")
		}
	}

	return sufstt, nil
}
