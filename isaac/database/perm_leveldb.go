package isaacdatabase

import (
	"context"
	"math"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
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

	if err := db.loadLastBlockMap(); err != nil {
		return nil, err
	}

	if err := db.loadLastSuffrageProof(); err != nil {
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

	return db.basePermanent.Clean()
}

func (db *LeveldbPermanent) SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error) {
	e := util.StringErrorFunc("failed to get suffrageproof by height")

	proof, found, err := db.LastSuffrageProof()

	switch {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	}

	stv, err := base.LoadSuffrageState(proof.State())
	if err != nil {
		return nil, false, e(err, "")
	}

	switch {
	case suffrageHeight > stv.Height():
		return nil, false, nil
	case suffrageHeight == stv.Height():
		return proof, true, nil
	}

	switch b, found, err := db.st.Get(leveldbSuffrageProofKey(suffrageHeight)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		var proof base.SuffrageProof

		if err := db.readHinter(b, &proof); err != nil {
			return nil, false, e(err, "")
		}

		return proof, true, nil
	}
}

func (db *LeveldbPermanent) SuffrageProofByBlockHeight(height base.Height) (base.SuffrageProof, bool, error) {
	e := util.StringErrorFunc("failed to get suffrage by block height")

	switch proof, found, err := db.LastSuffrageProof(); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	case height >= proof.State().Height():
		return proof, true, nil
	}

	switch b, found, err := db.st.Get(leveldbSuffrageProofByBlockHeightKey(height)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		var proof base.SuffrageProof

		if err := db.readHinter(b, &proof); err != nil {
			return nil, false, e(err, "")
		}

		return proof, true, nil
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

func (db *LeveldbPermanent) BlockMap(height base.Height) (m base.BlockMap, found bool, _ error) {
	e := util.StringErrorFunc("failed to load blockmap")

	switch i, found, err := db.LastBlockMap(); {
	case err != nil:
		return nil, false, e(err, "")
	case found:
		return i, true, nil
	}

	switch b, found, err := db.st.Get(leveldbBlockMapKey(height)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		if err := db.readHinter(b, &m); err != nil {
			return nil, false, e(err, "")
		}

		return m, true, nil
	}
}

func (db *LeveldbPermanent) MergeTempDatabase(ctx context.Context, temp isaac.TempDatabase) error {
	db.Lock()
	defer db.Unlock()

	e := util.StringErrorFunc("failed to merge TempDatabase")

	switch t := temp.(type) {
	case *TempLeveldb:
		if err := db.mergeTempDatabaseFromLeveldb(ctx, t); err != nil {
			return e(err, "")
		}

		return nil
	default:
		return e(nil, "unknown temp database, %T", temp)
	}
}

func (db *LeveldbPermanent) mergeTempDatabaseFromLeveldb(ctx context.Context, temp *TempLeveldb) error {
	e := util.StringErrorFunc("failed to merge LeveldbTempDatabase")

	if temp.mp == nil {
		return e(storage.NotFoundError.Errorf("blockmap not found in LeveldbTempDatabase"), "")
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
		return e(err, "")
	}

	// NOTE merge states
	if err := worker.NewJob(func(ctx context.Context, jobid uint64) error {
		if err := db.mergeStatesTempDatabaseFromLeveldb(temp); err != nil {
			return errors.Wrap(err, "failed to merge states")
		}

		return nil
	}); err != nil {
		return e(err, "")
	}

	// NOTE merge blockmap
	if err := worker.NewJob(func(ctx context.Context, jobid uint64) error {
		switch b, found, err := temp.st.Get(leveldbKeyPrefixBlockMap); {
		case err != nil || !found:
			return errors.Wrap(err, "failed to get blockmap from TempDatabase")
		default:
			if err := db.st.Put(leveldbBlockMapKey(temp.Height()), b, nil); err != nil {
				return errors.Wrap(err, "failed to put blockmap")
			}

			return nil
		}
	}); err != nil {
		return e(err, "")
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	_ = db.updateLast(temp.mp, temp.proof, temp.policy)

	return nil
}

func (db *LeveldbPermanent) mergeStatesTempDatabaseFromLeveldb(temp *TempLeveldb) error {
	e := util.StringErrorFunc("failed to merge states from LeveldbTempDatabase")

	sufst := temp.sufst

	// NOTE merge states
	if err := temp.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixState),
		func(key, b []byte) (bool, error) {
			if err := db.st.Put(key, b, nil); err != nil {
				return false, err
			}

			return true, nil
		}, true); err != nil {
		return e(err, "failed to merge states")
	}

	// NOTE merge suffrage state
	if sufst != nil {
		switch b, found, err := temp.st.Get(leveldbKeySuffrageProof); {
		case err != nil:
			return e(err, "failed to get SuffrageProof")
		case !found:
			return storage.NotFoundError.Errorf("failed to get SuffrageProof")
		default:
			if err := db.st.Put(leveldbSuffrageProofKey(temp.SuffrageHeight()), b, nil); err != nil {
				return e(err, "failed to set SuffrageProof")
			}

			if err := db.st.Put(leveldbSuffrageProofByBlockHeightKey(sufst.Height()), b, nil); err != nil {
				return errors.Wrap(err, "failed to set SuffrageProof by block height")
			}
		}
	}

	return nil
}

func (db *LeveldbPermanent) loadLastBlockMap() error {
	e := util.StringErrorFunc("failed to load last blockmap")

	var m base.BlockMap

	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixBlockMap),
		func(_, b []byte) (bool, error) {
			return false, db.readHinter(b, &m)
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

func (db *LeveldbPermanent) loadLastSuffrageProof() error {
	e := util.StringErrorFunc("failed to load last suffrage state")

	var proof base.SuffrageProof

	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeySuffrageProof),
		func(_, b []byte) (bool, error) {
			return false, db.readHinter(b, &proof)
		},
		false,
	); err != nil {
		return e(err, "")
	}

	_ = db.proof.SetValue(proof)

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
