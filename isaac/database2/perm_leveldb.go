package isaacdatabase2

import (
	"context"
	"math"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage2 "github.com/spikeekips/mitum/storage/leveldb2"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type LeveldbPermanent struct {
	*basePermanent
	*baseLeveldb
	batchlimit int
}

func NewLeveldbPermanent(
	st *leveldbstorage2.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*LeveldbPermanent, error) {
	return newLeveldbPermanent(st, encs, enc)
}

func newLeveldbPermanent(
	st *leveldbstorage2.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*LeveldbPermanent, error) {
	pst := leveldbstorage2.NewPrefixStorage(st, leveldbstorage2.HashPrefix(leveldbLabelPermanent))

	db := &LeveldbPermanent{
		basePermanent: newBasePermanent(),
		baseLeveldb:   newBaseLeveldb(pst, encs, enc),
		batchlimit:    333, //nolint:gomnd //...
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
	r := leveldbutil.BytesPrefix(db.st.Prefix())

	if _, err := leveldbstorage2.BatchRemove(db.st.Storage, r, 333); err != nil { //nolint:gomnd //...
		return errors.WithMessage(err, "failed to clean leveldb PermanentDatabase")
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
	case !found:
		return nil, false, nil
	case found && i.Manifest().Height() == height:
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

	worker := util.NewErrgroupWorker(ctx, math.MaxInt8)
	defer worker.Close()

	batch := db.st.NewBatch()
	defer batch.Reset()

	if err := temp.st.Iter(nil, func(k, v []byte) (bool, error) {
		if batch.Len() == db.batchlimit {
			b := batch

			if err := worker.NewJob(func(ctx context.Context, jobid uint64) error {
				return db.st.Batch(b, nil)
			}); err != nil {
				return false, err
			}

			batch = temp.st.NewBatch()
		}

		batch.Put(k, v)

		return true, nil
	}, false); err != nil {
		return e(err, "")
	}

	if batch.Len() > 0 {
		if err := worker.NewJob(func(ctx context.Context, jobid uint64) error {
			return db.st.Batch(batch, nil)
		}); err != nil {
			return e(err, "")
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	_ = db.updateLast(temp.mp, temp.proof, temp.policy)

	return nil
}

func (db *LeveldbPermanent) loadLastBlockMap() error {
	switch m, err := db.baseLeveldb.loadLastBlockMap(); {
	case err != nil:
		return err
	case m == nil:
		return nil
	default:
		_ = db.mp.SetValue(m)

		return nil
	}
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
		return err
	case !found:
		return nil
	default:
		_ = db.policy.SetValue(policy)

		return nil
	}
}
