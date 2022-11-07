package isaacdatabase

import (
	"context"
	"math"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type LeveldbPermanent struct {
	*logging.Logging
	*basePermanent
	*baseLeveldb
	batchlimit int
}

func NewLeveldbPermanent(
	st *leveldbstorage.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*LeveldbPermanent, error) {
	return newLeveldbPermanent(st, encs, enc)
}

func newLeveldbPermanent(
	st *leveldbstorage.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*LeveldbPermanent, error) {
	pst := leveldbstorage.NewPrefixStorage(st, leveldbLabelPermanent)

	db := &LeveldbPermanent{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "leveldb-permanent-database")
		}),
		basePermanent: newBasePermanent(encs, enc),
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

	if _, err := leveldbstorage.BatchRemove(db.st.Storage, r, 333); err != nil { //nolint:gomnd //...
		return errors.WithMessage(err, "failed to clean leveldb PermanentDatabase")
	}

	return db.basePermanent.Clean()
}

func (db *LeveldbPermanent) SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error) {
	e := util.StringErrorFunc("failed to get suffrageproof by height")

	switch proof, found, err := compareWithLastSuffrageProof(suffrageHeight, db.LastSuffrageProof); {
	case err != nil:
		return nil, false, e(err, "")
	case found:
		return proof, true, nil
	default:
		var proof base.SuffrageProof

		found, err := db.getRecord(leveldbSuffrageProofKey(suffrageHeight), db.st.Get, &proof)

		return proof, found, err
	}
}

func (db *LeveldbPermanent) SuffrageProofBytes(suffrageHeight base.Height) (
	enchint hint.Hint, meta, body []byte, found bool, err error,
) {
	e := util.StringErrorFunc("failed to get suffrageproof by height")

	switch _, found, err := compareWithLastSuffrageProof(suffrageHeight, db.LastSuffrageProof); {
	case err != nil:
		return enchint, nil, nil, false, e(err, "")
	case found:
		return db.LastSuffrageProofBytes()
	default:
		return db.getRecordBytes(leveldbSuffrageProofKey(suffrageHeight), db.st.Get)
	}
}

func (db *LeveldbPermanent) SuffrageProofByBlockHeight(height base.Height) (base.SuffrageProof, bool, error) {
	e := util.StringErrorFunc("failed to get suffrage by block height")

	switch m, found, err := db.LastBlockMap(); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	case height > m.Manifest().Height():
		return nil, false, nil
	}

	switch proof, found, err := db.LastSuffrageProof(); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	case height >= proof.State().Height():
		return proof, true, nil
	}

	var proof base.SuffrageProof

	found, err := db.getRecord(nil,
		func([]byte) ([]byte, bool, error) {
			r := leveldbutil.BytesPrefix(leveldbKeySuffrageProofByBlockHeight)
			r.Limit = leveldbSuffrageProofByBlockHeightKey(height + 1)

			var body []byte

			err := db.st.Iter(r, func(_, b []byte) (bool, error) {
				body = b

				return false, nil
			}, false)
			if err != nil {
				return nil, false, err
			}

			return body, body != nil, nil
		},
		&proof)

	return proof, found, err
}

func (db *LeveldbPermanent) State(key string) (st base.State, found bool, err error) {
	found, err = db.getRecord(leveldbStateKey(key), db.st.Get, &st)

	return st, found, err
}

func (db *LeveldbPermanent) StateBytes(key string) (enchint hint.Hint, meta, body []byte, found bool, err error) {
	return db.getRecordBytes(leveldbStateKey(key), db.st.Get)
}

func (db *LeveldbPermanent) ExistsInStateOperation(h util.Hash) (bool, error) {
	return db.existsInStateOperation(h)
}

func (db *LeveldbPermanent) ExistsKnownOperation(h util.Hash) (bool, error) {
	return db.existsKnownOperation(h)
}

func (db *LeveldbPermanent) BlockMap(height base.Height) (m base.BlockMap, _ bool, _ error) {
	e := util.StringErrorFunc("failed to load blockmap")

	switch i, found, err := db.LastBlockMap(); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	case found && i.Manifest().Height() == height:
		return i, true, nil
	}

	found, err := db.getRecord(leveldbBlockMapKey(height), db.st.Get, &m)

	return m, found, err
}

func (db *LeveldbPermanent) BlockMapBytes(height base.Height) (
	enchint hint.Hint, meta, body []byte, found bool, err error,
) {
	e := util.StringErrorFunc("failed to load blockmap bytes")

	switch i, found, err := db.LastBlockMap(); {
	case err != nil:
		return enchint, nil, nil, false, e(err, "")
	case !found:
		return enchint, nil, nil, false, nil
	case found && i.Manifest().Height() == height:
		return db.LastBlockMapBytes()
	}

	return db.getRecordBytes(leveldbBlockMapKey(height), db.st.Get)
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
		return e(storage.ErrNotFound.Errorf("blockmap not found in LeveldbTempDatabase"), "")
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

			batch = db.st.NewBatch()
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

	_ = db.updateLast(
		temp.enc.Hint(),
		temp.mp, temp.mpmeta, temp.mpbody,
		temp.proof, temp.proofmeta, temp.proofbody,
		temp.policy,
	)

	db.Log().Info().Interface("blockmap", temp.mp).Msg("new block merged")

	return nil
}

func (db *LeveldbPermanent) loadLastBlockMap() error {
	switch m, enchint, meta, body, err := db.baseLeveldb.loadLastBlockMap(); {
	case err != nil:
		return err
	case m == nil:
		return nil
	default:
		_ = db.lenc.SetValue(enchint)
		_ = db.mp.SetValue([3]interface{}{m, meta, body})

		return nil
	}
}

func (db *LeveldbPermanent) loadLastSuffrageProof() error {
	e := util.StringErrorFunc("failed to load last suffrage state")

	var proof base.SuffrageProof

	var enchint hint.Hint
	var meta, body []byte

	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeySuffrageProof),
		func(_, b []byte) (bool, error) {
			var err error

			enchint, meta, body, err = db.readHeader(b)
			if err != nil {
				return false, err
			}

			return false, db.readHinterWithEncoder(enchint, body, &proof)
		},
		false,
	); err != nil {
		return e(err, "")
	}

	if proof != nil {
		_ = db.proof.SetValue([3]interface{}{proof, meta, body})
	}

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

func compareWithLastSuffrageProof(
	suffrageHeight base.Height,
	last func() (base.SuffrageProof, bool, error),
) (base.SuffrageProof, bool, error) {
	proof, found, err := last()

	switch {
	case err != nil:
		return nil, false, err
	case !found:
		return nil, false, nil
	}

	stv, err := base.LoadSuffrageNodesStateValue(proof.State())
	if err != nil {
		return nil, false, err
	}

	switch {
	case suffrageHeight > stv.Height():
		return nil, false, nil
	case suffrageHeight == stv.Height():
		return proof, true, nil
	default:
		return nil, false, nil
	}
}
