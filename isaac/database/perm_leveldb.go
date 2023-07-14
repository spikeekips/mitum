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
	pst, err := db.st()
	if err != nil {
		return err
	}

	r := leveldbutil.BytesPrefix(pst.Prefix())

	if _, err := leveldbstorage.BatchRemove(pst.Storage, r, 333); err != nil { //nolint:gomnd //...
		return errors.WithMessage(err, "clean leveldb PermanentDatabase")
	}

	return db.basePermanent.Clean()
}

func (db *LeveldbPermanent) SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error) {
	e := util.StringError("get suffrageproof by height")

	pst, err := db.st()
	if err != nil {
		return nil, false, e.Wrap(err)
	}

	switch proof, found, err := compareWithLastSuffrageProof(suffrageHeight, db.LastSuffrageProof); {
	case err != nil:
		return nil, false, e.Wrap(err)
	case found:
		return proof, true, nil
	default:
		var proof base.SuffrageProof

		found, err := db.getRecord(leveldbSuffrageProofKey(suffrageHeight), pst.Get, &proof)

		return proof, found, err
	}
}

func (db *LeveldbPermanent) SuffrageProofBytes(suffrageHeight base.Height) (
	enchint hint.Hint, meta, body []byte, found bool, err error,
) {
	e := util.StringError("get suffrageproof by height")

	pst, err := db.st()
	if err != nil {
		return enchint, nil, nil, false, e.Wrap(err)
	}

	switch _, found, err := compareWithLastSuffrageProof(suffrageHeight, db.LastSuffrageProof); {
	case err != nil:
		return enchint, nil, nil, false, e.Wrap(err)
	case found:
		return db.LastSuffrageProofBytes()
	default:
		return db.getRecordBytes(leveldbSuffrageProofKey(suffrageHeight), pst.Get)
	}
}

func (db *LeveldbPermanent) SuffrageProofByBlockHeight(height base.Height) (base.SuffrageProof, bool, error) {
	e := util.StringError("get suffrage by block height")

	var pst *leveldbstorage.PrefixStorage

	switch i, err := db.st(); {
	case err != nil:
		return nil, false, e.Wrap(err)
	default:
		pst = i
	}

	switch m, found, err := db.LastBlockMap(); {
	case err != nil:
		return nil, false, e.Wrap(err)
	case !found:
		return nil, false, nil
	case height > m.Manifest().Height():
		return nil, false, nil
	}

	switch proof, found, err := db.LastSuffrageProof(); {
	case err != nil:
		return nil, false, e.Wrap(err)
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

			err := pst.Iter(r, func(_, b []byte) (bool, error) {
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
	pst, err := db.st()
	if err != nil {
		return nil, false, err
	}

	found, err = db.getRecord(leveldbStateKey(key), pst.Get, &st)

	return st, found, err
}

func (db *LeveldbPermanent) StateBytes(key string) (enchint hint.Hint, meta, body []byte, found bool, err error) {
	pst, err := db.st()
	if err != nil {
		return enchint, nil, nil, false, err
	}

	return db.getRecordBytes(leveldbStateKey(key), pst.Get)
}

func (db *LeveldbPermanent) ExistsInStateOperation(h util.Hash) (bool, error) {
	return db.existsInStateOperation(h)
}

func (db *LeveldbPermanent) ExistsKnownOperation(h util.Hash) (bool, error) {
	return db.existsKnownOperation(h)
}

func (db *LeveldbPermanent) BlockMap(height base.Height) (m base.BlockMap, _ bool, _ error) {
	e := util.StringError("load blockmap")

	switch i, found, err := db.LastBlockMap(); {
	case err != nil:
		return nil, false, e.Wrap(err)
	case !found:
		return nil, false, nil
	case found && i.Manifest().Height() == height:
		return i, true, nil
	}

	pst, err := db.st()
	if err != nil {
		return nil, false, e.Wrap(err)
	}

	found, err := db.getRecord(leveldbBlockMapKey(height), pst.Get, &m)

	return m, found, err
}

func (db *LeveldbPermanent) BlockMapBytes(height base.Height) (
	enchint hint.Hint, meta, body []byte, found bool, _ error,
) {
	e := util.StringError("load blockmap bytes")

	switch i, found, err := db.LastBlockMap(); {
	case err != nil:
		return enchint, nil, nil, false, e.Wrap(err)
	case !found:
		return enchint, nil, nil, false, nil
	case found && i.Manifest().Height() == height:
		return db.LastBlockMapBytes()
	}

	pst, err := db.st()
	if err != nil {
		return enchint, nil, nil, false, e.Wrap(err)
	}

	return db.getRecordBytes(leveldbBlockMapKey(height), pst.Get)
}

func (db *LeveldbPermanent) MergeTempDatabase(ctx context.Context, temp isaac.TempDatabase) error {
	db.Lock()
	defer db.Unlock()

	e := util.StringError("merge TempDatabase")

	switch t := temp.(type) {
	case *TempLeveldb:
		if err := db.mergeTempDatabaseFromLeveldb(ctx, t); err != nil {
			return e.Wrap(err)
		}

		return nil
	default:
		return e.Errorf("unknown temp database, %T", temp)
	}
}

func (db *LeveldbPermanent) mergeTempDatabaseFromLeveldb(ctx context.Context, temp *TempLeveldb) error {
	e := util.StringError("merge LeveldbTempDatabase")

	if temp.mp == nil {
		return e.Wrap(storage.ErrNotFound.Errorf("blockmap not found in LeveldbTempDatabase"))
	}

	pst := db.pst
	if pst == nil {
		return e.Wrap(storage.ErrClosed)
	}

	tpst, err := temp.st()
	if err != nil {
		return e.Wrap(err)
	}

	worker, err := util.NewErrgroupWorker(ctx, math.MaxInt8)
	if err != nil {
		return err
	}

	defer worker.Close()

	batch := pst.NewBatch()
	defer batch.Reset()

	if err := tpst.Iter(nil, func(k, v []byte) (bool, error) {
		if batch.Len() == db.batchlimit {
			b := batch

			if err := worker.NewJob(func(ctx context.Context, jobid uint64) error {
				return pst.Batch(b, nil)
			}); err != nil {
				return false, err
			}

			batch = pst.NewBatch()
		}

		batch.Put(k, v)

		return true, nil
	}, false); err != nil {
		return e.Wrap(err)
	}

	if batch.Len() > 0 {
		if err := worker.NewJob(func(ctx context.Context, jobid uint64) error {
			return pst.Batch(batch, nil)
		}); err != nil {
			return e.Wrap(err)
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e.Wrap(err)
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
	e := util.StringError("load last suffrage state")

	pst, err := db.st()
	if err != nil {
		return e.Wrap(err)
	}

	var proof base.SuffrageProof

	var enchint hint.Hint
	var meta, body []byte

	if err := pst.Iter(
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
		return e.Wrap(err)
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
