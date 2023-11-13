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
	stcachesize int,
) (*LeveldbPermanent, error) {
	pst := leveldbstorage.NewPrefixStorage(st, leveldbLabelPermanent[:])

	db := &LeveldbPermanent{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "leveldb-permanent-database")
		}),
		basePermanent: newBasePermanent(stcachesize),
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

func (db *LeveldbPermanent) SuffrageProof(suffrageHeight base.Height) (proof base.SuffrageProof, found bool, _ error) {
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
		switch b, found, err := pst.Get(leveldbSuffrageProofKey(suffrageHeight)); {
		case err != nil, !found:
			return nil, found, err
		default:
			if err := ReadDecodeFrame(db.encs, b, &proof); err != nil {
				return nil, true, err
			}

			return proof, true, nil
		}
	}
}

func (db *LeveldbPermanent) SuffrageProofBytes(suffrageHeight base.Height) (
	enchint string, meta, body []byte, found bool, err error,
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
		switch b, found, err := pst.Get(leveldbSuffrageProofKey(suffrageHeight)); {
		case err != nil, !found:
			return enchint, nil, nil, found, err
		default:
			enchint, meta, body, err := ReadOneHeaderFrame(b)
			if err != nil {
				return enchint, nil, nil, true, err
			}

			return enchint, meta, body, true, nil
		}
	}
}

func (db *LeveldbPermanent) SuffrageProofByBlockHeight(height base.Height) (
	proof base.SuffrageProof, found bool, _ error,
) {
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

	switch i, found, err := db.LastSuffrageProof(); {
	case err != nil:
		return nil, false, e.Wrap(err)
	case !found:
		return nil, false, nil
	case height >= i.State().Height():
		return i, true, nil
	}

	switch b, found, err := func() ([]byte, bool, error) {
		r := leveldbutil.BytesPrefix(leveldbKeySuffrageProofByBlockHeight[:])
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
	}(); {
	case err != nil, !found:
		return nil, found, err
	default:
		if err := ReadDecodeFrame(db.encs, b, &proof); err != nil {
			return nil, true, err
		}

		return proof, true, nil
	}
}

func (db *LeveldbPermanent) State(key string) (st base.State, found bool, _ error) {
	switch i, j, err := db.basePermanent.state(key); {
	case err != nil:
		return nil, false, err
	case j:
		return i, j, nil
	}

	pst, err := db.st()
	if err != nil {
		return nil, false, err
	}

	switch b, found, err := pst.Get(leveldbStateKey(key)); {
	case err != nil, !found:
		return nil, found, err
	default:
		if err := ReadDecodeFrame(db.encs, b, &st); err != nil {
			return nil, true, err
		}

		db.setState(st)

		return st, true, nil
	}
}

func (db *LeveldbPermanent) StateBytes(key string) (enchint string, meta, body []byte, found bool, err error) {
	pst, err := db.st()
	if err != nil {
		return enchint, nil, nil, false, err
	}

	switch b, found, err := pst.Get(leveldbStateKey(key)); {
	case err != nil, !found:
		return enchint, nil, nil, found, err
	default:
		enchint, meta, body, err := ReadOneHeaderFrame(b)
		if err != nil {
			return enchint, nil, nil, true, err
		}

		return enchint, meta, body, true, nil
	}
}

func (db *LeveldbPermanent) ExistsInStateOperation(h util.Hash) (bool, error) {
	if db.instateoperationcache != nil {
		switch found, incache := db.instateoperationcache.Get(h.String()); {
		case !incache:
		case found:
			return true, nil
		}
	}

	switch found, err := db.existsInStateOperation(h); {
	case err != nil:
		return false, err
	default:
		if db.instateoperationcache != nil && found {
			db.instateoperationcache.Set(h.String(), true, 0)
		}

		return found, nil
	}
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

	switch b, found, err := pst.Get(leveldbBlockMapKey(height)); {
	case err != nil, !found:
		return nil, found, err
	default:
		if err := ReadDecodeFrame(db.encs, b, &m); err != nil {
			return nil, true, err
		}

		return m, true, nil
	}
}

func (db *LeveldbPermanent) BlockMapBytes(height base.Height) (
	enchint string, meta, body []byte, found bool, _ error,
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

	switch b, found, err := pst.Get(leveldbBlockMapKey(height)); {
	case err != nil, !found:
		return enchint, nil, nil, found, err
	default:
		enchint, meta, body, err := ReadOneHeaderFrame(b)
		if err != nil {
			return enchint, nil, nil, true, err
		}

		return enchint, meta, body, true, nil
	}
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
		temp.enc.Hint().String(),
		temp.mp, temp.mpmeta, temp.mpbody,
		temp.proof, temp.proofmeta, temp.proofbody,
		temp.policy,
	)

	db.basePermanent.mergeTempCaches(temp.stcache, temp.instateoperationcache)

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
	var meta, body []byte

	if err := pst.Iter(
		leveldbutil.BytesPrefix(leveldbKeySuffrageProof[:]),
		func(_, b []byte) (bool, error) {
			var err error

			meta, err = ReadDecodeOneHeaderFrame(db.encs, b, &proof)

			return false, err
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
	switch st, policy, found, err := db.baseLeveldb.loadNetworkPolicy(); {
	case err != nil:
		return err
	case !found:
		return nil
	default:
		_ = db.policy.SetValue(policy)

		db.setState(st)

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
