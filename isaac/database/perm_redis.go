package isaacdatabase

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	redisstorage "github.com/spikeekips/mitum/storage/redis"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

var (
	redisStateKeyPrerfix                  = "stt"
	redisInStateOperationKeyPrerfix       = "iso"
	redisKnownOperationKeyPrerfix         = "kno"
	redisBlockMapKeyPrefix                = "bmp"
	redisSuffrageProofPrefix              = "sup"
	redisSuffrageProofByBlockHeightPrefix = "sph"
)

var (
	redisZKeyBlockMaps                     = "blockmaps"
	redisZBeginBlockMaps                   = redisBlockMapKey(base.GenesisHeight)
	redisZEndBlockMaps                     = fmt.Sprintf("%s-%s", redisBlockMapKeyPrefix, strings.Repeat("9", 20))
	redisZKeySuffrageProofsByBlockHeight   = "suffrageproofs_by_blockheight"
	redisZBeginSuffrageProofsByBlockHeight = redisSuffrageProofByBlockHeightKey(base.GenesisHeight)
	redisZEndSuffrageProofsByBlockHeight   = fmt.Sprintf("%s-%s",
		redisSuffrageProofByBlockHeightPrefix, strings.Repeat("9", 20))
)

type RedisPermanent struct {
	*logging.Logging
	*basePermanent
	encs *encoder.Encoders
	enc  encoder.Encoder
	st   *redisstorage.Storage
	l    sync.Mutex
}

func NewRedisPermanent(
	st *redisstorage.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	stcachesize int,
) (*RedisPermanent, error) {
	db := &RedisPermanent{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "redis-permanent-database")
		}),
		basePermanent: newBasePermanent(stcachesize),
		encs:          encs,
		enc:           enc,
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

func (db *RedisPermanent) Close() error {
	if err := db.st.Close(); err != nil {
		return errors.Wrap(err, "close RedisPermanentDatabase")
	}

	return nil
}

func (db *RedisPermanent) Clean() error {
	if err := db.st.Clean(context.Background()); err != nil {
		return errors.Wrap(err, "clean redis PermanentDatabase")
	}

	return db.basePermanent.Clean()
}

func (db *RedisPermanent) SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error) {
	e := util.StringError("get suffrageproof by height")

	switch proof, found, err := compareWithLastSuffrageProof(suffrageHeight, db.LastSuffrageProof); {
	case err != nil:
		return nil, false, e.Wrap(err)
	case found:
		return proof, true, nil
	default:
		proof, found, err := db.suffrageProofByKey(redisSuffrageProofKey(suffrageHeight))

		return proof, found, e.Wrap(err)
	}
}

func (db *RedisPermanent) suffrageProofByKey(key string) (proof base.SuffrageProof, found bool, _ error) {
	switch b, found, err := db.st.Get(context.Background(), key); {
	case err != nil, !found:
		return nil, found, err
	default:
		if err := ReadDecodeFrame(db.encs, b, &proof); err != nil {
			return nil, true, err
		}

		return proof, true, nil
	}
}

func (db *RedisPermanent) SuffrageProofBytes(suffrageHeight base.Height) (
	enchint string, meta, body []byte, found bool, err error,
) {
	e := util.StringError("get suffrageproof by height")

	switch _, found, err := compareWithLastSuffrageProof(suffrageHeight, db.LastSuffrageProof); {
	case err != nil:
		return enchint, nil, nil, false, e.Wrap(err)
	case found:
		return db.LastSuffrageProofBytes()
	default:
		switch b, found, err := db.st.Get(context.Background(), redisSuffrageProofKey(suffrageHeight)); {
		case err != nil, !found:
			return enchint, nil, nil, found, e.Wrap(err)
		default:
			enchint, meta, body, err := ReadOneHeaderFrame(b)

			return enchint, meta, body, true, e.Wrap(err)
		}
	}
}

func (db *RedisPermanent) SuffrageProofByBlockHeight(height base.Height) (base.SuffrageProof, bool, error) {
	e := util.StringError("get suffrage by block height")

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

	var key string

	if err := db.st.ZRangeArgs(
		context.Background(),
		redis.ZRangeArgs{
			Key:   redisZKeySuffrageProofsByBlockHeight,
			Start: "[" + redisZBeginSuffrageProofsByBlockHeight,
			Stop:  "[" + redisSuffrageProofByBlockHeightKey(height),
			ByLex: true,
			Rev:   true,
			Count: 1,
		},
		func(i string) (bool, error) {
			key = i

			return false, nil
		},
	); err != nil {
		return nil, false, e.Wrap(err)
	}

	if len(key) < 1 {
		return nil, false, nil
	}

	return db.suffrageProofByKey(key)
}

func (db *RedisPermanent) State(key string) (st base.State, found bool, _ error) {
	switch i, j, err := db.basePermanent.stateFromCache(key); {
	case err != nil:
		return nil, false, err
	case j:
		return i, j, nil
	}

	switch b, found, err := db.st.Get(context.Background(), redisStateKey(key)); {
	case err != nil, !found:
		return nil, found, err
	default:
		if err := ReadDecodeFrame(db.encs, b, &st); err != nil {
			return nil, true, err
		}

		db.setStateToCache(st)

		return st, true, nil
	}
}

func (db *RedisPermanent) StateBytes(key string) (enchint string, meta, body []byte, found bool, err error) {
	switch b, found, err := db.st.Get(context.Background(), redisStateKey(key)); {
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

func (db *RedisPermanent) ExistsInStateOperation(h util.Hash) (bool, error) {
	e := util.StringError("check instate operation")

	if db.instateoperationcache != nil {
		switch found, incache := db.instateoperationcache.Get(h.String()); {
		case !incache:
		case found:
			return true, nil
		}
	}

	switch found, err := db.st.Exists(context.Background(), redisInStateOperationKey(h.String())); {
	case err != nil:
		return false, e.Wrap(err)
	default:
		if db.instateoperationcache != nil && found {
			db.instateoperationcache.Set(h.String(), true, 0)
		}

		return found, nil
	}
}

func (db *RedisPermanent) ExistsKnownOperation(h util.Hash) (bool, error) {
	e := util.StringError("check known operation")

	switch found, err := db.st.Exists(context.Background(), redisKnownOperationKey(h)); {
	case err != nil:
		return false, e.Wrap(err)
	default:
		return found, nil
	}
}

func (db *RedisPermanent) BlockMap(height base.Height) (m base.BlockMap, _ bool, _ error) {
	e := util.StringError("load blockmap")

	switch i, found, err := db.LastBlockMap(); {
	case err != nil:
		return nil, false, e.Wrap(err)
	case !found:
		return nil, false, nil
	case found && i.Manifest().Height() == height:
		return i, true, nil
	}

	switch b, found, err := db.st.Get(context.Background(), redisBlockMapKey(height)); {
	case err != nil, !found:
		return nil, found, err
	default:
		if err := ReadDecodeFrame(db.encs, b, &m); err != nil {
			return nil, true, err
		}

		return m, true, nil
	}
}

func (db *RedisPermanent) BlockMapBytes(height base.Height) (
	enchint string, meta, body []byte, found bool, err error,
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

	switch b, found, err := db.st.Get(context.Background(), redisBlockMapKey(height)); {
	case err != nil, !found:
		return enchint, nil, nil, found, err
	default:
		enchint, meta, body, err := ReadOneHeaderFrame(b)

		return enchint, meta, body, true, err
	}
}

func (db *RedisPermanent) MergeTempDatabase(ctx context.Context, temp isaac.TempDatabase) error {
	db.l.Lock()
	defer db.l.Unlock()

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

func (db *RedisPermanent) mergeTempDatabaseFromLeveldb(ctx context.Context, temp *TempLeveldb) error {
	e := util.StringError("merge LeveldbTempDatabase")

	if temp.mp == nil {
		return e.Wrap(storage.ErrNotFound.Errorf("blockmap not found in LeveldbTempDatabase"))
	}

	if err := util.RunJobWorkerByJobs(
		ctx,
		func(ctx context.Context, _ uint64) error {
			if err := db.mergeOperationsTempDatabaseFromLeveldb(ctx, temp); err != nil {
				return errors.Wrap(err, "merge operations")
			}

			return nil
		},
		func(ctx context.Context, _ uint64) error {
			if err := db.mergeStatesTempDatabaseFromLeveldb(ctx, temp); err != nil {
				return errors.Wrap(err, "merge states")
			}

			return nil
		},
		func(ctx context.Context, _ uint64) error {
			if err := db.mergeSuffrageProofsTempDatabaseFromLeveldb(ctx, temp); err != nil {
				return errors.Wrap(err, "merge SuffrageProof")
			}

			return nil
		},
		func(ctx context.Context, _ uint64) error {
			if err := db.mergeSuffrageProofsByBlockHeightTempDatabaseFromLeveldb(ctx, temp); err != nil {
				return errors.Wrap(err, "merge SuffrageProof by block height")
			}

			return nil
		},
		func(ctx context.Context, _ uint64) error {
			if err := db.mergeBlockMapTempDatabaseFromLeveldb(ctx, temp); err != nil {
				return errors.Wrap(err, "merge blockmap")
			}

			return nil
		},
	); err != nil {
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

func (db *RedisPermanent) mergeOperationsTempDatabaseFromLeveldb(
	ctx context.Context, temp *TempLeveldb,
) error {
	e := util.StringError("merge operations from temp")

	tpst, err := temp.st()
	if err != nil {
		return e.Wrap(err)
	}

	if err := tpst.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixInStateOperation[:]),
		func(_, b []byte) (bool, error) {
			if err := db.st.Set(ctx, redisInStateOperationKey(string(b)), b); err != nil {
				return false, err
			}

			return true, nil
		}, true); err != nil {
		return e.Wrap(err)
	}

	if err := tpst.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixKnownOperation[:]),
		func(_, b []byte) (bool, error) {
			if err := db.st.Set(ctx, redisKnownOperationKey(valuehash.Bytes(b)), b); err != nil {
				return false, err
			}

			return true, nil
		}, true); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (db *RedisPermanent) mergeStatesTempDatabaseFromLeveldb(ctx context.Context, temp *TempLeveldb) error {
	tpst, err := temp.st()
	if err != nil {
		return err
	}

	return tpst.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixState[:]),
		func(key, b []byte) (bool, error) {
			if err := db.st.Set(ctx, redisStateKeyFromLeveldb(key), b); err != nil {
				return false, err
			}

			return true, nil
		}, true)
}

func (db *RedisPermanent) mergeSuffrageProofsTempDatabaseFromLeveldb(ctx context.Context, temp *TempLeveldb) error {
	tpst, err := temp.st()
	if err != nil {
		return err
	}

	return tpst.Iter(
		leveldbutil.BytesPrefix(leveldbKeySuffrageProof[:]),
		func(k, b []byte) (bool, error) {
			height, err := heightFromKey(k, leveldbKeySuffrageProof)
			if err != nil {
				return false, err
			}

			if err := db.st.Set(ctx, redisSuffrageProofKey(height), b); err != nil {
				return false, errors.Wrap(err, "set SuffrageProof")
			}

			return true, nil
		}, true)
}

func (db *RedisPermanent) mergeSuffrageProofsByBlockHeightTempDatabaseFromLeveldb(
	ctx context.Context, temp *TempLeveldb,
) error {
	tpst, err := temp.st()
	if err != nil {
		return err
	}

	return tpst.Iter(
		leveldbutil.BytesPrefix(leveldbKeySuffrageProofByBlockHeight[:]),
		func(k, b []byte) (bool, error) {
			height, err := heightFromKey(k, leveldbKeySuffrageProofByBlockHeight)
			if err != nil {
				return false, err
			}

			z := redis.ZAddArgs{
				NX:      true,
				Members: []redis.Z{{Score: 0, Member: redisSuffrageProofByBlockHeightKey(height)}},
			}
			if err := db.st.ZAddArgs(ctx, redisZKeySuffrageProofsByBlockHeight, z); err != nil {
				return false, errors.Wrap(err, "zadd suffrageproof by suffrage proof by block height")
			}

			if err := db.st.Set(ctx, redisSuffrageProofByBlockHeightKey(height), b); err != nil {
				return false, errors.Wrap(err, "set SuffrageProof by block height")
			}

			return true, nil
		}, true)
}

func (db *RedisPermanent) mergeBlockMapTempDatabaseFromLeveldb(
	ctx context.Context, temp *TempLeveldb,
) error {
	tpst, err := temp.st()
	if err != nil {
		return err
	}

	return tpst.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixBlockMap[:]),
		func(k, b []byte) (bool, error) {
			height, err := heightFromKey(k, leveldbKeyPrefixBlockMap)
			if err != nil {
				return false, err
			}

			key := redisBlockMapKey(height)
			z := redis.ZAddArgs{
				NX:      true,
				Members: []redis.Z{{Score: 0, Member: key}},
			}

			if err := db.st.ZAddArgs(ctx, redisZKeyBlockMaps, z); err != nil {
				return false, errors.Wrap(err, "zadd blockmap by block height")
			}

			if err := db.st.Set(ctx, key, b); err != nil {
				return false, errors.Wrap(err, "set blockmap")
			}

			return true, nil
		}, true)
}

func (db *RedisPermanent) loadLastBlockMap() error {
	e := util.StringError("load last blockmap")

	b, found, err := db.loadLast(redisZKeyBlockMaps, redisZBeginBlockMaps, redisZEndBlockMaps)

	switch {
	case err != nil:
		return e.Wrap(err)
	case !found:
		return nil
	default:
		var m base.BlockMap

		enchint, meta, body, err := ReadOneHeaderFrame(b)
		if err != nil {
			return e.Wrap(err)
		}

		if err := DecodeFrame(db.encs, enchint, body, &m); err != nil {
			return e.Wrap(err)
		}

		_ = db.lenc.SetValue(enchint)
		_ = db.mp.SetValue([3]interface{}{m, meta, body})
	}

	return nil
}

func (db *RedisPermanent) loadLastSuffrageProof() error {
	e := util.StringError("load last suffrage state")

	b, found, err := db.loadLast(
		redisZKeySuffrageProofsByBlockHeight,
		redisZBeginSuffrageProofsByBlockHeight,
		redisZEndSuffrageProofsByBlockHeight,
	)

	switch {
	case err != nil:
		return e.Wrap(err)
	case !found:
		return nil
	default:
		var proof base.SuffrageProof

		enchint, meta, body, err := ReadOneHeaderFrame(b)
		if err != nil {
			return err
		}

		if err := DecodeFrame(db.encs, enchint, body, &proof); err != nil {
			return err
		}

		_ = db.proof.SetValue([3]interface{}{proof, meta, body})

		return nil
	}
}

func (db *RedisPermanent) loadNetworkPolicy() error {
	e := util.StringError("load last network policy")

	switch st, found, err := db.State(isaac.NetworkPolicyStateKey); {
	case err != nil:
		return e.Wrap(err)
	case !found:
		return nil
	default:
		if !base.IsNetworkPolicyState(st) {
			return e.Errorf("not NetworkPolicy state: %T", st)
		}

		_ = db.policy.SetValue(st.Value().(base.NetworkPolicyStateValue).Policy()) //nolint:forcetypeassert //...

		db.setStateToCache(st)

		return nil
	}
}

func (db *RedisPermanent) loadLast(zkey, begin, end string) ([]byte, bool, error) {
	var key string

	if err := db.st.ZRangeArgs(
		context.Background(),
		redis.ZRangeArgs{
			Key:   zkey,
			Start: "[" + begin,
			Stop:  "[" + end,
			ByLex: true,
			Rev:   true,
			Count: 1,
		},
		func(i string) (bool, error) {
			key = i

			return false, nil
		},
	); err != nil {
		return nil, false, err
	}

	if len(key) < 1 {
		return nil, false, nil
	}

	switch b, found, err := db.st.Get(context.Background(), key); {
	case err != nil:
		return nil, false, err
	case !found:
		return nil, false, nil
	default:
		return b, true, nil
	}
}

func redisStateKey(key string) string {
	return redisStateKeyPrerfix + "-" + key
}

func redisInStateOperationKey(s string) string {
	return redisInStateOperationKeyPrerfix + "-" + s
}

func redisKnownOperationKey(h util.Hash) string {
	return redisKnownOperationKeyPrerfix + "-" + h.String()
}

func redisStateKeyFromLeveldb(b []byte) string {
	return redisStateKey(string(b[2:]))
}

func redisBlockMapKey(height base.Height) string {
	return redisBlockMapKeyPrefix + "-" + height.FixedString()
}

func redisSuffrageProofKey(suffrageheight base.Height) string {
	return redisSuffrageProofPrefix + "-" + suffrageheight.FixedString()
}

func redisSuffrageProofByBlockHeightKey(height base.Height) string {
	return redisSuffrageProofByBlockHeightPrefix + "-" + height.FixedString()
}
