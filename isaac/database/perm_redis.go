package isaacdatabase

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
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
	*baseDatabase
	*basePermanent
	st *redisstorage.Storage
	sync.Mutex
}

func NewRedisPermanent(
	st *redisstorage.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*RedisPermanent, error) {
	db := &RedisPermanent{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "redis-permanent-database")
		}),
		baseDatabase: newBaseDatabase(
			encs,
			enc,
		),
		basePermanent: newBasePermanent(),
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
		return errors.Wrap(err, "failed to close RedisPermanentDatabase")
	}

	return nil
}

func (db *RedisPermanent) Clean() error {
	if err := db.st.Clean(context.Background()); err != nil {
		return errors.Wrap(err, "failed to clean redis PermanentDatabase")
	}

	return db.basePermanent.Clean()
}

func (db *RedisPermanent) SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error) {
	e := util.StringErrorFunc("failed to get suffrageproof by height")

	proof, found, err := db.LastSuffrageProof()

	switch {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	}

	stv, err := base.LoadSuffrageNodesStateValue(proof.State())
	if err != nil {
		return nil, false, e(err, "")
	}

	switch {
	case suffrageHeight > stv.Height():
		return nil, false, nil
	case suffrageHeight == stv.Height():
		return proof, true, nil
	}

	switch b, found, err := db.st.Get(context.Background(), redisSuffrageProofKey(suffrageHeight)); {
	case err != nil:
		return nil, false, err
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

func (db *RedisPermanent) SuffrageProofByBlockHeight(height base.Height) (base.SuffrageProof, bool, error) {
	e := util.StringErrorFunc("failed to get suffrage by block height")

	switch proof, found, err := db.LastSuffrageProof(); {
	case err != nil:
		return nil, false, e(err, "")
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
		return nil, false, e(err, "")
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
		var proof base.SuffrageProof

		if err := db.readHinter(b, &proof); err != nil {
			return nil, false, e(err, "")
		}

		return proof, true, nil
	}
}

func (db *RedisPermanent) State(key string) (st base.State, found bool, _ error) {
	e := util.StringErrorFunc("failed to get state")

	switch b, found, err := db.st.Get(context.Background(), redisStateKey(key)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		if err := db.readHinter(b, &st); err != nil {
			return nil, false, e(err, "")
		}

		return st, true, nil
	}
}

func (db *RedisPermanent) ExistsInStateOperation(h util.Hash) (bool, error) {
	e := util.StringErrorFunc("failed to check instate operation")

	switch found, err := db.st.Exists(context.Background(), redisInStateOperationKey(h)); {
	case err != nil:
		return false, e(err, "")
	default:
		return found, nil
	}
}

func (db *RedisPermanent) ExistsKnownOperation(h util.Hash) (bool, error) {
	e := util.StringErrorFunc("failed to check known operation")

	switch found, err := db.st.Exists(context.Background(), redisKnownOperationKey(h)); {
	case err != nil:
		return false, e(err, "")
	default:
		return found, nil
	}
}

func (db *RedisPermanent) BlockMap(height base.Height) (m base.BlockMap, found bool, _ error) {
	e := util.StringErrorFunc("failed to load blockmap")

	switch i, found, err := db.LastBlockMap(); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	case found && i.Manifest().Height() == height:
		return i, true, nil
	}

	switch b, found, err := db.st.Get(context.Background(), redisBlockMapKey(height)); {
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

func (db *RedisPermanent) MergeTempDatabase(ctx context.Context, temp isaac.TempDatabase) error {
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

func (db *RedisPermanent) mergeTempDatabaseFromLeveldb(ctx context.Context, temp *TempLeveldb) error {
	e := util.StringErrorFunc("failed to merge LeveldbTempDatabase")

	if temp.mp == nil {
		return e(storage.NotFoundError.Errorf("blockmap not found in LeveldbTempDatabase"), "")
	}

	if err := util.RunErrgroupWorkerByJobs(
		ctx,
		func(ctx context.Context, jobid uint64) error {
			if err := db.mergeOperationsTempDatabaseFromLeveldb(ctx, temp); err != nil {
				return errors.Wrap(err, "failed to merge operations")
			}

			return nil
		},
		func(ctx context.Context, jobid uint64) error {
			if err := db.mergeStatesTempDatabaseFromLeveldb(ctx, temp); err != nil {
				return errors.Wrap(err, "failed to merge states")
			}

			return nil
		},
		func(ctx context.Context, jobid uint64) error {
			if err := db.mergeSuffrageProofsTempDatabaseFromLeveldb(ctx, temp); err != nil {
				return errors.Wrap(err, "failed to merge SuffrageProof")
			}

			return nil
		},
		func(ctx context.Context, jobid uint64) error {
			if err := db.mergeSuffrageProofsByBlockHeightTempDatabaseFromLeveldb(ctx, temp); err != nil {
				return errors.Wrap(err, "failed to merge SuffrageProof by block height")
			}

			return nil
		},
		func(ctx context.Context, jobid uint64) error {
			if err := db.mergeBlockMapTempDatabaseFromLeveldb(ctx, temp); err != nil {
				return errors.Wrap(err, "failed to merge blockmap")
			}

			return nil
		},
	); err != nil {
		return e(err, "")
	}

	_ = db.updateLast(temp.mp, temp.proof, temp.policy)

	db.Log().Info().Interface("blockmap", temp.mp).Msg("new block merged")

	return nil
}

func (db *RedisPermanent) mergeOperationsTempDatabaseFromLeveldb(
	ctx context.Context, temp *TempLeveldb,
) error {
	e := util.StringErrorFunc("failed to merge operations from temp")

	if err := temp.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixInStateOperation),
		func(_, b []byte) (bool, error) {
			if err := db.st.Set(ctx, redisInStateOperationKey(valuehash.Bytes(b)), b); err != nil {
				return false, err
			}

			return true, nil
		}, true); err != nil {
		return e(err, "")
	}

	if err := temp.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixKnownOperation),
		func(_, b []byte) (bool, error) {
			if err := db.st.Set(ctx, redisKnownOperationKey(valuehash.Bytes(b)), b); err != nil {
				return false, err
			}

			return true, nil
		}, true); err != nil {
		return e(err, "")
	}

	return nil
}

func (db *RedisPermanent) mergeStatesTempDatabaseFromLeveldb(ctx context.Context, temp *TempLeveldb) error {
	return temp.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixState),
		func(key, b []byte) (bool, error) {
			if err := db.st.Set(ctx, redisStateKeyFromLeveldb(key), b); err != nil {
				return false, err
			}

			return true, nil
		}, true)
}

func (db *RedisPermanent) mergeSuffrageProofsTempDatabaseFromLeveldb(ctx context.Context, temp *TempLeveldb) error {
	return temp.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeySuffrageProof),
		func(k, b []byte) (bool, error) {
			height, err := heightFromleveldbKey(k, leveldbKeySuffrageProof)
			if err != nil {
				return false, err
			}

			if err := db.st.Set(ctx, redisSuffrageProofKey(height), b); err != nil {
				return false, errors.Wrap(err, "failed to set SuffrageProof")
			}

			return true, nil
		}, true)
}

func (db *RedisPermanent) mergeSuffrageProofsByBlockHeightTempDatabaseFromLeveldb(
	ctx context.Context, temp *TempLeveldb,
) error {
	return temp.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeySuffrageProofByBlockHeight),
		func(k, b []byte) (bool, error) {
			height, err := heightFromleveldbKey(k, leveldbKeySuffrageProofByBlockHeight)
			if err != nil {
				return false, err
			}

			z := redis.ZAddArgs{
				NX:      true,
				Members: []redis.Z{{Score: 0, Member: redisSuffrageProofByBlockHeightKey(height)}},
			}
			if err := db.st.ZAddArgs(ctx, redisZKeySuffrageProofsByBlockHeight, z); err != nil {
				return false, errors.Wrap(err, "failed to zadd suffrageproof by suffrage proof by block height")
			}

			if err := db.st.Set(ctx, redisSuffrageProofByBlockHeightKey(height), b); err != nil {
				return false, errors.Wrap(err, "failed to set SuffrageProof by block height")
			}

			return true, nil
		}, true)
}

func (db *RedisPermanent) mergeBlockMapTempDatabaseFromLeveldb(
	ctx context.Context, temp *TempLeveldb,
) error {
	return temp.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixBlockMap),
		func(k, b []byte) (bool, error) {
			height, err := heightFromleveldbKey(k, leveldbKeyPrefixBlockMap)
			if err != nil {
				return false, err
			}

			key := redisBlockMapKey(height)
			z := redis.ZAddArgs{
				NX:      true,
				Members: []redis.Z{{Score: 0, Member: key}},
			}

			if err := db.st.ZAddArgs(ctx, redisZKeyBlockMaps, z); err != nil {
				return false, errors.Wrap(err, "failed to zadd blockmap by block height")
			}

			if err := db.st.Set(ctx, key, b); err != nil {
				return false, errors.Wrap(err, "failed to set blockmap")
			}

			return true, nil
		}, true)
}

func (db *RedisPermanent) loadLastBlockMap() error {
	e := util.StringErrorFunc("failed to load last blockmap")

	b, found, err := db.loadLast(redisZKeyBlockMaps, redisZBeginBlockMaps, redisZEndBlockMaps)

	switch {
	case err != nil:
		return e(err, "")
	case !found:
		return nil
	default:
		var m base.BlockMap

		if err := db.readHinter(b, &m); err != nil {
			return e(err, "")
		}

		_ = db.mp.SetValue(m)
	}

	return nil
}

func (db *RedisPermanent) loadLastSuffrageProof() error {
	e := util.StringErrorFunc("failed to load last suffrage state")

	b, found, err := db.loadLast(
		redisZKeySuffrageProofsByBlockHeight,
		redisZBeginSuffrageProofsByBlockHeight,
		redisZEndSuffrageProofsByBlockHeight,
	)

	switch {
	case err != nil:
		return e(err, "")
	case !found:
		return nil
	default:
		var proof base.SuffrageProof

		if err := db.readHinter(b, &proof); err != nil {
			return e(err, "")
		}

		_ = db.proof.SetValue(proof)

		return nil
	}
}

func (db *RedisPermanent) loadNetworkPolicy() error {
	e := util.StringErrorFunc("failed to load last network policy")

	switch st, found, err := db.State(isaac.NetworkPolicyStateKey); {
	case err != nil:
		return e(err, "")
	case !found:
		return nil
	default:
		if !base.IsNetworkPolicyState(st) {
			return e(nil, "not NetworkPolicy state: %T", st)
		}

		_ = db.policy.SetValue(st.Value().(base.NetworkPolicyStateValue).Policy()) //nolint:forcetypeassert //...

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
	return fmt.Sprintf("%s-%s", redisStateKeyPrerfix, key)
}

func redisInStateOperationKey(h util.Hash) string {
	return fmt.Sprintf("%s-%s", redisInStateOperationKeyPrerfix, h.String())
}

func redisKnownOperationKey(h util.Hash) string {
	return fmt.Sprintf("%s-%s", redisKnownOperationKeyPrerfix, h.String())
}

func redisStateKeyFromLeveldb(b []byte) string {
	return redisStateKey(string(b[2:]))
}

func redisBlockMapKey(height base.Height) string {
	return fmt.Sprintf("%s-%021d", redisBlockMapKeyPrefix, height)
}

func redisSuffrageProofKey(suffrageheight base.Height) string {
	return fmt.Sprintf("%s-%021d", redisSuffrageProofPrefix, suffrageheight)
}

func redisSuffrageProofByBlockHeightKey(height base.Height) string {
	return fmt.Sprintf("%s-%021d", redisSuffrageProofByBlockHeightPrefix, height)
}
