package isaacdatabase

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	redisstorage "github.com/spikeekips/mitum/storage/redis"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

var (
	redisSuffrageKeyPrerfix               = "suf"
	redisSuffrageByHeightKeyPrerfix       = "suh"
	redisStateKeyPrerfix                  = "stt"
	redisInStateOperationKeyPrerfix       = "iso"
	redisKnownOperationKeyPrerfix         = "kno"
	redisBlockMapKeyPrefix                = "bmp"
	redisSuffrageProofPrefix              = "sup"
	redisSuffrageProofByBlockHeightPrefix = "sph"
)

var (
	redisZKeySuffragesByHeight             = "suffrages_by_height"
	redisZBeginSuffrages                   = redisSuffrageKey(base.GenesisHeight)
	redisZEndSuffrages                     = fmt.Sprintf("%s-%s", redisSuffrageKeyPrerfix, strings.Repeat("9", 20))
	redisZKeyBlockMaps                     = "blockmaps"
	redisZBeginBlockMaps                   = redisBlockMapKey(base.GenesisHeight)
	redisZEndBlockMaps                     = fmt.Sprintf("%s-%s", redisBlockMapKeyPrefix, strings.Repeat("9", 20))
	redisZKeySuffrageProofsByBlockHeight   = "suffrageproofs_by_blockheight"
	redisZBeginSuffrageProofsByBlockHeight = redisSuffrageProofByBlockHeightKey(base.GenesisHeight)
	redisZEndSuffrageProofsByBlockHeight   = fmt.Sprintf("%s-%s",
		redisSuffrageProofByBlockHeightPrefix, strings.Repeat("9", 20))
)

type RedisPermanent struct {
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

	if err := db.loadLastSuffrage(); err != nil {
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

func (db *RedisPermanent) Suffrage(height base.Height) (base.State, bool, error) {
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

	var key string

	if err := db.st.ZRangeArgs(
		context.Background(),
		redis.ZRangeArgs{
			Key:   redisZKeySuffragesByHeight,
			Start: "[" + redisZBeginSuffrages,
			Stop:  "[" + redisSuffrageKey(height),
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
		st, err := db.decodeSuffrage(b)
		if err != nil {
			return nil, false, e(err, "")
		}

		return st, true, nil
	}
}

func (db *RedisPermanent) SuffrageByHeight(suffrageHeight base.Height) (base.State, bool, error) {
	e := util.StringErrorFunc("failed to get suffrage by height")

	switch st, found, err := db.LastSuffrage(); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	case suffrageHeight > st.Value().(base.SuffrageStateValue).Height(): //nolint:forcetypeassert //...
		return nil, false, nil
	case suffrageHeight == st.Value().(base.SuffrageStateValue).Height(): //nolint:forcetypeassert //...
		return st, true, nil
	}

	switch b, found, err := db.st.Get(context.Background(), redisSuffrageByHeightKey(suffrageHeight)); {
	case err != nil:
		return nil, false, err
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

func (db *RedisPermanent) SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error) {
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
	case height > proof.State().Height():
		return nil, false, nil
	case height == proof.State().Height():
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

func (db *RedisPermanent) Map(height base.Height) (m base.BlockMap, found bool, _ error) {
	e := util.StringErrorFunc("failed to load blockmap")

	switch i, found, err := db.LastMap(); {
	case err != nil:
		return nil, false, e(err, "")
	case found:
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

	sufst := temp.sufst
	var sufsv base.SuffrageStateValue

	if sufst != nil {
		sufsv = sufst.Value().(base.SuffrageStateValue) //nolint:forcetypeassert //...
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
			bsufst, err := db.mergeStatesTempDatabaseFromLeveldb(ctx, temp)
			if err != nil {
				return errors.Wrap(err, "failed to merge states")
			}

			// NOTE merge suffrage state
			if sufsv != nil && len(bsufst) > 0 {
				if err := db.mergeSuffrageStateTempDatabaseFromLeveldb(ctx, temp, sufsv, bsufst); err != nil {
					return errors.Wrap(err, "failed to merge suffrage state")
				}

				if err := db.mergeSuffrageProofTempDatabaseFromLeveldb(ctx, temp, sufst, sufsv); err != nil {
					return errors.Wrap(err, "failed to merge SuffrageProof")
				}
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

	_ = db.updateLast(temp.mp, temp.sufst, temp.proof, temp.policy)

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

func (db *RedisPermanent) mergeStatesTempDatabaseFromLeveldb(
	ctx context.Context, temp *TempLeveldb,
) ([]byte, error) {
	var bsufst []byte

	if err := temp.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixState),
		func(key, b []byte) (bool, error) {
			if err := db.st.Set(ctx, redisStateKeyFromLeveldb(key), b); err != nil {
				return false, err
			}

			if bytes.Equal(key, leveldbSuffrageStateKey) {
				bsufst = b
			}

			return true, nil
		}, true); err != nil {
		return nil, err
	}

	return bsufst, nil
}

func (db *RedisPermanent) mergeSuffrageStateTempDatabaseFromLeveldb(
	ctx context.Context,
	temp *TempLeveldb,
	sufsv base.SuffrageStateValue,
	bsufst []byte,
) error {
	z := redis.ZAddArgs{
		NX:      true,
		Members: []redis.Z{{Score: 0, Member: redisSuffrageKey(temp.Height())}},
	}
	if err := db.st.ZAddArgs(ctx, redisZKeySuffragesByHeight, z); err != nil {
		return errors.Wrap(err, "failed to zadd suffrage by block height")
	}

	if err := db.st.Set(ctx, redisSuffrageKey(temp.Height()), bsufst); err != nil {
		return errors.Wrap(err, "failed to set suffrage")
	}

	if err := db.st.Set(ctx, redisSuffrageByHeightKey(sufsv.Height()), bsufst); err != nil {
		return errors.Wrap(err, "failed to set suffrage by height")
	}

	return nil
}

func (db *RedisPermanent) mergeSuffrageProofTempDatabaseFromLeveldb(
	ctx context.Context,
	temp *TempLeveldb,
	sufst base.State,
	sufsv base.SuffrageStateValue,
) error {
	z := redis.ZAddArgs{
		NX:      true,
		Members: []redis.Z{{Score: 0, Member: redisSuffrageProofByBlockHeightKey(sufst.Height())}},
	}
	if err := db.st.ZAddArgs(ctx, redisZKeySuffrageProofsByBlockHeight, z); err != nil {
		return errors.Wrap(err, "failed to zadd suffrageproof by suffrage proof by block height")
	}

	// NOTE merge SuffrageProof
	switch b, found, err := temp.st.Get(leveldbKeySuffrageProof); {
	case err != nil:
		return errors.Wrap(err, "failed to get SuffrageProof")
	case !found:
		return storage.NotFoundError.Errorf("failed to get SuffrageProof")
	default:
		if err := db.st.Set(ctx, redisSuffrageProofKey(sufsv.Height()), b); err != nil {
			return errors.Wrap(err, "failed to set SuffrageProof")
		}

		if err := db.st.Set(ctx, redisSuffrageProofByBlockHeightKey(sufst.Height()), b); err != nil {
			return errors.Wrap(err, "failed to set SuffrageProof by block height")
		}
	}

	return nil
}

func (db *RedisPermanent) mergeBlockMapTempDatabaseFromLeveldb(
	ctx context.Context, temp *TempLeveldb,
) error {
	switch b, found, err := temp.st.Get(leveldbKeyPrefixBlockMap); {
	case err != nil || !found:
		return errors.Wrap(err, "failed to get blockmap from TempDatabase")
	default:
		key := redisBlockMapKey(temp.Height())
		z := redis.ZAddArgs{
			NX:      true,
			Members: []redis.Z{{Score: 0, Member: key}},
		}

		if err := db.st.ZAddArgs(ctx, redisZKeyBlockMaps, z); err != nil {
			return errors.Wrap(err, "failed to zadd blockmap by block height")
		}

		if err := db.st.Set(ctx, key, b); err != nil {
			return errors.Wrap(err, "failed to set blockmap")
		}

		return nil
	}
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

func (db *RedisPermanent) loadLastSuffrage() error {
	e := util.StringErrorFunc("failed to load last suffrage state")

	b, found, err := db.loadLast(redisZKeySuffragesByHeight, redisZBeginSuffrages, redisZEndSuffrages)

	switch {
	case err != nil:
		return e(err, "")
	case !found:
		return nil
	default:
		sufst, err := db.decodeSuffrage(b)
		if err != nil {
			return e(err, "")
		}

		_ = db.sufst.SetValue(sufst)

		return nil
	}
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
		return nil, false, errors.Wrap(err, "")
	}

	if len(key) < 1 {
		return nil, false, nil
	}

	switch b, found, err := db.st.Get(context.Background(), key); {
	case err != nil:
		return nil, false, errors.Wrap(err, "")
	case !found:
		return nil, false, nil
	default:
		return b, true, nil
	}
}

func redisSuffrageKey(height base.Height) string {
	return fmt.Sprintf("%s-%021d", redisSuffrageKeyPrerfix, height)
}

func redisSuffrageByHeightKey(suffrageheight base.Height) string {
	return fmt.Sprintf("%s-%021d", redisSuffrageByHeightKeyPrerfix, suffrageheight)
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
