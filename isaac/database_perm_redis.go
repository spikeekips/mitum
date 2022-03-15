package isaac

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	redisstorage "github.com/spikeekips/mitum/storage/redis"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

var (
	redisManifestKeyPrefix          = "mf"
	redisSuffrageKeyPrerfix         = "sf"
	redisSuffrageByHeightKeyPrerfix = "sh"
	redisStateKeyPrerfix            = "st"
	redisOperationKeyPrerfix        = "op"
)

var (
	redisZKeySuffragesByHeight = "suffrages_by_height"
	redisZBeginSuffrages       = redisSuffrageKey(base.GenesisHeight)
	redisZEndSuffrages         = fmt.Sprintf("%s-%s", redisSuffrageKeyPrerfix, strings.Repeat("9", 20))
	redisZKeyManifests         = "manifests"
	redisZBeginManifests       = redisManifestKey(base.GenesisHeight)
	redisZEndManifests         = fmt.Sprintf("%s-%s", redisManifestKeyPrefix, strings.Repeat("9", 20))
)

type RedisPermanentDatabase struct {
	sync.Mutex
	*baseDatabase
	*basePermanentDatabase
	st *redisstorage.Storage
}

func NewRedisPermanentDatabase(
	st *redisstorage.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*RedisPermanentDatabase, error) {
	db := &RedisPermanentDatabase{
		baseDatabase: newBaseDatabase(
			encs,
			enc,
		),
		basePermanentDatabase: newBasePermanentDatabase(),
		st:                    st,
	}

	if err := db.loadLastManifest(); err != nil {
		return nil, err
	}

	if err := db.loadLastSuffrage(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *RedisPermanentDatabase) Close() error {
	if err := db.st.Close(); err != nil {
		return errors.Wrap(err, "failed to close RedisPermanentDatabase")
	}

	return nil
}

func (db *RedisPermanentDatabase) Manifest(height base.Height) (base.Manifest, bool, error) {
	e := util.StringErrorFunc("failed to load manifest")

	switch m, found, err := db.LastManifest(); {
	case err != nil:
		return nil, false, e(err, "")
	case found:
		return m, true, nil
	}

	switch b, found, err := db.st.Get(context.Background(), redisManifestKey(height)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		m, err := db.decodeManifest(b)
		if err != nil {
			return nil, false, e(err, "")
		}

		return m, true, nil
	}
}

func (db *RedisPermanentDatabase) Suffrage(height base.Height) (base.State, bool, error) {
	e := util.StringErrorFunc("failed to get suffrage by block height")

	switch m, found, err := db.LastManifest(); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	case height > m.Height():
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

	keys, err := db.st.ZRangeArgs(context.Background(), redis.ZRangeArgs{
		Key:   redisZKeySuffragesByHeight,
		Start: "[" + redisZBeginSuffrages,
		Stop:  "[" + redisSuffrageKey(height),
		ByLex: true,
		Rev:   true,
		Count: 1,
	})
	switch {
	case err != nil:
		return nil, false, e(err, "")
	case len(keys) < 1:
		return nil, false, nil
	}

	switch b, found, err := db.st.Get(context.Background(), keys[0]); {
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

func (db *RedisPermanentDatabase) SuffrageByHeight(suffrageHeight base.Height) (base.State, bool, error) {
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

func (db *RedisPermanentDatabase) State(key string) (base.State, bool, error) {
	e := util.StringErrorFunc("failed to get state")

	switch b, found, err := db.st.Get(context.Background(), redisStateKey(key)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		i, err := db.decodeState(b)
		if err != nil {
			return nil, false, e(err, "")
		}

		return i, true, nil
	}
}

func (db *RedisPermanentDatabase) ExistsOperation(h util.Hash) (bool, error) {
	e := util.StringErrorFunc("failed to check operation")

	switch found, err := db.st.Exists(context.Background(), redisOperationKey(h)); {
	case err != nil:
		return false, e(err, "")
	default:
		return found, nil
	}
}

func (db *RedisPermanentDatabase) MergeTempDatabase(ctx context.Context, temp TempDatabase) error {
	db.Lock()
	defer db.Unlock()

	var m base.Manifest
	if _ = db.m.Value(&m); m != nil && m.Height() >= temp.Height() {
		return nil
	}

	e := util.StringErrorFunc("failed to merge TempDatabase")

	switch t := temp.(type) {
	case *TempLeveldbDatabase:
		m, sufstt, err := db.mergeTempDatabaseFromLeveldb(ctx, t)
		if err != nil {
			return e(err, "")
		}

		_ = db.m.SetValue(m)
		_ = db.sufstt.SetValue(sufstt)

		return nil
	default:
		return e(nil, "unknown temp database, %T", temp)
	}
}

func (db *RedisPermanentDatabase) mergeTempDatabaseFromLeveldb(ctx context.Context, temp *TempLeveldbDatabase) (
	base.Manifest, base.State, error,
) {
	e := util.StringErrorFunc("failed to merge LeveldbTempDatabase")

	var m base.Manifest
	switch i, err := temp.Manifest(); {
	case err != nil:
		return nil, nil, e(err, "")
	default:
		m = i
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
	if err := db.mergeOperationsTempDatabaseFromLeveldb(ctx, temp); err != nil {
		return nil, nil, e(err, "failed to merge operations")
	}

	// NOTE merge states
	bsufst, err := db.mergeStatesTempDatabaseFromLeveldb(ctx, temp)
	if err != nil {
		return nil, nil, e(err, "failed to merge states")
	}

	// NOTE merge suffrage state
	if sufsv != nil && len(bsufst) > 0 {
		if err := db.mergeSuffrageStateTempDatabaseFromLeveldb(ctx, temp, sufsv, bsufst); err != nil {
			return nil, nil, e(err, "failed to merge suffrage state")
		}
	}

	// NOTE merge manifest
	if err := db.mergeManifestTempDatabaseFromLeveldb(ctx, temp); err != nil {
		return nil, nil, e(err, "failed to merge manifest")
	}

	return m, sufstt, nil
}

func (db *RedisPermanentDatabase) mergeOperationsTempDatabaseFromLeveldb(
	ctx context.Context, temp *TempLeveldbDatabase,
) error {
	return temp.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixOperation),
		func(_, b []byte) (bool, error) {
			if err := db.st.Set(ctx, redisOperationKey(valuehash.Bytes(b)), b); err != nil {
				return false, err
			}

			return true, nil
		}, true)
}

func (db *RedisPermanentDatabase) mergeStatesTempDatabaseFromLeveldb(
	ctx context.Context, temp *TempLeveldbDatabase,
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

func (db *RedisPermanentDatabase) mergeSuffrageStateTempDatabaseFromLeveldb(
	ctx context.Context,
	temp *TempLeveldbDatabase,
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

func (db *RedisPermanentDatabase) mergeManifestTempDatabaseFromLeveldb(
	ctx context.Context, temp *TempLeveldbDatabase,
) error {
	switch b, found, err := temp.st.Get(leveldbKeyPrefixManifest); {
	case err != nil || !found:
		return errors.Wrap(err, "failed to get manifest from TempDatabase")
	default:
		key := redisManifestKey(temp.Height())
		z := redis.ZAddArgs{
			NX:      true,
			Members: []redis.Z{{Score: 0, Member: key}},
		}
		if err := db.st.ZAddArgs(ctx, redisZKeyManifests, z); err != nil {
			return errors.Wrap(err, "failed to zadd suffrage by block height")
		}

		if err := db.st.Set(ctx, key, b); err != nil {
			return errors.Wrap(err, "failed to set manifest")
		}

		return nil
	}
}

func (db *RedisPermanentDatabase) loadLastManifest() error {
	e := util.StringErrorFunc("failed to load last manifest")

	keys, err := db.st.ZRangeArgs(context.Background(), redis.ZRangeArgs{
		Key:   redisZKeyManifests,
		Start: "[" + redisZBeginManifests,
		Stop:  "[" + redisZEndManifests,
		ByLex: true,
		Rev:   true,
		Count: 1,
	})
	switch {
	case err != nil:
		return e(err, "")
	case len(keys) < 1:
		return nil
	}

	switch b, found, err := db.st.Get(context.Background(), keys[0]); {
	case err != nil:
		return err
	case !found:
		return nil
	default:
		m, err := db.decodeManifest(b)
		if err != nil {
			return e(err, "")
		}

		_ = db.m.SetValue(m)
	}

	return nil
}

func (db *RedisPermanentDatabase) loadLastSuffrage() error {
	e := util.StringErrorFunc("failed to load last suffrage state")

	keys, err := db.st.ZRangeArgs(context.Background(), redis.ZRangeArgs{
		Key:   redisZKeySuffragesByHeight,
		Start: "[" + redisZBeginSuffrages,
		Stop:  "[" + redisZEndSuffrages,
		ByLex: true,
		Rev:   true,
		Count: 1,
	})
	switch {
	case err != nil:
		return e(err, "")
	case len(keys) < 1:
		return nil
	}

	switch b, found, err := db.st.Get(context.Background(), keys[0]); {
	case err != nil:
		return err
	case !found:
		return nil
	default:
		sufstt, err := db.decodeSuffrage(b)
		if err != nil {
			return e(err, "")
		}
		_ = db.sufstt.SetValue(sufstt)

		return nil
	}
}

func redisManifestKey(height base.Height) string {
	return fmt.Sprintf("%s-%020d", redisManifestKeyPrefix, height)
}

func redisSuffrageKey(height base.Height) string {
	return fmt.Sprintf("%s-%020d", redisSuffrageKeyPrerfix, height)
}

func redisSuffrageByHeightKey(suffrageheight base.Height) string {
	return fmt.Sprintf("%s-%020d", redisSuffrageByHeightKeyPrerfix, suffrageheight)
}

func redisStateKey(key string) string {
	return fmt.Sprintf("%s-%s", redisStateKeyPrerfix, key)
}

func redisOperationKey(h util.Hash) string {
	return fmt.Sprintf("%s-%s", redisOperationKeyPrerfix, h.String())
}

func redisStateKeyFromLeveldb(b []byte) string {
	return redisStateKey(string(b[2:]))
}
