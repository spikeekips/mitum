package isaac

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/spikeekips/mitum/base"
	redisstorage "github.com/spikeekips/mitum/storage/redis"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testRedisPermanentDatabase struct {
	testCommonPermanentDatabase
}

func TestRedisPermanentDatabase(tt *testing.T) {
	t := new(testLeveldbPermanentDatabase)
	t.newDB = func() PermanentDatabase {
		st, err := redisstorage.NewStorage(context.Background(), &redis.Options{}, util.UUID().String())

		db, err := NewRedisPermanentDatabase(st, t.encs, t.enc)
		t.NoError(err)

		return db
	}

	t.newFromDB = func(db PermanentDatabase) (PermanentDatabase, error) {
		return NewRedisPermanentDatabase(db.(*RedisPermanentDatabase).st, t.encs, t.enc)
	}

	t.setState = func(perm PermanentDatabase, st base.State) error {
		db := perm.(*RedisPermanentDatabase)

		e := util.StringErrorFunc("failed to set state")

		b, err := db.marshal(st)
		if err != nil {
			return e(err, "")
		}

		if err := db.st.Set(context.TODO(), redisStateKey(st.Key()), b); err != nil {
			return e(err, "failed to put state")
		}

		if st.Key() == SuffrageStateKey {
			z := redis.ZAddArgs{
				NX:      true,
				Members: []redis.Z{{Score: 0, Member: redisSuffrageKey(st.Height())}},
			}
			if err := db.st.ZAddArgs(context.TODO(), redisZKeySuffragesByHeight, z); err != nil {
				return e(err, "failed to put suffrage by block height")
			}

			if err := db.st.Set(context.TODO(), redisSuffrageKey(st.Height()), b); err != nil {
				return e(err, "failed to put suffrage")
			}

			sv := st.Value().(base.SuffrageStateValue)
			if err := db.st.Set(context.TODO(), redisSuffrageByHeightKey(sv.Height()), b); err != nil {
				return e(err, "failed to put suffrage by height")
			}
		}

		return nil
	}

	suite.Run(tt, t)
}
