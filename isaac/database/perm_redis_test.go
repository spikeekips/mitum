//go:build test && redis
// +build test,redis

package isaacdatabase

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	redisstorage "github.com/spikeekips/mitum/storage/redis"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testRedisPermanent struct {
	testCommonPermanent
}

func TestRedisPermanent(tt *testing.T) {
	t := new(testRedisPermanent)
	t.newDB = func() isaac.PermanentDatabase {
		st, err := redisstorage.NewStorage(context.Background(), &redis.Options{}, util.UUID().String())
		t.NoError(err)

		db, err := NewRedisPermanent(st, t.Encs, t.Enc)
		t.NoError(err)

		return db
	}

	t.newFromDB = func(db isaac.PermanentDatabase) (isaac.PermanentDatabase, error) {
		return NewRedisPermanent(db.(*RedisPermanent).st, t.Encs, t.Enc)
	}

	t.setState = func(perm isaac.PermanentDatabase, st base.State) error {
		db := perm.(*RedisPermanent)

		e := util.StringErrorFunc("failed to set state")

		b, _, err := db.marshal(st, nil)
		if err != nil {
			return e(err, "")
		}

		if err := db.st.Set(context.TODO(), redisStateKey(st.Key()), b); err != nil {
			return e(err, "failed to put state")
		}

		return nil
	}

	suite.Run(tt, t)
}
