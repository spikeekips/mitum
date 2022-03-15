package isaac

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/spikeekips/mitum/base"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	redisstorage "github.com/spikeekips/mitum/storage/redis"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

func (db *RedisPermanentDatabase) setState(st base.State) error {
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

type testRedisPermanentDatabase struct {
	baseTestHandler
	baseTestDatabase
}

func (t *testRedisPermanentDatabase) SetupTest() {
	t.baseTestHandler.SetupTest()
	t.baseTestDatabase.SetupTest()
}

func (t *testRedisPermanentDatabase) newDB() *RedisPermanentDatabase {
	st, err := redisstorage.NewStorage(context.Background(), &redis.Options{}, util.UUID().String())

	db, err := NewRedisPermanentDatabase(st, t.encs, t.enc)
	t.NoError(err)

	return db
}

func (t *testRedisPermanentDatabase) TestNew() {
	db := t.newDB()
	defer db.Close()
	defer db.st.Clean(context.Background())

	_ = (interface{})(db).(PermanentDatabase)
}

func (t *testRedisPermanentDatabase) TestLastManifest() {
	height := base.Height(33)
	m := base.NewDummyManifest(height, valuehash.RandomSHA256())

	db := t.newDB()
	defer db.Close()
	defer db.st.Clean(context.Background())

	t.Run("empty manifest", func() {
		rm, found, err := db.LastManifest()
		t.NoError(err)
		t.False(found)
		t.Nil(rm)
	})

	_ = db.m.SetValue(m)

	t.Run("none-empty manifest", func() {
		rm, found, err := db.LastManifest()
		t.NoError(err)
		t.True(found)

		base.EqualManifest(t.Assert(), m, rm)
	})
}

func (t *testRedisPermanentDatabase) TestLastSuffrage() {
	height := base.Height(33)
	_, nodes := t.locals(3)

	sufstt, _ := t.suffrageState(height, base.Height(66), nodes)

	db := t.newDB()
	defer db.Close()
	defer db.st.Clean(context.Background())

	t.Run("empty suffrage", func() {
		rsufstt, found, err := db.LastSuffrage()
		t.NoError(err)
		t.False(found)
		t.Nil(rsufstt)
	})

	_ = db.sufstt.SetValue(sufstt)

	t.Run("none-empty suffrage", func() {
		rsufstt, found, err := db.LastSuffrage()
		t.NoError(err)
		t.True(found)

		t.True(base.IsEqualState(sufstt, rsufstt))
	})
}

func (t *testRedisPermanentDatabase) TestSuffrage() {
	baseheight := base.Height(33)
	_, nodes := t.locals(3)

	db := t.newDB()
	defer db.Close()
	defer db.st.Clean(context.Background())

	height := baseheight
	basesuffrageheight := base.Height(66)
	suffrageheight := basesuffrageheight

	stm := map[base.Height]base.State{}
	sthm := map[base.Height]base.State{}
	for range make([]int, 3) {
		st, _ := t.suffrageState(height, suffrageheight, nodes)
		t.NoError(db.setState(st))

		stm[height] = st
		sthm[suffrageheight] = st

		height++
		suffrageheight++
	}

	m := base.NewDummyManifest(height+10, valuehash.RandomSHA256())
	_ = db.m.SetValue(m)
	st, _ := t.suffrageState(height+10, suffrageheight, nodes)
	_ = db.sufstt.SetValue(st)

	t.Run("unknown suffrage", func() {
		rsufstt, found, err := db.Suffrage(baseheight - 1)
		t.NoError(err)
		t.False(found)
		t.Nil(rsufstt)
	})

	t.Run("> last height", func() {
		rsufstt, found, err := db.Suffrage(baseheight + 100)
		t.NoError(err)
		t.False(found)
		t.Nil(rsufstt)
	})

	t.Run("known suffrage", func() {
		height := baseheight
		for {
			if height >= baseheight+3 {
				break
			}

			rsufstt, found, err := db.Suffrage(height)
			t.NoError(err)
			t.True(found)
			t.NotNil(rsufstt)

			t.True(base.IsEqualState(stm[height], rsufstt))
			height++
		}
	})

	t.Run("known suffrage; last height", func() {
		rsufstt, found, err := db.Suffrage(m.Height())
		t.NoError(err)
		t.True(found)
		t.NotNil(rsufstt)

		t.True(base.IsEqualState(st, rsufstt))
	})

	t.Run("unknown suffrage by height", func() {
		rsufstt, found, err := db.SuffrageByHeight(basesuffrageheight - 1)
		t.NoError(err)
		t.False(found)
		t.Nil(rsufstt)
	})

	t.Run("suffrage by height; > last height", func() {
		rsufstt, found, err := db.SuffrageByHeight(basesuffrageheight + 100)
		t.NoError(err)
		t.False(found)
		t.Nil(rsufstt)
	})

	t.Run("known suffrage by height", func() {
		height := basesuffrageheight
		for {
			if height >= basesuffrageheight+3 {
				break
			}

			rsufstt, found, err := db.SuffrageByHeight(height)
			t.NoError(err)
			t.True(found)
			t.NotNil(rsufstt)

			t.True(base.IsEqualState(sthm[height], rsufstt))
			height++
		}
	})
}

func (t *testRedisPermanentDatabase) TestLoadEmptyDB() {
	st := leveldbstorage.NewMemWriteStorage()
	db, err := newLeveldbPermanentDatabase(st, t.encs, t.enc)
	t.NoError(err)

	defer db.Close()
}

func (t *testRedisPermanentDatabase) TestLoad() {
	height := base.Height(33)
	_, nodes := t.locals(3)

	sufstt, _ := t.suffrageState(height, base.Height(66), nodes)

	stts := t.states(height, 3)
	stts = append(stts, sufstt)

	m := base.NewDummyManifest(height, valuehash.RandomSHA256())

	ops := make([]util.Hash, 3)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	wst := t.newMemLeveldbBlockWriteDatabase(height)
	t.NoError(wst.SetManifest(m))
	t.NoError(wst.SetStates(stts))
	t.NoError(wst.SetOperations(ops))
	t.NoError(wst.Write())

	temp, err := wst.TempDatabase()
	t.NoError(err)

	perm := t.newDB()
	t.NoError(perm.MergeTempDatabase(context.TODO(), temp))

	t.Run("check manifest in perm", func() {
		nm, found, err := perm.LastManifest()
		t.NoError(err)
		t.True(found)
		base.EqualManifest(t.Assert(), m, nm)
	})

	t.Run("check suffrage state in perm", func() {
		nst, found, err := perm.LastSuffrage()
		t.NoError(err)
		t.True(found)
		t.True(base.IsEqualState(sufstt, nst))
	})

	newperm, err := NewRedisPermanentDatabase(perm.st, t.encs, t.enc)
	t.NoError(err)

	t.Run("check manifest in new perm", func() {
		nm, found, err := newperm.LastManifest()
		t.NoError(err)
		t.True(found)
		base.EqualManifest(t.Assert(), m, nm)
	})

	t.Run("check suffrage state in new perm", func() {
		nst, found, err := newperm.LastSuffrage()
		t.NoError(err)
		t.True(found)
		t.True(base.IsEqualState(sufstt, nst))
	})
}

func (t *testRedisPermanentDatabase) TestMergeTempDatabase() {
	height := base.Height(33)
	_, nodes := t.locals(3)

	sufstt, _ := t.suffrageState(height, base.Height(66), nodes)

	stts := t.states(height, 3)
	stts = append(stts, sufstt)

	m := base.NewDummyManifest(height, valuehash.RandomSHA256())

	ops := make([]util.Hash, 3)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	wst := t.newMemLeveldbBlockWriteDatabase(height)
	t.NoError(wst.SetManifest(m))
	t.NoError(wst.SetStates(stts))
	t.NoError(wst.SetOperations(ops))
	t.NoError(wst.Write())

	temp, err := wst.TempDatabase()
	t.NoError(err)

	t.Run("check opertions", func() {
		perm := t.newDB()
		defer perm.Close()
		defer perm.st.Clean(context.Background())

		for i := range ops {
			found, err := perm.ExistsOperation(ops[i])
			t.NoError(err)
			t.False(found)
		}

		t.NoError(perm.MergeTempDatabase(context.TODO(), temp))

		for i := range ops {
			found, err := perm.ExistsOperation(ops[i])
			t.NoError(err)
			t.True(found)
		}
	})

	t.Run("check states", func() {
		perm := t.newDB()
		defer perm.Close()
		defer perm.st.Clean(context.Background())

		for i := range stts {
			st := stts[i]
			rst, found, err := perm.State(st.Key())
			t.NoError(err)
			t.False(found)
			t.Nil(rst)
		}

		t.NoError(perm.MergeTempDatabase(context.TODO(), temp))

		for i := range ops {
			st := stts[i]

			rst, found, err := perm.State(st.Key())
			t.NoError(err)
			t.True(found)
			t.True(base.IsEqualState(st, rst))
		}
	})

	t.Run("check suffrage state", func() {
		perm := t.newDB()
		defer perm.Close()
		defer perm.st.Clean(context.Background())

		rst, found, err := perm.Suffrage(height)
		t.NoError(err)
		t.False(found)
		t.Nil(rst)

		t.NoError(perm.MergeTempDatabase(context.TODO(), temp))

		rst, found, err = perm.Suffrage(height)
		t.NoError(err)
		t.True(found)
		t.NotNil(rst)

		t.True(base.IsEqualState(sufstt, rst))
	})

	t.Run("check manifest", func() {
		perm := t.newDB()
		defer perm.Close()
		defer perm.st.Clean(context.Background())

		rm, found, err := perm.Manifest(height)
		t.NoError(err)
		t.False(found)
		t.Nil(rm)

		t.NoError(perm.MergeTempDatabase(context.TODO(), temp))

		rm, found, err = perm.Manifest(height)
		t.NoError(err)
		t.True(found)
		t.NotNil(rm)

		base.EqualManifest(t.Assert(), m, rm)
	})
}

func TestRedisPermanentDatabase(t *testing.T) {
	suite.Run(t, new(testRedisPermanentDatabase))
}
