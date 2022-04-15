package isaac

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

type testCommonPermanentDatabase struct {
	BaseTestBallots
	BaseTestDatabase
	newDB     func() PermanentDatabase
	newFromDB func(PermanentDatabase) (PermanentDatabase, error)
	setState  func(PermanentDatabase, base.State) error
}

func (t *testCommonPermanentDatabase) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testCommonPermanentDatabase) setMap(db PermanentDatabase, mp base.BlockDataMap) {
	switch t := db.(type) {
	case *LeveldbPermanentDatabase:
		_ = t.mp.SetValue(mp)
	case *RedisPermanentDatabase:
		_ = t.mp.SetValue(mp)
	default:
		panic("unknown PermanentDatabase")
	}
}

func (t *testCommonPermanentDatabase) setSuffrageState(db PermanentDatabase, st base.State) {
	switch t := db.(type) {
	case *LeveldbPermanentDatabase:
		_ = t.sufstt.SetValue(st)
	case *RedisPermanentDatabase:
		_ = t.sufstt.SetValue(st)
	default:
		panic("unknown PermanentDatabase")
	}
}

func (t *testCommonPermanentDatabase) TestNew() {
	db := t.newDB()
	defer db.Close()

	_ = (interface{})(db).(PermanentDatabase)
}

func (t *testCommonPermanentDatabase) TestLastMap() {
	height := base.Height(33)
	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
	mp := base.NewDummyBlockDataMap(manifest)

	db := t.newDB()
	defer db.Close()

	t.Run("empty blockdatamap", func() {
		rm, found, err := db.LastMap()
		t.NoError(err)
		t.False(found)
		t.Nil(rm)
	})

	t.setMap(db, mp)

	t.Run("none-empty blockdatamap", func() {
		rm, found, err := db.LastMap()
		t.NoError(err)
		t.True(found)

		base.EqualBlockDataMap(t.Assert(), mp, rm)
	})
}

func (t *testCommonPermanentDatabase) TestLastSuffrage() {
	height := base.Height(33)
	_, nodes := t.Locals(3)

	sufstt, _ := t.SuffrageState(height, base.Height(66), nodes)

	db := t.newDB()
	defer db.Close()

	t.Run("empty suffrage", func() {
		rsufstt, found, err := db.LastSuffrage()
		t.NoError(err)
		t.False(found)
		t.Nil(rsufstt)
	})

	t.setSuffrageState(db, sufstt)

	t.Run("none-empty suffrage", func() {
		rsufstt, found, err := db.LastSuffrage()
		t.NoError(err)
		t.True(found)

		t.True(base.IsEqualState(sufstt, rsufstt))
	})
}

func (t *testCommonPermanentDatabase) TestSuffrage() {
	baseheight := base.Height(33)
	_, nodes := t.Locals(3)

	db := t.newDB()
	defer db.Close()

	height := baseheight
	basesuffrageheight := base.Height(66)
	suffrageheight := basesuffrageheight

	stm := map[base.Height]base.State{}
	sthm := map[base.Height]base.State{}
	for range make([]int, 3) {
		st, _ := t.SuffrageState(height, suffrageheight, nodes)
		t.NoError(t.setState(db, st))

		stm[height] = st
		sthm[suffrageheight] = st

		height++
		suffrageheight++
	}

	manifest := base.NewDummyManifest(height+10, valuehash.RandomSHA256())
	mp := base.NewDummyBlockDataMap(manifest)
	t.setMap(db, mp)
	st, _ := t.SuffrageState(height+10, suffrageheight, nodes)
	t.setSuffrageState(db, st)

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
		rsufstt, found, err := db.Suffrage(manifest.Height())
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

func (t *testCommonPermanentDatabase) TestLoadEmptyDB() {
	db := t.newDB()
	defer db.Close()
}

func (t *testCommonPermanentDatabase) TestLoad() {
	height := base.Height(33)
	_, nodes := t.Locals(3)

	sufstt, _ := t.SuffrageState(height, base.Height(66), nodes)

	stts := t.States(height, 3)
	stts = append(stts, sufstt)

	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
	mp := base.NewDummyBlockDataMap(manifest)

	ops := make([]util.Hash, 3)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	wst := t.NewMemLeveldbBlockWriteDatabase(height)
	t.NoError(wst.SetMap(mp))
	t.NoError(wst.SetStates(stts))
	t.NoError(wst.SetOperations(ops))
	t.NoError(wst.Write())

	temp, err := wst.TempDatabase()
	t.NoError(err)

	perm := t.newDB()
	t.NoError(perm.MergeTempDatabase(context.TODO(), temp))

	t.Run("check blockdatamap in perm", func() {
		nm, found, err := perm.LastMap()
		t.NoError(err)
		t.True(found)
		base.EqualBlockDataMap(t.Assert(), mp, nm)
	})

	t.Run("check suffrage state in perm", func() {
		nst, found, err := perm.LastSuffrage()
		t.NoError(err)
		t.True(found)
		t.True(base.IsEqualState(sufstt, nst))
	})

	newperm, err := t.newFromDB(perm)
	t.NoError(err)

	t.Run("check blockdatamap in new perm", func() {
		nm, found, err := newperm.LastMap()
		t.NoError(err)
		t.True(found)
		base.EqualBlockDataMap(t.Assert(), mp, nm)
	})

	t.Run("check suffrage state in new perm", func() {
		nst, found, err := newperm.LastSuffrage()
		t.NoError(err)
		t.True(found)
		t.True(base.IsEqualState(sufstt, nst))
	})
}

func (t *testCommonPermanentDatabase) TestMergeTempDatabase() {
	height := base.Height(33)
	_, nodes := t.Locals(3)

	sufstt, _ := t.SuffrageState(height, base.Height(66), nodes)

	stts := t.States(height, 3)
	stts = append(stts, sufstt)

	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
	mp := base.NewDummyBlockDataMap(manifest)

	ops := make([]util.Hash, 3)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	wst := t.NewMemLeveldbBlockWriteDatabase(height)
	t.NoError(wst.SetMap(mp))
	t.NoError(wst.SetStates(stts))
	t.NoError(wst.SetOperations(ops))
	t.NoError(wst.Write())

	temp, err := wst.TempDatabase()
	t.NoError(err)

	t.Run("check opertions", func() {
		perm := t.newDB()

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

	t.Run("check blockdatamap", func() {
		perm := t.newDB()

		rm, found, err := perm.Map(height)
		t.NoError(err)
		t.False(found)
		t.Nil(rm)

		t.NoError(perm.MergeTempDatabase(context.TODO(), temp))

		rm, found, err = perm.Map(height)
		t.NoError(err)
		t.True(found)
		t.NotNil(rm)

		base.EqualBlockDataMap(t.Assert(), mp, rm)
	})
}
