package isaac

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

func (db *LeveldbPermanentDatabase) setState(st base.State) error {
	e := util.StringErrorFunc("failed to set state")

	b, err := db.marshal(st)
	if err != nil {
		return e(err, "")
	}

	if err := db.st.Put(stateDBKey(st.Key()), b, nil); err != nil {
		return e(err, "failed to put state")
	}

	if st.Key() == SuffrageStateKey {
		if err := db.st.Put(suffrageDBKey(st.Height()), b, nil); err != nil {
			return e(err, "failed to put suffrage by block height")
		}

		sv := st.Value().(base.SuffrageStateValue)
		if err := db.st.Put(suffrageHeightDBKey(sv.Height()), b, nil); err != nil {
			return e(err, "failed to put suffrage by height")
		}
	}

	return nil
}

type testLeveldbPermanentDatabase struct {
	baseTestHandler
	baseTestDatabase
}

func (t *testLeveldbPermanentDatabase) SetupTest() {
	t.baseTestHandler.SetupTest()
	t.baseTestDatabase.SetupTest()
}

func (t *testLeveldbPermanentDatabase) newDB() *LeveldbPermanentDatabase {
	st := leveldbstorage.NewMemWriteStorage()
	db, err := newLeveldbPermanentDatabase(st, t.encs, t.enc)
	t.NoError(err)

	return db
}

func (t *testLeveldbPermanentDatabase) TestNew() {
	db := t.newDB()
	defer db.Close()

	_ = (interface{})(db).(PermanentDatabase)
}

func (t *testLeveldbPermanentDatabase) TestLastManifest() {
	height := base.Height(33)
	m := base.NewDummyManifest(height, valuehash.RandomSHA256())

	db := t.newDB()
	defer db.Close()

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

func (t *testLeveldbPermanentDatabase) TestLastSuffrage() {
	height := base.Height(33)
	_, nodes := t.locals(3)

	sufstt, _ := t.suffrageState(height, base.Height(66), nodes)

	db := t.newDB()
	defer db.Close()

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

func (t *testLeveldbPermanentDatabase) TestSuffrage() {
	baseheight := base.Height(33)
	_, nodes := t.locals(3)

	db := t.newDB()
	defer db.Close()

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

func TestLeveldbPermanentDatabase(t *testing.T) {
	suite.Run(t, new(testLeveldbPermanentDatabase))
}
