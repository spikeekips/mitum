package isaacdatabase

import (
	"bytes"
	"sort"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testLeveldbBlockWrite struct {
	isaac.BaseTestBallots
	BaseTestDatabase
}

func (t *testLeveldbBlockWrite) SetupTest() {
	t.BaseTestBallots.SetupTest()
}

func (t *testLeveldbBlockWrite) TestNew() {
	t.Run("valid", func() {
		mst := leveldbstorage.NewMemStorage()
		wst := NewLeveldbBlockWrite(base.Height(33), mst, t.Encs, t.Enc)

		_ = (interface{})(wst).(isaac.BlockWriteDatabase)
	})
}

func (t *testLeveldbBlockWrite) Test2Writers() {
	height := base.Height(33)

	wsts := make([]*LeveldbBlockWrite, 33)
	sorted := make([]*LeveldbBlockWrite, 33)

	for i := range wsts {
		wst := t.NewLeveldbBlockWriteDatabase(height)
		wsts[i] = wst
		sorted[i] = wst
	}

	_, duplicated := util.CheckSliceDuplicated(wsts, func(_ interface{}, i int) string {
		return string(wsts[i].st.Prefix())
	})
	t.False(duplicated)

	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i].st.Prefix(), sorted[j].st.Prefix()) < 0
	})

	for i := range wsts {
		a := wsts[i]
		b := sorted[i]

		t.Equal(a.st.Prefix(), b.st.Prefix())
	}
}

func (t *testLeveldbBlockWrite) TestSetBlockMap() {
	height := base.Height(33)

	wst := t.NewLeveldbBlockWriteDatabase(height)
	defer wst.Close()
	defer wst.baseLeveldb.Close()

	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
	mp := base.NewDummyBlockMap(manifest)
	t.NoError(wst.SetBlockMap(mp))

	rst, err := wst.TempDatabase()
	t.NoError(err)

	t.Run("blockmap", func() {
		rm, err := rst.BlockMap()
		t.NoError(err)

		base.EqualBlockMap(t.Assert(), mp, rm)
	})
}

func (t *testLeveldbBlockWrite) TestSetStates() {
	height := base.Height(33)
	_, nodes := t.Locals(3)

	sufst, _ := t.SuffrageState(height, base.Height(33), nodes)
	policy := isaac.DefaultNetworkPolicy()
	policystt, _ := t.NetworkPolicyState(height, policy)

	stts := t.States(height, 3)
	stts = append(stts, sufst, policystt)

	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())

	wst := t.NewLeveldbBlockWriteDatabase(height)
	defer wst.Close()
	defer wst.baseLeveldb.Close()

	mp := base.NewDummyBlockMap(manifest)
	t.NoError(wst.SetBlockMap(mp))
	t.NoError(wst.SetStates(stts))

	rst, err := wst.TempDatabase()
	t.NoError(err)

	t.Run("check suffrage", func() {
		h := rst.SuffrageHeight()
		t.Equal(h, sufst.Value().(base.SuffrageStateValue).Height())
	})

	t.Run("check network policy", func() {
		rpolicy := rst.NetworkPolicy()
		t.NotNil(rpolicy)

		base.EqualNetworkPolicy(t.Assert(), policy, rpolicy)
	})

	t.Run("check states", func() {
		for i := range stts {
			stt := stts[i]

			rstt, found, err := rst.State(stt.Key())
			t.NotNil(rstt)
			t.True(found)
			t.NoError(err)

			t.True(base.IsEqualState(stt, rstt))
		}
	})

	t.Run("check unknown states", func() {
		rstt, found, err := rst.State(util.UUID().String())
		t.Nil(rstt)
		t.False(found)
		t.NoError(err)
	})

	t.Run("check instate operations", func() {
		for i := range stts {
			ops := stts[i].Operations()
			for j := range ops {
				op := ops[j]

				found, err := rst.ExistsInStateOperation(op)
				t.NoError(err)
				t.True(found)

				found, err = rst.ExistsKnownOperation(op)
				t.NoError(err)
				t.False(found)
			}
		}
	})
}

func (t *testLeveldbBlockWrite) TestSetOperations() {
	wst := t.NewLeveldbBlockWriteDatabase(base.Height(33))
	defer wst.Close()
	defer wst.baseLeveldb.Close()

	ops := make([]util.Hash, 33)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	t.NoError(wst.SetOperations(ops))

	manifest := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
	mp := base.NewDummyBlockMap(manifest)
	t.NoError(wst.SetBlockMap(mp))

	rst, err := wst.TempDatabase()
	t.NoError(err)

	t.Run("check operation exists", func() {
		for i := range ops {
			found, err := rst.ExistsKnownOperation(ops[i])
			t.NoError(err)
			t.True(found)

			found, err = rst.ExistsInStateOperation(ops[i])
			t.NoError(err)
			t.False(found)
		}
	})

	t.Run("check unknown operation", func() {
		found, err := rst.ExistsKnownOperation(valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
	})
}

func TestLeveldbBlockWrite(t *testing.T) {
	suite.Run(t, new(testLeveldbBlockWrite))
}
