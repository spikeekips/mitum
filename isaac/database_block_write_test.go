package isaac

import (
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testLeveldbBlockWriteDatabase struct {
	BaseTestBallots
	BaseTestDatabase
}

func (t *testLeveldbBlockWriteDatabase) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testLeveldbBlockWriteDatabase) TestNew() {
	t.Run("valid", func() {
		wst, err := NewLeveldbBlockWriteDatabase(base.Height(33), t.Root, t.Encs, t.Enc)
		t.NoError(err)

		_ = (interface{})(wst).(BlockWriteDatabase)
	})

	t.Run("root exists", func() {
		_, err := NewLeveldbBlockWriteDatabase(base.Height(33), t.Root, t.Encs, t.Enc)
		t.Error(err)
		t.Contains(err.Error(), "failed batch leveldb storage")
	})
}

func (t *testLeveldbBlockWriteDatabase) TestSetMap() {
	height := base.Height(33)

	wst := t.NewMemLeveldbBlockWriteDatabase(height)
	defer wst.Close()

	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
	mp := base.NewDummyBlockDataMap(manifest)
	t.NoError(wst.SetMap(mp))

	t.NoError(wst.Write())

	rst, err := wst.TempDatabase()
	t.NoError(err)

	t.Run("blockdatamap", func() {
		rm, err := rst.Map()
		t.NoError(err)

		base.EqualBlockDataMap(t.Assert(), mp, rm)
	})
}

func (t *testLeveldbBlockWriteDatabase) TestSetStates() {
	height := base.Height(33)
	_, nodes := t.Locals(3)

	sufstt, _ := t.SuffrageState(height, base.Height(33), nodes)

	stts := t.States(height, 3)
	stts = append(stts, sufstt)

	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())

	wst := t.NewMemLeveldbBlockWriteDatabase(height)
	defer wst.Close()

	mp := base.NewDummyBlockDataMap(manifest)
	t.NoError(wst.SetMap(mp))
	t.NoError(wst.SetStates(stts))
	t.NoError(wst.Write())

	rst, err := wst.TempDatabase()
	t.NoError(err)

	t.Run("check suffrage", func() {
		rstt, found, err := rst.Suffrage()
		t.NotNil(rstt)
		t.True(found)
		t.NoError(err)

		t.True(base.IsEqualState(sufstt, rstt))
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
}

func (t *testLeveldbBlockWriteDatabase) TestSetOperations() {
	wst := t.NewMemLeveldbBlockWriteDatabase(base.Height(33))
	defer wst.Close()

	ops := make([]util.Hash, 33)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	t.NoError(wst.SetOperations(ops))

	manifest := base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256())
	mp := base.NewDummyBlockDataMap(manifest)
	t.NoError(wst.SetMap(mp))
	t.NoError(wst.Write())

	rst, err := wst.TempDatabase()
	t.NoError(err)

	t.Run("check operation exists", func() {
		for i := range ops {
			found, err := rst.ExistsOperation(ops[i])
			t.NoError(err)
			t.True(found)
		}
	})

	t.Run("check unknown operation", func() {
		found, err := rst.ExistsOperation(valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
	})
}

func (t *testLeveldbBlockWriteDatabase) TestRemove() {
	height := base.Height(33)

	wst := t.NewLeveldbBlockWriteDatabase(height)
	defer wst.Close()

	t.T().Log("check root directory created")
	fi, err := os.Stat(t.Root)
	t.NoError(err)
	t.True(fi.IsDir())

	t.NoError(wst.Write())

	t.NoError(wst.Remove())

	t.T().Log("check root directory removed")
	_, err = os.Stat(t.Root)
	t.True(os.IsNotExist(err))

	t.T().Log("remove again")
	err = wst.Remove()
	t.True(errors.Is(err, storage.InternalError))
}

func TestLeveldbBlockWriteDatabase(t *testing.T) {
	suite.Run(t, new(testLeveldbBlockWriteDatabase))
}
