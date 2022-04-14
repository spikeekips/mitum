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
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

func (db *TempLeveldbDatabase) States(f func(base.State) (bool, error)) error {
	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixState),
		func(key []byte, raw []byte) (bool, error) {
			i, err := db.decodeState(raw)
			if err != nil {
				return false, errors.Wrap(err, "")
			}

			return f(i)
		},
		true,
	); err != nil {
		return errors.Wrap(err, "failed to iter states")
	}

	return nil
}

type testTempLeveldbDatabase struct {
	baseTestHandler
	baseTestDatabase
}

func (t *testTempLeveldbDatabase) SetupTest() {
	t.baseTestHandler.SetupTest()
	t.baseTestDatabase.SetupTest()
}

func (t *testTempLeveldbDatabase) TestLoad() {
	height := base.Height(33)
	_, nodes := t.locals(3)

	sufstt, _ := t.suffrageState(height, base.Height(66), nodes)

	stts := t.states(height, 3)
	stts = append(stts, sufstt)

	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
	m := base.NewDummyBlockDataMap(manifest)

	ops := make([]util.Hash, 33)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	wst := t.newLeveldbBlockWriteDatabase(height)
	t.NoError(wst.SetManifest(manifest))
	t.NoError(wst.SetMap(m))
	t.NoError(wst.SetStates(stts))
	t.NoError(wst.SetOperations(ops))
	t.NoError(wst.Write())

	t.NoError(wst.Close())

	rst, err := NewTempLeveldbDatabase(t.root, t.encs, t.enc)
	t.NoError(err)
	defer rst.Remove()

	_ = (interface{})(rst).(TempDatabase)

	t.Run("Manifest", func() {
		rm, err := rst.Manifest()
		t.NoError(err)

		base.EqualManifest(t.Assert(), manifest, rm)
	})

	t.Run("check last suffrage", func() {
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

	t.Run("remove", func() {
		t.NoError(rst.Remove())

		_, err = os.Stat(t.root)
		t.True(os.IsNotExist(err))
	})

	t.Run("remove again", func() {
		err := rst.Remove()
		t.True(errors.Is(err, storage.InternalError))
	})
}

func TestTempLeveldbDatabase(t *testing.T) {
	suite.Run(t, new(testTempLeveldbDatabase))
}
