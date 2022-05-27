package isaacdatabase

import (
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

func (db *TempLeveldb) States(f func(base.State) (bool, error)) error {
	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixState),
		func(key []byte, raw []byte) (bool, error) {
			var st base.State
			if err := db.readHinter(raw, &st); err != nil {
				return false, errors.Wrap(err, "")
			}

			return f(st)
		},
		true,
	); err != nil {
		return errors.Wrap(err, "failed to iter states")
	}

	return nil
}

type testTempLeveldb struct {
	isaac.BaseTestBallots
	BaseTestDatabase
}

func (t *testTempLeveldb) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testTempLeveldb) TestLoad() {
	height := base.Height(33)
	_, nodes := t.Locals(3)

	sufstt, _ := t.SuffrageState(height, base.Height(66), nodes)

	policy := isaac.DefaultNetworkPolicy()
	policystt, _ := t.NetworkPolicyState(height, policy)

	stts := t.States(height, 3)
	stts = append(stts, sufstt, policystt)

	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
	mp := base.NewDummyBlockMap(manifest)

	ops := make([]util.Hash, 33)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	wst := t.NewLeveldbBlockWriteDatabase(height)
	t.NoError(wst.SetMap(mp))
	t.NoError(wst.SetStates(stts))
	t.NoError(wst.SetOperations(ops))
	t.NoError(wst.Write())

	t.NoError(wst.Close())

	rst, err := NewTempLeveldb(t.Root, t.Encs, t.Enc)
	t.NoError(err)
	defer rst.Remove()

	_ = (interface{})(rst).(isaac.TempDatabase)

	t.Run("blockmap", func() {
		rm, err := rst.Map()
		t.NoError(err)

		base.EqualBlockMap(t.Assert(), mp, rm)
	})

	t.Run("check last suffrage", func() {
		rstt, found, err := rst.Suffrage()
		t.NotNil(rstt)
		t.True(found)
		t.NoError(err)

		t.True(base.IsEqualState(sufstt, rstt))
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

	t.Run("check known operation exists", func() {
		for i := range ops {
			found, err := rst.ExistsKnownOperation(ops[i])
			t.NoError(err)
			t.True(found)
		}
	})

	t.Run("check unknown operation", func() {
		found, err := rst.ExistsKnownOperation(valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
	})

	t.Run("remove", func() {
		t.NoError(rst.Remove())

		_, err = os.Stat(t.Root)
		t.True(os.IsNotExist(err))
	})

	t.Run("remove again", func() {
		err := rst.Remove()
		t.True(errors.Is(err, storage.InternalError))
	})
}

func TestTempLeveldb(t *testing.T) {
	suite.Run(t, new(testTempLeveldb))
}
