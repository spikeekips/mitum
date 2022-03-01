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

type testRODatabase struct {
	baseTestHandler
	baseTestDatabase
}

func (t *testRODatabase) SetupTest() {
	t.baseTestHandler.SetupTest()
	t.baseTestDatabase.SetupTest()
}

func (t *testRODatabase) TestLoad() {
	height := base.Height(33)
	locals := t.nodes(3)

	nodes := make([]base.Node, len(locals))
	for i := range locals {
		nodes[i] = locals[i]
	}

	sv := NewSuffrageStateValue(
		base.Height(33),
		valuehash.RandomSHA256(),
		nodes,
	)

	_ = (interface{})(sv).(base.SuffrageStateValue)

	sufstt := base.NewBaseState(
		height,
		util.UUID().String(),
		sv,
	)
	sufstt.SetOperations([]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256(), valuehash.RandomSHA256()})

	stts := t.states(height, 3)
	stts = append(stts, sufstt)

	m := base.NewDummyManifest(height, valuehash.RandomSHA256())

	ops := make([]util.Hash, 33)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	wst := t.newWO(height)
	t.NoError(wst.SetManifest(m))
	t.NoError(wst.SetStates(stts))
	t.NoError(wst.SetOperations(ops))
	t.NoError(wst.Write())

	t.NoError(wst.Close())

	rst, err := newTempRODatabase(t.root, t.encs, t.enc)
	t.NoError(err)
	defer rst.Remove()

	t.Run("LastManifest", func() {
		rm, found, err := rst.LastManifest()
		t.NoError(err)
		t.True(found)

		base.CompareManifest(t.Assert(), m, rm)
	})

	t.Run("Manifest by height", func() {
		rm, found, err := rst.Manifest(m.Height())
		t.NoError(err)
		t.True(found)
		base.CompareManifest(t.Assert(), m, rm)
	})

	t.Run("Manifest by unknown height", func() {
		rm, found, err := rst.Manifest(m.Height() + 1)
		t.NoError(err)
		t.False(found)
		t.Nil(rm)
	})

	t.Run("check last suffrage", func() {
		rstt, found, err := rst.LastSuffrage()
		t.NotNil(rstt)
		t.True(found)
		t.NoError(err)

		t.True(base.IsEqualState(sufstt, rstt))
	})

	t.Run("check suffrage by height", func() {
		rstt, found, err := rst.Suffrage(sv.Height())
		t.NotNil(rstt)
		t.True(found)
		t.NoError(err)

		t.True(base.IsEqualState(sufstt, rstt))
	})

	t.Run("check unknonwn suffrage", func() {
		rstt, found, err := rst.Suffrage(sv.Height() + 1)
		t.Nil(rstt)
		t.NoError(err)
		t.False(found)
	})

	t.Run("check states", func() {
		for i := range stts {
			stt := stts[i]

			rstt, found, err := rst.State(stt.Hash())
			t.NotNil(rstt)
			t.True(found)
			t.NoError(err)

			t.True(base.IsEqualState(stt, rstt))
		}
	})

	t.Run("check unknown states", func() {
		rstt, found, err := rst.State(valuehash.RandomSHA256())
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
		t.True(errors.Is(err, storage.ConnectionError))
	})
}

func TestRODatabase(t *testing.T) {
	suite.Run(t, new(testRODatabase))
}
