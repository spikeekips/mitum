package blockdata

import (
	"context"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/tree"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"golang.org/x/sync/semaphore"
)

type dummyBlockWriteDatabase struct {
	*database.LeveldbBlockWrite
	setOperationsf func([]util.Hash) error
}

func (db *dummyBlockWriteDatabase) SetOperations(ops []util.Hash) error {
	if db.setOperationsf != nil {
		return db.setOperationsf(ops)
	}

	return db.LeveldbBlockWrite.SetOperations(ops)
}

type testWriter struct {
	isaac.BaseTestBallots
	database.BaseTestDatabase
}

func (t *testWriter) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testWriter) TestNew() {
	height := base.Height(33)
	db := t.NewMemLeveldbBlockWriteDatabase(height)
	defer db.Close()

	fswriter := &DummyBlockDataFSWriter{}
	writer := NewWriter(db, func(isaac.BlockWriteDatabase) error {
		return nil
	}, fswriter)

	_ = (interface{})(writer).(isaac.BlockDataWriter)

	t.Run("empty previous", func() {
		m, err := writer.Manifest(context.Background(), nil)
		t.Error(err)
		t.Nil(m)
	})

	t.Run("empty proposal", func() {
		previous := base.NewDummyManifest(height-1, valuehash.RandomSHA256())
		m, err := writer.Manifest(context.Background(), previous)
		t.Error(err)
		t.Nil(m)
	})
}

func (t *testWriter) TestSetProposal() {
	point := base.RawPoint(33, 44)

	db := t.NewMemLeveldbBlockWriteDatabase(point.Height())
	defer db.Close()

	fswriter := &DummyBlockDataFSWriter{}

	prch := make(chan base.ProposalSignedFact, 1)
	fswriter.setProposalf = func(_ context.Context, pr base.ProposalSignedFact) error {
		prch <- pr

		return nil
	}

	writer := NewWriter(db, func(isaac.BlockWriteDatabase) error {
		return nil
	}, fswriter)

	ops := make([]util.Hash, 33)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	pr := isaac.NewProposalSignedFact(isaac.NewProposalFact(point, t.Local.Address(), ops))
	_ = pr.Sign(t.Local.Privatekey(), t.Policy.NetworkID())

	t.NoError(writer.SetProposal(context.Background(), pr))

	base.EqualProposalSignedFact(t.Assert(), pr, writer.proposal)

	upr := <-prch
	base.EqualProposalSignedFact(t.Assert(), pr, upr)
}

func (t *testWriter) TestSetOperations() {
	point := base.RawPoint(33, 44)

	db := t.NewMemLeveldbBlockWriteDatabase(point.Height())
	defer db.Close()

	fswriter := &DummyBlockDataFSWriter{}

	writer := NewWriter(db, func(isaac.BlockWriteDatabase) error { return nil }, fswriter)

	ops := make([]util.Hash, 33)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	writer.SetOperationsSize(uint64(len(ops)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sem := semaphore.NewWeighted(int64(len(ops)))
	for i := range ops {
		index := i
		_ = sem.Acquire(ctx, 1)

		go func() {
			defer sem.Release(1)

			var errorreason base.OperationProcessReasonError
			if index%3 == 0 {
				errorreason = base.NewBaseOperationProcessReasonError("%d", index)
			}
			writer.SetProcessResult(ctx, index, ops[index], errorreason == nil, errorreason)
		}()
	}

	_ = sem.Acquire(ctx, int64(len(ops)))

	t.Equal(len(ops), writer.opstreeg.Len())

	uops := make([]util.Hash, writer.opstreeg.Len())
	writer.opstreeg.Traverse(func(i tree.FixedTreeNode) (bool, error) {
		node := i.(base.OperationFixedTreeNode)
		uops[node.Index()] = node.Operation()

		if node.Index()%3 == 0 {
			t.NotNil(node.Reason())
			t.False(node.InState())
		}

		return true, nil
	})

	for i := range ops {
		a := ops[i]
		b := uops[i]

		t.True(a.Equal(b))
	}
}

func (t *testWriter) TestSetStates() {
	point := base.RawPoint(33, 44)

	db := t.NewMemLeveldbBlockWriteDatabase(point.Height())
	defer db.Close()

	fswriter := &DummyBlockDataFSWriter{}

	writer := NewWriter(db, func(isaac.BlockWriteDatabase) error { return nil }, fswriter)

	ophs := make([]util.Hash, 33)
	ops := make([]base.Operation, 33)
	for i := range ops {
		fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := isaac.NewDummyOperationProcessable(fact, t.Local.Privatekey(), t.Policy.NetworkID())
		t.NoError(err)

		ops[i] = op
		ophs[i] = op.Fact().Hash()
	}
	states := make([][]base.State, len(ops))
	for i := range ops {
		states[i] = t.States(point.Height(), i%5+1)
	}

	pr := isaac.NewProposalSignedFact(isaac.NewProposalFact(point, t.Local.Address(), ophs))
	_ = pr.Sign(t.Local.Privatekey(), t.Policy.NetworkID())

	t.NoError(writer.SetProposal(context.Background(), pr))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sem := semaphore.NewWeighted(int64(len(ops)))
	for i := range states {
		index := i
		_ = sem.Acquire(ctx, 1)

		go func() {
			defer sem.Release(1)

			t.NoError(writer.SetStates(ctx, index, states[index], ops[index]))
		}()
	}

	_ = sem.Acquire(ctx, int64(len(ops)))

	for i := range states {
		sts := states[i]

		for j := range sts {
			st := sts[j]
			k, found := writer.states.Value(st.Key())
			t.True(found)

			merger, ok := k.(base.StateValueMerger)
			t.True(ok)

			t.NoError(merger.Close())

			t.Equal(st.Key(), merger.Key())
			t.True(st.Value().Equal(merger.Value()))
		}
	}
}

func (t *testWriter) TestSetStatesAndClose() {
	point := base.RawPoint(33, 44)

	db := t.NewMemLeveldbBlockWriteDatabase(point.Height())
	defer db.Close()

	fswriter := &DummyBlockDataFSWriter{}

	var sufststored base.State
	fswriter.setStatef = func(_ context.Context, _ int, st base.State) error {
		if string(st.Key()) == isaac.SuffrageStateKey {
			sufststored = st
		}

		return nil
	}

	writer := NewWriter(db, func(isaac.BlockWriteDatabase) error { return nil }, fswriter)

	ophs := make([]util.Hash, 33)
	ops := make([]base.Operation, len(ophs))
	for i := range ops {
		fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := isaac.NewDummyOperationProcessable(fact, t.Local.Privatekey(), t.Policy.NetworkID())
		t.NoError(err)

		ops[i] = op
		ophs[i] = op.Fact().Hash()
	}
	states := make([][]base.State, len(ops))
	for i := range ops[:len(ops)-1] {
		states[i] = t.States(point.Height(), i%5+1)
	}

	{
		sufst, _ := t.SuffrageState(point.Height(), base.Height(22), nil)
		states[len(ops)-1] = []base.State{sufst}
	}

	pr := isaac.NewProposalSignedFact(isaac.NewProposalFact(point, t.Local.Address(), ophs))
	_ = pr.Sign(t.Local.Privatekey(), t.Policy.NetworkID())

	t.NoError(writer.SetProposal(context.Background(), pr))
	writer.SetOperationsSize(uint64(len(ops)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sem := semaphore.NewWeighted(int64(len(ops)))
	for i := range states {
		index := i
		_ = sem.Acquire(ctx, 1)

		go func() {
			defer sem.Release(1)

			t.NoError(writer.SetProcessResult(ctx, index, ophs[index], true, nil))
			t.NoError(writer.SetStates(ctx, index, states[index], ops[index]))
		}()
	}

	_ = sem.Acquire(ctx, int64(len(ops)))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest, err := writer.Manifest(ctx, previous)
	t.NoError(err)
	t.NotNil(manifest)

	t.NotNil(writer.opstree.Root())
	t.NotNil(writer.ststree.Root())

	var sufnodefound bool
	writer.ststree.Traverse(func(i tree.FixedTreeNode) (bool, error) {
		node := i.(base.StateFixedTreeNode)
		if string(node.Key()) == isaac.SuffrageStateKey {
			sufnodefound = true

			return false, nil
		}
		return true, nil
	})
	t.NoError(err)
	t.True(sufnodefound)

	t.NotNil(sufststored)

	t.True(manifest.Suffrage().Equal(sufststored.Hash())) // NOTE suffrage hash
}

func (t *testWriter) TestManifest() {
	point := base.RawPoint(33, 44)

	db := t.NewMemLeveldbBlockWriteDatabase(point.Height())
	defer db.Close()

	fswriter := &DummyBlockDataFSWriter{}

	mch := make(chan base.Manifest, 1)
	fswriter.setManifestf = func(_ context.Context, m base.Manifest) error {
		mch <- m

		return nil
	}

	writer := NewWriter(db, func(isaac.BlockWriteDatabase) error {
		return nil
	}, fswriter)

	ops := make([]util.Hash, 33)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	pr := isaac.NewProposalSignedFact(isaac.NewProposalFact(point, t.Local.Address(), ops))
	_ = pr.Sign(t.Local.Privatekey(), t.Policy.NetworkID())

	t.NoError(writer.SetProposal(context.Background(), pr))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest, err := writer.Manifest(context.Background(), previous)
	t.NoError(err)
	t.NotNil(manifest)

	t.True(manifest.Previous().Equal(previous.Hash()))
	t.True(manifest.Proposal().Equal(pr.Fact().Hash()))
	t.True(manifest.OperationsTree().Equal(valuehash.Bytes(writer.opstree.Root())))
	t.True(manifest.StatesTree().Equal(valuehash.Bytes(writer.ststree.Root())))

	um := <-mch
	base.EqualManifest(t.Assert(), manifest, um)
}

func TestWriter(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testWriter))
}
