package isaac

import (
	"context"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/tree"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"golang.org/x/sync/semaphore"
)

type dummyBlockWriteDatabase struct {
	*LeveldbBlockWriteDatabase
	setOperationsf func([]util.Hash) error
}

func (db *dummyBlockWriteDatabase) SetOperations(ops []util.Hash) error {
	if db.setOperationsf != nil {
		return db.setOperationsf(ops)
	}

	return db.LeveldbBlockWriteDatabase.SetOperations(ops)
}

type DummyBlockDataFSWriter struct {
	setProposalf        func(context.Context, base.ProposalSignedFact) error
	setOperationf       func(context.Context, int, base.Operation) error
	setOperationsTreef  func(context.Context, tree.FixedTree) error
	setStatef           func(context.Context, int, base.State) error
	setStatesTreef      func(context.Context, tree.FixedTree) error
	setManifestf        func(context.Context, base.Manifest) error
	setINITVoteprooff   func(context.Context, base.INITVoteproof) error
	setACCEPTVoteprooff func(context.Context, base.ACCEPTVoteproof) error
	mapf                func(context.Context) (base.BlockDataMap, error)
	cancelf             func() error
}

func (w *DummyBlockDataFSWriter) SetProposal(ctx context.Context, pr base.ProposalSignedFact) error {
	if w.setProposalf != nil {
		return w.setProposalf(ctx, pr)
	}
	return nil
}

func (w *DummyBlockDataFSWriter) SetOperation(ctx context.Context, index int, op base.Operation) error {
	if w.setOperationf != nil {
		return w.setOperationf(ctx, index, op)
	}
	return nil
}

func (w *DummyBlockDataFSWriter) SetOperationsTree(ctx context.Context, tr tree.FixedTree) error {
	if w.setOperationsTreef != nil {
		return w.setOperationsTreef(ctx, tr)
	}
	return nil
}

func (w *DummyBlockDataFSWriter) SetState(ctx context.Context, index int, st base.State) error {
	if w.setStatef != nil {
		return w.setStatef(ctx, index, st)
	}
	return nil
}

func (w *DummyBlockDataFSWriter) SetStatesTree(ctx context.Context, tr tree.FixedTree) error {
	if w.setStatesTreef != nil {
		return w.setStatesTreef(ctx, tr)
	}
	return nil
}

func (w *DummyBlockDataFSWriter) SetManifest(ctx context.Context, m base.Manifest) error {
	if w.setManifestf != nil {
		return w.setManifestf(ctx, m)
	}
	return nil
}

func (w *DummyBlockDataFSWriter) SetINITVoteproof(ctx context.Context, vp base.INITVoteproof) error {
	if w.setINITVoteprooff != nil {
		return w.setINITVoteprooff(ctx, vp)
	}
	return nil
}

func (w *DummyBlockDataFSWriter) SetACCEPTVoteproof(ctx context.Context, vp base.ACCEPTVoteproof) error {
	if w.setACCEPTVoteprooff != nil {
		return w.setACCEPTVoteprooff(ctx, vp)
	}
	return nil
}

func (w *DummyBlockDataFSWriter) Map(ctx context.Context) (base.BlockDataMap, error) {
	if w.mapf != nil {
		return w.mapf(ctx)
	}
	return nil, nil
}

func (w *DummyBlockDataFSWriter) Cancel() error {
	if w.cancelf != nil {
		return w.cancelf()
	}
	return nil
}

type testDefaultBlockDataWriter struct {
	baseStateTestHandler
	baseTestDatabase
}

func (t *testDefaultBlockDataWriter) SetupTest() {
	t.baseStateTestHandler.SetupTest()
	t.baseTestDatabase.SetupTest()
}

func (t *testDefaultBlockDataWriter) TestNew() {
	height := base.Height(33)
	db := t.newMemLeveldbBlockWriteDatabase(height)
	defer db.Close()

	fswriter := &DummyBlockDataFSWriter{}
	writer := NewDefaultBlockDataWriter(db, func(BlockWriteDatabase) error {
		return nil
	}, fswriter)

	_ = (interface{})(writer).(BlockDataWriter)

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

func (t *testDefaultBlockDataWriter) TestSetProposal() {
	point := base.RawPoint(33, 44)

	db := t.newMemLeveldbBlockWriteDatabase(point.Height())
	defer db.Close()

	fswriter := &DummyBlockDataFSWriter{}

	prch := make(chan base.ProposalSignedFact, 1)
	fswriter.setProposalf = func(_ context.Context, pr base.ProposalSignedFact) error {
		prch <- pr

		return nil
	}

	writer := NewDefaultBlockDataWriter(db, func(BlockWriteDatabase) error {
		return nil
	}, fswriter)

	ops := make([]util.Hash, 33)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	pr := NewProposalSignedFact(NewProposalFact(point, t.local.Address(), ops))
	_ = pr.Sign(t.local.Privatekey(), t.policy.NetworkID())

	t.NoError(writer.SetProposal(context.Background(), pr))

	base.EqualProposalSignedFact(t.Assert(), pr, writer.proposal)

	upr := <-prch
	base.EqualProposalSignedFact(t.Assert(), pr, upr)
}

func (t *testDefaultBlockDataWriter) TestSetOperations() {
	point := base.RawPoint(33, 44)

	db := t.newMemLeveldbBlockWriteDatabase(point.Height())
	defer db.Close()

	fswriter := &DummyBlockDataFSWriter{}

	writer := NewDefaultBlockDataWriter(db, func(BlockWriteDatabase) error { return nil }, fswriter)

	ops := make([]util.Hash, 33)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	writer.SetOperationsSize(0, uint64(len(ops)))

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
			writer.SetOperation(ctx, index, ops[index], errorreason == nil, errorreason)
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

func (t *testDefaultBlockDataWriter) TestSetStates() {
	point := base.RawPoint(33, 44)

	db := t.newMemLeveldbBlockWriteDatabase(point.Height())
	defer db.Close()

	fswriter := &DummyBlockDataFSWriter{}

	writer := NewDefaultBlockDataWriter(db, func(BlockWriteDatabase) error { return nil }, fswriter)

	ophs := make([]util.Hash, 33)
	ops := make([]base.Operation, 33)
	for i := range ops {
		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := NewDummyOperationProcessable(fact, t.local.Privatekey(), t.policy.NetworkID())
		t.NoError(err)

		ops[i] = op
		ophs[i] = op.Fact().Hash()
	}
	states := make([][]base.State, len(ops))
	for i := range ops {
		states[i] = t.states(point.Height(), i%5+1)
	}

	pr := NewProposalSignedFact(NewProposalFact(point, t.local.Address(), ophs))
	_ = pr.Sign(t.local.Privatekey(), t.policy.NetworkID())

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

func (t *testDefaultBlockDataWriter) TestSetStatesAndClose() {
	point := base.RawPoint(33, 44)

	db := t.newMemLeveldbBlockWriteDatabase(point.Height())
	defer db.Close()

	fswriter := &DummyBlockDataFSWriter{}

	var sufststored base.State
	fswriter.setStatef = func(_ context.Context, _ int, st base.State) error {
		if string(st.Key()) == SuffrageStateKey {
			sufststored = st
		}

		return nil
	}

	writer := NewDefaultBlockDataWriter(db, func(BlockWriteDatabase) error { return nil }, fswriter)

	ophs := make([]util.Hash, 33)
	ops := make([]base.Operation, len(ophs))
	for i := range ops {
		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := NewDummyOperationProcessable(fact, t.local.Privatekey(), t.policy.NetworkID())
		t.NoError(err)

		ops[i] = op
		ophs[i] = op.Fact().Hash()
	}
	states := make([][]base.State, len(ops))
	for i := range ops[:len(ops)-1] {
		states[i] = t.states(point.Height(), i%5+1)
	}

	{
		sufst, _ := t.suffrageState(point.Height(), base.Height(22), nil)
		states[len(ops)-1] = []base.State{sufst}
	}

	pr := NewProposalSignedFact(NewProposalFact(point, t.local.Address(), ophs))
	_ = pr.Sign(t.local.Privatekey(), t.policy.NetworkID())

	t.NoError(writer.SetProposal(context.Background(), pr))
	writer.SetOperationsSize(0, uint64(len(ops)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sem := semaphore.NewWeighted(int64(len(ops)))
	for i := range states {
		index := i
		_ = sem.Acquire(ctx, 1)

		go func() {
			defer sem.Release(1)

			t.NoError(writer.SetOperation(ctx, index, ophs[index], true, nil))
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
		if string(node.Key()) == SuffrageStateKey {
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

func (t *testDefaultBlockDataWriter) TestManifest() {
	point := base.RawPoint(33, 44)

	db := t.newMemLeveldbBlockWriteDatabase(point.Height())
	defer db.Close()

	fswriter := &DummyBlockDataFSWriter{}

	mch := make(chan base.Manifest, 1)
	fswriter.setManifestf = func(_ context.Context, m base.Manifest) error {
		mch <- m

		return nil
	}

	writer := NewDefaultBlockDataWriter(db, func(BlockWriteDatabase) error {
		return nil
	}, fswriter)

	ops := make([]util.Hash, 33)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	pr := NewProposalSignedFact(NewProposalFact(point, t.local.Address(), ops))
	_ = pr.Sign(t.local.Privatekey(), t.policy.NetworkID())

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

func TestDefaultBlockDataWriter(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testDefaultBlockDataWriter))
}
