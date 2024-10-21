package isaacblock

import (
	"context"
	"math"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"golang.org/x/sync/semaphore"
)

type testWriter struct {
	BaseTestLocalBlockFS
}

func (t *testWriter) TestNew() {
	height := base.Height(33)
	db := t.NewLeveldbBlockWriteDatabase(height)
	defer db.DeepClose()

	pr := isaac.NewProposalSignFact(isaac.NewProposalFact(base.RawPoint(33, 44), t.Local.Address(), valuehash.RandomSHA256(), nil))

	fswriter := &DummyBlockFSWriter{}
	writer := NewWriter(pr, nil, db, func(isaac.BlockWriteDatabase) error {
		return nil
	}, fswriter, math.MaxInt8)

	_ = (interface{})(writer).(isaac.BlockWriter)

	t.Run("empty previous", func() {
		m, err := writer.Manifest(context.Background(), nil)
		t.Error(err)
		t.Nil(m)
	})
}

func (t *testWriter) TestSetOperations() {
	point := base.RawPoint(33, 44)

	db := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer db.DeepClose()

	fswriter := &DummyBlockFSWriter{}

	ophs := make([][2]util.Hash, 33)
	ops := make([]util.Hash, len(ophs))
	facts := make([]util.Hash, len(ophs))
	for i := range facts {
		ops[i] = valuehash.RandomSHA256()
		facts[i] = valuehash.RandomSHA256()
		ophs[i] = [2]util.Hash{ops[i], facts[i]}
	}

	pr := isaac.NewProposalSignFact(isaac.NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))
	_ = pr.Sign(t.Local.Privatekey(), t.LocalParams.NetworkID())

	writer := NewWriter(pr, nil, db, func(isaac.BlockWriteDatabase) error { return nil }, fswriter, math.MaxInt8)

	writer.SetOperationsSize(uint64(len(facts)))

	unodes := make([]fixedtree.Node, len(facts))
	fswriter.setOperationsTreef = func(_ context.Context, tr fixedtree.Tree) error {
		return tr.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
			unodes[index] = n

			return true, nil
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sem := semaphore.NewWeighted(int64(len(facts)))
	for i := range facts {
		index := uint64(i)
		_ = sem.Acquire(ctx, 1)

		go func() {
			defer sem.Release(1)

			var errorreason base.OperationProcessReasonError
			if index%3 == 0 {
				errorreason = base.NewBaseOperationProcessReasonf("%d", index)
			}
			writer.SetProcessResult(ctx, index, ops[index], facts[index], errorreason == nil, errorreason)
		}()
	}

	_ = sem.Acquire(ctx, int64(len(facts)))

	opstree, err := writer.opstreeg.Tree()
	t.NoError(err)
	t.NoError(fswriter.SetOperationsTree(context.Background(), opstree))

	t.Equal(len(facts), writer.opstreeg.Len())

	for i := range unodes {
		n := unodes[i].(base.OperationFixedtreeNode)

		switch {
		case i%3 == 0:
			t.NotNil(n.Reason())
			t.False(n.InState())
		default:
			t.Nil(n.Reason())
			t.True(n.InState())
		}
	}
}

func (t *testWriter) TestSetStates() {
	point := base.RawPoint(33, 44)

	db := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer db.DeepClose()

	fswriter := &DummyBlockFSWriter{}

	ophs := make([][2]util.Hash, 33)
	ops := make([]base.Operation, 33)
	for i := range ops {
		fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := isaac.NewDummyOperation(fact, t.Local.Privatekey(), t.LocalParams.NetworkID())
		t.NoError(err)

		ops[i] = op
		ophs[i] = [2]util.Hash{op.Hash(), op.Fact().Hash()}
	}
	states := make([][]base.StateMergeValue, len(ops))
	for i := range ops {
		sts := t.States(point.Height(), i%5+1)

		stvs := make([]base.StateMergeValue, len(sts))
		for j := range sts {
			st := sts[j]
			stvs[j] = base.NewBaseStateMergeValue(st.Key(), st.Value(), nil)
		}
		states[i] = stvs
	}

	pr := isaac.NewProposalSignFact(isaac.NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))
	_ = pr.Sign(t.Local.Privatekey(), t.LocalParams.NetworkID())

	writer := NewWriter(pr, base.NilGetState, db, func(isaac.BlockWriteDatabase) error { return nil }, fswriter, math.MaxInt8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer.SetOperationsSize(uint64(len(ops)))

	sem := semaphore.NewWeighted(int64(len(ops)))
	for i := range states {
		index := uint64(i)
		_ = sem.Acquire(ctx, 1)

		go func() {
			defer sem.Release(1)

			t.NoError(writer.SetStates(ctx, index, states[index], ops[index]))
		}()
	}

	_ = sem.Acquire(ctx, int64(len(ops)))

	for i := range states {
		sts := states[i]
		stmerger := writer.statesMerger.(*DefaultStatesMerger)

		for j := range sts {
			st := sts[j]
			merger, found := stmerger.stvmmap.Value(st.Key())
			t.True(found)

			nst, err := merger.CloseValue()
			t.NoError(err)

			t.Equal(st.Key(), nst.Key())
			t.True(base.IsEqualStateValue(st, nst.Value()))
		}
	}
}

func (t *testWriter) TestSetStatesAndClose() {
	point := base.RawPoint(33, 44)

	db := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer db.DeepClose()

	fswriter := &DummyBlockFSWriter{}

	var sufststored base.State
	fswriter.setStatef = func(_ context.Context, _, _ uint64, st base.State) error {
		if string(st.Key()) == isaac.SuffrageStateKey {
			sufststored = st
		}

		return nil
	}

	var ststree fixedtree.Tree

	fswriter.setStatesTreef = func(_ context.Context, tr fixedtree.Tree) error {
		ststree = tr

		return nil
	}

	ophs := make([][2]util.Hash, 33)
	facts := make([]util.Hash, len(ophs))
	ops := make([]base.Operation, len(ophs))
	for i := range ops {
		fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := isaac.NewDummyOperation(fact, t.Local.Privatekey(), t.LocalParams.NetworkID())
		t.NoError(err)

		ops[i] = op
		ophs[i] = [2]util.Hash{op.Hash(), op.Fact().Hash()}
		facts[i] = op.Fact().Hash()
	}
	states := make([][]base.StateMergeValue, len(ops))
	for i := range ops[:len(ops)-1] {
		sts := t.States(point.Height(), i%5+1)

		stvs := make([]base.StateMergeValue, len(sts))
		for j := range sts {
			st := sts[j]
			stvs[j] = base.NewBaseStateMergeValue(st.Key(), st.Value(), nil)
		}
		states[i] = stvs

	}

	sufst, _ := t.SuffrageState(point.Height(), base.Height(22), nil)
	{
		states[len(ops)-1] = []base.StateMergeValue{base.NewBaseStateMergeValue(sufst.Key(), sufst.Value(), nil)}
	}

	pr := isaac.NewProposalSignFact(isaac.NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))
	_ = pr.Sign(t.Local.Privatekey(), t.LocalParams.NetworkID())

	writer := NewWriter(pr, base.NilGetState, db, func(isaac.BlockWriteDatabase) error { return nil }, fswriter, math.MaxInt8)

	writer.SetOperationsSize(uint64(len(ops)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sem := semaphore.NewWeighted(int64(len(ops)))
	for i := range states {
		index := uint64(i)
		_ = sem.Acquire(ctx, 1)

		go func() {
			defer sem.Release(1)

			t.NoError(writer.SetProcessResult(ctx, index, ops[index].Hash(), facts[index], true, nil))
			t.NoError(writer.SetStates(ctx, index, states[index], ops[index]))
		}()
	}

	_ = sem.Acquire(ctx, int64(len(ops)))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest, err := writer.Manifest(ctx, previous)
	t.NoError(err)
	t.NotNil(manifest)

	t.NoError(writer.waitSaveWorker(context.Background()))

	t.NotNil(sufststored)
	t.True(manifest.Suffrage().Equal(sufststored.Hash())) // NOTE suffrage hash

	var sufnodefound bool

	ststree.Traverse(func(_ uint64, n fixedtree.Node) (bool, error) {
		if n.Key() == sufststored.Hash().String() {
			sufnodefound = true

			return false, nil
		}

		return true, nil
	})

	t.True(sufnodefound)
}

func (t *testWriter) TestManifest() {
	point := base.RawPoint(33, 44)

	db := t.NewLeveldbBlockWriteDatabase(point.Height())
	defer db.DeepClose()

	fswriter := &DummyBlockFSWriter{}

	mch := make(chan base.Manifest, 1)
	fswriter.setManifestf = func(_ context.Context, m base.Manifest) error {
		mch <- m

		return nil
	}

	ops := make([][2]util.Hash, 33)
	for i := range ops {
		ops[i] = [2]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}
	}

	pr := isaac.NewProposalSignFact(isaac.NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ops))
	_ = pr.Sign(t.Local.Privatekey(), t.LocalParams.NetworkID())

	writer := NewWriter(pr, nil, db, func(isaac.BlockWriteDatabase) error {
		return nil
	}, fswriter, math.MaxInt8)

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest, err := writer.Manifest(context.Background(), previous)
	t.NoError(err)
	t.NotNil(manifest)

	t.True(manifest.Previous().Equal(previous.Hash()))
	t.True(manifest.Proposal().Equal(pr.Fact().Hash()))
	t.Equal(manifest.OperationsTree(), nil)
	t.Equal(manifest.StatesTree(), nil)

	um := <-mch
	base.EqualManifest(t.Assert(), manifest, um)
}

func TestWriter(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testWriter))
}
