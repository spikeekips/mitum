package isaacblock

import (
	"context"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"golang.org/x/sync/semaphore"
)

type testDefaultStatesMerger struct {
	BaseTestLocalBlockFS
}

func (t *testDefaultStatesMerger) TestSetStates() {
	height := base.Height(33)

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
		sts := t.States(height, i%5+1)

		stvs := make([]base.StateMergeValue, len(sts))
		for j := range sts {
			st := sts[j]
			stvs[j] = base.NewBaseStateMergeValue(st.Key(), st.Value(), nil)
		}
		states[i] = stvs
	}

	sm := NewDefaultStatesMerger(height, base.NilGetState, 1<<3)
	defer sm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sem := semaphore.NewWeighted(int64(len(ops)))
	for i := range states {
		index := uint64(i)
		_ = sem.Acquire(ctx, 1)

		go func() {
			defer sem.Release(1)

			t.NoError(sm.SetStates(ctx, index, states[index], ops[index].Fact().Hash()))
		}()
	}

	_ = sem.Acquire(ctx, int64(len(ops)))

	for i := range states {
		sts := states[i]

		for j := range sts {
			st := sts[j]
			merger, found := sm.stvmmap.Value(st.Key())
			t.True(found)

			nst, err := merger.CloseValue()
			t.NoError(err)

			t.Equal(st.Key(), nst.Key())
			t.True(base.IsEqualStateValue(st, nst.Value()))
		}
	}
}

func (t *testDefaultStatesMerger) TestClose() {
	height := base.Height(33)

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
		sts := t.States(height, i%5+1)

		stvs := make([]base.StateMergeValue, len(sts))
		for j := range sts {
			st := sts[j]
			stvs[j] = base.NewBaseStateMergeValue(st.Key(), st.Value(), nil)
		}
		states[i] = stvs
	}

	sm := NewDefaultStatesMerger(height, base.NilGetState, 1<<3)
	defer sm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sem := semaphore.NewWeighted(int64(len(ops)))
	for i := range states {
		index := uint64(i)
		_ = sem.Acquire(ctx, 1)

		go func() {
			defer sem.Release(1)

			t.NoError(sm.SetStates(ctx, index, states[index], ops[index].Fact().Hash()))
		}()
	}

	_ = sem.Acquire(ctx, int64(len(ops)))

	t.NoError(sm.Close())

	t.Equal(0, sm.stvmmap.Len())
}

func TestDefaultStatesMerger(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testDefaultStatesMerger))
}
