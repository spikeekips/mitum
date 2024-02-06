package testopsworkers

import (
	"context"
	"fmt"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"gonum.org/v1/gonum/stat/combin"
)

type testPhaseStatesMerger struct {
	baseTestAccountStateMerger
}

func (t *testPhaseStatesMerger) newMergers(
	ctx context.Context,
	getStateValue func(string) (base.StateValue, bool, error),
	workersize int64,
) (
	_ *phaseStatesMerger,
	merge, unmerge statesMergerXMergeFunc,
	done func() ([]uint64, []uint64, func(phaseStatesMergerUpdatedFunc) error, error),
	cancel func(),
) {
	if getStateValue == nil {
		getStateValue = func(string) (base.StateValue, bool, error) { return nil, false, nil }
	}

	sourceMergers, err := util.NewShardedMap[string, StateMerger](1<<9, nil)
	t.NoError(err)

	valuespool, err := util.NewShardedMap[uint64, []StateMergeValue](1<<9, nil)
	t.NoError(err)

	sm, mergef, unmergef, donef, cancelf, err := newPhaseStatesMerger(
		ctx,
		sourceMergers.Value,
		valuespool,
		func(ctx context.Context) (distributedWorker, error) {
			return util.NewErrgroupWorker(ctx, workersize)
		},
		getStateValue,
		1<<9,
	)
	t.NoError(err)

	sm.SetLogging(logging.TestNilLogging)

	return sm, mergef, unmergef, donef, cancelf
}

func (t *testPhaseStatesMerger) newMergeValues(
	a ...interface{},
) (vs []StateMergeValue) {
	switch {
	case len(a)%2 != 0:
		panic("invalid number of arguments")
	default:
		vs = make([]StateMergeValue, len(a)/2)
	}

	for i := range vs {
		address, ok := a[i*2].(base.Publickey)
		t.True(ok)
		balance, ok := a[(i*2)+1].(int)
		t.True(ok)

		mv, err := NewAccountStateMergeValue(address, int64(balance))
		t.NoError(err)

		vs[i] = mv
	}

	return vs
}

func (t *testPhaseStatesMerger) TestSimple() {
	t.Run("add", func() {
		_, mergef, _, donef, cancel := t.newMergers(context.Background(), nil, 1<<9)
		defer cancel()

		priv := base.NewMPrivatekey()
		vs := t.newMergeValues(priv.Publickey(), 1)

		t.NoError(mergef(1, vs))
		t.NoError(mergef(1, nil))

		merged, unmerged, updated, err := donef()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal(0, len(unmerged))

		t.NoError(updated(func(key string, merger StateMerger, mergedops []uint64) error {
			t.Equal(vs[0].Key(), key)
			t.Equal([]uint64{1}, mergedops)

			mergedValue, err := merger.Close()
			t.NoError(err)

			v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
			t.NoError(err)
			t.True(priv.Publickey().Equal(v.Address))
			t.Equal(int64(1), v.Balance)

			return nil
		}))
	})

	t.Run("adds", func() {
		_, mergef, _, donef, cancel := t.newMergers(context.Background(), nil, 1<<9)
		defer cancel()

		priv := base.NewMPrivatekey()
		vs := t.newMergeValues(priv.Publickey(), 1)

		for i := uint64(1); i < 4; i++ {
			t.NoError(mergef(i, vs))
		}
		t.NoError(mergef(1, nil))

		merged, unmerged, updated, err := donef()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal(0, len(unmerged))

		t.NoError(updated(func(key string, merger StateMerger, mergedops []uint64) error {
			t.Equal(vs[0].Key(), key)
			t.Equal([]uint64{1, 2, 3}, mergedops)

			mergedValue, err := merger.Close()
			t.NoError(err)

			v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
			t.NoError(err)
			t.True(priv.Publickey().Equal(v.Address))
			t.Equal(int64(3), v.Balance)

			return nil
		}))
	})

	t.Run("accounts", func() {
		_, mergef, _, donef, cancel := t.newMergers(context.Background(), nil, 1<<9)
		defer cancel()

		ops := map[string]uint64{}

		for i := uint64(1); i < 4; i++ {
			priv := base.NewMPrivatekey()
			vs := t.newMergeValues(priv.Publickey(), 1)

			ops[vs[0].Key()] = i

			t.NoError(mergef(i, vs))
		}
		t.NoError(mergef(1, nil))

		merged, unmerged, updated, err := donef()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal(0, len(unmerged))

		t.NoError(updated(func(key string, merger StateMerger, mergedops []uint64) error {
			t.Equal([]uint64{ops[key]}, mergedops)

			mergedValue, err := merger.Close()
			t.NoError(err)

			v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
			t.NoError(err)
			t.Equal(int64(1), v.Balance)

			return nil
		}))
	})

	t.Run("unmerge", func() {
		_, mergef, _, donef, cancel := t.newMergers(context.Background(), nil, 1<<9)
		defer cancel()

		priv := base.NewMPrivatekey()
		vs := t.newMergeValues(priv.Publickey(), 1)

		for i := uint64(1); i < 6; i++ {
			switch i {
			case 2, 4:
				t.NoError(mergef(i, t.newMergeValues(priv.Publickey(), -1)))
			default:
				t.NoError(mergef(i, vs))
			}
		}

		t.NoError(mergef(1, nil))

		merged, unmerged, updated, err := donef()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal([]uint64{2, 4}, unmerged)

		t.NoError(updated(func(key string, merger StateMerger, mergedops []uint64) error {
			t.Equal([]uint64{1, 3, 5}, mergedops)

			t.Equal(vs[0].Key(), key)

			mergedValue, err := merger.Close()
			t.NoError(err)

			v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
			t.NoError(err)
			t.True(priv.Publickey().Equal(v.Address))
			t.Equal(int64(3), v.Balance)

			return nil
		}))
	})
}

func (t *testPhaseStatesMerger) TestMerges2Accounts() {
	pub0, _ := base.ParseMPublickey("03d79ff7a295b52607d7b8c8eb6b6769d1bddc5406021e06ef9f1f0412ac2751fampu")
	pub1, _ := base.ParseMPublickey("03e34507a38ce4f18a3b4f500e0b49f00329e3c83247c9eb0a7700bb44051a5e14mpu")

	t.T().Log("account 0:", AccountStateKey(pub0))
	t.T().Log("account 1:", AccountStateKey(pub1))

	prev0 := NewAccountStateValue(pub0, 2)
	prev1 := NewAccountStateValue(pub1, 2)

	ops := map[uint64][]StateMergeValue{
		0: {
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		1: { // ignored
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		2: {
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, 2),
		},
		3: { // ignored
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		4: {
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub1, 2),
		},
	}

	getStateValue := func(key string) (base.StateValue, bool, error) {
		switch key {
		case AccountStateKey(pub0):
			return prev0, true, nil
		case AccountStateKey(pub1):
			return prev1, true, nil
		default:
			return nil, false, nil
		}
	}

	gen := combin.NewPermutationGenerator(len(ops), len(ops))
	nextseq := func() bool {
		return gen.Next()
	}
	getseq := func() []int {
		return gen.Permutation(nil)
	}
	workersizes := func() (s []int64) {
		s = make([]int64, 16)
		for i := range s {
			s[i] = int64(i + 1)
		}

		return s
	}

	//{ // uncomment for replay
	//	var used bool
	//
	//	nextseq = func() bool {
	//		if used {
	//			return false
	//		}
	//
	//		used = true
	//
	//		return true
	//	}
	//
	//	getseq = func() []int {
	//		return []int{0, 1, 2, 3, 4}
	//	}
	//
	//	workersizes = func() (s []int64) {
	//		return []int64{3}
	//	}
	//}

	var n int

	for nextseq() {
		seq := getseq()

		run := func(workersize int64) func() {
			return func() {
				_, mergef, _, donef, cancel := t.newMergers(context.Background(), getStateValue, workersize)
				defer cancel()

				//sm.eventf = func(name string, swphase, opindex uint64, format string, a ...any) {
				//	t.T().Logf("> name=%s swphase=%d op=%d %s", name, swphase, opindex, fmt.Sprintf(format, a))
				//}

				for i := range seq {
					op := uint64(seq[i])

					t.NoError(mergef(op, ops[op]))
				}

				t.NoError(mergef(1, nil))

				merged, unmerged, updated, err := donef()
				t.NoError(err)
				t.Equal(0, len(merged))
				t.Equal([]uint64{1, 3}, unmerged)

				t.NoError(updated(func(key string, merger StateMerger, mergedops []uint64) error {
					t.Equal([]uint64{0, 2, 4}, mergedops)

					mergedValue, err := merger.Close()
					t.NoError(err)

					v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
					t.NoError(err)

					switch {
					case v.Address.Equal(pub0):
						t.Equal(int64(3), v.Balance)
					case v.Address.Equal(pub1):
						t.Equal(int64(4), v.Balance)
					}

					return nil
				}))
			}
		}

		for _, i := range workersizes() {
			t.Run(fmt.Sprintf("seq: %d %d workersize=%d", n, seq, i), run(i))
		}

		n++
	}
}

func TestPhaseStatesMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testPhaseStatesMerger))
}
