package testopsworkers

import (
	"context"
	"fmt"
	"testing"

	"github.com/pkg/errors"
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

func (t *testPhaseStatesMerger) newMerger(
	ctx context.Context,
	getStateValue func(string) (base.StateValue, bool, error),
	workersize int64,
) (
	_ *phaseStatesMerger,
	valuespool util.LockedMap[uint64, []StateMergeValue],
	merge, unmerge statesMergerXMergeFunc,
	done func() ([]uint64, []uint64, func(phaseStatesMergerUpdatedFunc) error, error),
	cancel func(),
) {
	if getStateValue == nil {
		getStateValue = func(string) (base.StateValue, bool, error) { return nil, false, nil }
	}

	sourceMergers, err := util.NewShardedMap[string, StateMerger](1<<9, nil)
	t.NoError(err)

	vp, err := util.NewShardedMap[uint64, []StateMergeValue](1<<9, nil)
	t.NoError(err)

	sm, err := newPhaseStatesMerger(
		ctx,
		sourceMergers.Value,
		vp.Value,
		func(ctx context.Context) (distributedWorker, error) {
			return util.NewBaseJobWorker(ctx, workersize)
		},
		getStateValue,
		nil,
		1<<9,
	)
	t.NoError(err)

	sm.SetLogging(logging.TestNilLogging)

	return sm, vp, sm.Merge, sm.Unmerge, sm.Done, sm.Cancel
}

func (t *testPhaseStatesMerger) TestCancelBeforeDone() {
	t.Run("merge and cancel", func() {
		_, vp, mergef, _, _, cancel := t.newMerger(context.Background(), nil, 1<<9)

		priv := base.NewMPrivatekey()
		vs := t.newMergeValues(priv.Publickey(), 1)

		vp.SetValue(1, vs)

		t.NoError(mergef(1, vs))

		cancel()

		vs = t.newMergeValues(priv.Publickey(), 1)
		vp.SetValue(2, vs)

		err := mergef(2, vs)
		t.Error(err)
		t.True(errors.Is(err, context.Canceled))
	})

	t.Run("cancel and merge", func() {
		_, vp, mergef, _, _, cancel := t.newMerger(context.Background(), nil, 1<<9)

		cancel()

		priv := base.NewMPrivatekey()
		vs := t.newMergeValues(priv.Publickey(), 1)
		vp.SetValue(1, vs)

		err := mergef(1, vs)
		t.Error(err)
		t.True(errors.Is(err, context.Canceled))
	})
}

func (t *testPhaseStatesMerger) TestSimple() {
	t.Run("add", func() {
		_, vp, mergef, _, donef, cancel := t.newMerger(context.Background(), nil, 1<<9)
		defer cancel()

		priv := base.NewMPrivatekey()
		vs := t.newMergeValues(priv.Publickey(), 1)

		vp.SetValue(1, vs)

		t.NoError(mergef(1, vs))
		t.NoError(mergef(1, nil))

		merged, unmerged, updated, err := donef()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal(0, len(unmerged))

		t.NoError(updated(func(key string, merger StateMerger, mergedOps []uint64, maxMergersLength int) error {
			t.Equal(vs[0].Key(), key)
			t.Equal([]uint64{1}, mergedOps)

			mergedValue, err := merger.Close()
			t.NoError(err)

			v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
			t.NoError(err)
			t.True(priv.Publickey().Equal(v.Address))
			t.Equal(int64(1), v.Balance)
			t.Equal(1, maxMergersLength)

			return nil
		}))
	})

	t.Run("adds", func() {
		_, vp, mergef, _, donef, cancel := t.newMerger(context.Background(), nil, 1<<9)
		defer cancel()

		priv := base.NewMPrivatekey()
		vs := t.newMergeValues(priv.Publickey(), 1)
		vp.SetValue(1, vs)

		for i := uint64(1); i < 4; i++ {
			t.NoError(mergef(i, vs))
		}
		t.NoError(mergef(1, nil))

		merged, unmerged, updated, err := donef()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal(0, len(unmerged))

		t.NoError(updated(func(key string, merger StateMerger, mergedOps []uint64, maxMergersLength int) error {
			t.Equal(vs[0].Key(), key)
			t.Equal([]uint64{1, 2, 3}, mergedOps)

			mergedValue, err := merger.Close()
			t.NoError(err)

			v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
			t.NoError(err)
			t.True(priv.Publickey().Equal(v.Address))
			t.Equal(int64(3), v.Balance)
			t.Equal(1, maxMergersLength)

			return nil
		}))
	})

	t.Run("accounts", func() {
		_, vp, mergef, _, donef, cancel := t.newMerger(context.Background(), nil, 1<<9)
		defer cancel()

		ops := map[string]uint64{}

		for i := uint64(1); i < 4; i++ {
			priv := base.NewMPrivatekey()
			vs := t.newMergeValues(priv.Publickey(), 1)
			vp.SetValue(i, vs)

			ops[vs[0].Key()] = i

			t.NoError(mergef(i, vs))
		}
		t.NoError(mergef(1, nil))

		merged, unmerged, updated, err := donef()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal(0, len(unmerged))

		t.NoError(updated(func(key string, merger StateMerger, mergedOps []uint64, maxMergersLength int) error {
			t.Equal([]uint64{ops[key]}, mergedOps)

			mergedValue, err := merger.Close()
			t.NoError(err)

			v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
			t.NoError(err)
			t.Equal(int64(1), v.Balance)
			t.Equal(1, maxMergersLength)

			return nil
		}))
	})

	t.Run("unmerge", func() {
		_, vp, mergef, _, donef, cancel := t.newMerger(context.Background(), nil, 1<<9)
		defer cancel()

		priv := base.NewMPrivatekey()
		vs := t.newMergeValues(priv.Publickey(), 1)

		for i := uint64(1); i < 6; i++ {
			var nvs []StateMergeValue

			switch i {
			case 2, 4:
				nvs = t.newMergeValues(priv.Publickey(), -1)
			default:
				nvs = vs
			}

			vp.SetValue(i, nvs)
			t.NoError(mergef(i, nvs))
		}

		t.NoError(mergef(1, nil))

		merged, unmerged, updated, err := donef()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal([]uint64{2, 4}, unmerged)

		t.NoError(updated(func(key string, merger StateMerger, mergedOps []uint64, maxMergersLength int) error {
			t.Equal([]uint64{1, 3, 5}, mergedOps)

			t.Equal(vs[0].Key(), key)

			mergedValue, err := merger.Close()
			t.NoError(err)

			v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
			t.NoError(err)
			t.True(priv.Publickey().Equal(v.Address))
			t.Equal(int64(3), v.Balance)
			t.Equal(1, maxMergersLength)

			return nil
		}))
	})
}

func (t *testPhaseStatesMerger) TestMergedOPs() {
	pub0, _ := base.ParseMPublickey("03d79ff7a295b52607d7b8c8eb6b6769d1bddc5406021e06ef9f1f0412ac2751fampu")
	pub1, _ := base.ParseMPublickey("03e34507a38ce4f18a3b4f500e0b49f00329e3c83247c9eb0a7700bb44051a5e14mpu")

	t.T().Log("account 0:", AccountStateKey(pub0))
	t.T().Log("account 1:", AccountStateKey(pub1))

	prev0 := NewAccountStateValue(pub0, 2)
	prev1 := NewAccountStateValue(pub1, 2)

	ops := [][]StateMergeValue{
		{
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{ // ignored
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, 2),
		},
		{ // ignored
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{
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

	t.Run("no previously merged ops", func() {
		_, vp, mergef, _, donef, cancel := t.newMerger(context.Background(), getStateValue, 1)
		defer cancel()

		for i := range ops {
			vp.SetValue(uint64(i), ops[i])

			t.NoError(mergef(uint64(i), ops[i]))
		}
		t.NoError(mergef(1, nil))

		merged, unmerged, updated, err := donef()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal(2, len(unmerged))

		t.NoError(updated(func(key string, merger StateMerger, mergedOps []uint64, maxMergersLength int) error {
			t.Equal([]uint64{0, 2, 4}, mergedOps)

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

			t.Equal(1, maxMergersLength)

			return nil
		}))
	})

	t.Run("merged op", func() {
		sm, vp, mergef, _, donef, cancel := t.newMerger(context.Background(), getStateValue, 1)
		defer cancel()

		sm.prevMergedOps = func(key string) ([]uint64, bool) {
			if key == AccountStateKey(pub0) {
				return []uint64{0, 2}, true // 0, 2 already merged, so 0, 2 skipped and 3 ignored
			}

			return nil, false
		}

		for i := range ops {
			vp.SetValue(uint64(i), ops[i])

			t.NoError(mergef(uint64(i), ops[i]))
		}
		t.NoError(mergef(1, nil))

		merged, unmerged, updated, err := donef()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal([]uint64{3}, unmerged)

		t.NoError(updated(func(key string, merger StateMerger, mergedOps []uint64, maxMergersLength int) error {
			if key == AccountStateKey(pub0) {
				t.Equal([]uint64{0, 1, 2, 4}, mergedOps) // including prev merged ops, [0 2]
			} else {
				t.Equal([]uint64{1, 4}, mergedOps)
			}

			mergedValue, err := merger.Close()
			t.NoError(err)

			v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
			t.NoError(err)

			switch {
			case v.Address.Equal(pub0):
				t.Equal(int64(2), v.Balance)
			case v.Address.Equal(pub1):
				t.Equal(int64(2), v.Balance)
			}

			return nil
		}))
	})

	t.Run("unmerged op", func() {
		sm, vp, mergef, _, donef, cancel := t.newMerger(context.Background(), getStateValue, 1)
		defer cancel()

		sm.prevMergedOps = func(key string) ([]uint64, bool) {
			if key == AccountStateKey(pub0) {
				return []uint64{1, 2, 3}, true
			}

			return nil, false
		}

		seq := []uint64{1, 2, 0, 3, 4}
		for i := range seq {
			op := seq[i]

			vp.SetValue(op, ops[op])

			t.NoError(mergef(op, ops[op]))
		}
		t.NoError(mergef(1, nil))

		merged, unmerged, updated, err := donef()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal(0, len(unmerged))

		t.NoError(updated(func(key string, merger StateMerger, mergedOps []uint64, maxMergersLength int) error {
			if key == AccountStateKey(pub0) {
				t.Equal([]uint64{0, 1, 2, 3, 4}, mergedOps)
			} else {
				t.Equal([]uint64{0, 4}, mergedOps)
			}

			mergedValue, err := merger.Close()
			t.NoError(err)

			v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
			t.NoError(err)

			switch {
			case v.Address.Equal(pub0):
				t.Equal(int64(4), v.Balance)
			case v.Address.Equal(pub1):
				t.Equal(int64(2), v.Balance)
			}

			return nil
		}))
	})
}

func (t *testPhaseStatesMerger) testMergesAccounts(
	maxworkersize uint64,
	ops [][]StateMergeValue,
	getStateValue func(string) (base.StateValue, bool, error),
	expectedMerged, expectedUnmerged []uint64,
	checkUpdated phaseStatesMergerUpdatedFunc,
) {
	gen := combin.NewPermutationGenerator(len(ops), len(ops))
	nextseq := func() bool {
		return gen.Next()
	}
	getseq := func() []int {
		return gen.Permutation(nil)
	}
	workersizes := func() (s []int64) {
		for i := int64(1); i < int64(maxworkersize); i++ {
			if i%3 == 0 || i == int64(len(s)-1) {
				s = append(s, i+1)
			}
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
				_, vp, mergef, _, donef, cancel := t.newMerger(context.Background(), getStateValue, workersize)
				defer cancel()

				//sm.eventf = func(name string, swphase, opindex uint64, format string, a ...any) {
				//	t.T().Logf("> name=%s swphase=%d op=%d %s", name, swphase, opindex, fmt.Sprintf(format, a))
				//}

				for i := range seq {
					op := uint64(seq[i])

					vp.SetValue(op, ops[op])
					t.NoError(mergef(op, ops[op]))
				}

				t.NoError(mergef(1, nil))

				merged, unmerged, updated, err := donef()
				t.NoError(err)
				t.Equal(expectedMerged, merged)
				t.Equal(expectedUnmerged, unmerged)

				t.NoError(updated(checkUpdated))
			}
		}

		for _, i := range workersizes() {
			t.Run(fmt.Sprintf("seq: %d %d workersize=%d", n, seq, i), run(i))
		}

		n++
	}
}

func (t *testPhaseStatesMerger) TestMerges2Accounts() {
	pub0, _ := base.ParseMPublickey("03d79ff7a295b52607d7b8c8eb6b6769d1bddc5406021e06ef9f1f0412ac2751fampu")
	pub1, _ := base.ParseMPublickey("03e34507a38ce4f18a3b4f500e0b49f00329e3c83247c9eb0a7700bb44051a5e14mpu")

	t.T().Log("account 0:", AccountStateKey(pub0))
	t.T().Log("account 1:", AccountStateKey(pub1))

	prev0 := NewAccountStateValue(pub0, 2)
	prev1 := NewAccountStateValue(pub1, 2)

	ops := [][]StateMergeValue{
		{
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{ // 1: ignored
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, 2),
		},
		{ // 3: ignored
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{
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

	t.testMergesAccounts(16, ops, getStateValue, nil, []uint64{1, 3}, func(key string, merger StateMerger, mergedOps []uint64, maxMergersLength int) error {
		t.Equal([]uint64{0, 2, 4}, mergedOps)

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

		t.Equal(1, maxMergersLength)

		return nil
	})
}

func (t *testPhaseStatesMerger) TestMerges3Accounts() {
	pub0, _ := base.ParseMPublickey("03d79ff7a295b52607d7b8c8eb6b6769d1bddc5406021e06ef9f1f0412ac2751fampu")
	pub1, _ := base.ParseMPublickey("03e34507a38ce4f18a3b4f500e0b49f00329e3c83247c9eb0a7700bb44051a5e14mpu")
	pub2, _ := base.ParseMPublickey("026a90539fbff8e374c7d125bea1ebc6318649004963de2c54ed42d3abd0b7fa66mpu")

	t.T().Log("account 0:", AccountStateKey(pub0))
	t.T().Log("account 1:", AccountStateKey(pub1))
	t.T().Log("account 2:", AccountStateKey(pub2))

	prev0 := NewAccountStateValue(pub0, 1)
	prev1 := NewAccountStateValue(pub1, 2)
	prev2 := NewAccountStateValue(pub2, 3)

	ops := [][]StateMergeValue{
		{
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{ // 1: ignored
			t.newAccountStateMergeValue(pub1, -2),
			t.newAccountStateMergeValue(pub2, -3),
		},
		{
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, 2),
		},
		{ // 3: ignored
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub2, -3),
		},
		{
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub2, 3),
		},
	}

	getStateValue := func(key string) (base.StateValue, bool, error) {
		switch key {
		case AccountStateKey(pub0):
			return prev0, true, nil
		case AccountStateKey(pub1):
			return prev1, true, nil
		case AccountStateKey(pub2):
			return prev2, true, nil
		default:
			return nil, false, nil
		}
	}

	t.testMergesAccounts(16, ops, getStateValue, nil, []uint64{1, 3}, func(key string, merger StateMerger, mergedOps []uint64, maxMergersLength int) error {
		mergedValue, err := merger.Close()
		t.NoError(err)

		v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
		t.NoError(err)

		switch {
		case v.Address.Equal(pub0):
			t.Equal(int64(3), v.Balance, "account 0")
			t.Equal([]uint64{0, 2, 4, 5}, mergedOps, "account 0")
		case v.Address.Equal(pub1):
			t.Equal(int64(2), v.Balance, "account 1")
			t.Equal([]uint64{0, 2}, mergedOps, "account 1")
		case v.Address.Equal(pub2):
			t.Equal(int64(3), v.Balance, "account 2")
			t.Equal([]uint64{4, 5}, mergedOps, "account 2")
		}

		t.Equal(1, maxMergersLength)

		return nil
	})
}

func TestPhaseStatesMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testPhaseStatesMerger))
}
