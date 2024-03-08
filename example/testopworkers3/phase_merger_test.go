package testopsworkers

import (
	"cmp"
	"context"
	"fmt"
	"slices"
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
) (*phaseStatesMerger, util.LockedMap[uint64, []StateMergeValue]) {
	if getStateValue == nil {
		getStateValue = func(string) (base.StateValue, bool, error) { return nil, false, nil }
	}

	mergers, err := util.NewShardedMap[string, StateMerger](1<<9, nil)
	t.NoError(err)

	vp, err := util.NewShardedMap[uint64, []StateMergeValue](1<<9, nil)
	t.NoError(err)

	sm, err := newPhaseStatesMerger(
		ctx,
		mergers.Value,
		vp.Value,
		getStateValue,
		func(ctx context.Context) (distributedWorker, error) {
			return util.NewErrgroupWorker(ctx, workersize)
		},
		1<<9,
	)
	t.NoError(err)

	sm.SetLogging(logging.TestNilLogging)

	return sm, vp
}

func (t *testPhaseStatesMerger) TestCancelBeforeDone() {
	t.Run("merge and cancel", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)

		_ = t.randomMergeValuesVP(vp, 1, 1)
		_ = t.randomMergeValuesVP(vp, 2, 1)

		t.NoError(psm.Merge(1))

		psm.Cancel()

		err := psm.Merge(2)
		t.Error(err)
		t.True(errors.Is(err, context.Canceled))
	})

	t.Run("merge, cancel and done", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)

		_ = t.randomMergeValuesVP(vp, 1, 1)
		_ = t.randomMergeValuesVP(vp, 2, 1)

		t.NoError(psm.Merge(1))

		psm.Cancel()

		t.NoError(psm.MergeDone())

		_, _, _, err := psm.Done()
		t.Error(err)
		t.True(errors.Is(err, context.Canceled))
	})

	t.Run("cancel and merge", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)

		psm.Cancel()

		_ = t.randomMergeValuesVP(vp, 1, 1)

		err := psm.Merge(1)
		t.Error(err)
		t.True(errors.Is(err, context.Canceled))
	})
}

func (t *testPhaseStatesMerger) TestInitialMerge() {
	t.Run("ok", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)
		defer psm.Cancel()

		_ = t.randomMergeValuesVP(vp, 1, 1)
		t.NoError(psm.Merge(1))
	})

	t.Run("values not found", func() {
		psm, _ := t.newMerger(context.Background(), nil, 1<<9)
		defer psm.Cancel()

		t.ErrorIs(psm.Merge(1), util.ErrNotFound)
	})

	t.Run("initial op status", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)
		defer psm.Cancel()

		_ = t.randomMergeValuesVP(vp, 1, 1)
		t.NoError(psm.Merge(1))

		v, found := psm.opStatus.Value(1)
		t.True(found)
		t.Equal(mergeFlag, v[0])
	})

	t.Run("known initial op status", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)
		defer psm.Cancel()

		psm.opStatus.SetValue(1, [3]interface{}{mergeFlag, nil, nil})

		_ = t.randomMergeValuesVP(vp, 1, 1)

		err := psm.Merge(1)
		t.ErrorIs(err, util.ErrFound)
		t.ErrorContains(err, "opindex already merged initiallay")
	})
}

func (t *testPhaseStatesMerger) TestInitialUnmerge() {
	t.Run("ok", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)
		defer psm.Cancel()

		_ = t.randomMergeValuesVP(vp, 1, 1)
		t.NoError(psm.Unmerge(1))
	})

	t.Run("values not found", func() {
		psm, _ := t.newMerger(context.Background(), nil, 1<<9)
		defer psm.Cancel()

		t.ErrorIs(psm.Unmerge(1), util.ErrNotFound)
	})

	t.Run("initial op status", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)
		defer psm.Cancel()

		_ = t.randomMergeValuesVP(vp, 1, 1)
		t.NoError(psm.Unmerge(1))

		v, found := psm.opStatus.Value(1)
		t.True(found)
		t.Equal(unmergeFlag, v[0])
	})

	t.Run("known initial op status", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)
		defer psm.Cancel()

		_ = t.randomMergeValuesVP(vp, 1, 1)

		t.NoError(psm.Unmerge(1))

		err := psm.Unmerge(1)
		t.ErrorIs(err, util.ErrFound)
		t.ErrorContains(err, "opindex already unmerged initiallay")
	})
}

func (t *testPhaseStatesMerger) TestUnmerge() {
	t.Run("merger not found", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)
		defer psm.Cancel()

		_ = t.randomMergeValuesVP(vp, 1, 1)
		t.NoError(psm.Unmerge(1))
		t.NoError(psm.MergeDone())

		merged, unmerged, updatef, err := psm.Done()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal(0, len(unmerged))

		var valuesupdated, reasonupdated bool

		t.NoError(updatef(
			func(string, StateMerger, []uint64, []uint64, uint64) error {
				valuesupdated = true

				return nil
			},
			func(opindex uint64, reason string) error {
				reasonupdated = true

				return nil
			},
		))

		t.False(valuesupdated)
		t.False(reasonupdated)
	})
}

func (t *testPhaseStatesMerger) TestDone() {
	t.Run("ok", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)
		defer psm.Cancel()

		vs := t.randomMergeValuesVP(vp, 1, 1)

		t.NoError(psm.Merge(1))
		t.NoError(psm.MergeDone())

		merged, unmerged, updatef, err := psm.Done()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal(0, len(unmerged))

		var reasonupdated bool

		t.NoError(updatef(
			func(stateKey string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
				t.Equal(uint64(1), maxUpdatedValues)

				av := vs[0].(AccountStateMergeValue)

				t.Equal(AccountStateKey(av.Address), stateKey)
				t.Equal([]uint64{1}, mergedOps)
				t.Empty(unmergedOps)

				v, err := merger.Close()
				t.NoError(err)

				bv := v.(AccountStateValue)

				t.True(av.Address.Equal(bv.Address))
				t.Equal(av.Balance, bv.Balance)

				return nil
			},
			func(opindex uint64, reason string) error {
				reasonupdated = true

				return nil
			},
		))

		t.False(reasonupdated)
	})

	t.Run("empty ops", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)
		defer psm.Cancel()

		_ = t.randomMergeValuesVP(vp, 1, 1)

		t.NoError(psm.MergeDone())

		merged, unmerged, updatef, err := psm.Done()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal(0, len(unmerged))

		var valuesupdated, reasonupdated bool

		t.NoError(updatef(
			func(stateKey string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
				valuesupdated = true

				return nil
			},
			func(opindex uint64, reason string) error {
				reasonupdated = true

				return nil
			},
		))

		t.False(valuesupdated)
		t.False(reasonupdated)
	})
}

func (t *testPhaseStatesMerger) TestSimple() {
	t.Run("add", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)
		defer psm.Cancel()

		priv := base.NewMPrivatekey()
		vs := t.newMergeValuesVP(vp, 1, priv.Publickey(), 1)

		t.NoError(psm.Merge(1))
		t.NoError(psm.MergeDone())

		merged, unmerged, updatef, err := psm.Done()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal(0, len(unmerged))

		t.NoError(updatef(
			func(stateKey string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
				t.Equal(uint64(1), maxUpdatedValues)

				t.Equal(vs[0].Key(), stateKey)
				t.Equal([]uint64{1}, mergedOps)

				mergedValue, err := merger.Close()
				t.NoError(err)

				v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
				t.NoError(err)
				t.True(priv.Publickey().Equal(v.Address))
				t.Equal(int64(1), v.Balance)

				return nil
			},
			func(opindex uint64, reason string) error {
				return nil
			},
		))
	})

	t.Run("adds", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)
		defer psm.Cancel()

		priv := base.NewMPrivatekey()

		var vs []StateMergeValue

		for i := uint64(1); i < 4; i++ {
			j := t.newMergeValuesVP(vp, i, priv.Publickey(), 1)

			vs = append(vs, j...)

			t.NoError(psm.Merge(i))
		}
		t.NoError(psm.MergeDone())

		merged, unmerged, updatef, err := psm.Done()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal(0, len(unmerged))

		t.NoError(updatef(
			func(stateKey string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
				t.Equal(uint64(1), maxUpdatedValues)

				t.Equal(vs[0].Key(), stateKey)
				t.Equal([]uint64{1, 2, 3}, mergedOps)
				t.Empty(unmergedOps)

				mergedValue, err := merger.Close()
				t.NoError(err)

				v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
				t.NoError(err)
				t.True(priv.Publickey().Equal(v.Address))
				t.Equal(int64(3), v.Balance)

				return nil
			},
			func(opindex uint64, reason string) error {
				return nil
			},
		))
	})

	t.Run("accounts", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)
		defer psm.Cancel()

		ops := map[string]uint64{}

		for i := uint64(1); i < 4; i++ {
			priv := base.NewMPrivatekey()
			vs := t.newMergeValuesVP(vp, i, priv.Publickey(), 1)

			ops[vs[0].Key()] = i

			t.NoError(psm.Merge(i))
		}
		t.NoError(psm.MergeDone())

		merged, unmerged, updatef, err := psm.Done()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal(0, len(unmerged))

		t.NoError(updatef(
			func(stateKey string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
				t.Equal(uint64(3), maxUpdatedValues)

				t.Equal([]uint64{ops[stateKey]}, mergedOps)
				t.Empty(unmergedOps)

				mergedValue, err := merger.Close()
				t.NoError(err)

				v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
				t.NoError(err)
				t.Equal(int64(1), v.Balance)

				return nil
			},
			func(opindex uint64, reason string) error {
				return nil
			},
		))
	})

	t.Run("unmerge", func() {
		psm, vp := t.newMerger(context.Background(), nil, 1<<9)
		defer psm.Cancel()

		priv := base.NewMPrivatekey()

		var vs []StateMergeValue

		for i := uint64(1); i < 6; i++ {
			var j []StateMergeValue

			switch i {
			case 2, 4:
				j = t.newMergeValuesVP(vp, i, priv.Publickey(), -1)
			default:
				j = t.newMergeValuesVP(vp, i, priv.Publickey(), 1)
			}

			vs = append(vs, j...)

			t.NoError(psm.Merge(i))
		}

		t.NoError(psm.MergeDone())

		merged, unmerged, updatef, err := psm.Done()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal([]uint64{2, 4}, unmerged)

		t.NoError(updatef(
			func(key string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
				t.Equal(uint64(1), maxUpdatedValues)

				t.Equal([]uint64{1, 3, 5}, mergedOps)
				t.Equal([]uint64{2, 4}, unmergedOps)

				t.Equal(vs[0].Key(), key)

				mergedValue, err := merger.Close()
				t.NoError(err)

				v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
				t.NoError(err)
				t.True(priv.Publickey().Equal(v.Address))
				t.Equal(int64(3), v.Balance)

				return nil
			},
			func(opindex uint64, reason string) error {
				return nil
			},
		))
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
		psm, vp := t.newMerger(context.Background(), getStateValue, 1)
		defer psm.Cancel()

		for i := range ops {
			vp.SetValue(uint64(i), ops[i])

			t.NoError(psm.Merge(uint64(i)))
		}
		t.NoError(psm.MergeDone())

		merged, unmerged, updatef, err := psm.Done()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal(2, len(unmerged))
		t.Equal([]uint64{1, 3}, unmerged)

		t.NoError(updatef(
			func(key string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
				t.Equal(uint64(2), maxUpdatedValues)

				t.Equal([]uint64{0, 2, 4}, mergedOps)
				t.Equal([]uint64{1, 3}, unmergedOps)

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
			},
			func(opindex uint64, reason string) error {
				switch opindex {
				case 1, 3:
				default:
					t.Fail("not 1, 3")
				}
				t.Contains(reason, "previous issuer found")

				return nil
			},
		))
	})

	t.Run("merged op", func() {
		psm, vp := t.newMerger(context.Background(), getStateValue, 1)
		defer psm.Cancel()

		for i := range ops {
			vp.SetValue(uint64(i), ops[i])

			t.NoError(psm.Merge(uint64(i)))
		}
		t.NoError(psm.MergeDone())

		merged, unmerged, updatef, err := psm.Done()
		t.NoError(err)
		t.Equal(0, len(merged))
		t.Equal([]uint64{1, 3}, unmerged)

		t.NoError(updatef(
			func(key string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
				t.Equal(uint64(2), maxUpdatedValues)

				t.Equal([]uint64{0, 2, 4}, mergedOps)
				t.Equal([]uint64{1, 3}, unmergedOps)

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
			},
			func(opindex uint64, reason string) error {
				switch opindex {
				case 1, 3:
				default:
					t.Fail("not 1, 3")
				}
				t.Contains(reason, "previous issuer found")

				return nil
			},
		))
	})
}

func (t *testPhaseStatesMerger) testMergesAccounts(
	maxworkersize uint64,
	ops [][]StateMergeValue,
	getStateValue func(string) (base.StateValue, bool, error),
	expectedMerged, expectedUnmerged []uint64,
	updatef phaseStatesMergerDoneUpdatedValuesFunc,
	reasonf phaseStatesMergerDoneUpdatedReasonsFunc,
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
	//		return []int{2, 1, 4, 3, 0}
	//	}
	//
	//	workersizes = func() (s []int64) {
	//		return []int64{1}
	//	}
	//}

	var n int

	for nextseq() {
		seq := getseq()

		run := func(workersize int64) func() {
			return func() {
				psm, vp := t.newMerger(context.Background(), getStateValue, workersize)
				defer psm.Cancel()

				psm.SetLogging(logging.TestLogging)

				for i := range ops {
					vp.SetValue(uint64(i), ops[i])
				}

				for i := range seq {
					op := uint64(seq[i])

					t.NoError(psm.Merge(op))
				}

				t.NoError(psm.MergeDone())

				merged, unmerged, u, err := psm.Done()
				t.NoError(err)
				t.Equal(expectedMerged, merged)
				t.Equal(expectedUnmerged, unmerged)

				t.NoError(u(updatef, reasonf))
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

	t.testMergesAccounts(16, ops, getStateValue, nil, []uint64{1, 3},
		func(key string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
			mergedValue, err := merger.Close()
			t.NoError(err)

			v, err := util.AssertInterfaceValue[AccountStateValue](mergedValue)
			t.NoError(err)

			t.Equal([]uint64{0, 2, 4}, mergedOps)
			t.Equal([]uint64{1, 3}, unmergedOps)

			switch {
			case v.Address.Equal(pub0):
				t.Equal(int64(3), v.Balance)
			case v.Address.Equal(pub1):
				t.Equal(int64(4), v.Balance)
			}

			return nil
		},
		func(opindex uint64, reason string) error {
			switch opindex {
			case 1, 3:
			default:
				t.Fail("not 1, 3")
			}
			t.Contains(reason, "previous issuer found")

			return nil
		},
	)
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

	t.testMergesAccounts(16, ops, getStateValue, nil, []uint64{1, 3},
		func(key string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
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

			return nil
		},
		func(opindex uint64, reason string) error {
			switch opindex {
			case 1, 3:
			default:
				t.Fail("not 1, 3")
			}
			t.Contains(reason, "previous issuer found")

			return nil
		},
	)
}

func (t *testPhaseStatesMerger) TestMutualSubStates() {
	workers := make([]string, 3)
	for i := range workers {
		workers[i] = fmt.Sprintf("p%d", i)
	}

	pub0, _ := base.ParseMPublickey("03d79ff7a295b52607d7b8c8eb6b6769d1bddc5406021e06ef9f1f0412ac2751fampu")
	pub1, _ := base.ParseMPublickey("03e34507a38ce4f18a3b4f500e0b49f00329e3c83247c9eb0a7700bb44051a5e14mpu")
	pub2, _ := base.ParseMPublickey("02143a5ee64d8cb207ec44fc862d29e307ebb20a39fecaac3352c9acf039a791c2mpu")

	t.T().Log("state 0:", AccountStateKey(pub0))
	t.T().Log("state 1:", AccountStateKey(pub1))
	t.T().Log("state 2:", AccountStateKey(pub2))

	stv0 := NewAccountStateValue(pub0, 2)
	stv1 := NewAccountStateValue(pub1, 2)
	stv2 := NewAccountStateValue(pub2, 2)

	getStateValue := func(key string) (base.StateValue, bool, error) {
		switch key {
		case AccountStateKey(pub0):
			return stv0, true, nil
		case AccountStateKey(pub1):
			return stv1, true, nil
		case AccountStateKey(pub2):
			return stv2, true, nil
		default:
			return nil, false, nil
		}
	}

	ops := [][]StateMergeValue{
		{
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{ // ignored: mutual sub; worker0, worker1 should merge
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, 2),
		},
		{ // ignored: mutual sub; worker0, worker1 should merge
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub1, 2),
		},
		{
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub2, -1),
		},
		{ // ignored
			t.newAccountStateMergeValue(pub1, -1),
			t.newAccountStateMergeValue(pub2, 2),
		},
		{ // ignored
			t.newAccountStateMergeValue(pub1, 1),
			t.newAccountStateMergeValue(pub2, -1),
		},
	}

	stateOwners := map[string][]string{
		AccountStateKey(pub0): {workers[0], workers[1]},
		AccountStateKey(pub1): {workers[0], workers[1]},
		AccountStateKey(pub2): {workers[2]},
	}
	filterValues := func(worker string, opindex uint64) []StateMergeValue {
		return util.FilterSlice(ops[opindex], func(value StateMergeValue) bool {
			return slices.Index(stateOwners[value.Key()], worker) >= 0
		})
	}

	expectedMergedOps := []uint64{0, 2, 4, 5}
	var expectedUnmergedOps []uint64
	for i := range ops {
		opindex := uint64(i)
		if slices.Index(expectedMergedOps, opindex) < 0 {
			expectedUnmergedOps = append(expectedUnmergedOps, opindex)
		}
	}

	t.T().Logf("expected: merged_ops=%v unmerged_ops=%v", expectedMergedOps, expectedUnmergedOps)

	mergers := make([]util.LockedMap[string, StateMerger], len(workers))
	vps := make([]util.LockedMap[uint64, []StateMergeValue], len(workers))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := range workers {
		mergers[i], _ = util.NewShardedMap[string, StateMerger](1<<9, nil)
		vps[i], _ = util.NewShardedMap[uint64, []StateMergeValue](1<<9, nil)
	}

	for i := range ops {
		opindex := uint64(i)

		for j := range vps {
			worker := workers[j]
			vs := filterValues(worker, opindex)
			if len(vs) < 1 {
				continue
			}

			keys := make([]string, len(vs))
			for k := range vs {
				keys[k] = vs[k].Key()
			}

			t.T().Logf("values: opindex=%d worker=%q keys=%v", opindex, worker, keys)

			vps[j].SetValue(opindex, vs)
		}
	}

	newWorker := func(
		ms util.LockedMap[string, StateMerger],
		vp util.LockedMap[uint64, []StateMergeValue],
	) *phaseStatesMerger {
		psm, err := newPhaseStatesMerger(
			ctx,
			ms.Value,
			vp.Value,
			getStateValue,
			func(ctx context.Context) (distributedWorker, error) {
				return util.NewErrgroupWorker(ctx, 1<<9)
			},
			1<<9,
		)
		t.NoError(err)

		psm.SetLogging(logging.TestNilLogging)

		return psm
	}

	psms := map[ /* phase */ int][]*phaseStatesMerger{}
	newPhase := func(phase int) {
		psms[phase] = make([]*phaseStatesMerger, len(workers))

		for i := range workers {
			psm := newWorker(mergers[i], vps[i])
			psm.SetWorkerName(workers[i])

			psms[phase][i] = psm
		}
	}

	workerMergedOps := map[ /* phase */ int]map[ /* worker */ string][]uint64{} /* opindex */
	psmMergeOps := func(worker string, phase int, mergeOps, unmergeOps []uint64) error {
		psm := psms[phase][slices.Index(workers, worker)]

		if err := util.TraverseSliceV1(mergeOps, func(_ int, opindex uint64) (bool, error) {
			if len(filterValues(worker, opindex)) < 1 {
				return true, nil
			}

			prev, found := workerMergedOps[phase-1][worker]
			switch {
			case !found:
			case slices.Index(prev, opindex) >= 0:
				return true, nil
			}

			t.T().Logf("\tmerge: worker=%q prev=%v merge_ops=%v op=%d", worker, prev, mergeOps, opindex)

			return true, psm.Merge(opindex)
		}); err != nil {
			return err
		}

		if err := util.TraverseSliceV1(unmergeOps, func(_ int, opindex uint64) (bool, error) {
			prev, found := workerMergedOps[phase-1][worker]

			switch {
			case !found:
			case slices.Index(prev, opindex) < 0:
				return true, nil
			}

			t.T().Logf("\tunmerge: worker=%q prev=%v merge_ops=%v op=%d", worker, prev, mergeOps, opindex)
			return true, psm.Unmerge(opindex)
		}); err != nil {
			return err
		}

		return nil
	}
	psmMergeDone := func(worker string, phase int) error {
		psm := psms[phase][slices.Index(workers, worker)]

		return psm.MergeDone()
	}

	updateWorkerMergedOps := func(phase int, worker string, merged, unmerged []uint64) []uint64 {
		if _, found := workerMergedOps[phase]; !found {
			workerMergedOps[phase] = map[string][]uint64{}
		}

		var prev []uint64
		switch i, found := workerMergedOps[phase][worker]; {
		case !found:
			prev = workerMergedOps[phase-1][worker]
		default:
			prev = i
		}

		add, done := util.UniqCompactAppendSlice[uint64](len(prev) + len(merged))
		util.TraverseSliceV1(merged, func(_ int, opindex uint64) (bool, error) {
			if slices.Index(prev, opindex) < 0 {
				add(opindex)
			}

			return true, nil
		})

		prev = slices.DeleteFunc(prev, func(i uint64) bool {
			return slices.Index(unmerged, i) >= 0
		})

		for i := range prev {
			add(prev[i])
		}

		d := done()
		slices.Sort(d)
		workerMergedOps[phase][worker] = d

		return workerMergedOps[phase][worker]
	}

	mergedOps := map[ /* phase */ int]map[ /* worker */ string][]uint64{}   /* ops */
	unmergedOps := map[ /* phase */ int]map[ /* worker */ string][]uint64{} /* ops */

	runPhase := func(phase int, worker string) bool {
		workerindex := slices.Index(workers, worker)

		others := make([]string, len(workers)-1)
		for i := 0; i < len(workers)-1; i++ {
			others[i] = workers[(workerindex+i+1)%len(workers)]
		}

		t.T().Logf("> phase %d: worker=%q", phase, worker)
		newPhase(phase)

		psm := psms[phase][workerindex]
		//if phase == 1 && worker == "p1" {
		//	psm.SetLogging(logging.TestLogging)
		//}

		addPrevMerged, donePrevMerged := util.UniqCompactAppendSlice[uint64](len(ops))
		addPrevUnmerged, donePrevUnmerged := util.UniqCompactAppendSlice[uint64](len(ops))

		switch {
		case phase == 0:
			util.TraverseSliceV1(mergedOps[phase-1][worker], func(_ int, opindex uint64) (bool, error) {
				addPrevMerged(opindex)

				return true, nil
			})
		default:
			for i := range others {
				util.TraverseSliceV1(mergedOps[phase-1][others[i]], func(_ int, opindex uint64) (bool, error) {
					addPrevMerged(opindex)

					return true, nil
				})

				util.TraverseSliceV1(unmergedOps[phase-1][others[i]], func(_ int, opindex uint64) (bool, error) {
					addPrevUnmerged(opindex)

					return true, nil
				})
			}
		}

		prevMerged := donePrevMerged()
		prevUnmerged := donePrevUnmerged()
		slices.Sort(prevMerged)
		slices.Sort(prevUnmerged)

		t.T().Logf("> merge: %v -> %q merge_ops=%v unmerge_ops=%v", others, worker, prevMerged, prevUnmerged)

		t.NoError(psmMergeOps(worker, phase, nil, prevUnmerged))
		t.NoError(psmMergeOps(worker, phase, prevMerged, nil))
		t.NoError(psmMergeDone(worker, phase))

		merged, unmerged, updatef, err := psm.Done()
		t.NoError(err, worker)

		if phase == 0 {
			addRealMerged, doneRealMerged := util.CompactAppendSlice[uint64](len(ops))
			for i := range ops {
				opindex := uint64(i)
				if slices.Index(unmerged, opindex) < 0 {
					addRealMerged(opindex)
				}
			}

			merged = doneRealMerged()
		}

		if _, found := mergedOps[phase]; !found {
			mergedOps[phase] = map[string][]uint64{}
		}
		if _, found := unmergedOps[phase]; !found {
			unmergedOps[phase] = map[string][]uint64{}
		}

		mergedOps[phase][worker] = merged
		unmergedOps[phase][worker] = unmerged

		t.T().Logf("done: worker=%q merged_ops=%v unmerged_ops=%v", worker, merged, unmerged)

		var updated bool
		var updatedOps []uint64

		t.NoError(updatef(
			func(stateKey string, merger StateMerger, merged, unmerged []uint64, maxUpdatedValues uint64) error {
				updated = true

				t.T().Logf(
					"\tvalue: worker=%q state_key=%q merged_ops=%v unmerged_ops=%v max=%d",
					worker, stateKey, merged, unmerged, maxUpdatedValues,
				)

				if merger != nil {
					mergers[workerindex].SetValue(stateKey, merger)
				}

				updatedOps = updateWorkerMergedOps(phase, worker, merged, unmerged)

				return nil
			},
			func(opindex uint64, reason string) error {
				t.T().Logf("\treason: worker=%q opindex=%d reason=%q", worker, opindex, reason)

				return nil
			},
		))

		if !updated {
			updatedOps = updateWorkerMergedOps(phase, worker, nil, nil)
		}

		t.T().Logf("< done: worker=%q ops=%v updated=%v", worker, updatedOps, updated)

		return updated
	}

	mergedOps[-1] = map[string][]uint64{}
	unmergedOps[-1] = map[string][]uint64{}

	for i := range workers {
		worker := workers[i]
		addOps, doneOps := util.CompactAppendSlice[uint64](len(ops))

		for j := range ops {
			opindex := uint64(j)

			if len(filterValues(worker, opindex)) > 0 {
				addOps(opindex)
			}
		}

		mergedOps[-1][worker] = doneOps()
		unmergedOps[-1][worker] = nil
	}

	var phase int
	for {
		var updated int
		for i := range workers {
			if runPhase(phase, workers[i]) {
				updated++
			}
		}

		if updated < 1 {
			break
		}

		phase++
	}

	for i := range workers {
		worker := workers[i]
		addExpected, doneExpected := util.SortCompactAppendSlice[uint64](len(expectedMergedOps), cmp.Less[uint64])

		util.TraverseSliceV1(expectedMergedOps, func(_ int, opindex uint64) (bool, error) {
			if len(filterValues(worker, opindex)) > 0 {
				addExpected(opindex)
			}

			return true, nil
		})

		t.Equal(doneExpected(), workerMergedOps[phase][worker], worker)
	}
}

func TestPhaseStatesMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testPhaseStatesMerger))
}
