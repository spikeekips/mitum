package main

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type baseTestMergeOp struct {
	baseTestAccountStateMerger
}

func (t *baseTestMergeOp) closeMergers(mergers util.LockedMap[string, StateMerger]) (map[string]int64, error) {
	t.T().Helper()

	m := map[string]int64{}

	var gerr error

	mergers.Traverse(func(stateKey string, merger StateMerger) bool {
		st, err := merger.Close()
		if err != nil {
			gerr = err
		}

		if st != nil {
			m[stateKey] = st.(AccountStateValue).Balance
		}

		return true
	})

	return m, gerr
}

func (t *baseTestMergeOp) getFuncs() (
	_ util.LockedMap[uint64, []StateMergeValue],
	_ util.LockedMap[string, StateMerger],
	_ util.LockedMap[string, base.State],
	getMerger func(StateMergeValue) (StateMerger, error),
	getValues func(uint64) ([]StateMergeValue, error),
) {
	t.T().Helper()

	vp, err := util.NewShardedMap[uint64, []StateMergeValue](1<<9, nil)
	t.NoError(err)

	mergers, err := util.NewShardedMap[string, StateMerger](1<<9, nil)
	t.NoError(err)

	states, err := util.NewShardedMap[string, base.State](1<<9, nil)
	t.NoError(err)

	return vp, mergers, states, func(value StateMergeValue) (StateMerger, error) {
			var merger StateMerger

			err := mergers.GetOrCreate(
				value.Key(),
				func(i StateMerger, _ bool) error {
					merger = i

					return nil
				},
				func() (StateMerger, error) {
					var stv base.StateValue

					if i, found := states.Value(value.Key()); found {
						stv = i.Value()
					}

					return value.Merger(stv)
				},
			)

			return merger, err
		},
		func(opindex uint64) ([]StateMergeValue, error) {
			values, found := vp.Value(opindex)
			if !found {
				return nil, util.ErrNotFound.Errorf("state merge values, %d", opindex)
			}

			return values, nil
		}
}

type testMergeOp struct {
	baseTestMergeOp
}

func (t *testMergeOp) TestMergeSingleValue() {
	t.Run("ok", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var opindex uint64 = 0

		vs := t.randomMergeValuesVP(vp, opindex, 1)

		isMerged, nexts, reason, err := mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.Empty(nexts)
		t.Empty(reason)

		merged, err := t.closeMergers(mergers)
		t.NoError(err)
		t.Equal(1, len(merged))
		t.Equal(int64(1), merged[vs[0].Key()])
	})

	t.Run("merge same op", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var opindex uint64 = 0

		vs := t.randomMergeValuesVP(vp, opindex, 1)

		isMerged, nexts, reason, err := mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.Empty(nexts)
		t.Empty(reason)

		isMerged, nexts, reason, err = mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.Empty(nexts)
		t.Empty(reason)

		merged, err := t.closeMergers(mergers)
		t.NoError(err)
		t.Equal(1, len(merged))
		t.Equal(int64(2), merged[vs[0].Key()])
	})

	t.Run("merge same -op", func() {
		vp, mergers, states, getMerger, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pub := base.NewDummyPublickey(util.UUID().String())
		prevst := t.newState(pub, 1)
		states.SetValue(prevst.Key(), prevst)

		var opindex uint64 = 0

		vs := t.newMergeValuesVP(vp, opindex, pub, -1)

		isMerged, nexts, reason, err := mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.Empty(nexts)
		t.Empty(reason)

		isMerged, nexts, reason, err = mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.False(isMerged)
		t.Empty(nexts)
		t.Empty(reason)

		merged, err := t.closeMergers(mergers)
		t.NoError(err)
		t.Equal(1, len(merged))
		t.Equal(int64(0), merged[vs[0].Key()])
	})

	t.Run("getMerger error", func() {
		vp, mergers, _, _, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vs := t.randomMergeValuesVP(vp, 1, 1)

		getMerger := func(value StateMergeValue) (StateMerger, error) {
			return nil, errors.Errorf("showme")
		}

		isMerged, nexts, reason, err := mergeOp(ctx, 1, vs, getMerger)
		t.Error(err)
		t.False(isMerged)
		t.Empty(nexts)
		t.Empty(reason)
		t.ErrorContains(err, "showme")

		merged, err := t.closeMergers(mergers)
		t.NoError(err)
		t.Equal(0, len(merged))
	})

	t.Run("op reason", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var opindex uint64 = 2

		vs := t.randomMergeValuesVP(vp, opindex, -1)

		isMerged, nexts, reason, err := mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.False(isMerged)
		t.NotEmpty(reason)
		t.Empty(nexts)

		merged, err := t.closeMergers(mergers)
		t.NoError(err)
		t.Equal(0, len(merged))
	})

	t.Run("next unmerge op reason", func() {
		vp, mergers, states, getMerger, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var opindex3, opindex4 uint64 = 3, 4

		pub := base.NewDummyPublickey(util.UUID().String())
		prevst := t.newState(pub, 1)
		states.SetValue(prevst.Key(), prevst)

		vs3 := t.newMergeValuesVP(vp, opindex3, pub, -1)
		vs4 := t.newMergeValuesVP(vp, opindex4, pub, -1)

		isMerged, nexts, reasons, err := mergeOp(ctx, opindex4, vs4, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.Empty(reasons)
		t.Empty(nexts)

		isMerged, nexts, reasons, err = mergeOp(ctx, opindex3, vs3, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.NotEmpty(reasons)

		reason, found := reasons[opindex4]
		t.True(found)
		t.Equal("previous issuer found", reason)
		t.Equal([]uint64{opindex4}, nexts)

		merged, err := t.closeMergers(mergers)
		t.Error(err)
		t.Equal(1, len(merged))
		t.Equal(int64(-1), merged[vs3[0].Key()])
	})
}

func (t *testMergeOp) TestUnmergeSingleValue() {
	t.Run("ok", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var opindex uint64 = 0

		vs := t.randomMergeValuesVP(vp, opindex, 1)

		isUnmerged, nexts, err := unmergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.True(isUnmerged)
		t.Empty(nexts)

		merged, err := t.closeMergers(mergers)
		t.Error(err)
		t.Equal(1, len(merged))
		t.Equal(int64(-1), merged[vs[0].Key()])
	})

	t.Run("getMerger error", func() {
		vp, mergers, _, _, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vs := t.randomMergeValuesVP(vp, 1, 1)

		getMerger := func(value StateMergeValue) (StateMerger, error) {
			return nil, errors.Errorf("showme")
		}

		isUnmerged, _, err := unmergeOp(ctx, 1, vs, getMerger)
		t.Error(err)
		t.False(isUnmerged)
		t.ErrorContains(err, "showme")

		merged, err := t.closeMergers(mergers)
		t.NoError(err)
		t.Equal(0, len(merged))
	})

	t.Run("next merge op", func() {
		vp, mergers, states, getMerger, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var opindex3, opindex4 uint64 = 3, 4

		pub := base.NewDummyPublickey(util.UUID().String())
		prevst := t.newState(pub, 1)
		states.SetValue(prevst.Key(), prevst)

		vs3 := t.newMergeValuesVP(vp, opindex3, pub, -1)
		vs4 := t.newMergeValuesVP(vp, opindex4, pub, -1)

		isUnmerged, nexts, err := unmergeOp(ctx, opindex4, vs4, getMerger)
		t.NoError(err)
		t.True(isUnmerged)
		t.Empty(nexts)

		isUnmerged, nexts, err = unmergeOp(ctx, opindex3, vs3, getMerger)
		t.NoError(err)
		t.True(isUnmerged)
		t.Equal([]uint64{opindex4}, nexts)

		merged, err := t.closeMergers(mergers)
		t.NoError(err)
		t.Equal(1, len(merged))
		t.Equal(int64(3), merged[vs3[0].Key()])
	})
}

func (t *testMergeOp) TestMergeValues() {
	t.Run("ok", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var opindex uint64 = 0

		vs := t.randomMergeValuesVP(vp, opindex, 1, 2, 3)

		isMerged, nexts, reason, err := mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.Empty(nexts)
		t.Empty(reason)

		merged, err := t.closeMergers(mergers)
		t.NoError(err)
		t.Equal(len(vs), len(merged))

		util.TraverseSliceV1(vs, func(_ int, value StateMergeValue) (bool, error) {
			var balance int64

			switch value.Key() {
			case vs[0].Key():
				balance = 1
			case vs[1].Key():
				balance = 2
			case vs[2].Key():
				balance = 3
			}

			t.Equal(balance, merged[value.Key()])

			return true, nil
		})
	})

	t.Run("multiple reasons", func() {
		vp, mergers, states, getMerger, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pub0 := base.NewDummyPublickey(util.UUID().String())
		pub1 := base.NewDummyPublickey(util.UUID().String())
		pub2 := base.NewDummyPublickey(util.UUID().String())

		prevst0 := t.newState(pub0, 1)
		prevst1 := t.newState(pub1, 1)
		prevst2 := t.newState(pub2, 1)
		states.SetValue(prevst0.Key(), prevst0)
		states.SetValue(prevst1.Key(), prevst1)
		states.SetValue(prevst2.Key(), prevst2)

		var opindex0, opindex1 uint64 = 0, 1

		vs0 := ConcatSlice(
			t.newMergeValues(pub0, -1),
			t.newMergeValues(pub1, -1),
			t.newMergeValues(pub2, -1),
		)

		vs1 := ConcatSlice(
			t.newMergeValues(pub0, -1),
			t.newMergeValues(pub1, -1),
			t.newMergeValues(pub2, -1),
		)
		t.True(vp.SetValue(opindex0, vs0))
		t.True(vp.SetValue(opindex1, vs1))

		isMerged, nexts, reasons, err := mergeOp(ctx, opindex1, vs1, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.Empty(nexts)
		t.Empty(reasons)

		isMerged, nexts, reasons, err = mergeOp(ctx, opindex0, vs0, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.Equal([]uint64{opindex1}, nexts)

		reason, found := reasons[opindex1]
		t.True(found)
		t.T().Logf("reason: %q", reason)
		t.Contains(reason, "previous issuer found")

		merged, err := t.closeMergers(mergers)
		t.Error(err)
		t.Equal(len(vs0), len(merged))
	})
}

func (t *testMergeOp) TestUnmergeValues() {
	t.Run("ok", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var opindex uint64 = 0

		vs := t.randomMergeValuesVP(vp, opindex, 1, 2, 3)

		isUnmerged, nexts, err := unmergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.True(isUnmerged)
		t.Empty(nexts)

		merged, err := t.closeMergers(mergers)
		t.Error(err)
		t.Equal(3, len(merged))

		util.TraverseSliceV1(vs, func(_ int, value StateMergeValue) (bool, error) {
			var balance int64

			switch value.Key() {
			case vs[0].Key():
				balance = -1
			case vs[1].Key():
				balance = -2
			case vs[2].Key():
				balance = -3
			}

			t.Equal(balance, merged[value.Key()])

			return true, nil
		})
	})
}

func (t *testMergeOp) TestNotCommit() {
	t.Run("no reason", func() {
		vp, mergers, states, getMerger, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pub := base.NewDummyPublickey(util.UUID().String())
		st := t.newState(pub, 1)
		states.SetValue(st.Key(), st)

		var opindex uint64 = 0

		v := t.newMergeValuesVP(vp, opindex, pub, -1)[0]

		{
			merger, err := v.Merger(st.Value())
			t.NoError(err)

			commit, next, reason, err := merger.Merge(opindex, v.Value())
			t.NoError(err)
			t.Empty(reason)
			t.Equal(opindex, next)
			t.NoError(commit())

			mergers.SetValue(v.Key(), merger)
		}

		vs := t.randomMergeValuesVP(vp, 1, 1, 1, 1)
		vs = append(vs, v)

		isMerged, nexts, reason, err := mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.False(isMerged)
		t.Empty(nexts)
		t.Empty(reason)

		merged, err := t.closeMergers(mergers)
		t.NoError(err)
		t.Equal(1, len(merged))

		mv, found := merged[v.Key()]
		t.True(found)
		t.Equal(int64(0), mv)
	})

	t.Run("reason", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var opindex uint64 = 1

		vs := t.randomMergeValuesVP(vp, 1, 1, 1, -1) // one value has -1

		isMerged, nexts, reasons, err := mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.False(isMerged)
		t.Empty(nexts)

		reason, found := reasons[opindex]
		t.True(found)
		t.Contains(reason, "insufficient balance")

		merged, err := t.closeMergers(mergers)
		t.NoError(err)
		t.Equal(0, len(merged))
	})
}

func TestMergeOp(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testMergeOp))
}
