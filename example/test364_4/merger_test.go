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

func (t *baseTestMergeOp) closeMergers(mergers util.LockedMap[string, StateMerger]) map[string]int64 {
	m := map[string]int64{}

	mergers.Traverse(func(stateKey string, merger StateMerger) bool {
		st, err := merger.Close()
		t.NoError(err)

		if st != nil {
			m[stateKey] = st.(AccountStateValue).Balance
		}

		return true
	})

	return m
}

func (t *baseTestMergeOp) getFuncs() (
	_ util.LockedMap[uint64, []StateMergeValue],
	_ util.LockedMap[string, StateMerger],
	_ util.LockedMap[string, base.State],
	getMerger func(StateMergeValue) (StateMerger, error),
	getValues func(uint64) ([]StateMergeValue, error),
) {
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

		nexts, reason, err := mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.Empty(nexts)
		t.Empty(reason)

		merged := t.closeMergers(mergers)
		t.Equal(1, len(merged))
		t.Equal(int64(1), merged[vs[0].Key()])
	})

	t.Run("merge same op", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var opindex uint64 = 0

		vs := t.randomMergeValuesVP(vp, opindex, 1)

		nexts, reason, err := mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.Empty(nexts)
		t.Empty(reason)

		nexts, reason, err = mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.Empty(nexts)
		t.Empty(reason)

		merged := t.closeMergers(mergers)
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

		nexts, reason, err := mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.Empty(nexts)
		t.Empty(reason)

		nexts, reason, err = mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.Empty(nexts)
		t.Empty(reason)

		merged := t.closeMergers(mergers)
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

		nexts, reason, err := mergeOp(ctx, 1, vs, getMerger)
		t.Error(err)
		t.Empty(nexts)
		t.Empty(reason)
		t.ErrorContains(err, "showme")

		merged := t.closeMergers(mergers)
		t.Equal(0, len(merged))
	})

	t.Run("op reason", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var opindex uint64 = 2

		vs := t.randomMergeValuesVP(vp, opindex, -1)

		nexts, reason, err := mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.NotEmpty(reason)
		t.Empty(nexts)

		merged := t.closeMergers(mergers)
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

		nexts, reason, err := mergeOp(ctx, opindex4, vs4, getMerger)
		t.NoError(err)
		t.Empty(reason)
		t.Empty(nexts)

		nexts, reason, err = mergeOp(ctx, opindex3, vs3, getMerger)
		t.NoError(err)
		t.Equal("previous issuer found", reason)
		t.Equal([]uint64{opindex4}, nexts)

		merged := t.closeMergers(mergers)
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

		nexts, err := unmergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.Empty(nexts)

		merged := t.closeMergers(mergers)
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

		_, err := unmergeOp(ctx, 1, vs, getMerger)
		t.Error(err)
		t.ErrorContains(err, "showme")

		merged := t.closeMergers(mergers)
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

		nexts, err := unmergeOp(ctx, opindex4, vs4, getMerger)
		t.NoError(err)
		t.Empty(nexts)

		nexts, err = unmergeOp(ctx, opindex3, vs3, getMerger)
		t.NoError(err)
		t.Equal([]uint64{opindex4}, nexts)

		merged := t.closeMergers(mergers)
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

		nexts, reason, err := mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.Empty(nexts)
		t.Empty(reason)

		merged := t.closeMergers(mergers)
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

		nexts, reason, err := mergeOp(ctx, opindex1, vs1, getMerger)
		t.NoError(err)
		t.Empty(nexts)
		t.Empty(reason)

		nexts, reason, err = mergeOp(ctx, opindex0, vs0, getMerger)
		t.NoError(err)
		t.Equal([]uint64{opindex1}, nexts)
		t.T().Logf("reason: %q", reason)
		t.NotEmpty(reason)
		t.Contains(reason, "; ")

		merged := t.closeMergers(mergers)
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

		nexts, err := unmergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.Empty(nexts)

		merged := t.closeMergers(mergers)
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

		nexts, reason, err := mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.Empty(nexts)
		t.Empty(reason)

		merged := t.closeMergers(mergers)
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

		nexts, reason, err := mergeOp(ctx, opindex, vs, getMerger)
		t.NoError(err)
		t.Empty(nexts)
		t.Contains(reason, "insufficient balance")

		merged := t.closeMergers(mergers)
		t.Equal(0, len(merged))
	})
}

func TestMergeOp(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testMergeOp))
}
