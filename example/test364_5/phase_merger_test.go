package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"gonum.org/v1/gonum/stat/combin"
)

type baseTestMergeOp struct {
	baseTestAccountSVM
}

func (t *baseTestMergeOp) closeMergersWithError(mergers dutil.LockedMap[string, SVM]) (map[string]int64, error) {
	m := map[string]int64{}

	var gerr error

	for stateKey, merger := range mergers.Seq2 {
		st, err := merger.Close()
		if err != nil {
			gerr = err
		}

		if st != nil {
			if v := st.(*accountSV); v != nil {
				m[stateKey] = v.Balance
			}
		}
	}

	return m, gerr
}

func (t *baseTestMergeOp) getFuncs() (
	_ dutil.LockedMap[uint64, []SMV],
	_ dutil.LockedMap[string, SVM],
	_ dutil.LockedMap[string, base.State],
	getMerger func(SMV) (SVM, error),
	getValues func(uint64) ([]SMV, error),
) {
	vp, err := dutil.NewShardedLockedMap[uint64, []SMV](nil, 1<<9, 1<<9)
	t.IfErrPanic(err)

	mergers, err := dutil.NewShardedLockedMap[string, SVM](nil, 1<<9, 1<<9)
	t.IfErrPanic(err)

	states, err := dutil.NewShardedLockedMap[string, base.State](nil, 1<<9, 1<<9)
	t.IfErrPanic(err)

	return vp, mergers, states, func(value SMV) (SVM, error) {
			var merger SVM

			err := mergers.GetOrCreate(
				value.Key(),
				func() (SVM, error) {
					var stv base.StateValue

					if i, found := states.Value(value.Key()); found {
						stv = i.Value()
					}

					return value.Merger(stv)
				},
				func(i SVM, _ bool) error {
					merger = i

					return nil
				},
			)

			return merger, err
		},
		newFuncGetSMVsFromPool(vp)
}

func (t *baseTestMergeOp) newJobWorker() dutil.NewJobWorkerFunc {
	return dutil.NewFuncNewJobWorkerFunc(1 << 6)
}

func (t *baseTestMergeOp) extractSMVKeys() (
	extractSMVKeysFunc,
	func(),
) {
	return newFuncExtractSMVKeys(1<<9, 1<<9)
}

type testMergeOp struct {
	suite.Suite
	baseTestMergeOp
}

func (t *testMergeOp) TestMergeSingleValue() {
	t.Run("ok", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		var opindex uint64 = 0

		vs := t.randomMergeValuesVP(vp, opindex, 1)

		isMerged, nexts, reason, err := mergeOpValues(opindex, vs, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.Empty(nexts)
		t.Empty(reason)

		merged, err := t.closeMergersWithError(mergers)
		t.NoError(err)
		t.Equal(1, len(merged))
		t.Equal(int64(1), merged[vs[0].Key()])
	})

	t.Run("merge same op", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		var opindex uint64 = 0

		vs := t.randomMergeValuesVP(vp, opindex, 1)

		isMerged, nexts, reason, err := mergeOpValues(opindex, vs, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.Empty(nexts)
		t.Empty(reason)

		isMerged, nexts, reason, err = mergeOpValues(opindex, vs, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.Empty(nexts)
		t.Empty(reason)

		merged, err := t.closeMergersWithError(mergers)
		t.NoError(err)
		t.Equal(1, len(merged))
		t.Equal(int64(2), merged[vs[0].Key()])
	})

	t.Run("merge same -op", func() {
		vp, mergers, states, getMerger, _ := t.getFuncs()

		pub := base.NewDummyPublickey(util.UUID().String())
		prevst := t.newState(pub, 1)
		states.SetValue(prevst.Key(), prevst)

		var opindex uint64 = 0

		vs := t.newMergeValuesVP(vp, opindex, pub, -1)

		isMerged, nexts, reason, err := mergeOpValues(opindex, vs, getMerger)
		t.NoError(err)
		t.Empty(nexts)
		t.Empty(reason)

		isMerged, nexts, reason, err = mergeOpValues(opindex, vs, getMerger)
		t.NoError(err)
		t.False(isMerged)
		t.Empty(nexts)
		t.Empty(reason)

		merged, err := t.closeMergersWithError(mergers)
		t.NoError(err)
		t.Equal(1, len(merged))
		t.Equal(int64(0), merged[vs[0].Key()])
	})

	t.Run("getMerger error", func() {
		vp, mergers, _, _, _ := t.getFuncs()

		vs := t.randomMergeValuesVP(vp, 1, 1)

		getMerger := func(value SMV) (SVM, error) {
			return nil, errors.Errorf("showme")
		}

		isMerged, nexts, reason, err := mergeOpValues(1, vs, getMerger)
		t.Error(err)
		t.False(isMerged)
		t.Empty(nexts)
		t.Empty(reason)
		t.ErrorContains(err, "showme")

		merged, err := t.closeMergersWithError(mergers)
		t.NoError(err)
		t.Equal(0, len(merged))
	})

	t.Run("op reason", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		var opindex uint64 = 2

		vs := t.randomMergeValuesVP(vp, opindex, -1)

		isMerged, nexts, reason, err := mergeOpValues(opindex, vs, getMerger)
		t.NoError(err)
		t.False(isMerged)
		t.NotEmpty(reason)
		t.Empty(nexts)

		merged, err := t.closeMergersWithError(mergers)
		t.NoError(err)
		t.Equal(0, len(merged))
	})

	t.Run("next unmerge op reason", func() {
		vp, mergers, states, getMerger, _ := t.getFuncs()

		var opindex3, opindex4 uint64 = 3, 4

		pub := base.NewDummyPublickey(util.UUID().String())
		prevst := t.newState(pub, 1)
		states.SetValue(prevst.Key(), prevst)

		vs3 := t.newMergeValuesVP(vp, opindex3, pub, -1)
		vs4 := t.newMergeValuesVP(vp, opindex4, pub, -1)

		isMerged, nexts, reasons, err := mergeOpValues(opindex4, vs4, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.Empty(reasons)
		t.Empty(nexts)

		isMerged, nexts, reasons, err = mergeOpValues(opindex3, vs3, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.NotEmpty(reasons)

		reason, found := reasons[opindex4]
		t.True(found)
		t.Equal("previous issuer found", reason)
		t.Equal([]uint64{opindex4}, nexts)

		merged, err := t.closeMergersWithError(mergers)
		t.Error(err)
		t.Equal(1, len(merged))
		t.Equal(int64(-1), merged[vs3[0].Key()])
	})
}

func (t *testMergeOp) TestUnmergeSingleValue() {
	t.Run("ok", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		var opindex uint64 = 0

		vs := t.randomMergeValuesVP(vp, opindex, 1)

		isUnmerged, nexts, _, err := unmergeOpValues(opindex, vs, getMerger)
		t.NoError(err)
		t.True(isUnmerged)
		t.Empty(nexts)

		merged, err := t.closeMergersWithError(mergers)
		t.Error(err)
		t.Equal(1, len(merged))
		t.Equal(int64(-1), merged[vs[0].Key()])
	})

	t.Run("getMerger error", func() {
		vp, mergers, _, _, _ := t.getFuncs()

		vs := t.randomMergeValuesVP(vp, 1, 1)

		getMerger := func(value SMV) (SVM, error) {
			return nil, errors.Errorf("showme")
		}

		isUnmerged, _, _, err := unmergeOpValues(1, vs, getMerger)
		t.Error(err)
		t.False(isUnmerged)
		t.ErrorContains(err, "showme")

		merged, err := t.closeMergersWithError(mergers)
		t.NoError(err)
		t.Equal(0, len(merged))
	})

	t.Run("next merge op", func() {
		vp, mergers, states, getMerger, _ := t.getFuncs()

		var opindex3, opindex4 uint64 = 3, 4

		pub := base.NewDummyPublickey(util.UUID().String())
		prevst := t.newState(pub, 1)
		states.SetValue(prevst.Key(), prevst)

		vs3 := t.newMergeValuesVP(vp, opindex3, pub, -1)
		vs4 := t.newMergeValuesVP(vp, opindex4, pub, -1)

		isUnmerged, nexts, _, err := unmergeOpValues(opindex4, vs4, getMerger)
		t.NoError(err)
		t.True(isUnmerged)
		t.Empty(nexts)

		isUnmerged, nexts, _, err = unmergeOpValues(opindex3, vs3, getMerger)
		t.NoError(err)
		t.True(isUnmerged)
		t.Equal([]uint64{opindex4}, nexts)

		merged, err := t.closeMergersWithError(mergers)
		t.NoError(err)
		t.Equal(1, len(merged))
		t.Equal(int64(3), merged[vs3[0].Key()])
	})
}

func (t *testMergeOp) TestMergeValues() {
	t.Run("ok", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		var opindex uint64 = 0

		vs := t.randomMergeValuesVP(vp, opindex, 1, 2, 3)

		isMerged, nexts, reason, err := mergeOpValues(opindex, vs, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.Empty(nexts)
		t.Empty(reason)

		merged, err := t.closeMergersWithError(mergers)
		t.NoError(err)
		t.Equal(len(vs), len(merged))

		for value := range slices.Values(vs) {
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
		}
	})

	t.Run("multiple reasons", func() {
		vp, mergers, states, getMerger, _ := t.getFuncs()

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

		vs0 := slices.Concat(
			t.newMergeValues(pub0, -1),
			t.newMergeValues(pub1, -1),
			t.newMergeValues(pub2, -1),
		)

		vs1 := slices.Concat(
			t.newMergeValues(pub0, -1),
			t.newMergeValues(pub1, -1),
			t.newMergeValues(pub2, -1),
		)
		t.True(vp.SetValue(opindex0, vs0))
		t.True(vp.SetValue(opindex1, vs1))

		isMerged, nexts, reasons, err := mergeOpValues(opindex1, vs1, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.Empty(nexts)
		t.Empty(reasons)

		isMerged, nexts, reasons, err = mergeOpValues(opindex0, vs0, getMerger)
		t.NoError(err)
		t.True(isMerged)
		t.Equal([]uint64{opindex1}, nexts)

		reason, found := reasons[opindex1]
		t.True(found)
		t.T().Logf("reason: %q", reason)
		t.Contains(reason, "previous issuer found")

		merged, err := t.closeMergersWithError(mergers)
		t.Error(err)
		t.Equal(len(vs0), len(merged))
	})
}

func (t *testMergeOp) TestUnmergeValues() {
	t.Run("ok", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		var opindex uint64 = 0

		vs := t.randomMergeValuesVP(vp, opindex, 1, 2, 3)

		isUnmerged, nexts, _, err := unmergeOpValues(opindex, vs, getMerger)
		t.NoError(err)
		t.True(isUnmerged)
		t.Empty(nexts)

		merged, err := t.closeMergersWithError(mergers)
		t.Error(err)
		t.Equal(3, len(merged))

		for value := range slices.Values(vs) {
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
		}
	})
}

func (t *testMergeOp) TestNotCommit() {
	t.Run("no reason", func() {
		vp, mergers, states, getMerger, _ := t.getFuncs()

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

		isMerged, nexts, reason, err := mergeOpValues(opindex, vs, getMerger)
		t.NoError(err)
		t.False(isMerged)
		t.Empty(nexts)
		t.Empty(reason)

		merged, err := t.closeMergersWithError(mergers)
		t.NoError(err)
		t.Equal(1, len(merged))

		mv, found := merged[v.Key()]
		t.True(found)
		t.Equal(int64(0), mv)
	})

	t.Run("reason", func() {
		vp, mergers, _, getMerger, _ := t.getFuncs()

		var opindex uint64 = 1

		vs := t.randomMergeValuesVP(vp, 1, 1, 1, -1) // one value has -1

		isMerged, nexts, reasons, err := mergeOpValues(opindex, vs, getMerger)
		t.NoError(err)
		t.False(isMerged)
		t.Empty(nexts)

		reason, found := reasons[opindex]
		t.True(found)
		t.Contains(reason, "insufficient balance")

		merged, err := t.closeMergersWithError(mergers)
		t.NoError(err)
		t.Equal(0, len(merged))
	})
}

func TestMergeOp(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testMergeOp))
}

type testCollectXmergedOps struct {
	suite.Suite
	baseTestMergeOp
}

func (t *testCollectXmergedOps) TestAdd() {
	t.Run("no add", func() {
		_, done, err := newFuncCollectXmergedOps(1 << 9)
		t.NoError(err)

		merged, unmerged, err := done()
		t.NoError(err)
		t.Empty(merged)
		t.Empty(unmerged)
	})

	t.Run("add merge", func() {
		add, done, err := newFuncCollectXmergedOps(1 << 9)
		t.NoError(err)

		add(startPhase, 1, mergeFlag)
		add(startPhase, 2, mergeFlag)

		merged, unmerged, err := done()
		t.NoError(err)
		t.Equal([]uint64{1, 2}, merged)
		t.Empty(unmerged)
	})

	t.Run("add unmerge", func() {
		add, done, err := newFuncCollectXmergedOps(1 << 9)
		t.NoError(err)

		add(startPhase, 1, unmergeFlag)
		add(startPhase, 2, unmergeFlag)

		merged, unmerged, err := done()
		t.NoError(err)
		t.Empty(merged)
		t.Equal([]uint64{1, 2}, unmerged)
	})

	t.Run("add same", func() {
		add, done, err := newFuncCollectXmergedOps(1 << 9)
		t.NoError(err)

		add(startPhase, 1, mergeFlag)
		add(startPhase, 1, mergeFlag)

		merged, unmerged, err := done()
		t.NoError(err)
		t.Equal([]uint64{1}, merged)
		t.Empty(unmerged)
	})

	t.Run("add same, but dismiss", func() {
		add, done, err := newFuncCollectXmergedOps(1 << 9)
		t.NoError(err)

		add(startPhase, 1, mergeFlag)
		add(startPhase, 1, unmergeFlag)

		merged, unmerged, err := done()
		t.NoError(err)
		t.Empty(merged)
		t.Empty(unmerged)
	})
}

func (t *testCollectXmergedOps) TestWithBase() {
	t.Run("no add", func() {
		_, done, err := newFuncCollectXmergedOps(1 << 9)
		t.NoError(err)

		merged, unmerged, err := done()
		t.NoError(err)
		t.Empty(merged)
		t.Empty(unmerged)
	})

	t.Run("add; same in base", func() {
		add, done, err := newFuncCollectXmergedOps(1 << 9)
		t.NoError(err)

		add(basePhase, 1, mergeFlag)
		add(startPhase, 1, mergeFlag)
		add(startPhase, 2, mergeFlag)

		merged, unmerged, err := done()
		t.NoError(err)
		t.Equal([]uint64{2}, merged)
		t.Empty(unmerged)
	})

	t.Run("add; not same in base", func() {
		add, done, err := newFuncCollectXmergedOps(1 << 9)
		t.NoError(err)

		add(basePhase, 1, mergeFlag)
		add(startPhase, 1, unmergeFlag)
		add(startPhase, 2, mergeFlag)

		merged, unmerged, err := done()
		t.NoError(err)
		t.Equal([]uint64{2}, merged)
		t.Equal([]uint64{1}, unmerged)
	})

	t.Run("all same in base", func() {
		add, done, err := newFuncCollectXmergedOps(1 << 9)
		t.NoError(err)

		add(basePhase, 1, mergeFlag)
		add(basePhase, 2, mergeFlag)
		add(startPhase, 1, mergeFlag)
		add(startPhase, 2, mergeFlag)

		merged, unmerged, err := done()
		t.NoError(err)
		t.Empty(merged)
		t.Empty(unmerged)
	})
}

func TestCollectXmergedOpsFuncs(t *testing.T) {
	suite.Run(t, new(testCollectXmergedOps))
}

type testPhaseMerger struct {
	dutil.BaseSuite
	baseTestMergeOp
}

func (t *testPhaseMerger) newFuncs() (
	vp dutil.LockedMap[uint64, []SMV],
	mergers dutil.LockedMap[string, SVM],
	states dutil.LockedMap[string, base.State],
	_ opindexGetSMVsFunc,
	getState getStateFunc,
) {
	t.T().Helper()

	vp, mergers, states, _, getValues := t.baseTestMergeOp.getFuncs()

	getState = func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}

	return vp, mergers, states, getValues, getState
}

func (t *testPhaseMerger) TestCancel() {
	t.Run("ctx cancel; merge", func() {
		ctx, cancel := t.Ctx()

		vp, mergers, _, getValues, getState := t.newFuncs()

		extractKeys, closeExtractKeys := t.extractSMVKeys()
		defer closeExtractKeys()

		getMergers, closeMergers := newFuncGetSVMs(mergers, getState, extractKeys)
		defer closeMergers()

		merge, _, _, _, err := newFuncPhaseMerger(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, getValues, getMergers)
		t.NoError(err)

		var opindex uint64 = 1

		t.randomMergeValuesVP(vp, opindex, 1)

		cancel(nil)

		err = merge(opindex)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("ctx cancel; unmerge", func() {
		ctx, cancel := t.Ctx()

		vp, mergers, _, getValues, getState := t.newFuncs()

		extractKeys, closeExtractKeys := t.extractSMVKeys()
		defer closeExtractKeys()

		getMergers, closeMergers := newFuncGetSVMs(mergers, getState, extractKeys)
		defer closeMergers()

		_, unmerge, _, _, err := newFuncPhaseMerger(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, getValues, getMergers)
		t.NoError(err)

		var opindex uint64 = 1

		_ = t.randomMergeValuesVP(vp, opindex, 1)

		cancel(nil)

		err = unmerge(opindex)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("ctx cancel; wait", func() {
		ctx, cancel := t.Ctx()

		vp, mergers, _, getValues, getState := t.newFuncs()

		extractKeys, closeExtractKeys := t.extractSMVKeys()
		defer closeExtractKeys()

		getMergers, closeMergers := newFuncGetSVMs(mergers, getState, extractKeys)
		defer closeMergers()

		merge, _, done, wait, err := newFuncPhaseMerger(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, getValues, getMergers)
		t.NoError(err)

		var opindex uint64 = 1

		t.randomMergeValuesVP(vp, opindex, 1)

		t.NoError(merge(opindex))

		cancel(nil)

		done()

		_, _, _, err = wait()
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testPhaseMerger) TestMerge() {
	t.Run("ok, 1", func() {
		ctx, _ := t.Ctx()

		vp, mergers, _, getValues, getState := t.newFuncs()

		extractKeys, closeExtractKeys := t.extractSMVKeys()
		defer closeExtractKeys()

		getMergers, closeMergers := newFuncGetSVMs(mergers, getState, extractKeys)
		defer closeMergers()

		merge, _, done, wait, err := newFuncPhaseMerger(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, getValues, getMergers)
		t.NoError(err)

		var opindex uint64 = 1

		vs := t.randomMergeValuesVP(vp, opindex, 3)

		t.NoError(merge(opindex))

		done()

		newMerged, newUnmerged, reasons, err := wait()
		t.NoError(err)
		t.Empty(newMerged)
		t.Empty(newUnmerged)
		t.Empty(reasons)

		merged, err := t.closeMergersWithError(mergers)
		t.NoError(err)

		m, found := merged[vs[0].Key()]
		t.True(found)
		t.Equal(int64(3), m)
	})

	t.Run("ok, 33", func() {
		ctx, _ := t.Ctx()

		vp, mergers, _, getValues, getState := t.newFuncs()

		extractKeys, closeExtractKeys := t.extractSMVKeys()
		defer closeExtractKeys()

		getMergers, closeMergers := newFuncGetSVMs(mergers, getState, extractKeys)
		defer closeMergers()

		merge, _, done, wait, err := newFuncPhaseMerger(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, getValues, getMergers)
		t.NoError(err)

		vss := make([][]SMV, 33)
		for i := range vss {
			vss[i] = t.randomMergeValuesVP(vp, uint64(i), i+1)
		}

		for i := range vss {
			t.NoError(merge(uint64(i)))
		}

		done()
		newMerged, newUnmerged, reasons, err := wait()
		t.NoError(err)
		t.Empty(newMerged)
		t.Empty(newUnmerged)
		t.Empty(reasons)

		merged, err := t.closeMergersWithError(mergers)
		t.NoError(err)
		t.Equal(33, len(merged))

		for values := range slices.Values(vss) {
			for value := range slices.Values(values) {
				i, found := merged[value.Key()]
				t.True(found)

				t.Equal(value.(*accountSMV).Balance, i)
			}
		}
	})

	t.Run("Merge() error", func() {
		ctx, _ := t.Ctx()

		vp, mergers, _, getValues, getState := t.newFuncs()

		var opindex0, opindex1 uint64 = 1, 2

		vs0 := t.randomMergeValuesVP(vp, opindex0, 1)
		t.randomMergeValuesVP(vp, opindex1, 1)

		vs0keys := extractSMVKeysFromValues(vs0)

		extractKeys, closeExtractKeys := t.extractSMVKeys()
		defer closeExtractKeys()

		origGetMergers, closeMergers := newFuncGetSVMs(mergers, getState, extractKeys)
		defer closeMergers()

		getMergers := func(
			opindex uint64,
			values []SMV,
			f func(func(SMV) (SVM, error)) error,
		) error {
			stateKeys, err := extractKeys(opindex, values)
			if err != nil {
				return err
			}

			if slices.Equal(stateKeys, vs0keys) {
				return errors.Errorf("showme")
			}

			return origGetMergers(opindex, values, f)
		}

		merge, _, done, wait, err := newFuncPhaseMerger(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, getValues, getMergers)
		t.NoError(err)

		t.NoError(merge(opindex0))

		err = merge(opindex1)
		if err != nil {
			t.ErrorContains(err, "showme")
		}

		done()
		_, _, _, err = wait()
		t.ErrorContains(err, "showme")
	})

	t.Run("reason", func() {
		ctx, _ := t.Ctx()

		vp, mergers, _, getValues, getState := t.newFuncs()

		extractKeys, closeExtractKeys := t.extractSMVKeys()
		defer closeExtractKeys()

		getMergers, closeMergers := newFuncGetSVMs(mergers, getState, extractKeys)
		defer closeMergers()

		merge, _, done, wait, err := newFuncPhaseMerger(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, getValues, getMergers)
		t.NoError(err)

		var opindex0, opindex1 uint64 = 1, 2

		vs := t.randomMergeValuesVP(vp, opindex0, -1)
		t.NoError(merge(opindex0))

		vs = t.randomMergeValuesVP(vp, opindex1, 1)
		t.NoError(merge(opindex1))

		done()

		newMerged, newUnmerged, reasons, err := wait()
		t.NoError(err)
		t.Empty(newMerged)
		t.Equal([]uint64{1}, newUnmerged)
		t.Equal(map[uint64]string{1: "insufficient balance"}, reasons)

		merged, err := t.closeMergersWithError(mergers)
		t.NoError(err)

		m, found := merged[vs[0].Key()]
		t.True(found)
		t.Equal(int64(1), m)
	})
}

func (t *testPhaseMerger) TestUnmerge() {
	t.Run("ok, 1", func() {
		ctx, _ := t.Ctx()

		vp, mergers, states, getValues, getState := t.newFuncs()

		pub := base.NewDummyPublickey(util.UUID().String())
		prevst := t.newState(pub, 1)
		states.SetValue(prevst.Key(), prevst)

		extractKeys, closeExtractKeys := t.extractSMVKeys()
		defer closeExtractKeys()

		getMergers, closeMergers := newFuncGetSVMs(mergers, getState, extractKeys)
		defer closeMergers()

		_, unmerge, done, wait, err := newFuncPhaseMerger(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, getValues, getMergers)
		t.NoError(err)

		var opindex uint64 = 1

		vs := t.newMergeValuesVP(vp, opindex, pub, 1)

		t.NoError(unmerge(opindex))

		done()
		newMerged, newUnmerged, reasons, err := wait()
		t.NoError(err)
		t.Empty(newMerged)
		t.Equal([]uint64{1}, newUnmerged)
		t.Empty(reasons)

		merged, err := t.closeMergersWithError(mergers)
		t.NoError(err)

		m, found := merged[vs[0].Key()]
		t.True(found)
		t.Equal(int64(0), m)
	})

	t.Run("ok, 33", func() {
		ctx, _ := t.Ctx()

		vp, mergers, states, getValues, getState := t.newFuncs()

		vss := make([][]SMV, 33)

		pub := base.NewDummyPublickey(util.UUID().String())

		extractKeys, closeExtractKeys := t.extractSMVKeys()
		defer closeExtractKeys()

		getMergers, closeMergers := newFuncGetSVMs(mergers, getState, extractKeys)
		defer closeMergers()

		_, unmerge, done, wait, err := newFuncPhaseMerger(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, getValues, getMergers)
		t.NoError(err)

		expectedUnmerged := make([]uint64, len(vss))
		var subed int64
		for i := range vss {
			opindex := uint64(i)
			expectedUnmerged[i] = opindex

			vss[i] = t.newMergeValuesVP(vp, opindex, pub, i+1)

			subed -= int64(i) + 1
		}

		prevst := t.newState(pub, (subed)*-1)
		states.SetValue(prevst.Key(), prevst)

		for i := range vss {
			t.NoError(unmerge(uint64(i)))
		}

		done()
		newMerged, newUnmerged, reasons, err := wait()
		t.NoError(err)
		t.Empty(newMerged)
		t.Equal(expectedUnmerged, newUnmerged)
		t.Empty(reasons)

		merged, err := t.closeMergersWithError(mergers)
		t.NoError(err)
		t.Equal(1, len(merged))

		m, found := merged[vss[0][0].Key()]
		t.True(found)
		t.Equal(subed+prevst.Value().(*accountSV).Balance, m)
	})

	t.Run("Merge() error", func() {
		ctx, _ := t.Ctx()

		vp, mergers, states, getValues, getState := t.newFuncs()

		pub0 := base.NewDummyPublickey(util.UUID().String())
		prevst0 := t.newState(pub0, 1)
		pub1 := base.NewDummyPublickey(util.UUID().String())
		prevst1 := t.newState(pub1, 1)

		states.SetValue(prevst0.Key(), prevst0)
		states.SetValue(prevst1.Key(), prevst1)

		var opindex0, opindex1 uint64 = 1, 2

		vs0 := t.newMergeValuesVP(vp, opindex0, pub0, 1)
		_ = t.newMergeValuesVP(vp, opindex1, pub1, 1)

		vs0keys := extractSMVKeysFromValues(vs0)

		extractKeys, closeExtractKeys := t.extractSMVKeys()
		defer closeExtractKeys()

		origGetMergers, closeMergers := newFuncGetSVMs(mergers, getState, extractKeys)
		defer closeMergers()

		getMergers := func(
			opindex uint64,
			values []SMV,
			f func(func(SMV) (SVM, error)) error,
		) error {
			stateKeys, err := extractKeys(opindex, values)
			if err != nil {
				return err
			}

			if slices.Equal(stateKeys, vs0keys) {
				return errors.Errorf("showme")
			}

			return origGetMergers(opindex, values, f)
		}

		_, unmerge, done, wait, err := newFuncPhaseMerger(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, getValues, getMergers)
		t.NoError(err)

		t.NoError(unmerge(opindex0))

		err = unmerge(opindex1)
		if err != nil {
			switch {
			case strings.Contains(err.Error(), "showme"):
			case errors.Is(err, context.Canceled):
			default:
				t.Fail("unexpected error: %+v", err)
			}
		}

		done()
		_, _, _, err = wait()
		t.ErrorContains(err, "showme")
	})
}

func (t *testPhaseMerger) baseTestMergeUnmerge() (
	pubs []base.Publickey,
	states []base.State,
	ops [][]SMV,
) {
	pub0 := base.NewDummyPublickey("0")
	pub1 := base.NewDummyPublickey("1")

	st0 := t.newState(pub0, 2)
	st1 := t.newState(pub1, 2)

	ops = [][]SMV{
		{ // 0
			t.newMergeValue(pub0, 1),
			t.newMergeValue(pub1, -2),
		},
		{ // 1: ignored
			t.newMergeValue(pub0, -1),
			t.newMergeValue(pub1, -2),
		},
		{ // 2
			t.newMergeValue(pub0, -1),
			t.newMergeValue(pub1, 2),
		},
		{ // 3: ignored
			t.newMergeValue(pub0, -1),
			t.newMergeValue(pub1, -2),
		},
		{ // 4
			t.newMergeValue(pub0, 1),
			t.newMergeValue(pub1, 2),
		},
	}

	return []base.Publickey{pub0, pub1},
		[]base.State{st0, st1},
		ops
}

func (t *testPhaseMerger) testMergeUnmerge(semSize int64, seq []int) {
	t.T().Helper()

	ctx, _ := t.Ctx()

	_, sts, ops := t.baseTestMergeUnmerge()

	vp, mergers, states, getValues, getState := t.newFuncs()

	for i := range sts {
		states.SetValue(sts[i].Key(), sts[i])
	}

	extractKeys, closeExtractKeys := t.extractSMVKeys()
	defer closeExtractKeys()

	getMergers, closeMergers := newFuncGetSVMs(mergers, getState, extractKeys)
	defer closeMergers()

	merge, _, done, wait, err := newFuncPhaseMerger(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, getValues, getMergers)
	t.NoError(err)

	if seq == nil {
		seq = make([]int, len(ops))
		for i := range ops {
			seq[i] = i
		}
	}

	for i := range seq {
		opindex := uint64(seq[i])
		vs := ops[opindex]

		vp.SetValue(opindex, vs)

		t.NoError(merge(opindex))
	}

	done()

	var newMerged, newUnmerged []uint64

	newMerged, newUnmerged, reasons, err := wait()
	t.NoError(err)

	reason, found := reasons[1]
	t.True(found)
	t.Equal("previous issuer found", reason)

	reason, found = reasons[3]
	t.True(found)
	t.Equal("previous issuer found", reason)

	merged, err := t.closeMergersWithError(mergers)
	t.NoError(err)

	for key, v := range maps.All(merged) {
		switch key {
		case sts[0].Key():
			t.Equal(int64(3), v)
		case sts[1].Key():
			t.Equal(int64(4), v)
		default:
			t.Fail("unknown state key, %q", key)
		}
	}

	t.Empty(newMerged)
	t.Equal([]uint64{1, 3}, newUnmerged)
}

func (t *testPhaseMerger) TestMergeReplay() {
	seq := []int{3, 2, 1, 4, 0}
	t.testMergeUnmerge(9, seq)
}

func (t *testPhaseMerger) TestMergeUnmergeReplay() {
	_, sts, ops := t.baseTestMergeUnmerge()
	vp, mergers, states, _, _ := t.newFuncs()

	for i := range sts {
		states.SetValue(sts[i].Key(), sts[i])
	}

	for i := range ops {
		opindex := uint64(i)
		vs := ops[opindex]

		vp.SetValue(opindex, vs)
	}

	getState := func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}

	getMerger := newFuncGetOrCreateMerger(mergers, getState)

	merge := func(opindex uint64) error {
		t.T().Logf("> merge: opindex=%d", opindex)

		var values []SMV

		switch i, found := vp.Value(opindex); {
		case !found:
			return util.ErrNotFound.Errorf("merge values, %d", opindex)
		default:
			values = i
		}

		switch isMerged, nexts, reason, err := mergeOpValues(
			opindex,
			values,
			getMerger,
		); {
		case err != nil:
			return err
		default:
			t.T().Logf("< merge: opindex=%d nexts=%v reason=%q is_merged=%v", opindex, nexts, reason, isMerged)

			return nil
		}
	}

	unmerge := func(opindex uint64) error {
		t.T().Logf("> unmerge: opindex=%d", opindex)

		var values []SMV

		switch i, found := vp.Value(opindex); {
		case !found:
			return util.ErrNotFound.Errorf("unmerge values, %d", opindex)
		default:
			values = i
		}

		switch isUnmerged, nexts, _, err := unmergeOpValues(
			opindex,
			values,
			getMerger,
		); {
		case err != nil:
			return err
		default:
			t.T().Logf("< unmerge: opindex=%d nexts=%v is_unmerged=%v", opindex, nexts, isUnmerged)

			return nil
		}
	}

	seq := [][2]interface{}{
		{"m", 3},
		{"m", 2},
		{"m", 4},
		{"m", 1},
		{"m", 0},
		{"u", 2},
		{"u", 1},
		{"u", 3},
		{"m", 3},
		{"m", 2},
	}

	for i := range seq {
		s := seq[i]

		opindex := uint64(s[1].(int))

		switch s[0].(string) {
		case "m":
			t.NoError(merge(opindex))
		case "u":
			t.NoError(unmerge(opindex))
		}
	}

	merged, err := t.closeMergersWithError(mergers)
	t.NoError(err)

	for key, v := range maps.All(merged) {
		switch key {
		case sts[0].Key():
			t.Equal(int64(3), v)
		case sts[1].Key():
			t.Equal(int64(4), v)
		default:
			t.Fail("unknown state key, %q", key)
		}
	}
}

func (t *testPhaseMerger) TestMergeUnmerge() {
	for i := 0; i < 99; i++ {
		gen := combin.NewPermutationGenerator(5, 5)
		nextseq := func() bool {
			return gen.Next()
		}
		getseq := func() []int {
			return gen.Permutation(nil)
		}

		for nextseq() {
			seq := getseq()

			t.Run(fmt.Sprintf("%02d: %v", i, seq), func() {
				t.testMergeUnmerge(1<<9, seq)
			})
		}
	}
}

func (t *testPhaseMerger) testMerges3Accounts(semSize int64, seq []int) {
	t.T().Helper()

	ctx, _ := t.Ctx()

	pub0 := base.NewDummyPublickey(util.UUID().String())
	pub1 := base.NewDummyPublickey(util.UUID().String())
	pub2 := base.NewDummyPublickey(util.UUID().String())

	sts := make([]base.State, 3)
	sts[0] = t.newState(pub0, 1)
	sts[1] = t.newState(pub1, 2)
	sts[2] = t.newState(pub2, 3)

	ops := [][]SMV{
		{ // 0
			t.newMergeValue(pub0, 1),
			t.newMergeValue(pub1, -2),
		},
		{ // 1: ignored
			t.newMergeValue(pub1, -2),
			t.newMergeValue(pub2, -3),
		},
		{ // 2
			t.newMergeValue(pub0, -1),
			t.newMergeValue(pub1, 2),
		},
		{ // 3: ignored
			t.newMergeValue(pub0, -1),
			t.newMergeValue(pub1, -2),
		},
		{ // 4
			t.newMergeValue(pub0, 1),
			t.newMergeValue(pub2, -3),
		},
		{ // 5
			t.newMergeValue(pub1, 1),
			t.newMergeValue(pub2, 2),
		},
	}

	vp, mergers, states, getValues, getState := t.newFuncs()

	for i := range sts {
		states.SetValue(sts[i].Key(), sts[i])
	}

	extractKeys, closeExtractKeys := t.extractSMVKeys()
	defer closeExtractKeys()

	getMergers, closeMergers := newFuncGetSVMs(mergers, getState, extractKeys)
	defer closeMergers()

	merge, _, done, wait, err := newFuncPhaseMerger(ctx, dutil.NewFuncNewJobWorkerFunc(1<<9), 1<<9, getValues, getMergers)
	t.NoError(err)

	if seq == nil {
		seq = make([]int, len(ops))
		for i := range ops {
			seq[i] = i
		}
	}

	for i := range seq {
		opindex := uint64(seq[i])
		vs := ops[opindex]

		vp.SetValue(opindex, vs)

		t.NoError(merge(opindex))
	}

	done()

	var newMerged, newUnmerged []uint64

	newMerged, newUnmerged, reasons, err := wait()
	t.NoError(err)

	reason, found := reasons[1]
	t.True(found)
	t.Equal("previous issuer found", reason)

	reason, found = reasons[3]
	t.True(found)
	t.Equal("previous issuer found", reason)

	merged, err := t.closeMergersWithError(mergers)
	t.NoError(err)
	t.Equal(3, len(merged))

	for key, v := range maps.All(merged) {
		switch key {
		case sts[0].Key():
			t.Equal(int64(2), v)
		case sts[1].Key():
			t.Equal(int64(3), v)
		case sts[2].Key():
			t.Equal(int64(2), v)
		default:
			t.Fail("unknown state key, %q", key)
		}
	}

	t.Empty(newMerged)
	t.Equal([]uint64{1, 3}, newUnmerged)
}

func (t *testPhaseMerger) TestMerge3Accounts() {
	t.Run("sem size 1", func() {
		t.testMerges3Accounts(1, nil)
	})

	t.Run("default sem size", func() {
		t.testMerges3Accounts(1<<9, nil)
	})

	t.Run("permutation", func() {
		for i := 0; i < 33; i++ {
			gen := combin.NewPermutationGenerator(6, 6)
			nextseq := func() bool {
				return gen.Next()
			}
			getseq := func() []int {
				return gen.Permutation(nil)
			}

			for nextseq() {
				seq := getseq()

				t.Run(fmt.Sprintf("%02d: %v", i, seq), func() {
					t.testMerges3Accounts(1<<9, seq)
				})
			}
		}
	})
}

func TestPhaseMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testPhaseMerger))
}
