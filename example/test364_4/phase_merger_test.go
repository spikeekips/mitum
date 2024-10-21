package main

import (
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

type testIsXMergedOPsFuncs struct {
	baseTestMergeOp
}

func (t *testIsXMergedOPsFuncs) TestAdd() {
	t.Run("no add", func() {
		_, done, err := isXMergedOps(1 << 9)
		t.NoError(err)

		t.NoError(done(func(merged, unmerged []uint64) error {
			t.Empty(merged)
			t.Empty(unmerged)

			return nil
		}))
	})

	t.Run("add merge", func() {
		add, done, err := isXMergedOps(1 << 9)
		t.NoError(err)

		add(0, 1, mergeFlag)
		add(0, 2, mergeFlag)

		t.NoError(done(func(merged, unmerged []uint64) error {
			t.Equal([]uint64{1, 2}, merged)
			t.Empty(unmerged)

			return nil
		}))
	})

	t.Run("add unmerge", func() {
		add, done, err := isXMergedOps(1 << 9)
		t.NoError(err)

		add(0, 1, unmergeFlag)
		add(0, 2, unmergeFlag)

		t.NoError(done(func(merged, unmerged []uint64) error {
			t.Empty(merged)
			t.Equal([]uint64{1, 2}, unmerged)

			return nil
		}))
	})

	t.Run("add same", func() {
		add, done, err := isXMergedOps(1 << 9)
		t.NoError(err)

		add(0, 1, mergeFlag)
		add(0, 1, mergeFlag)

		t.NoError(done(func(merged, unmerged []uint64) error {
			t.Equal([]uint64{1}, merged)
			t.Empty(unmerged)

			return nil
		}))
	})

	t.Run("add same, but dismiss", func() {
		add, done, err := isXMergedOps(1 << 9)
		t.NoError(err)

		add(0, 1, mergeFlag)
		add(0, 1, unmergeFlag)

		t.NoError(done(func(merged, unmerged []uint64) error {
			t.Empty(merged)
			t.Empty(unmerged)

			return nil
		}))
	})
}

func (t *testIsXMergedOPsFuncs) TestWithBase() {
	t.Run("no add", func() {
		_, done, err := isXMergedOps(1 << 9)
		t.NoError(err)

		t.NoError(done(func(merged, unmerged []uint64) error {
			t.Empty(merged)
			t.Empty(unmerged)

			return nil
		}))
	})

	t.Run("add; same in base", func() {
		add, done, err := isXMergedOps(1 << 9)
		t.NoError(err)

		add(-1, 1, mergeFlag)
		add(0, 1, mergeFlag)
		add(0, 2, mergeFlag)

		t.NoError(done(func(merged, unmerged []uint64) error {
			t.Equal([]uint64{2}, merged)
			t.Empty(unmerged)

			return nil
		}))
	})

	t.Run("add; not same in base", func() {
		add, done, err := isXMergedOps(1 << 9)
		t.NoError(err)

		add(-1, 1, mergeFlag)
		add(0, 1, unmergeFlag)
		add(0, 2, mergeFlag)

		t.NoError(done(func(merged, unmerged []uint64) error {
			t.Equal([]uint64{2}, merged)
			t.Equal([]uint64{1}, unmerged)

			return nil
		}))
	})

	t.Run("all same in base", func() {
		add, done, err := isXMergedOps(1 << 9)
		t.NoError(err)

		add(-1, 1, mergeFlag)
		add(-1, 2, mergeFlag)
		add(0, 1, mergeFlag)
		add(0, 2, mergeFlag)

		t.NoError(done(func(merged, unmerged []uint64) error {
			t.Empty(merged)
			t.Empty(unmerged)

			return nil
		}))
	})
}

func TestIsXMergedOPsFuncs(t *testing.T) {
	suite.Run(t, new(testIsXMergedOPsFuncs))
}

type testPhaseMerger struct {
	baseTestMergeOp
}

func (t *testPhaseMerger) newArgs() (
	vp util.LockedMap[uint64, []StateMergeValue],
	mergers util.LockedMap[string, StateMerger],
	states util.LockedMap[string, base.State],
	args *phaseMergerArgs,
	_ phaseMergerGetStateMergeValuesFunc,
	getState getStateFunc,
) {
	t.T().Helper()

	vp, mergers, states, _, getValues := t.baseTestMergeOp.getFuncs()

	getState = func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}

	args = newPhaseMergerArgs()

	return vp, mergers, states, args, getValues, getState
}

func (t *testPhaseMerger) TestCancel() {
	t.Run("ctx cancel; merge", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, mergers, _, args, getValues, getState := t.newArgs()

		getMergers, closeMergers := newGetMergers(mergers, getState)
		defer closeMergers()

		extractKeys, closeExtractKeys := newExtractStateMergeValueKeys(1 << 9)
		defer closeExtractKeys()

		merge, _, _, _, err := newPhaseMerger(ctx, args, getValues, getMergers, extractKeys)
		t.NoError(err)

		var opindex uint64 = 1

		t.randomMergeValuesVP(vp, opindex, 1)

		cancel()

		err = merge(opindex)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("ctx cancel; unmerge", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, mergers, _, args, getValues, getState := t.newArgs()

		getMergers, closeMergers := newGetMergers(mergers, getState)
		defer closeMergers()

		extractKeys, closeExtractKeys := newExtractStateMergeValueKeys(1 << 9)
		defer closeExtractKeys()

		_, unmerge, _, _, err := newPhaseMerger(ctx, args, getValues, getMergers, extractKeys)
		t.NoError(err)

		var opindex uint64 = 1

		_ = t.randomMergeValuesVP(vp, opindex, 1)

		cancel()

		err = unmerge(opindex)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("ctx cancel; wait", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, mergers, _, args, getValues, getState := t.newArgs()

		getMergers, closeMergers := newGetMergers(mergers, getState)
		defer closeMergers()

		extractKeys, closeExtractKeys := newExtractStateMergeValueKeys(1 << 9)
		defer closeExtractKeys()

		merge, _, done, wait, err := newPhaseMerger(ctx, args, getValues, getMergers, extractKeys)
		t.NoError(err)

		var opindex uint64 = 1

		t.randomMergeValuesVP(vp, opindex, 1)

		t.NoError(merge(opindex))

		cancel()

		done()
		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testPhaseMerger) TestMerge() {
	t.Run("ok, 1", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, mergers, _, args, getValues, getState := t.newArgs()

		getMergers, closeMergers := newGetMergers(mergers, getState)
		defer closeMergers()

		extractKeys, closeExtractKeys := newExtractStateMergeValueKeys(1 << 9)
		defer closeExtractKeys()

		merge, _, done, wait, err := newPhaseMerger(ctx, args, getValues, getMergers, extractKeys)
		t.NoError(err)

		var opindex uint64 = 1

		vs := t.randomMergeValuesVP(vp, opindex, 3)

		t.NoError(merge(opindex))

		done()

		t.NoError(wait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string) error {
			t.Empty(newMerged)
			t.Empty(newUnmerged)
			t.Empty(reasons)

			return nil
		}))

		merged, err := t.closeMergers(mergers)
		t.NoError(err)

		m, found := merged[vs[0].Key()]
		t.True(found)
		t.Equal(int64(3), m)
	})

	t.Run("ok, 33", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, mergers, _, args, getValues, getState := t.newArgs()

		getMergers, closeMergers := newGetMergers(mergers, getState)
		defer closeMergers()

		extractKeys, closeExtractKeys := newExtractStateMergeValueKeys(1 << 9)
		defer closeExtractKeys()

		merge, _, done, wait, err := newPhaseMerger(ctx, args, getValues, getMergers, extractKeys)
		t.NoError(err)

		vss := make([][]StateMergeValue, 33)
		for i := range vss {
			vss[i] = t.randomMergeValuesVP(vp, uint64(i), i+1)
		}

		t.NoError(util.TraverseSliceV1(vss, func(i int, values []StateMergeValue) (bool, error) {
			return true, merge(uint64(i))
		}))

		done()
		t.NoError(wait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string) error {
			t.Empty(newMerged)
			t.Empty(newUnmerged)
			t.Empty(reasons)

			return nil
		}))

		merged, err := t.closeMergers(mergers)
		t.NoError(err)
		t.Equal(33, len(merged))

		util.TraverseSliceV1(vss, func(_ int, values []StateMergeValue) (bool, error) {
			return true, util.TraverseSliceV1(values, func(_ int, value StateMergeValue) (bool, error) {
				i, found := merged[value.Key()]
				t.True(found)

				t.Equal(value.(AccountStateMergeValue).Balance, i)

				return true, nil
			})
		})
	})

	t.Run("Merge() error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, mergers, _, args, getValues, getState := t.newArgs()

		var opindex0, opindex1 uint64 = 1, 2

		vs0 := t.randomMergeValuesVP(vp, opindex0, 1)
		t.randomMergeValuesVP(vp, opindex1, 1)

		vs0keys := extractStateMergeValueKeysFromValues(vs0)

		origGetMergers, closeMergers := newGetMergers(mergers, getState)
		defer closeMergers()

		getMergers := func(
			stateKeys []string,
			f func(func(StateMergeValue) (StateMerger, error)) error,
		) error {
			if slices.Equal(stateKeys, vs0keys) {
				return errors.Errorf("showme")
			}

			return origGetMergers(stateKeys, f)
		}

		extractKeys, closeExtractKeys := newExtractStateMergeValueKeys(1 << 9)
		defer closeExtractKeys()

		merge, _, done, wait, err := newPhaseMerger(ctx, args, getValues, getMergers, extractKeys)
		t.NoError(err)

		t.NoError(merge(opindex0))

		err = merge(opindex1)
		if err != nil {
			t.ErrorContains(err, "showme")
		}

		done()
		err = wait(nil)
		t.ErrorContains(err, "showme")
	})

	t.Run("reason", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, mergers, _, args, getValues, getState := t.newArgs()

		getMergers, closeMergers := newGetMergers(mergers, getState)
		defer closeMergers()

		extractKeys, closeExtractKeys := newExtractStateMergeValueKeys(1 << 9)
		defer closeExtractKeys()

		merge, _, done, wait, err := newPhaseMerger(ctx, args, getValues, getMergers, extractKeys)
		t.NoError(err)

		var opindex0, opindex1 uint64 = 1, 2

		vs := t.randomMergeValuesVP(vp, opindex0, -1)
		t.NoError(merge(opindex0))

		vs = t.randomMergeValuesVP(vp, opindex1, 1)
		t.NoError(merge(opindex1))

		done()

		t.NoError(wait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string) error {
			t.Empty(newMerged)
			t.Equal([]uint64{1}, newUnmerged)
			t.Equal(map[uint64]string{1: "insufficient balance"}, reasons)

			return nil
		}))

		merged, err := t.closeMergers(mergers)
		t.NoError(err)

		m, found := merged[vs[0].Key()]
		t.True(found)
		t.Equal(int64(1), m)
	})
}

func (t *testPhaseMerger) TestUnmerge() {
	t.Run("ok, 1", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, mergers, states, args, getValues, getState := t.newArgs()

		pub := base.NewDummyPublickey(util.UUID().String())
		prevst := t.newState(pub, 1)
		states.SetValue(prevst.Key(), prevst)

		getMergers, closeMergers := newGetMergers(mergers, getState)
		defer closeMergers()

		extractKeys, closeExtractKeys := newExtractStateMergeValueKeys(1 << 9)
		defer closeExtractKeys()

		_, unmerge, done, wait, err := newPhaseMerger(ctx, args, getValues, getMergers, extractKeys)
		t.NoError(err)

		var opindex uint64 = 1

		vs := t.newMergeValuesVP(vp, opindex, pub, 1)

		t.NoError(unmerge(opindex))

		done()
		t.NoError(wait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string) error {
			t.Empty(newMerged)
			t.Empty(newUnmerged)
			t.Empty(reasons)

			return nil
		}))

		merged, err := t.closeMergers(mergers)
		t.NoError(err)

		m, found := merged[vs[0].Key()]
		t.True(found)
		t.Equal(int64(0), m)
	})

	t.Run("ok, 33", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, mergers, states, args, getValues, getState := t.newArgs()

		vss := make([][]StateMergeValue, 33)

		pub := base.NewDummyPublickey(util.UUID().String())

		getMergers, closeMergers := newGetMergers(mergers, getState)
		defer closeMergers()

		extractKeys, closeExtractKeys := newExtractStateMergeValueKeys(1 << 9)
		defer closeExtractKeys()

		_, unmerge, done, wait, err := newPhaseMerger(ctx, args, getValues, getMergers, extractKeys)
		t.NoError(err)

		var subed int64
		for i := range vss {
			opindex := uint64(i)

			vss[i] = t.newMergeValuesVP(vp, opindex, pub, i+1)

			subed -= int64(i) + 1
		}

		prevst := t.newState(pub, (subed)*-1)
		states.SetValue(prevst.Key(), prevst)

		t.NoError(util.TraverseSliceV1(vss, func(i int, values []StateMergeValue) (bool, error) {
			return true, unmerge(uint64(i))
		}))

		done()
		t.NoError(wait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string) error {
			t.Empty(newMerged)
			t.Empty(newUnmerged)
			t.Empty(reasons)

			return nil
		}))

		merged, err := t.closeMergers(mergers)
		t.NoError(err)
		t.Equal(1, len(merged))

		m, found := merged[vss[0][0].Key()]
		t.True(found)
		t.Equal(subed+prevst.Value().(AccountStateValue).Balance, m)
	})

	t.Run("Merge() error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, mergers, states, args, getValues, getState := t.newArgs()

		pub0 := base.NewDummyPublickey(util.UUID().String())
		prevst0 := t.newState(pub0, 1)
		pub1 := base.NewDummyPublickey(util.UUID().String())
		prevst1 := t.newState(pub1, 1)

		states.SetValue(prevst0.Key(), prevst0)
		states.SetValue(prevst1.Key(), prevst1)

		var opindex0, opindex1 uint64 = 1, 2

		vs0 := t.newMergeValuesVP(vp, opindex0, pub0, 1)
		_ = t.newMergeValuesVP(vp, opindex1, pub1, 1)

		vs0keys := extractStateMergeValueKeysFromValues(vs0)

		origGetMergers, closeMergers := newGetMergers(mergers, getState)
		defer closeMergers()

		getMergers := func(
			stateKeys []string,
			f func(func(StateMergeValue) (StateMerger, error)) error,
		) error {
			if slices.Equal(stateKeys, vs0keys) {
				return errors.Errorf("showme")
			}

			return origGetMergers(stateKeys, f)
		}

		extractKeys, closeExtractKeys := newExtractStateMergeValueKeys(1 << 9)
		defer closeExtractKeys()

		_, unmerge, done, wait, err := newPhaseMerger(ctx, args, getValues, getMergers, extractKeys)
		t.NoError(err)

		t.NoError(unmerge(opindex0))

		err = unmerge(opindex1)
		if err != nil {
			t.ErrorContains(err, "showme")
		}

		done()
		err = wait(nil)
		t.ErrorContains(err, "showme")
	})
}

func (t *testPhaseMerger) baseTestMergeUnmerge() (
	pubs []base.Publickey,
	states []base.State,
	ops [][]StateMergeValue,
) {
	pub0 := base.NewDummyPublickey("0")
	pub1 := base.NewDummyPublickey("1")

	st0 := t.newState(pub0, 2)
	st1 := t.newState(pub1, 2)

	ops = [][]StateMergeValue{
		{ // 0
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{ // 1: ignored
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{ // 2
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, 2),
		},
		{ // 3: ignored
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{ // 4
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub1, 2),
		},
	}

	return []base.Publickey{pub0, pub1},
		[]base.State{st0, st1},
		ops
}

func (t *testPhaseMerger) testMergeUnmerge(semSize int64, seq []int) {
	t.T().Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, sts, ops := t.baseTestMergeUnmerge()

	vp, mergers, states, args, getValues, getState := t.newArgs()
	args.NewPhaseJobWorker = func(ctx context.Context) (
		phaseJobWorkerNewJobFunc,
		func(),
		func() error,
		error,
	) {
		args := newConcPhaseJobWorkerArgs()
		args.NewJobWorker = func(context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, semSize)
		}

		args.SetLogging(logging.TestNilLogging)

		return newFuncConcPhaseJobWorker(ctx, args)
	}

	for i := range sts {
		states.SetValue(sts[i].Key(), sts[i])
	}

	getMergers, closeMergers := newGetMergers(mergers, getState)
	defer closeMergers()

	extractKeys, closeExtractKeys := newExtractStateMergeValueKeys(1 << 9)
	defer closeExtractKeys()

	merge, _, done, wait, err := newPhaseMerger(ctx, args, getValues, getMergers, extractKeys)
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
	var reasons map[uint64]string

	t.NoError(wait(func(ms, us []uint64, rs map[uint64]string) error {
		newMerged = slices.Clone(ms)
		newUnmerged = slices.Clone(us)
		reasons = rs

		return nil
	}))

	merged, err := t.closeMergers(mergers)
	t.NoError(err)
	TraverseMap(merged, func(key string, v int64) (bool, error) {
		switch key {
		case sts[0].Key():
			t.Equal(int64(3), v)
		case sts[1].Key():
			t.Equal(int64(4), v)
		default:
			t.Fail("unknown state key, %q", key)
		}

		return true, nil
	})

	t.Empty(newMerged)
	t.Equal([]uint64{1, 3}, newUnmerged)
	t.Equal(map[uint64]string{1: "previous issuer found", 3: "previous issuer found"}, reasons)
}

func (t *testPhaseMerger) TestMergeReplay() {
	seq := []int{3, 2, 1, 4, 0}
	t.testMergeUnmerge(9, seq)
}

func (t *testPhaseMerger) TestMergeUnmergeReplay() {
	_, sts, ops := t.baseTestMergeUnmerge()
	vp, mergers, states, _, _, _ := t.newArgs()

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

	getMerger := getOrCreateMergerFunc(mergers, getState)

	merge := func(opindex uint64) error {
		t.T().Logf("> merge: opindex=%d", opindex)

		var values []StateMergeValue

		switch i, found := vp.Value(opindex); {
		case !found:
			return util.ErrNotFound.Errorf("merge values, %d", opindex)
		default:
			values = i
		}

		switch isMerged, nexts, reason, err := mergeOp(
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

		var values []StateMergeValue

		switch i, found := vp.Value(opindex); {
		case !found:
			return util.ErrNotFound.Errorf("unmerge values, %d", opindex)
		default:
			values = i
		}

		switch isUnmerged, nexts, err := unmergeOp(
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

	merged, err := t.closeMergers(mergers)
	t.NoError(err)
	TraverseMap(merged, func(key string, v int64) (bool, error) {
		switch key {
		case sts[0].Key():
			t.Equal(int64(3), v)
		case sts[1].Key():
			t.Equal(int64(4), v)
		default:
			t.Fail("unknown state key, %q", key)
		}

		return true, nil
	})
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub0 := base.NewDummyPublickey(util.UUID().String())
	pub1 := base.NewDummyPublickey(util.UUID().String())
	pub2 := base.NewDummyPublickey(util.UUID().String())

	sts := make([]base.State, 3)
	sts[0] = t.newState(pub0, 1)
	sts[1] = t.newState(pub1, 2)
	sts[2] = t.newState(pub2, 3)

	ops := [][]StateMergeValue{
		{ // 0
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{ // 1: ignored
			t.newAccountStateMergeValue(pub1, -2),
			t.newAccountStateMergeValue(pub2, -3),
		},
		{ // 2
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, 2),
		},
		{ // 3: ignored
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{ // 4
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub2, -3),
		},
		{ // 5
			t.newAccountStateMergeValue(pub1, 1),
			t.newAccountStateMergeValue(pub2, 2),
		},
	}

	vp, mergers, states, args, getValues, getState := t.newArgs()
	args.NewPhaseJobWorker = func(ctx context.Context) (
		phaseJobWorkerNewJobFunc,
		func(),
		func() error,
		error,
	) {
		args := newConcPhaseJobWorkerArgs()
		args.NewJobWorker = func(context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, semSize)
		}
		args.SetLogging(logging.TestNilLogging)

		return newFuncConcPhaseJobWorker(ctx, args)
	}

	for i := range sts {
		states.SetValue(sts[i].Key(), sts[i])
	}

	getMergers, closeMergers := newGetMergers(mergers, getState)
	defer closeMergers()

	extractKeys, closeExtractKeys := newExtractStateMergeValueKeys(1 << 9)
	defer closeExtractKeys()

	merge, _, done, wait, err := newPhaseMerger(ctx, args, getValues, getMergers, extractKeys)
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
	var reasons map[uint64]string

	t.NoError(wait(func(ms, us []uint64, rs map[uint64]string) error {
		newMerged = slices.Clone(ms)
		newUnmerged = slices.Clone(us)
		reasons = rs

		return nil
	}))

	merged, err := t.closeMergers(mergers)
	t.NoError(err)
	t.Equal(3, len(merged))

	TraverseMap(merged, func(key string, v int64) (bool, error) {
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

		return true, nil
	})

	t.Empty(newMerged)
	t.Equal([]uint64{1, 3}, newUnmerged)
	t.Equal(map[uint64]string{1: "previous issuer found", 3: "previous issuer found"}, reasons)
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
