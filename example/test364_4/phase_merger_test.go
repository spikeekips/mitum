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

type testXIsMergedOPsFuncs struct {
	baseTestMergeOp
}

func (t *testXIsMergedOPsFuncs) TestAddOne() {
	t.Run("add merged; no previous", func() {
		prevous := func(opindex uint64) xMergedFlag {
			return ""
		}

		add, done, err := addXMergedOpsFuncs(prevous, 2)
		t.NoError(err)

		add(0, MergedFlag)

		t.NoError(done(func(mergedOps, unmergedOps []uint64) error {
			t.Equal([]uint64{0}, mergedOps)
			t.Empty(unmergedOps)

			return nil
		}))
	})

	t.Run("add merged; same with previous", func() {
		prevous := func(opindex uint64) xMergedFlag {
			return MergedFlag
		}

		add, done, err := addXMergedOpsFuncs(prevous, 2)
		t.NoError(err)

		add(0, MergedFlag)

		t.NoError(done(func(mergedOps, unmergedOps []uint64) error {
			t.Empty(mergedOps)
			t.Empty(unmergedOps)

			return nil
		}))
	})

	t.Run("add merged; not same with previous", func() {
		prevous := func(opindex uint64) xMergedFlag {
			return UnmergedFlag
		}

		add, done, err := addXMergedOpsFuncs(prevous, 2)
		t.NoError(err)

		add(0, MergedFlag)

		t.NoError(done(func(mergedOps, unmergedOps []uint64) error {
			t.Equal([]uint64{0}, mergedOps)
			t.Empty(unmergedOps)

			return nil
		}))
	})

	t.Run("add unmerged; no previous", func() {
		prevous := func(opindex uint64) xMergedFlag {
			return ""
		}

		add, done, err := addXMergedOpsFuncs(prevous, 2)
		t.NoError(err)

		add(0, UnmergedFlag)

		t.NoError(done(func(mergedOps, unmergedOps []uint64) error {
			t.Empty(mergedOps)
			t.Equal([]uint64{0}, unmergedOps)

			return nil
		}))
	})

	t.Run("add unmerged; same with previous", func() {
		prevous := func(opindex uint64) xMergedFlag {
			return UnmergedFlag
		}

		add, done, err := addXMergedOpsFuncs(prevous, 2)
		t.NoError(err)

		add(0, UnmergedFlag)

		t.NoError(done(func(mergedOps, unmergedOps []uint64) error {
			t.Empty(mergedOps)
			t.Empty(unmergedOps)

			return nil
		}))
	})

	t.Run("add unmerged; not same with previous", func() {
		prevous := func(opindex uint64) xMergedFlag {
			return MergedFlag
		}

		add, done, err := addXMergedOpsFuncs(prevous, 2)
		t.NoError(err)

		add(0, UnmergedFlag)

		t.NoError(done(func(mergedOps, unmergedOps []uint64) error {
			t.Empty(mergedOps)
			t.Equal([]uint64{0}, unmergedOps)

			return nil
		}))
	})
}

func (t *testXIsMergedOPsFuncs) newBaseOps() util.LockedMap[uint64, xMergedFlag] {
	i, err := util.NewShardedMap[uint64, xMergedFlag](1<<9, nil)
	t.NoError(err)

	return i
}

func (t *testXIsMergedOPsFuncs) TestAdd() {
	t.Run("no add", func() {
		baseOps := t.newBaseOps()

		_, done, err := isXMergedOpsFuncs(baseOps, 1<<9)
		t.NoError(err)

		t.NoError(done(func(mergedOps, unmergedOps []uint64) error {
			t.Empty(mergedOps)
			t.Empty(unmergedOps)

			return nil
		}))
		t.Equal(0, baseOps.Len())
	})

	t.Run("add -1 phase", func() {
		baseOps := t.newBaseOps()

		add, done, err := isXMergedOpsFuncs(baseOps, 1<<9)
		t.NoError(err)

		add(-1, 1, MergedFlag)
		add(-1, 2, UnmergedFlag)

		t.NoError(done(func(mergedOps, unmergedOps []uint64) error {
			t.Empty(mergedOps)
			t.Empty(unmergedOps)

			return nil
		}))
		t.Equal(map[uint64]xMergedFlag{
			1: MergedFlag,
			2: UnmergedFlag,
		}, baseOps.Map())
	})

	t.Run("add over 0 phase; empty 0 phase", func() {
		baseOps := t.newBaseOps()

		add, done, err := isXMergedOpsFuncs(baseOps, 1<<9)
		t.NoError(err)

		add(1, 1, MergedFlag)
		add(1, 2, UnmergedFlag)

		t.NoError(done(func(mergedOps, unmergedOps []uint64) error {
			t.Equal([]uint64{1}, mergedOps)
			t.Equal([]uint64{2}, unmergedOps)

			return nil
		}))
		t.Equal(map[uint64]xMergedFlag{
			1: MergedFlag,
			2: UnmergedFlag,
		}, baseOps.Map())
	})

	t.Run("add over 0 phase; not found in 0 phase", func() {
		baseOps := t.newBaseOps()
		baseOps.SetValue(0, MergedFlag)

		add, done, err := isXMergedOpsFuncs(baseOps, 1<<9)
		t.NoError(err)

		add(1, 1, MergedFlag)
		add(1, 2, UnmergedFlag)

		t.NoError(done(func(mergedOps, unmergedOps []uint64) error {
			t.Equal([]uint64{1}, mergedOps)
			t.Equal([]uint64{2}, unmergedOps)

			return nil
		}))
		t.Equal(map[uint64]xMergedFlag{
			0: MergedFlag,
			1: MergedFlag,
			2: UnmergedFlag,
		}, baseOps.Map())
	})

	t.Run("add over 0 phase; matched in 0 phase", func() {
		baseOps := t.newBaseOps()
		baseOps.SetValue(1, MergedFlag)

		add, done, err := isXMergedOpsFuncs(baseOps, 1<<9)
		t.NoError(err)

		add(1, 1, MergedFlag)
		add(1, 2, UnmergedFlag)

		t.NoError(done(func(mergedOps, unmergedOps []uint64) error {
			t.Empty(mergedOps)
			t.Equal([]uint64{2}, unmergedOps)

			return nil
		}))
		t.Equal(map[uint64]xMergedFlag{
			1: MergedFlag,
			2: UnmergedFlag,
		}, baseOps.Map())
	})

	t.Run("add over 0 phase; matched later in 0 phase", func() {
		baseOps := t.newBaseOps()

		add, done, err := isXMergedOpsFuncs(baseOps, 1<<9)
		t.NoError(err)

		add(1, 1, MergedFlag)
		add(1, 2, UnmergedFlag)

		baseOps.SetValue(1, MergedFlag)

		t.NoError(done(func(mergedOps, unmergedOps []uint64) error {
			t.Empty(mergedOps)
			t.Equal([]uint64{2}, unmergedOps)

			return nil
		}))
		t.Equal(map[uint64]xMergedFlag{
			1: MergedFlag,
			2: UnmergedFlag,
		}, baseOps.Map())
	})

	t.Run("add from 0 phase", func() {
		baseOps := t.newBaseOps()

		add, done, err := isXMergedOpsFuncs(baseOps, 1<<9)
		t.NoError(err)

		add(-1, 0, MergedFlag)
		add(-1, 1, UnmergedFlag)
		add(1, 2, MergedFlag)
		add(1, 3, UnmergedFlag)

		t.NoError(done(func(mergedOps, unmergedOps []uint64) error {
			t.Equal([]uint64{2}, mergedOps)
			t.Equal([]uint64{3}, unmergedOps)

			return nil
		}))
		t.Equal(map[uint64]xMergedFlag{
			0: MergedFlag,
			1: UnmergedFlag,
			2: MergedFlag,
			3: UnmergedFlag,
		}, baseOps.Map())
	})
}

func TestXIsMergedOPsFuncs(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testXIsMergedOPsFuncs))
}

type testPhaseMerger struct {
	baseTestMergeOp
}

func (t *testPhaseMerger) newArgs() (
	vp util.LockedMap[uint64, []StateMergeValue],
	mergers util.LockedMap[string, StateMerger],
	states util.LockedMap[string, base.State],
	args *PhaseMergerArgs,
) {
	vp, mergers, states, _, getValues := t.baseTestMergeOp.getFuncs()

	getState := func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}

	args = NewPhaseMergerArgs()
	args.GetMergersFunc, _ = newGetMergersFunc(mergers, getState)
	args.GetValuesFunc = getValues

	extractKeys, _ := newExtractStateMergeValueKeysFunc(1 << 9)
	args.ExtractStateMergeValueKeysFunc = extractKeys

	return vp, mergers, states, args
}

func (t *testPhaseMerger) TestCancel() {
	t.Run("ctx cancel; merge", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, _, _, args := t.newArgs()

		merge, _, _, _, err := NewPhaseMerger(ctx, args)
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

		vp, _, _, args := t.newArgs()

		_, unmerge, _, _, err := NewPhaseMerger(ctx, args)
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

		vp, _, _, args := t.newArgs()

		merge, _, done, wait, err := NewPhaseMerger(ctx, args)
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

		vp, mergers, _, args := t.newArgs()

		merge, _, done, wait, err := NewPhaseMerger(ctx, args)
		t.NoError(err)

		var opindex uint64 = 1

		vs := t.randomMergeValuesVP(vp, opindex, 3)

		t.NoError(merge(opindex))

		done()

		t.NoError(wait(func(newlyMergedOps, newlyUnmergedOps []uint64, reasons map[uint64]string) error {
			t.Empty(newlyMergedOps)
			t.Empty(newlyUnmergedOps)
			t.Empty(reasons)

			return nil
		}))

		merged := t.closeMergers(mergers)

		m, found := merged[vs[0].Key()]
		t.True(found)
		t.Equal(int64(3), m)
	})

	t.Run("ok, 33", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, mergers, _, args := t.newArgs()

		merge, _, done, wait, err := NewPhaseMerger(ctx, args)
		t.NoError(err)

		vss := make([][]StateMergeValue, 33)
		for i := range vss {
			vss[i] = t.randomMergeValuesVP(vp, uint64(i), i+1)
		}

		t.NoError(util.TraverseSliceV1(vss, func(i int, values []StateMergeValue) (bool, error) {
			return true, merge(uint64(i))
		}))

		done()
		t.NoError(wait(func(newlyMergedOps, newlyUnmergedOps []uint64, reasons map[uint64]string) error {
			t.Empty(newlyMergedOps)
			t.Empty(newlyUnmergedOps)
			t.Empty(reasons)

			return nil
		}))

		merged := t.closeMergers(mergers)
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

		vp, _, _, args := t.newArgs()

		var opindex0, opindex1 uint64 = 1, 2

		vs0 := t.randomMergeValuesVP(vp, opindex0, 1)
		t.randomMergeValuesVP(vp, opindex1, 1)

		vs0keys := extractStateMergeValueKeys(vs0)

		getMergersFunc := args.GetMergersFunc

		args.GetMergersFunc = func(
			stateKeys []string,
			f func(func(StateMergeValue) (StateMerger, error)) error,
		) error {
			if slices.Equal(stateKeys, vs0keys) {
				return errors.Errorf("showme")
			}

			return getMergersFunc(stateKeys, f)
		}

		merge, _, done, wait, err := NewPhaseMerger(ctx, args)
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

		vp, mergers, _, args := t.newArgs()

		merge, _, done, wait, err := NewPhaseMerger(ctx, args)
		t.NoError(err)

		var opindex0, opindex1 uint64 = 1, 2

		vs := t.randomMergeValuesVP(vp, opindex0, -1)
		t.NoError(merge(opindex0))

		vs = t.randomMergeValuesVP(vp, opindex1, 1)
		t.NoError(merge(opindex1))

		done()

		t.NoError(wait(func(newlyMergedOps, newlyUnmergedOps []uint64, reasons map[uint64]string) error {
			t.Empty(newlyMergedOps)
			t.Equal([]uint64{1}, newlyUnmergedOps)
			t.Equal(map[uint64]string{1: "insufficient balance"}, reasons)

			return nil
		}))

		merged := t.closeMergers(mergers)

		m, found := merged[vs[0].Key()]
		t.True(found)
		t.Equal(int64(1), m)
	})
}

func (t *testPhaseMerger) TestUnmerge() {
	t.Run("ok, 1", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, mergers, states, args := t.newArgs()

		pub := base.NewDummyPublickey(util.UUID().String())
		prevst := t.newState(pub, 1)
		states.SetValue(prevst.Key(), prevst)

		_, unmerge, done, wait, err := NewPhaseMerger(ctx, args)
		t.NoError(err)

		var opindex uint64 = 1

		vs := t.newMergeValuesVP(vp, opindex, pub, 1)

		t.NoError(unmerge(opindex))

		done()
		t.NoError(wait(func(newlyMergedOps, newlyUnmergedOps []uint64, reasons map[uint64]string) error {
			t.Empty(newlyMergedOps)
			t.Empty(newlyUnmergedOps)
			t.Empty(reasons)

			return nil
		}))

		merged := t.closeMergers(mergers)

		m, found := merged[vs[0].Key()]
		t.True(found)
		t.Equal(int64(0), m)
	})

	t.Run("ok, 33", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, mergers, states, args := t.newArgs()

		vss := make([][]StateMergeValue, 33)

		pub := base.NewDummyPublickey(util.UUID().String())
		prevst := t.newState(pub, int64(len(vss)+1))
		states.SetValue(prevst.Key(), prevst)

		_, unmerge, done, wait, err := NewPhaseMerger(ctx, args)
		t.NoError(err)

		var subed int64
		for i := range vss {
			vss[i] = t.newMergeValuesVP(vp, uint64(i), pub, i+1)

			subed -= int64(i) + 1
		}

		t.NoError(util.TraverseSliceV1(vss, func(i int, values []StateMergeValue) (bool, error) {
			return true, unmerge(uint64(i))
		}))

		done()
		t.NoError(wait(func(newlyMergedOps, newlyUnmergedOps []uint64, reasons map[uint64]string) error {
			t.Empty(newlyMergedOps)
			t.Empty(newlyUnmergedOps)
			t.Empty(reasons)

			return nil
		}))

		merged := t.closeMergers(mergers)
		t.Equal(1, len(merged))

		m, found := merged[vss[0][0].Key()]
		t.True(found)
		t.Equal(subed+prevst.Value().(AccountStateValue).Balance, m)
	})

	t.Run("Merge() error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, _, states, args := t.newArgs()

		pub0 := base.NewDummyPublickey(util.UUID().String())
		prevst0 := t.newState(pub0, 1)
		pub1 := base.NewDummyPublickey(util.UUID().String())
		prevst1 := t.newState(pub1, 1)

		states.SetValue(prevst0.Key(), prevst0)
		states.SetValue(prevst1.Key(), prevst1)

		var opindex0, opindex1 uint64 = 1, 2

		vs0 := t.newMergeValuesVP(vp, opindex0, pub0, 1)
		_ = t.newMergeValuesVP(vp, opindex1, pub1, 1)

		vs0keys := extractStateMergeValueKeys(vs0)

		getMergersFunc := args.GetMergersFunc

		args.GetMergersFunc = func(
			stateKeys []string,
			f func(func(StateMergeValue) (StateMerger, error)) error,
		) error {
			if slices.Equal(stateKeys, vs0keys) {
				return errors.Errorf("showme")
			}

			return getMergersFunc(stateKeys, f)
		}

		_, unmerge, done, wait, err := NewPhaseMerger(ctx, args)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, sts, ops := t.baseTestMergeUnmerge()

	vp, mergers, states, args := t.newArgs()
	args.NewPhaseJobWorkerFunc = func(ctx context.Context) (
		PhaseJobWorkerNewJobFunc,
		func(),
		func() error,
		error,
	) {
		args := newConcPhaseJobWorkerArgs()
		args.NewJobWorker = func(context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, semSize)
		}

		args.SetLogging(logging.TestNilLogging)

		return NewConcPhaseJobWorker(ctx, args)
	}

	for i := range sts {
		states.SetValue(sts[i].Key(), sts[i])
	}

	merge, _, done, wait, err := NewPhaseMerger(ctx, args)
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

	var newlyMergedOps, newlyUnmergedOps []uint64
	var reasons map[uint64]string

	t.NoError(wait(func(ms, us []uint64, rs map[uint64]string) error {
		newlyMergedOps = slices.Clone(ms)
		newlyUnmergedOps = slices.Clone(us)
		reasons = rs

		return nil
	}))

	m := t.closeMergers(mergers)
	TraverseMap(m, func(key string, v int64) (bool, error) {
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

	t.Empty(newlyMergedOps)
	t.Equal([]uint64{1, 3}, newlyUnmergedOps)
	t.Equal(map[uint64]string{1: "previous issuer found", 3: "previous issuer found"}, reasons)
}

func (t *testPhaseMerger) TestMergeReplay() {
	seq := []int{3, 2, 1, 4, 0}
	t.testMergeUnmerge(9, seq)
}

func (t *testPhaseMerger) TestMergeUnmergeReplay() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, sts, ops := t.baseTestMergeUnmerge()
	vp, mergers, states, _ := t.newArgs()

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
			ctx,
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
			ctx,
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

	m := t.closeMergers(mergers)
	TraverseMap(m, func(key string, v int64) (bool, error) {
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

	vp, mergers, states, args := t.newArgs()
	args.NewPhaseJobWorkerFunc = func(ctx context.Context) (
		PhaseJobWorkerNewJobFunc,
		func(),
		func() error,
		error,
	) {
		args := newConcPhaseJobWorkerArgs()
		args.NewJobWorker = func(context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, semSize)
		}
		args.SetLogging(logging.TestNilLogging)

		return NewConcPhaseJobWorker(ctx, args)
	}

	for i := range sts {
		states.SetValue(sts[i].Key(), sts[i])
	}

	merge, _, done, wait, err := NewPhaseMerger(ctx, args)
	t.NoError(err)

	if seq == nil {
		seq = make([]int, len(ops))
		for i := range ops {
			seq[i] = i
		}
	}

	t.T().Log("seq:", seq)
	for i := range seq {
		opindex := uint64(seq[i])
		vs := ops[opindex]

		vp.SetValue(opindex, vs)

		t.NoError(merge(opindex))
	}

	done()

	var newlyMergedOps, newlyUnmergedOps []uint64
	var reasons map[uint64]string

	t.NoError(wait(func(ms, us []uint64, rs map[uint64]string) error {
		newlyMergedOps = slices.Clone(ms)
		newlyUnmergedOps = slices.Clone(us)
		reasons = rs

		return nil
	}))

	m := t.closeMergers(mergers)
	TraverseMap(m, func(key string, v int64) (bool, error) {
		switch key {
		case sts[0].Key():
			t.Equal(int64(3), v)
		default:
			t.Fail("unknown state key, %q", key)
		}

		return true, nil
	})

	t.Empty(newlyMergedOps)
	t.Equal([]uint64{1, 3}, newlyUnmergedOps)
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
