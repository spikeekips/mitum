package main

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

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
	args.UpdateReasonFunc = func(uint64, string) error { return nil }

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

		vs := t.randomMergeValuesVP(vp, opindex, 1)

		cancel()

		err = merge(opindex, vs)
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

		vs := t.randomMergeValuesVP(vp, opindex, 1)

		t.NoError(merge(opindex, vs))

		cancel()

		done()
		err = wait()
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

		t.NoError(merge(opindex, vs))

		done()
		t.NoError(wait())

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
			return true, merge(uint64(i), values)
		}))

		done()
		t.NoError(wait())

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
		vs1 := t.randomMergeValuesVP(vp, opindex1, 1)

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

		t.NoError(merge(opindex0, vs0))

		err = merge(opindex1, vs1)
		if err != nil {
			t.ErrorContains(err, "showme")
		}

		done()
		err = wait()
		t.ErrorContains(err, "showme")
	})

	t.Run("reason", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		vp, mergers, _, args := t.newArgs()

		reasonch := make(chan [2]interface{}, 1)
		args.UpdateReasonFunc = func(opindex uint64, reason string) error {
			reasonch <- [2]interface{}{opindex, reason}

			return nil
		}

		merge, _, done, wait, err := NewPhaseMerger(ctx, args)
		t.NoError(err)

		var opindex0, opindex1 uint64 = 1, 2

		vs := t.randomMergeValuesVP(vp, opindex0, -1)
		t.NoError(merge(opindex0, vs))

		vs = t.randomMergeValuesVP(vp, opindex1, 1)
		t.NoError(merge(opindex1, vs))

		done()
		t.NoError(wait())

		select {
		case <-time.After(time.Second):
			t.Fail("wait reason")
		case i := <-reasonch:
			t.Equal(opindex0, i[0])
			t.Equal("insufficient balance", i[1])
		}

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
		t.NoError(wait())

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
		t.NoError(wait())

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
		err = wait()
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
		return NewPhaseJobWorker(
			ctx,
			func(ctx context.Context) (JobWorker, error) {
				return NewBaseJobWorker(ctx, semSize)
			},
			1<<9,
		)
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

		t.NoError(merge(opindex, vs))
	}

	done()
	t.NoError(wait())

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

		switch nexts, reason, err := mergeOp(
			ctx,
			opindex,
			values,
			getMerger,
		); {
		case err != nil:
			return err
		default:
			t.T().Logf("< merge: opindex=%d nexts=%v reason=%q", opindex, nexts, reason)

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

		switch nexts, err := unmergeOp(
			ctx,
			opindex,
			values,
			getMerger,
		); {
		case err != nil:
			return err
		default:
			t.T().Logf("< unmerge: opindex=%d nexts=%v", opindex, nexts)

			return nil
		}
	}

	seq := [][2]interface{}{
		{"m", 3},
		{"m", 2},
		{"m", 1},
		{"u", 2},
		{"u", 3},
		{"m", 4},
		{"m", 0},
		{"u", 1},
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
	for i := 0; i < 9999; i++ {
		t.Run(fmt.Sprintf("%04d", i), func() {
			t.testMergeUnmerge(1<<9, nil)
		})
	}
}

func TestPhaseMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testPhaseMerger))
}
