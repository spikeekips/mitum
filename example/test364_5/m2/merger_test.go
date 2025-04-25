package m2

import (
	"context"
	"fmt"
	"iter"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	dhash "github.com/spikeekips/mitum/example/test364_5/hash"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"gonum.org/v1/gonum/stat/combin"
)

type testMerger struct {
	dutil.BaseSuite
}

func (t *testMerger) newState(pub base.Publickey, balance int64) base.State {
	return dop5.NewBaseState(
		33,
		accountStateKey(pub),
		NewAccountSV(pub, balance),
		nil,
		[]util.Hash{dhash.RandomHex64()},
	)
}

func (t *testMerger) newMergeValue(address base.Publickey, balance int64) *AccountSMV {
	v, err := NewAccountSMV(address, balance)
	t.NoError(err)

	return v
}

func (t *testMerger) preset() (
	states map[string]base.State,
	mergeValues [][2][]SMV,
	expectedMerged map[string]int64,
	expectedReasons map[uint64]string,
) {
	pub0 := base.NewDummyPublickey("p0")
	pub1 := base.NewDummyPublickey("p1")
	pub2 := base.NewDummyPublickey("p2")

	s0 := t.newState(pub0, 1)
	s1 := t.newState(pub1, 2)
	s2 := t.newState(pub2, 3)
	states = map[string]base.State{
		s0.Key(): s0,
		s1.Key(): s1,
		s2.Key(): s2,
	}

	nv, nnv := t.newMergeValue, t.newMergeValue

	mergeValues = [][2][]SMV{
		{{nv(pub1, -2)}, {nv(pub0, 1)}},    // 0
		{{nv(pub1, -2)}, {nv(pub2, 3)}},    // 1: ignored
		{{nv(pub0, -1)}, {nv(pub1, 3)}},    // 2
		{{nv(pub0, -1)}, {nv(pub1, 2)}},    // 3: ignored
		{{nv(pub2, -2)}, {nv(pub0, 1)}},    // 4
		{nil, {nnv(pub2, 3), nv(pub1, 1)}}, // 5
	}

	expectedMerged = map[string]int64{
		s0.Key(): 2,
		s1.Key(): 4,
		s2.Key(): 4,
	}
	expectedReasons = map[uint64]string{
		1: "previous issuer found",
		3: "previous issuer found",
	}

	return
}

func (t *testMerger) TestMerge() {
	states, mergeValues, expectedMerged, expectedReasons := t.preset()

	setSMVs, getSMVs, closeSMVs, err := newFuncAddValuePool(1 << 9)
	t.NoError(err)
	defer closeSMVs()

	for i, sv := range slices.All(mergeValues) {
		if len(sv[1]) < 1 {
			continue
		}

		var sub SMV
		if len(sv[0]) > 0 {
			sub = sv[0][0]
		}

		t.True(setSMVs(uint64(i), sub, sv[1]))
	}

	getAccountSVM := NewFuncAccountSVM(func(stateKey string) (base.StateValue, bool, error) {
		sv, found := states[stateKey]
		if found {
			return sv.Value(), true, nil
		}

		return nil, false, nil
	})

	addSVMFunc, newGetSVM, closeSVMFuncSet := newSVMFuncSet()
	defer closeSVMFuncSet()

	t.True(addSVMFunc(AccountSMVHint, getAccountSVM))

	run := func(seq []int, verbose bool) {
		logf := func(s string, a ...any) {}

		if verbose {
			logf = func(s string, a ...any) {
				t.T().Logf(s, a...)
			}
		}

		getSVM, seqSVM, err := newGetSVM(1 << 9)
		t.NoError(err)

		setReason, doneReasons, err := newFuncUnmergedReason(1 << 9)
		t.NoError(err)

		subMergeJobf := newFuncSubMergeJob(getSVM)
		addMergeJobf := newFuncAddMergeJob(getSVM)
		unmergeJobf := newFuncUnmergeJob(getSVM)

		{
			for i := range slices.Values(seq) {
				opindex := uint64(i)
				vs := mergeValues[opindex]

				for v := range slices.Values(vs[0]) {
					next, reason, err := subMergeJobf(opindex, v)
					logf("sub merge: opindex=%d next=%d reason=%q err=%v", opindex, next, reason, err)
					t.NoError(err)

					if reason != "" {
						t.NoError(setReason(next, reason))

						_, vs, found := getSMVs(next)
						t.True(found)

						logf("\tnext=%d values=%d", next, len(vs))

						for v := range slices.Values(vs) {
							err := unmergeJobf(next, v)
							logf("unmerge: opindex=%d err=%v", next, err)
						}
					}
				}

				for v := range slices.Values(vs[1]) {
					err := addMergeJobf(opindex, v)
					logf("add merge: opindex=%d err=%v", opindex, err)
					t.NoError(err)
				}
			}
		}

		merged := map[string]int64{}
		for m := range seqSVM() {
			sv, err := m.Close()
			t.NoError(err)
			if sv == nil {
				continue
			}

			merged[m.Key()] = sv.(*AccountSV).Balance
		}

		t.Equal(expectedMerged, merged)
		t.Equal(expectedReasons, doneReasons())
	}

	gen := combin.NewPermutationGenerator(len(mergeValues), len(mergeValues))

	var count int

	for gen.Next() {
		seq := gen.Permutation(nil)

		fmt.Fprintf(os.Stdout, "%v ", seq)
		count++

		if count%99 == 0 {
			fmt.Fprintln(os.Stdout)
		}

		run(seq, false)
	}
	fmt.Fprintln(os.Stdout)
}

func (t *testMerger) TestMerger() {
	states, mergeValues, expectedMerged, expectedReasons := t.preset()
	unmergeOps := []uint64{1, 5}
	expectedMerged = map[string]int64{
		"ta-p0dpu": 2,
		"ta-p1dpu": 3,
		"ta-p2dpu": 1,
	}

	expectedOps := map[string][]uint64{
		"ta-p0dpu": {0, 2, 4},
		"ta-p1dpu": {0, 2},
		"ta-p2dpu": {4},
	}

	setSMVs, getSMVs, closeSMVs, err := newFuncAddValuePool(1 << 9)
	t.NoError(err)
	defer closeSMVs()

	for i, sv := range slices.All(mergeValues) {
		if len(sv[1]) < 1 {
			continue
		}

		var sub SMV
		if len(sv[0]) > 0 {
			sub = sv[0][0]
		}

		t.True(setSMVs(uint64(i), sub, sv[1]))
	}

	getAccountSVM := NewFuncAccountSVM(func(stateKey string) (base.StateValue, bool, error) {
		sv, found := states[stateKey]
		if found {
			return sv.Value(), true, nil
		}

		return nil, false, nil
	})

	addSVMFunc, newGetSVM, closeSVMFuncSet := newSVMFuncSet()
	defer closeSVMFuncSet()

	t.True(addSVMFunc(AccountSMVHint, getAccountSVM))

	run := func(seq []int) {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		getSVM, seqSVM, err := newGetSVM(1 << 9)
		t.NoError(err)

		reasons := util.NewSingleLockedMap[uint64, string]()
		defer reasons.Close()

		extractKeys, closeExtractKeys := newFuncExtractSMVKeys(ctx, 1<<9)
		defer closeExtractKeys()

		merge, mergeDone, unmerge, unmergeDone, wait, err := NewMerger(
			ctx,
			1<<9,
			getSMVs,
			getSVM,
			extractKeys,
			func(_ context.Context, opindex uint64, reason string) error {
				_ = reasons.SetValue(opindex, reason)

				return nil
			},
		)
		t.NoError(err)

		var wg sync.WaitGroup
		var unmerged int64

		for i := range slices.Values(seq) {
			wg.Add(1)

			opindex := uint64(i)

			go func() {
				defer wg.Done()

				switch {
				case i == len(mergeValues):
					t.IfErrPanic(unmerge(unmergeOps[0]))
					atomic.AddInt64(&unmerged, 1)
				case i == len(mergeValues)+1:
					t.IfErrPanic(unmerge(unmergeOps[1]))
					atomic.AddInt64(&unmerged, 1)
				default:
					t.IfErrPanic(merge(opindex))
				}

				if atomic.LoadInt64(&unmerged) == int64(len(unmergeOps)) {
					unmergeDone()
				}
			}()
		}

		wg.Wait()

		mergeDone()

		t.NoError(wait(
			func(size int, getStateOps getStateOpsFunc) error {
				merged := map[string]int64{}
				for m := range seqSVM() {
					sv, err := m.Close()
					t.NoError(err)

					var balance int64
					if sv != nil {
						balance = sv.(*AccountSV).Balance
					}

					merged[m.Key()] = balance

					ops := getStateOps(m.Key())
					slices.Sort(ops)

					t.Equal(expectedOps[m.Key()], ops)
				}

				t.Equal(expectedMerged, merged)

				return nil
			},
		))

		t.Equal(expectedReasons, reasons.Map())
	}

	// run([]int{0, 4, 6, 3, 7, 5, 2, 1})
	// return

	gen := combin.NewPermutationGenerator(len(mergeValues)+2, len(mergeValues)+2)

	var count int

	for gen.Next() {
		seq := gen.Permutation(nil)

		fmt.Fprintf(os.Stdout, "%v ", seq)

		if count%99 == 0 {
			fmt.Fprintln(os.Stdout)
		}

		run(seq)

		count++
	}
	fmt.Fprintln(os.Stdout)
}

func TestMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testMerger))
}

func runBenchmarkMerger(n int) (func(int), func()) {
	poolSize := uint64(min(n, 1<<9))

	pubs := make([]base.Publickey, n)
	for i := range pubs {
		pubs[i] = base.NewDummyPublickey(strconv.FormatInt(int64(i), 10))
	}

	states := map[string]base.State{}
	for i := range n {
		st := dop5.NewBaseState(
			33,
			accountStateKey(pubs[i]),
			NewAccountSV(pubs[i], 1),
			nil,
			[]util.Hash{dhash.RandomHex64()},
		)
		states[st.Key()] = st
	}

	setSMVs, getSMVs, closeSMVs, _ := newFuncAddValuePool(poolSize)

	values := make([][2]SMV, n)

	for i := range values {
		opindex := uint64(i)
		sub, _ := NewAccountSMV(pubs[i], -1)
		add, _ := NewAccountSMV(pubs[(i+1)%n], 1)

		values[i] = [2]SMV{sub, add}

		setSMVs(opindex, sub, []SMV{add})
	}

	addSVMFunc, newGetSVM, closeSVMFuncSet := newSVMFuncSet()

	getAccountSVM := NewFuncAccountSVM(func(stateKey string) (base.StateValue, bool, error) {
		sv, found := states[stateKey]
		if found {
			return sv.Value(), true, nil
		}

		return nil, false, nil
	})
	_ = addSVMFunc(AccountSMVHint, getAccountSVM)

	return func(n int) {
			getSVM, seqSVM, _ := newGetSVM(poolSize)
			defer func() {
				_, stop := iter.Pull(seqSVM())
				stop()
			}()

			ctx, cancel := dutil.ContextWithCancelCause(context.Background())
			defer cancel(nil)

			extractKeys, closeExtractKeys := newFuncExtractSMVKeys(ctx, 1<<9)
			defer closeExtractKeys()

			merge, mergeDone, _, unmergeDone, wait, err := NewMerger(
				ctx,
				poolSize,
				getSMVs,
				getSVM,
				extractKeys,
				func(_ context.Context, opindex uint64, reason string) error { return nil },
			)
			if err != nil {
				panic(err)
			}

			for i := range n {
				opindex := uint64(i)

				if err := merge(opindex); err != nil {
					panic(err)
				}
			}

			mergeDone()
			unmergeDone()

			if err := wait(
				func(int, getStateOpsFunc) error { return nil },
			); err != nil {
				panic(err)
			}
		},
		func() {
			closeSMVs()
			closeSVMFuncSet()

			dutil.ClearSlice(&pubs)
			dutil.ClearSlice(&values)
			clear(states)
		}
}

func BenchmarkMerger(bb *testing.B) {
	size := 50000
	run, closef := runBenchmarkMerger(size)
	defer closef()

	for i := 0; i <= 8000; i += 2000 {
		if i < 1 {
			continue
		}

		bb.Run(fmt.Sprintf("%d", i), func(b *testing.B) {
			for b.Loop() {
				run(i)
			}
		})
	}

	for i := 0; i <= size; i += 10000 {
		if i < 1 {
			continue
		}

		bb.Run(fmt.Sprintf("%d", i), func(b *testing.B) {
			for b.Loop() {
				run(i)
			}
		})
	}
}

func newFuncAddMergeJob(
	mergeStateValueMerger MergeSVMFunc,
) func(uint64, SMV) error {
	return func(opindex uint64, value SMV) error {
		return mergeStateValueMerger(value, func(merger SVM) error {
			return merger.AddMerge(opindex, value.Value())
		})
	}
}

func newFuncUnmergeJob(
	mergeStateValueMerger MergeSVMFunc,
) func(uint64, SMV) error {
	return func(opindex uint64, value SMV) error {
		return mergeStateValueMerger(value, func(merger SVM) error {
			return merger.Unmerge(opindex, value.Value())
		})
	}
}
