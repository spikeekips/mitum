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
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"gonum.org/v1/gonum/stat/combin"
)

type baseMergerSuite struct {
	dutil.BaseSuite
}

func (t *baseMergerSuite) workers(n int) (string, []string) {
	w := make([]string, n)
	for i := range w {
		w[i] = fmt.Sprintf("w%d", i)
	}

	return w[0], w
}

func (t *baseMergerSuite) newState(pub base.Publickey, balance int64) (string, base.StateValue) {
	return accountStateKey(pub),
		NewAccountSV(pub, balance)
}

func (t *baseMergerSuite) newMergeValue(address base.Publickey, balance int64) *AccountSMV {
	v, err := NewAccountSMV(address, balance)
	t.NoError(err)

	return v
}

func (t *baseMergerSuite) randomMergeValues(n int) []SMV {
	v := make([]SMV, n)

	for i := range v {
		v[i] = t.newMergeValue(base.NewDummyPublickey("p0"), 1)
	}

	return v
}

func (t *baseMergerSuite) newGetSVM(states map[string]base.StateValue) NewSVMPoolFunc {
	addSVMFunc, newGetSVM, closeSVMFuncSet := newSVMFuncSet()

	t.T().Cleanup(closeSVMFuncSet)

	getAccountSVM := NewFuncAccountSVM(func(stateKey string) (base.StateValue, bool, error) {
		sv, found := states[stateKey]
		if found {
			return sv, true, nil
		}

		return nil, false, nil
	})
	_ = addSVMFunc(AccountSMVHint, getAccountSVM)

	return newGetSVM
}

func (t baseMergerSuite) closeMergers(getStateOps getStateOpsFunc, seqSVM iter.Seq[SVM]) (
	merged map[string]int64,
	mergedOps map[string][]uint64,
) {
	merged = map[string]int64{}
	mergedOps = map[string][]uint64{}

	for m := range seqSVM {
		sv, err := m.Close()
		t.NoError(err)

		var balance int64
		if sv != nil {
			balance = sv.(*AccountSV).Balance
		}

		merged[m.Key()] = balance

		ops := getStateOps(m.Key())
		slices.Sort(ops)

		mergedOps[m.Key()] = ops
	}

	return
}

func (t *baseMergerSuite) preset() (
	states map[string]base.StateValue,
	mergeValues [][2][]SMV,
	expectedMerged map[string]int64,
	expectedReasons map[uint64]string,
	expectedOps map[string][]uint64,
) {
	pub0 := base.NewDummyPublickey("p0")
	pub1 := base.NewDummyPublickey("p1")
	pub2 := base.NewDummyPublickey("p2")

	k0, s0 := t.newState(pub0, 1)
	k1, s1 := t.newState(pub1, 2)
	k2, s2 := t.newState(pub2, 3)
	states = map[string]base.StateValue{
		k0: s0,
		k1: s1,
		k2: s2,
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
		k0: 2,
		k1: 4,
		k2: 4,
	}
	expectedReasons = map[uint64]string{
		1: "previous issuer found",
		3: "previous issuer found",
	}

	expectedOps = map[string][]uint64{
		"ta-p0dpu": {0, 2, 4},
		"ta-p1dpu": {0, 2, 5},
		"ta-p2dpu": {4, 5},
	}

	return
}

type testMerger struct {
	baseMergerSuite
}

func (t *testMerger) TestMerger() {
	states, mergeValues, expectedMerged, expectedReasons, expectedOps := t.preset()
	unmergeOps := []uint64{1, 5}
	expectedMerged = map[string]int64{
		"ta-p0dpu": 2,
		"ta-p1dpu": 3,
		"ta-p2dpu": 1,
	}

	expectedOps = map[string][]uint64{
		"ta-p0dpu": {0, 2, 4},
		"ta-p1dpu": {0, 2},
		"ta-p2dpu": {4},
	}

	newGetSVM := t.newGetSVM(states)

	run := func(seq []int) error {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		getSVM, seqSVM, err := newGetSVM(1 << 9)
		t.NoError(err)

		reasons := dutil.NewSingleLockedMap[uint64, string]()
		defer reasons.Close()

		merge, mergeDone, unmerge, unmergeDone, wait, err := newMerger(
			ctx,
			1<<9,
			getSVM,
			func(opindex uint64, reason string) error {
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
					vs := mergeValues[opindex]
					var sub SMV
					if len(vs[0]) > 0 {
						sub = vs[0][0]
					}
					t.IfErrPanic(merge(opindex, sub, vs[1]))
				}

				if atomic.LoadInt64(&unmerged) == int64(len(unmergeOps)) {
					unmergeDone()
				}
			}()
		}

		wg.Wait()

		mergeDone()

		if err := wait(
			func(size int, getStateOps getStateOpsFunc) error {
				merged, mergedOps := t.closeMergers(getStateOps, seqSVM())

				if !t.Equal(expectedMerged, merged) {
					return errors.Errorf("t.Equal(expectedMerged, merged)")
				}

				if !t.Equal(expectedOps, mergedOps) {
					return errors.Errorf("t.Equal(expectedOps, mergedOps)")
				}

				return nil
			},
		); err != nil {
			return err
		}

		if !t.Equal(expectedReasons, reasons.Map()) {
			return errors.Errorf("t.Equal(expectedReasons, reasons.Map())")
		}

		return nil
	}

	// run([]int{0, 1, 4, 6, 2, 5, 3, 7})
	// return

	gen := combin.NewPermutationGenerator(len(mergeValues)+2, len(mergeValues)+2)

	var count int

	for gen.Next() {
		seq := gen.Permutation(nil)

		if count > 0 && count%666 == 0 {
			fmt.Fprintf(os.Stdout, "%d ", count)
		}

		t.NoError(run(seq), "%v", seq)

		count++
	}
	fmt.Fprintf(os.Stdout, "%d\n", count)
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

	states := map[string]base.StateValue{}
	for i := range n {
		states[accountStateKey(pubs[i])] = NewAccountSV(pubs[i], 1)
	}

	mergeValues := make([][2]SMV, n)

	for i := range mergeValues {
		sub, _ := NewAccountSMV(pubs[i], -1)
		add, _ := NewAccountSMV(pubs[(i+1)%n], 1)

		mergeValues[i] = [2]SMV{sub, add}
	}

	addSVMFunc, newGetSVM, closeSVMFuncSet := newSVMFuncSet()

	getAccountSVM := NewFuncAccountSVM(func(stateKey string) (base.StateValue, bool, error) {
		sv, found := states[stateKey]
		if found {
			return sv, true, nil
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

			merge, mergeDone, _, unmergeDone, wait, err := newMerger(
				ctx,
				poolSize,
				getSVM,
				func(opindex uint64, reason string) error { return nil },
			)
			if err != nil {
				panic(err)
			}

			for i := range n {
				opindex := uint64(i)

				vs := mergeValues[opindex]

				if err := merge(opindex, vs[0], []SMV{vs[1]}); err != nil {
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
			closeSVMFuncSet()

			dutil.ClearSlice(&pubs)
			dutil.ClearSlice(&mergeValues)
			clear(states)
		}
}

func TestRunBenchmarkMerger(t *testing.T) {
	size := 50000
	run, closef := runBenchmarkMerger(size)
	defer closef()

	started := time.Now()

	run(size)

	t.Log("elapsed", time.Since(started))
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
