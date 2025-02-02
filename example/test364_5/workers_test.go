package main

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type baseTestWorkers struct{}

func (t *baseTestWorkers) workers(n int) []string {
	w := make([]string, n)

	for i := 0; i < n; i++ {
		w[i] = fmt.Sprintf("w%d", i)
	}

	return w
}

func (t *baseTestWorkers) randomOphs(n int) [][2]util.Hash {
	h := make([][2]util.Hash, n)
	for i := range n {
		h[i] = [2]util.Hash{
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
		}
	}

	return h
}

func (t *baseTestWorkers) getOperationFunc(ops []operation) getOperationFunc {
	return func(_ context.Context, opindex uint64, _, _ util.Hash) (operation, bool, string, error) {
		if opindex >= uint64(len(ops)) {
			return nil, false, "", nil
		}

		return ops[opindex], true, "", nil
	}
}

func (t *baseTestWorkers) newWorkerOptions(local string) *workerOptions {
	return newWorkerOptions(local)
}

func (t *baseTestWorkers) defaultDistributeValuesFunc(local string) newWorkerDistributeStateMergeValuesFunc {
	return func(_ context.Context, _, worker string) (
		workerProcessInsertStateMergeValuesFunc,
		dutil.In2ErrorFunc[[]string, []string],
		error,
	) {
		return func(opindex uint64, fact util.Hash, values []stateMergeValue) error {
				fmt.Printf("worker distribute: local=%q worker=%q opindex=%d fact=%q values=%d\n", local, worker, opindex, fact, len(values))

				return nil
			},
			func(subws []string, affected []string) error {
				fmt.Printf("worker distribute: local=%q worker=%q subs=%v affected=%v\n", local, worker, subws, affected)

				return nil
			},
			nil
	}
}

func (t *baseTestWorkers) ophsSeq2(ops []operation) iter.Seq2[uint64, [2]util.Hash] {
	return func(yield func(uint64, [2]util.Hash) bool) {
		for i, op := range slices.All(ops) {
			if !yield(uint64(i), [2]util.Hash{op.Hash(), op.Fact().Hash()}) {
				return
			}
		}
	}
}

type testCheckEnoughFailedWorkers struct {
	dutil.BaseSuite
}

func (t *testCheckEnoughFailedWorkers) TestNew() {
	t.Run("1 workers, 1 share", func() {
		_, err := newFuncCheckWeightFailedWorkers(1, 1, nil)
		t.NoError(err)
	})

	t.Run("0 workers, 0 share", func() {
		_, err := newFuncCheckWeightFailedWorkers(0, 0, nil)
		t.ErrorContains(err, "zero workers")
	})
}

func (t *testCheckEnoughFailedWorkers) TestEnough() {
	t.Run("enough", func() {
		w, err := newFuncCheckWeightFailedWorkers(1, 1, nil)
		t.NoError(err)

		ok, enough, failed := w("w0", 1, nil)
		t.True(ok)
		t.True(enough)
		t.NoError(failed)
	})

	t.Run("enough; weight", func() {
		w, err := newFuncCheckWeightFailedWorkers(3, 1, nil)
		t.NoError(err)

		ok, enough, failed := w("w0", 1, nil)
		t.True(ok)
		t.False(enough)
		t.NoError(failed)

		ok, enough, failed = w("w1", 2, nil)
		t.True(ok)
		t.True(enough)
		t.NoError(failed)
	})

	t.Run("same worker", func() {
		w, err := newFuncCheckWeightFailedWorkers(1, 1, nil)
		t.NoError(err)

		ok, enough, failed := w("w0", 1, nil)
		t.True(ok)
		t.True(enough)
		t.NoError(failed)

		ok, enough, failed = w("w0", 1, nil)
		t.False(ok)
		t.False(enough)
		t.NoError(failed)
	})

	t.Run("ok, but after closed", func() {
		w, err := newFuncCheckWeightFailedWorkers(1, 1, nil)
		t.NoError(err)

		ok, enough, failed := w("w0", 1, nil)
		t.True(ok)
		t.True(enough)
		t.NoError(failed)

		ok, enough, failed = w("w1", 1, nil)
		t.False(ok)
		t.False(enough)
		t.NoError(failed)
	})

	t.Run("error, but after closed", func() {
		w, err := newFuncCheckWeightFailedWorkers(1, 1, nil)
		t.NoError(err)

		ok, enough, failed := w("w0", 1, nil)
		t.True(ok)
		t.True(enough)
		t.NoError(failed)

		ok, enough, failed = w("w1", 1, errors.Errorf("showme"))
		t.False(ok)
		t.False(enough)
		t.NoError(failed)
	})
}

func (t *testCheckEnoughFailedWorkers) TestBlockUntil() {
	t.Run("enough, but not ready", func() {
		w, err := newFuncCheckWeightFailedWorkers(1, 1, func(worker string) bool { return worker == "w1" })
		t.NoError(err)

		ok, enough, failed := w("w0", 1, nil)
		t.True(ok)
		t.False(enough)
		t.NoError(failed)

		ok, enough, failed = w("w1", 1, nil)
		t.True(ok)
		t.True(enough)
		t.NoError(failed)
	})

	t.Run("enough, ready", func() {
		w, err := newFuncCheckWeightFailedWorkers(1, 1, func(worker string) bool { return worker == "w0" })
		t.NoError(err)

		ok, enough, failed := w("w0", 1, nil)
		t.True(ok)
		t.True(enough)
		t.NoError(failed)
	})

	t.Run("failed and ready", func() {
		w, err := newFuncCheckWeightFailedWorkers(1, 1, func(worker string) bool { return worker == "w1" })
		t.NoError(err)

		ok, enough, failed := w("w0", 1, errors.Errorf("showme"))
		t.True(ok)
		t.False(enough)
		t.NoError(failed)

		ok, enough, failed = w("w1", 1, nil)
		t.True(ok)
		t.False(enough)
		t.Error(failed)
		t.ErrorIs(failed, errWorkersFailed)
	})

	t.Run("ready, but failed", func() {
		w, err := newFuncCheckWeightFailedWorkers(1, 1, func(worker string) bool { return worker == "w1" })
		t.NoError(err)

		ok, enough, failed := w("w0", 1, nil)
		t.True(ok)
		t.False(enough)
		t.NoError(failed)

		ok, enough, failed = w("w1", 1, errors.Errorf("showme"))
		t.True(ok)
		t.False(enough)
		t.Error(failed)
		t.ErrorIs(failed, errWorkersFailed)
	})
}

func (t *testCheckEnoughFailedWorkers) TestFailed() {
	t.Run("failed", func() {
		w, err := newFuncCheckWeightFailedWorkers(1, 1, nil)
		t.NoError(err)

		ok, enough, failed := w("w0", 1, errors.Errorf("showme"))
		t.True(ok)
		t.False(enough)
		t.Error(failed)
		t.ErrorIs(failed, errWorkersFailed)
	})

	t.Run("failed; weight", func() {
		w, err := newFuncCheckWeightFailedWorkers(3, 3, nil)
		t.NoError(err)

		ok, enough, failed := w("w0", 1, errors.Errorf("showme"))
		t.True(ok)
		t.False(enough)
		t.NoError(failed)

		ok, enough, failed = w("w1", 2, errors.Errorf("findme"))
		t.True(ok)
		t.False(enough)
		t.Error(failed)
		t.ErrorIs(failed, errWorkersFailed)
	})

	t.Run("error after failed", func() {
		w, err := newFuncCheckWeightFailedWorkers(1, 1, nil)
		t.NoError(err)

		ok, enough, failed := w("w0", 1, errors.Errorf("showme"))
		t.True(ok)
		t.False(enough)
		t.Error(failed)
		t.ErrorIs(failed, errWorkersFailed)

		ok, enough, failed = w("w1", 1, nil)
		t.False(ok)
		t.False(enough)
		t.Error(failed)
		t.ErrorIs(failed, errWorkersFailed)
	})

	t.Run("error, but under max failed", func() {
		w, err := newFuncCheckWeightFailedWorkers(2, 2, nil)
		t.NoError(err)

		ok, enough, failed := w("w0", 1, errors.Errorf("showme"))
		t.True(ok)
		t.False(enough)
		t.NoError(failed)

		ok, enough, failed = w("w1", 1, errors.Errorf("showme"))
		t.True(ok)
		t.False(enough)
		t.Error(failed)
		t.ErrorIs(failed, errWorkersFailed)
	})

	t.Run("ok, but after closed", func() {
		w, err := newFuncCheckWeightFailedWorkers(1, 1, nil)
		t.NoError(err)

		ok, enough, failed := w("w0", 1, errors.Errorf("showme"))
		t.True(ok)
		t.False(enough)
		t.Error(failed)
		t.ErrorIs(failed, errWorkersFailed)

		ok, enough, failed = w("w1", 1, nil)
		t.False(ok)
		t.False(enough)
		t.Error(failed)
		t.ErrorIs(failed, errWorkersFailed)
	})
}

func TestCheckEnoughFailedWorkersFunc(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testCheckEnoughFailedWorkers))
}

func TestMinEnoughMaxFailed(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	cases := []struct {
		workers   uint64
		share     uint64
		err       string
		minEnough uint64
		maxFailed uint64
	}{
		{workers: 0, share: 1, err: "zero workers"},
		{workers: 1, share: 0, err: "zero share"},
		{workers: 1, share: 1, minEnough: 1, maxFailed: 1},
		{workers: 1, share: 2, minEnough: 1, maxFailed: 1},
		{workers: 2, share: 1, minEnough: 2, maxFailed: 1},
		{workers: 2, share: 2, minEnough: 1, maxFailed: 2},
	}

	for i, c := range cases {
		index := i
		c := c

		name := fmt.Sprintf("workers=%d share=%d", c.workers, c.share)

		t.Run(name, func() {
			me, mf, err := minEnoughMaxFailed(c.workers, c.share)

			if c.err != "" {
				if err == nil {
					t.Fail("no error", `%d: expected=%q`, index, c.err)

					return
				}

				t.ErrorContains(err, c.err, "%d", index)

				return
			} else if err != nil {
				t.Fail("unexpected error", `%d: error=%+v`, index, err)

				return
			}

			t.Equal(c.minEnough, me, "%s(%d): min enough; expected=%d result=%d", name, index, c.minEnough, me)
			t.Equal(c.maxFailed, mf, "%s(%d): max failed; expected=%d result=%d", name, index, c.maxFailed, mf)
		})
	}
}

func fixedStateOwnersFunc(owners map[string][]string) (getStateOwnersFunc, func(), error) {
	return func(stateKey string) []string {
			return owners[stateKey]
		},
		func() {},
		nil
}

type testSetStateOps struct {
	dutil.BaseSuite
}

func (t *testSetStateOps) TestError() {
	t.Run("zero map size", func() {
		_, _, _, err := newFuncSetStateOps(0)
		t.ErrorContains(err, "")
	})
}

func (t *testSetStateOps) Test() {
	t.Run("unmerge, not merged", func() {
		_, u, tr, err := newFuncSetStateOps(1 << 9)
		t.NoError(err)

		t.False(u([]uint64{3}))

		var called bool

		t.NoError(tr(func(l int, f getStateOpsFunc) error {
			called = true

			t.Equal(0, l)

			return nil
		}))

		t.True(called)
	})

	t.Run("merge, and then unmerge", func() {
		m, u, tr, err := newFuncSetStateOps(1 << 9)
		t.NoError(err)

		stateKeys := []string{"1", "2", "3"}
		t.True(m(3, valuehash.RandomSHA256(), stateKeys))
		t.True(u([]uint64{3}))

		var called bool

		t.NoError(tr(func(l int, f getStateOpsFunc) error {
			called = true

			t.Equal(3, l)

			for stateKey := range slices.Values(stateKeys) {
				t.Empty(f(stateKey))
			}

			return nil
		}))

		t.True(called)
	})

	t.Run("merge, merge, and then unmerge", func() {
		m, u, tr, err := newFuncSetStateOps(1 << 9)
		t.NoError(err)

		expectedFacts := []util.Hash{
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
		}

		slices.SortFunc(expectedFacts, func(a, b util.Hash) int {
			return strings.Compare(a.String(), b.String())
		})

		stateKeys := []string{"1", "2", "3"}
		t.True(m(0, expectedFacts[0], stateKeys))
		t.True(m(1, expectedFacts[1], stateKeys))
		t.True(m(2, expectedFacts[2], stateKeys))

		t.True(m(3, valuehash.RandomSHA256(), stateKeys))
		t.True(u([]uint64{3}))

		var called bool

		t.NoError(tr(func(l int, f getStateOpsFunc) error {
			called = true

			t.Equal(3, l)

			for stateKey := range slices.Values(stateKeys) {
				facts, err := f(stateKey)
				t.NoError(err)

				t.Equal(len(facts), len(facts))

				for i := range facts {
					t.True(expectedFacts[i].Equal(facts[i]))
				}
			}

			return nil
		}))

		t.True(called)
	})

	t.Run("async", func() {
		m, u, tr, err := newFuncSetStateOps(1 << 9)
		t.NoError(err)

		var nstateKeys uint64 = 30
		stateKeys := make([]string, nstateKeys)

		for i := range stateKeys {
			stateKeys[i] = fmt.Sprintf("s%d", i)
		}

		var nops uint64 = 33

		opindexStateKeys := map[uint64][]string{}
		facts := map[uint64]util.Hash{}

		for i := range nops {
			opindex := uint64(i)

			opindexStateKeys[opindex] = []string{
				stateKeys[((i * 3) % nstateKeys)],
				stateKeys[((i*3)+1)%nstateKeys],
				stateKeys[((i*3)+2)%nstateKeys],
			}
			facts[opindex] = valuehash.RandomSHA256()
		}

		unmergeOps := []uint64{0, 9, 30}

		expectedStateKeyFact := map[string][]util.Hash{}

		for opindex, stateKeys := range maps.All(opindexStateKeys) {
			if slices.Index(unmergeOps, opindex) >= 0 {
				continue
			}

			for stateKey := range slices.Values(stateKeys) {
				s := expectedStateKeyFact[stateKey]
				s = append(s, facts[opindex])

				expectedStateKeyFact[stateKey] = s
			}
		}

		newJob, jobDone, jobWait, err := dutil.NewFuncJobWorker(context.Background(), nops)
		t.NoError(err)

		go func() {
			defer jobDone()

			for opindex, stateKeys := range maps.All(opindexStateKeys) {
				t.IfErrPanic(newJob(func(context.Context) error {
					if !m(opindex, facts[opindex], stateKeys) {
						return errors.Errorf("not merged, %d", opindex)
					}

					return nil
				}))
			}
		}()

		t.NoError(jobWait(nil))

		newJob, jobDone, jobWait, err = dutil.NewFuncJobWorker(context.Background(), nops)
		t.NoError(err)

		go func() {
			defer jobDone()

			t.IfErrPanic(newJob(func(context.Context) error {
				if !u(unmergeOps) {
					return errors.Errorf("not unmerged, %v", unmergeOps)
				}

				return nil
			}))
		}()

		t.NoError(jobWait(nil))

		var called bool

		t.NoError(tr(func(l int, f getStateOpsFunc) error {
			called = true

			t.Equal(nstateKeys, uint64(l))

			for stateKey := range slices.Values(stateKeys) {
				e := expectedStateKeyFact[stateKey]
				slices.SortFunc(e, func(a, b util.Hash) int {
					return strings.Compare(a.String(), b.String())
				})

				s, err := f(stateKey)
				t.NoError(err)

				t.Equal(e, s, "state key=%q expected=%v collected=%v", stateKey, e, s)
			}

			return nil
		}))

		t.True(called)
	})
}

func TestSetStateOps(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testSetStateOps))
}
