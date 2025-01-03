package main

import (
	"context"
	"fmt"
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

type testCheckFailedWorkers struct {
	suite.Suite
}

func (t *testCheckFailedWorkers) TestNew() {
	t.Run("0 max workers size", func() {
		_, _, err := newFuncCheckFailedWorkers(0, 1)
		t.ErrorContains(err, "zero max workers size")
	})

	t.Run("0 max failed", func() {
		_, _, err := newFuncCheckFailedWorkers(1, 0)
		t.NoError(err)
	})
}

func (t *testCheckFailedWorkers) TestFailed() {
	t.Run("failed", func() {
		w, c, err := newFuncCheckFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.True(failed)
		t.ErrorContains(err, "showme")
	})

	t.Run("error after failed", func() {
		w, c, err := newFuncCheckFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.True(failed)
		t.ErrorContains(err, "showme")

		ok, failed, err = w("w1", func() error { return nil })
		t.False(ok)
		t.True(failed)
		t.ErrorIs(err, errWorkersFailed)
	})

	t.Run("failed, over zero", func() {
		w, c, err := newFuncCheckFailedWorkers(1, 1)
		t.NoError(err)
		defer c()

		ok, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.True(failed)
	})

	t.Run("error, but under max failed", func() {
		w, c, err := newFuncCheckFailedWorkers(1, 2)
		t.NoError(err)
		defer c()

		ok, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(failed)
		t.ErrorContains(err, "showme")

		ok, failed, err = w("w1", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.True(failed)
		t.ErrorContains(err, "showme")
	})

	t.Run("ok, but after closed", func() {
		w, c, err := newFuncCheckFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.True(failed)
		t.ErrorContains(err, "showme")

		ok, failed, err = w("w1", func() error { return nil })
		t.False(ok)
		t.True(failed)
		t.ErrorIs(err, errWorkersFailed)
	})
}

func TestCheckFailedWorkersFunc(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testCheckFailedWorkers))
}

type testCheckEnoughFailedWorkers struct {
	suite.Suite
}

func (t *testCheckEnoughFailedWorkers) TestNew() {
	t.Run("1 min enough + 0 max failed", func() {
		_, _, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
	})

	t.Run("0 min enough + 0 max failed", func() {
		_, _, err := newFuncCheckEnoughFailedWorkers(0, 0)
		t.ErrorContains(err, "zero min enough")
	})
}

func (t *testCheckEnoughFailedWorkers) TestEnough() {
	t.Run("enough", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.NoError(err)
		t.True(ok)
		t.True(enough)
		t.False(failed)
	})

	t.Run("same worker", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.True(ok)
		t.True(enough)
		t.False(failed)

		ok, enough, failed, err = w("w0", func() error { return nil })
		t.False(ok)
		t.False(enough)
		t.False(failed)
	})

	t.Run("ok, but after closed", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.True(ok)
		t.True(enough)
		t.False(failed)

		ok, enough, failed, err = w("w1", func() error { return nil })
		t.False(ok)
		t.True(enough)
		t.False(failed)
	})

	t.Run("error, but after closed", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.True(ok)
		t.True(enough)
		t.False(failed)

		ok, enough, failed, err = w("w1", func() error { return errors.Errorf("showme") })
		t.False(ok)
		t.True(enough)
		t.False(failed)
	})
}

func (t *testCheckEnoughFailedWorkers) TestEnoughUntil() {
	t.Run("enough, but not ready", func() {
		w, c, err := newFuncBaseCheckEnoughFailedWorkers(1, 0, func(worker string) bool { return worker == "w1" })
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.NoError(err)
		t.True(ok)
		t.False(enough)
		t.False(failed)

		ok, enough, failed, err = w("w1", func() error { return nil })
		t.NoError(err)
		t.True(ok)
		t.True(enough)
		t.False(failed)
	})

	t.Run("enough, not ready", func() {
		w, c, err := newFuncBaseCheckEnoughFailedWorkers(1, 0, func(worker string) bool { return worker == "w0" })
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.NoError(err)
		t.True(ok)
		t.True(enough)
		t.False(failed)
	})

	t.Run("failed and ready", func() {
		w, c, err := newFuncBaseCheckEnoughFailedWorkers(1, 0, func(worker string) bool { return worker == "w1" })
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.ErrorContains(err, "showme")
		t.True(ok)
		t.False(enough)
		t.True(failed)

		ok, enough, failed, err = w("w1", func() error { return nil })
		t.ErrorIs(err, errWorkersFailed)
		t.False(ok)
		t.False(enough)
		t.True(failed)
	})

	t.Run("ready, but failed", func() {
		w, c, err := newFuncBaseCheckEnoughFailedWorkers(1, 0, func(worker string) bool { return worker == "w1" })
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.NoError(err)
		t.True(ok)
		t.False(enough)
		t.False(failed)

		ok, enough, failed, err = w("w1", func() error { return errors.Errorf("showme") })
		t.ErrorContains(err, "showme")
		t.True(ok)
		t.False(enough)
		t.True(failed)
	})
}

func (t *testCheckEnoughFailedWorkers) TestFailed() {
	t.Run("failed", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)
		t.ErrorContains(err, "showme")
	})

	t.Run("error after failed", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)
		t.ErrorContains(err, "showme")

		ok, enough, failed, err = w("w1", func() error { return nil })
		t.False(ok)
		t.False(enough)
		t.True(failed)
		t.ErrorIs(err, errWorkersFailed)
	})

	t.Run("failed, over zero", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 1)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)
	})

	t.Run("error, but under max failed", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 2)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.False(failed)

		ok, enough, failed, err = w("w1", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)
	})

	t.Run("ok, but after closed", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)

		ok, enough, failed, err = w("w1", func() error { return nil })
		t.False(ok)
		t.False(enough)
		t.True(failed)
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

			t.Equal(0, l)

			dutil.TraverseSlice(stateKeys, func(_ int, stateKey string) (bool, error) {
				t.Empty(f(stateKey))

				return true, nil
			})

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

			dutil.TraverseSlice(stateKeys, func(_ int, stateKey string) (bool, error) {
				facts, err := f(stateKey)
				t.NoError(err)

				t.Equal(len(facts), len(facts))

				for i := range facts {
					t.True(expectedFacts[i].Equal(facts[i]))
				}

				return true, nil
			})

			return nil
		}))

		t.True(called)
	})

	t.Run("async", func() {
		m, u, tr, err := newFuncSetStateOps(1 << 9)
		t.NoError(err)

		var nstateKeys uint64 = 30
		stateKeys := make([]string, nstateKeys)
		dutil.TraverseSlice(stateKeys, func(i int, _ string) (bool, error) {
			stateKeys[i] = fmt.Sprintf("s%d", i)

			return true, nil
		})

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
		dutil.TraverseMap(opindexStateKeys, func(opindex uint64, stateKeys []string) (bool, error) {
			if slices.Index(unmergeOps, opindex) >= 0 {
				return true, nil
			}

			return true, dutil.TraverseSlice(stateKeys, func(_ int, stateKey string) (bool, error) {
				s := expectedStateKeyFact[stateKey]
				s = append(s, facts[opindex])

				expectedStateKeyFact[stateKey] = s

				return true, nil
			})
		})

		newJob, jobDone, jobWait, err := dutil.NewFuncJobWorker(context.Background(), nops)
		t.NoError(err)

		go func() {
			defer jobDone()

			t.IfErrPanic(dutil.TraverseMap(opindexStateKeys, func(opindex uint64, stateKeys []string) (bool, error) {
				return true, newJob(func(context.Context) error {
					if !m(opindex, facts[opindex], stateKeys) {
						return errors.Errorf("not merged, %d", opindex)
					}

					return nil
				})
			}))
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

			dutil.TraverseSlice(stateKeys, func(_ int, stateKey string) (bool, error) {
				e := expectedStateKeyFact[stateKey]
				slices.SortFunc(e, func(a, b util.Hash) int {
					return strings.Compare(a.String(), b.String())
				})

				s, err := f(stateKey)
				t.NoError(err)

				t.Equal(e, s, "state key=%q expected=%v collected=%v", stateKey, e, s)

				return true, nil
			})

			return nil
		}))

		t.True(called)
	})
}

func TestSetStateOps(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testSetStateOps))
}
