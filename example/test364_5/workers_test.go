package dop5

import (
	"cmp"
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type baseTestWorkers struct {
	base dutil.BaseTest
}

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

func (t *baseTestWorkers) getOperationFunc(ops []Operation) getOperationFunc {
	return func(_ context.Context, opindex uint64, _, _ util.Hash) (Operation, bool, string, error) {
		if opindex >= uint64(len(ops)) {
			return nil, false, "", nil
		}

		return ops[opindex], true, "", nil
	}
}

func (t *baseTestWorkers) newWorkerOptions(local string) *workerOptions {
	return newWorkerOptions(local)
}

func (t *baseTestWorkers) defaultDistributeValuesFunc(local string) newWorkerDistributeSMVsFunc {
	return func(_ context.Context, _, worker string) (
		workerProcessInsertSMVsFunc,
		dutil.In2ErrorFunc[[]string, []string],
		error,
	) {
		return func(opindex uint64, fact util.Hash, values []SMV) error {
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

func (t *baseTestWorkers) ophsSeq2(ops []Operation) iter.Seq2[uint64, [2]util.Hash] {
	return func(yield func(uint64, [2]util.Hash) bool) {
		for i, op := range slices.All(ops) {
			if !yield(uint64(i), [2]util.Hash{op.Hash(), op.Fact().Hash()}) {
				return
			}
		}
	}
}

func (t *baseTestWorkers) newWorkerProcessDistributeSMVsFunc() (
	newWorkerProcessDistributeSMVsFunc,
	dutil.OutFunc[[]interface{}],
) {
	add, done, err := dutil.NewLockAppendSlice[interface{}](1<<9, 1<<9)
	t.base.IfErrPanic(err)

	return func(_ context.Context, worker string) (
			workerProcessInsertSMVsFunc,
			dutil.In2ErrorFunc[[]string, []string],
			error,
		) {
			return func(opindex uint64, fact util.Hash, values []SMV) error {
					add([3]interface{}{opindex, fact, values})

					return nil
				},
				func(subws, affected []string) error {
					add([2][]string{subws, affected})

					return nil
				},
				nil
		},
		done
}

func (t *baseTestWorkers) newWorkerDistributeSMVsFunc() (
	newWorkerDistributeSMVsFunc,
	dutil.OutFunc[[]interface{}],
) {
	d, o := t.newWorkerProcessDistributeSMVsFunc()

	return func(ctx context.Context, workID, worker string) (
			workerProcessInsertSMVsFunc,
			dutil.In2ErrorFunc[[]string, []string],
			error,
		) {
			return d(ctx, worker)
		},
		o
}

func (t *baseTestWorkers) newWorkersProcessDistributeSMVsFunc() (
	newWorkerProcessDistributeSMVsFunc,
	iter.Seq2[string, dutil.OutFunc[[]interface{}]],
) {
	pool, err := dutil.NewLockedMap[string, [2]interface{}](nil, 1<<9, 1<<9)
	t.base.IfErrPanic(err)

	return func(_ context.Context, worker string) (
			workerProcessInsertSMVsFunc,
			dutil.In2ErrorFunc[[]string, []string],
			error,
		) {
			var add dutil.AppendSliceAddFunc[interface{}]

			if _, err := pool.Set(worker, func(i [2]interface{}, found bool) (v [2]interface{}, _ error) {
				if found {
					add = i[0].(dutil.AppendSliceAddFunc[interface{}])

					return v, dutil.ErrLockedSetIgnore
				}

				a, done, err := dutil.NewLockAppendSlice[interface{}](1<<9, 1<<9)
				add = a

				return [2]interface{}{add, done}, err
			}); err != nil {
				return nil, nil, err
			}

			return func(opindex uint64, fact util.Hash, values []SMV) error {
					add([3]interface{}{opindex, fact, values})

					return nil
				},
				func(subws, affected []string) error {
					add([2][]string{subws, affected})

					return nil
				},
				nil
		},
		func(yield func(string, dutil.OutFunc[[]interface{}]) bool) {
			defer func() {
				pool.Close()
			}()

			for worker, i := range pool.Seq2 {
				if !yield(worker, i[1].(dutil.OutFunc[[]interface{}])) {
					break
				}
			}
		}
}

func (t *baseTestWorkers) newWorkerDistributeSMVsContextChans(ctx context.Context) (
	dutil.InErrorFunc[interface{}],
	func(),
	iter.Seq[interface{}],
) {
	return dutil.NewContextUnlimitedChans[interface{}](ctx, 1<<9, 1<<9)
}

func (t *baseTestWorkers) newWorkersDistributeSMVsContextChans(ctx context.Context) (
	dutil.InErrorFunc[interface{}],
	func(),
	iter.Seq[interface{}],
) {
	return dutil.NewContextUnlimitedChans[interface{}](ctx, 1<<9, 1<<9)
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

type testDelayedWorkerDistributeSMVs struct {
	baseTestAccountSVM
	baseTestWorkers
}

func (t *testDelayedWorkerDistributeSMVs) TestCtxCanceled() {
	t.Run("before new", func() {
		newf, insertedDone := t.newWorkerDistributeSMVsFunc()
		f := newFuncDelayedWorkerDistributeSMVs(
			newf,
			t.newWorkerDistributeSMVsContextChans,
		)

		ctx, cancel := t.Ctx()
		cancel(nil)

		sendOp, sendOwners, wait, err := f(ctx, util.UUID().String(), "w0")
		t.NoError(err)

		err = sendOp(3, valuehash.RandomSHA256(), t.randomMergeValues(3))
		t.Error(err, context.Canceled)

		err = sendOwners(
			[]string{util.UUID().String(), util.UUID().String()},
			[]string{util.UUID().String(), util.UUID().String()},
		)
		t.Error(err, context.Canceled)

		err = wait()
		t.Error(err, context.Canceled)

		t.Empty(insertedDone())
	})

	t.Run("during send op", func() {
		newf, _ := t.newWorkerDistributeSMVsFunc()
		f := newFuncDelayedWorkerDistributeSMVs(
			newf,
			t.newWorkerDistributeSMVsContextChans,
		)

		ctx, cancel := t.Ctx()

		sendOp, sendOwners, wait, err := f(ctx, util.UUID().String(), "w0")
		t.NoError(err)

		for i := range 3 {
			err = sendOp(uint64(i), valuehash.RandomSHA256(), t.randomMergeValues(3))
			if i == 2 {
				cancel(nil)
			}

			switch {
			case i < 2:
				t.NoError(err)
			case i > 2:
				if !errors.Is(err, context.Canceled) && !errors.Is(err, dutil.ErrContextUnlimitedChansInsertDone) {
					t.Fail("unexpected error, %+v", err)
				}
			}
		}

		err = sendOwners(
			[]string{util.UUID().String(), util.UUID().String()},
			[]string{util.UUID().String(), util.UUID().String()},
		)
		t.Error(err, context.Canceled)

		err = wait()
		t.Error(err, context.Canceled)
	})
}

func (t *testDelayedWorkerDistributeSMVs) TestNew() {
	t.Run("ops and owners", func() {
		newf, insertedDone := t.newWorkerDistributeSMVsFunc()
		f := newFuncDelayedWorkerDistributeSMVs(
			newf,
			t.newWorkerDistributeSMVsContextChans,
		)

		ctx, cancel := t.Ctx()
		defer cancel(nil)

		ops := make([][3]interface{}, 3)
		for i := range ops {
			ops[i] = [3]interface{}{
				uint64(i),
				valuehash.RandomSHA256(),
				t.randomMergeValues(3),
			}
		}

		owners := [2][]string{
			{util.UUID().String(), util.UUID().String()},
			{util.UUID().String(), util.UUID().String()},
		}

		sendOp, sendOwners, wait, err := f(ctx, util.UUID().String(), "w0")
		t.NoError(err)

		var wg sync.WaitGroup

		for i := range slices.Values(ops) {
			wg.Add(1)

			go func() {
				defer wg.Done()

				err := sendOp(i[0].(uint64), i[1].(util.Hash), i[2].([]SMV))
				t.Print("send op: opindex=%d err=%v", i[0], err)
			}()
		}

		wg.Wait()

		err = sendOwners(owners[0], owners[1])
		t.NoError(err)
		t.Print("send owners: err=%v", err)

		t.NoError(wait())

		inserted := insertedDone()
		insertedOps := inserted[:len(inserted)-1]
		insertedOwners := inserted[len(inserted)-1].([2][]string)

		t.Equal(len(ops), len(insertedOps))

		for i := range ops {
			slices.SortFunc(insertedOps, func(a, b interface{}) int {
				return cmp.Compare(
					a.([3]interface{})[0].(uint64),
					b.([3]interface{})[0].(uint64),
				)
			})
			t.Equal(ops[i], insertedOps[i])
		}

		t.Equal(owners, insertedOwners)
	})

	t.Run("no ops and owners", func() {
		newf, insertedDone := t.newWorkerDistributeSMVsFunc()
		f := newFuncDelayedWorkerDistributeSMVs(
			newf,
			t.newWorkerDistributeSMVsContextChans,
		)

		ctx, cancel := t.Ctx()
		defer cancel(nil)

		owners := [2][]string{
			{util.UUID().String(), util.UUID().String()},
			{util.UUID().String(), util.UUID().String()},
		}

		_, sendOwners, wait, err := f(ctx, util.UUID().String(), "w0")
		t.NoError(err)

		err = sendOwners(owners[0], owners[1])
		t.NoError(err)
		t.Print("send owners: err=%v", err)

		t.NoError(wait())

		insertedOwners := insertedDone()

		t.Equal(owners, insertedOwners[0])
	})

	t.Run("send ops after sending owners", func() {
		newf, insertedDone := t.newWorkerDistributeSMVsFunc()
		f := newFuncDelayedWorkerDistributeSMVs(
			newf,
			t.newWorkerDistributeSMVsContextChans,
		)

		ctx, cancel := t.Ctx()
		defer cancel(nil)

		owners := [2][]string{
			{util.UUID().String(), util.UUID().String()},
			{util.UUID().String(), util.UUID().String()},
		}

		sendOp, sendOwners, wait, err := f(ctx, util.UUID().String(), "w0")
		t.NoError(err)

		err = sendOwners(owners[0], owners[1])
		t.NoError(err)
		t.Print("send owners: err=%v", err)

		err = sendOp(
			3,
			valuehash.RandomSHA256(),
			t.randomMergeValues(3),
		)
		switch {
		case errors.Is(err, context.Canceled),
			errors.Is(err, dutil.ErrContextUnlimitedChansInsertDone):
		default:
			t.Fail("unexpected error, %+v", err)
		}

		t.NoError(wait())

		insertedOwners := insertedDone()

		t.Equal(owners, insertedOwners[0])
	})
}

func TestDelayedWorkerDistributeSMVs(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testDelayedWorkerDistributeSMVs))
}

type testWorkerProcessWorkersDistributeSMVs struct {
	baseTestAccountSVM
	baseTestWorkers
}

func (t *testWorkerProcessWorkersDistributeSMVs) TestCancel() {
	workers := t.workers(3)
	local := workers[0]

	ops := make([][3]interface{}, len(workers))
	for i := range ops {
		ops[i] = [3]interface{}{
			uint64(i),
			valuehash.RandomSHA256(),
			t.randomMergeValues(3),
		}
	}

	owners := make([][2][]string, len(workers))

	for i := range owners {
		owners[i] = [2][]string{
			{util.UUID().String(), util.UUID().String()},
			{util.UUID().String(), util.UUID().String()},
		}
	}

	t.Run("before new", func() {
		ctx, cancel := t.Ctx()
		cancel(nil)

		newf, insertedDone := t.newWorkersProcessDistributeSMVsFunc()

		sendOp, sendOwners, c, err := newFuncWorkerProcessWorkersDistributeSMVs(
			ctx, local, workers,
			newf,
			t.newWorkersDistributeSMVsContextChans,
		)
		t.NoError(err)

		{
			var wg sync.WaitGroup

			for i, j := range dutil.SeqAll(slices.Values(ops)) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					worker := workers[i]

					switch err := sendOp(worker, j[0].(uint64), j[1].(util.Hash), j[2].([]SMV)); {
					case errors.Is(err, context.Canceled),
						errors.Is(err, dutil.ErrContextUnlimitedChansInsertDone):
					default:
						t.IfErrPanicf(err, "send op: worker=%q opindex=%d", worker, j[0])
					}
				}()
			}

			wg.Wait()
		}

		{
			var wg sync.WaitGroup

			for i, j := range dutil.SeqAll(slices.Values(owners)) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					worker := workers[i]

					switch err := sendOwners(worker, j[0], j[1]); {
					case errors.Is(err, context.Canceled),
						errors.Is(err, dutil.ErrContextUnlimitedChansInsertDone):
					default:
						t.IfErrPanicf(err, "send owners: worker=%qv", worker)
					}
				}()
			}

			wg.Wait()
		}

		switch err = c(); {
		case errors.Is(err, context.Canceled),
			errors.Is(err, dutil.ErrContextUnlimitedChansInsertDone):
		default:
			t.IfErrPanicf(err, "c")
		}

		for worker, done := range insertedDone {
			t.Empty(done(), worker)
		}
	})

	t.Run("during send op", func() {
		ctx, cancel := t.Ctx()

		newf, _ := t.newWorkersProcessDistributeSMVsFunc()

		sendOp, sendOwners, c, err := newFuncWorkerProcessWorkersDistributeSMVs(
			ctx, local, workers,
			newf,
			t.newWorkersDistributeSMVsContextChans,
		)
		t.NoError(err)

		{
			var wg sync.WaitGroup

			for i, j := range dutil.SeqAll(slices.Values(ops)) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					worker := workers[i]

					err := sendOp(worker, j[0].(uint64), j[1].(util.Hash), j[2].([]SMV))
					if i == 2 {
						cancel(nil)
					}

					switch {
					case errors.Is(err, context.Canceled),
						errors.Is(err, dutil.ErrContextUnlimitedChansInsertDone):
					default:
						t.IfErrPanicf(err, "send op: unexpected error: worker=%q", worker)
					}
				}()
			}

			wg.Wait()
		}

		{
			var wg sync.WaitGroup

			for i, j := range dutil.SeqAll(slices.Values(owners)) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					worker := workers[i]

					switch err := sendOwners(worker, j[0], j[1]); {
					case errors.Is(err, context.Canceled),
						errors.Is(err, dutil.ErrContextUnlimitedChansInsertDone):
					default:
						t.IfErrPanicf(err, "send owners: worker=%qv", worker)
					}
				}()
			}

			wg.Wait()
		}

		switch err = c(); {
		case errors.Is(err, context.Canceled),
			errors.Is(err, dutil.ErrContextUnlimitedChansInsertDone):
		default:
			t.IfErrPanicf(err, "c")
		}
	})

	t.Run("during send owners", func() {
		ctx, cancel := t.Ctx()

		newf, _ := t.newWorkersProcessDistributeSMVsFunc()

		sendOp, sendOwners, c, err := newFuncWorkerProcessWorkersDistributeSMVs(
			ctx, local, workers,
			newf,
			t.newWorkersDistributeSMVsContextChans,
		)
		t.NoError(err)

		{
			var wg sync.WaitGroup

			for i, j := range dutil.SeqAll(slices.Values(ops)) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					worker := workers[i]

					err := sendOp(worker, j[0].(uint64), j[1].(util.Hash), j[2].([]SMV))
					t.IfErrPanicf(err, "send op: unexpected error: worker=%q", worker)
				}()
			}

			wg.Wait()
		}

		{
			var wg sync.WaitGroup

			for i, j := range dutil.SeqAll(slices.Values(owners)) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					worker := workers[i]

					err := sendOwners(worker, j[0], j[1])

					if i == 2 {
						cancel(nil)
					}

					switch {
					case errors.Is(err, context.Canceled),
						errors.Is(err, dutil.ErrContextUnlimitedChansInsertDone):
					default:
						t.IfErrPanicf(err, "send owners: worker=%qv", worker)
					}
				}()
			}

			wg.Wait()
		}

		switch err = c(); {
		case errors.Is(err, context.Canceled),
			errors.Is(err, dutil.ErrContextUnlimitedChansInsertDone):
		default:
			t.IfErrPanicf(err, "c")
		}
	})

	t.Run("after send owners", func() {
		ctx, cancel := t.Ctx()

		newf, _ := t.newWorkersProcessDistributeSMVsFunc()

		sendOp, sendOwners, c, err := newFuncWorkerProcessWorkersDistributeSMVs(
			ctx, local, workers,
			newf,
			t.newWorkersDistributeSMVsContextChans,
		)
		t.NoError(err)

		{
			var wg sync.WaitGroup

			for i, j := range dutil.SeqAll(slices.Values(ops)) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					worker := workers[i]

					err := sendOp(worker, j[0].(uint64), j[1].(util.Hash), j[2].([]SMV))
					t.IfErrPanicf(err, "send op: unexpected error: worker=%q", worker)
				}()
			}

			wg.Wait()
		}

		{
			var wg sync.WaitGroup

			for i, j := range dutil.SeqAll(slices.Values(owners)) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					worker := workers[i]

					err := sendOwners(worker, j[0], j[1])
					t.IfErrPanicf(err, "send owners: worker=%qv", worker)
				}()
			}

			wg.Wait()
		}

		cancel(nil)

		switch err = c(); {
		case errors.Is(err, context.Canceled),
			errors.Is(err, dutil.ErrContextUnlimitedChansInsertDone):
		default:
			t.IfErrPanicf(err, "c")
		}
	})
}

func (t *testWorkerProcessWorkersDistributeSMVs) TestNew() {
	workers := t.workers(3)
	local := workers[0]

	ops := make([][3]interface{}, len(workers))
	for i := range ops {
		ops[i] = [3]interface{}{
			uint64(i),
			valuehash.RandomSHA256(),
			t.randomMergeValues(3),
		}
	}

	owners := make([][2][]string, len(workers))

	for i := range owners {
		owners[i] = [2][]string{
			{util.UUID().String(), util.UUID().String()},
			{util.UUID().String(), util.UUID().String()},
		}
	}

	t.Run("ops and owners", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		newf, insertedDone := t.newWorkersProcessDistributeSMVsFunc()

		sendOp, sendOwners, c, err := newFuncWorkerProcessWorkersDistributeSMVs(
			ctx, local, workers,
			newf,
			t.newWorkersDistributeSMVsContextChans,
		)
		t.NoError(err)

		{
			var wg sync.WaitGroup

			for i, j := range dutil.SeqAll(slices.Values(ops)) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					worker := workers[i]

					err := sendOp(worker, j[0].(uint64), j[1].(util.Hash), j[2].([]SMV))
					t.IfErrPanicf(err, "send op: worker=%q opindex=%d", worker, j[0])
				}()
			}

			wg.Wait()
		}

		{
			var wg sync.WaitGroup

			for i, j := range dutil.SeqAll(slices.Values(owners)) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					worker := workers[i]

					err := sendOwners(worker, j[0], j[1])
					t.IfErrPanicf(err, "send owners: worker=%qv", worker)
				}()
			}

			wg.Wait()
		}

		t.NoError(c())

		for worker, done := range insertedDone {
			inserted := done()
			insertedOps := inserted[:len(inserted)-1]
			insertedOwners := inserted[len(inserted)-1].([2][]string)

			i := slices.Index(workers, worker)
			t.Equal(ops[i], insertedOps[0], worker)
			t.Equal(owners[i], insertedOwners, worker)
		}
	})

	t.Run("some worker has empty ops and owners", func() {
		emptyOpsWorker := workers[1]

		ctx, cancel := t.Ctx()
		defer cancel(nil)

		newf, insertedDone := t.newWorkersProcessDistributeSMVsFunc()

		sendOp, sendOwners, c, err := newFuncWorkerProcessWorkersDistributeSMVs(
			ctx, local, workers,
			newf,
			t.newWorkersDistributeSMVsContextChans,
		)
		t.NoError(err)

		{
			var wg sync.WaitGroup

			for i, j := range dutil.SeqAll(slices.Values(ops)) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					worker := workers[i]
					if worker == emptyOpsWorker {
						t.Print("worker empty ops: worker=%q", worker)

						return
					}

					err := sendOp(worker, j[0].(uint64), j[1].(util.Hash), j[2].([]SMV))
					t.IfErrPanicf(err, "send op: worker=%q opindex=%d", worker, j[0])
				}()
			}

			wg.Wait()
		}

		{
			var wg sync.WaitGroup

			for i, j := range dutil.SeqAll(slices.Values(owners)) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					worker := workers[i]

					err := sendOwners(worker, j[0], j[1])
					t.IfErrPanicf(err, "send owners: worker=%qv", worker)
				}()
			}

			wg.Wait()
		}

		t.NoError(c())

		for worker, done := range insertedDone {
			inserted := done()
			insertedOps := inserted[:len(inserted)-1]
			insertedOwners := inserted[len(inserted)-1].([2][]string)

			i := slices.Index(workers, worker)

			if worker == emptyOpsWorker {
				t.Empty(insertedOps, worker)
			} else {
				t.Equal(ops[i], insertedOps[0], worker)
			}

			t.Equal(owners[i], insertedOwners, worker)
		}
	})

	t.Run("empty ops and owners", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		newf, insertedDone := t.newWorkersProcessDistributeSMVsFunc()

		_, sendOwners, c, err := newFuncWorkerProcessWorkersDistributeSMVs(
			ctx, local, workers,
			newf,
			t.newWorkersDistributeSMVsContextChans,
		)
		t.NoError(err)

		{
			var wg sync.WaitGroup

			for i, j := range dutil.SeqAll(slices.Values(owners)) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					worker := workers[i]

					err := sendOwners(worker, j[0], j[1])
					t.IfErrPanicf(err, "send owners: worker=%qv", worker)
				}()
			}

			wg.Wait()
		}

		t.NoError(c())

		for worker, done := range insertedDone {
			inserted := done()
			insertedOps := inserted[:len(inserted)-1]
			insertedOwners := inserted[len(inserted)-1].([2][]string)

			i := slices.Index(workers, worker)
			t.Empty(insertedOps, worker)
			t.Equal(owners[i], insertedOwners, worker)
		}
	})

	t.Run("empty ops and empty owners", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		newf, insertedDone := t.newWorkersProcessDistributeSMVsFunc()

		_, _, c, err := newFuncWorkerProcessWorkersDistributeSMVs(
			ctx, local, workers,
			newf,
			t.newWorkersDistributeSMVsContextChans,
		)
		t.NoError(err)

		t.NoError(c())

		for worker, done := range insertedDone {
			inserted := done()
			insertedOwners := inserted[len(inserted)-1].([2][]string)

			t.Equal([2][]string{nil, nil}, insertedOwners, worker)
		}
	})
}

func TestWorkerProcessWorkersDistributeSMVs(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerProcessWorkersDistributeSMVs))
}
