package main

import (
	"context"
	"slices"
	"testing"

	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testUnmerger struct {
	dutil.BaseSuite
	baseTestMergeOp
}

func (t *testUnmerger) newFuncs() (
	vp util.LockedMap[uint64, []StateMergeValue],
	mergers util.LockedMap[string, StateMerger],
	states util.LockedMap[string, base.State],
	_ opindexGetStateMergeValuesFunc,
	getState getStateFunc,
	getMergers mergerGetStateMergersFunc,
) {
	t.T().Helper()

	vp, mergers, states, _, getValues := t.baseTestMergeOp.getFuncs()

	getState = func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}

	extractKeys, _ := t.extractStateMergeValueKeys()
	getMergers, _ = newFuncGetStateMergers(mergers, getState, extractKeys)

	return vp, mergers, states, getValues, getState, getMergers
}

func (t *testUnmerger) TestError() {
	t.Run("zero max ops size", func() {
		_, _, _, err := newFuncUnmerger(context.Background(), t.newJobWorker(), 0, nil, nil)
		t.ErrorContains(err, "under 1")
	})
}

func (t *testUnmerger) TestUnmerge() {
	t.Run("no merged/unmerged ops", func() {
		ctx, _ := t.Ctx()

		// ctx = dutil.SetContextValueLogger(ctx, logging.TestLogging.Log())

		vp, mergers, _, getValues, _, getMergers := t.newFuncs()

		unmerge, done, wait, err := newFuncUnmerger(ctx, t.newJobWorker(), 1<<9, getValues, getMergers)
		t.NoError(err)

		ops := make([][]StateMergeValue, 33)
		for i := range ops {
			vs := t.randomMergeValues(1, 2)
			vp.SetValue(uint64(i), vs)

			ops[i] = vs
		}

		t.T().Log("insert ops")
		{
			newJob, jobDone, jobWait, err := dutil.NewFuncJobWorker(ctx, 33)
			t.NoError(err)

			go func() {
				for i := range ops {
					opindex := uint64(i)

					if err := newJob(func(context.Context) error {
						return unmerge(opindex)
					}); err != nil {
						panic(err)
					}
				}

				jobDone()
			}()

			t.NoError(jobWait())
		}

		t.T().Log("insert done")
		done()

		t.NoError(wait(func(unmerged []uint64) error {
			slices.Sort(unmerged)

			t.T().Logf("wait done: unmerged ops=%v", unmerged)

			t.Equal(len(ops), len(unmerged))
			t.Equal(uint64(0), unmerged[0])
			t.Equal(uint64(len(ops)-1), unmerged[len(unmerged)-1])

			return nil
		}))

		_, err = t.closeMergers(mergers)
		t.ErrorContains(err, "under zero balance")
	})

	t.Run("no merged, but unmerged ops", func() {
		ctx, _ := t.Ctx()

		// ctx = dutil.SetContextValueLogger(ctx, logging.TestLogging.Log())

		vp, mergers, _, getValues, _, getMergers := t.newFuncs()

		ops := make([][]StateMergeValue, 33)

		for i := range ops {
			vs := t.randomMergeValues(1, 2)
			vp.SetValue(uint64(i), vs)

			ops[i] = vs
		}

		unmerge, done, wait, err := newFuncUnmerger(ctx, t.newJobWorker(), 1<<9, getValues, getMergers)
		t.NoError(err)

		t.T().Log("insert ops")
		{
			newJob, jobDone, jobWait, err := dutil.NewFuncJobWorker(ctx, 33)
			t.NoError(err)

			go func() {
				for i := range ops {
					opindex := uint64(i)

					if err := newJob(func(context.Context) error {
						return unmerge(opindex)
					}); err != nil {
						panic(err)
					}
				}

				jobDone()
			}()

			t.NoError(jobWait())
		}

		t.T().Log("insert done")
		done()

		t.NoError(wait(func(unmerged []uint64) error {
			slices.Sort(unmerged)

			t.T().Logf("wait done: unmerged ops=%v", unmerged)

			t.Equal(len(ops), len(unmerged))
			t.Equal(uint64(0), unmerged[0])
			t.Equal(uint64(len(ops)-1), unmerged[len(unmerged)-1])

			return nil
		}))

		_, err = t.closeMergers(mergers)
		t.ErrorContains(err, "under zero balance")
	})
}

func TestUnmerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testUnmerger))
}
