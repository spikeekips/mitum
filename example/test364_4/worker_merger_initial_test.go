package main

import (
	"context"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testInitialValuesWorkerMerger struct {
	baseTestWorkerMerger
}

func (t *testInitialValuesWorkerMerger) TestInsertValues() {
	t.Run("one worker; ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, args, vp, mergers, _, getState := t.newInitialValuesArgs("local")

		getMergers, closeMergers := newGetMergers(mergers, getState)
		defer closeMergers()

		setStateOp, _, err := newSetStateOpFuncsWithLogger(1 << 9)
		t.NoError(err)

		initial, _, _, err := newFuncInitialValuesWorkerMerger(args, pargs)(ctx, 1<<3, 1, 1<<9, getMergers, setStateOp)
		t.NoError(err)

		insert, done, err := initial(localWorkerName)
		t.NoError(err)
		defer done()

		ops := [][]StateMergeValue{
			t.randomMergeValues(1, 2, 3),
			t.randomMergeValues(1, 2, 3),
			t.randomMergeValues(1, 2, 3),
		}

		for i := range ops {
			t.NoError(insert(uint64(i), ops[i]))
		}

		vp.Traverse(func(opindex uint64, b []StateMergeValue) bool {
			a := ops[opindex]

			t.Equal(len(a), len(b))
			for i := range a {
				t.Equal(a[i].Key(), b[i].Key())
				t.True(base.IsEqualStateValue(a[i].Value(), b[i].Value()))
			}

			return true
		})
	})

	t.Run("2 worker; ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, args, vp, mergers, _, getState := t.newInitialValuesArgs("local")

		getMergers, closeMergers := newGetMergers(mergers, getState)
		defer closeMergers()

		setStateOp, _, err := newSetStateOpFuncsWithLogger(1 << 9)
		t.NoError(err)

		initial, _, _, err := newFuncInitialValuesWorkerMerger(args, pargs)(ctx, 1<<3, 2, 1<<9, getMergers, setStateOp)
		t.NoError(err)

		insertLocal, doneLocal, err := initial(localWorkerName)
		t.NoError(err)
		defer doneLocal()

		insertOther, doneOther, err := initial("w0")
		t.NoError(err)
		defer doneOther()

		ops := [][]StateMergeValue{
			t.randomMergeValues(1, 2, 3),
			t.randomMergeValues(1, 2, 3),
			t.randomMergeValues(1, 2, 3),
			t.randomMergeValues(1, 2, 3),
		}

		for i := 0; i < 2; i++ {
			t.NoError(insertLocal(uint64(i), ops[i]))
		}

		for i := 2; i < len(ops); i++ {
			t.NoError(insertOther(uint64(i), ops[i]))
		}

		vp.Traverse(func(opindex uint64, b []StateMergeValue) bool {
			a := ops[opindex]

			t.Equal(len(a), len(b))
			for i := range a {
				t.Equal(a[i].Key(), b[i].Key())
				t.True(base.IsEqualStateValue(a[i].Value(), b[i].Value()))
			}

			return true
		})
	})

	t.Run("after enough", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, args, _, mergers, _, getState := t.newInitialValuesArgs("local")

		getMergers, closeMergers := newGetMergers(mergers, getState)
		defer closeMergers()

		setStateOp, _, err := newSetStateOpFuncsWithLogger(1 << 9)
		t.NoError(err)

		initial, _, _, err := newFuncInitialValuesWorkerMerger(args, pargs)(ctx, 1<<3, 1, 1<<9, getMergers, setStateOp)
		t.NoError(err)

		insertLocal, doneLocal, err := initial(localWorkerName)
		t.NoError(err)
		defer doneLocal()

		ops := [][]StateMergeValue{
			t.randomMergeValues(1, 2, 3),
			t.randomMergeValues(1, 2, 3),
		}

		t.NoError(insertLocal(0, ops[0]))
		t.NoError(doneLocal())

		insertOther, doneOther, err := initial("w0")
		t.NoError(err)

		err = insertOther(1, ops[1]) // NOTE util.ErrJobWorkerDone
		t.ErrorIs(err, util.ErrJobWorkerDone)

		t.NoError(doneOther())
	})

	t.Run("same op", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, args, vp, mergers, _, getState := t.newInitialValuesArgs("local")

		getMergers, closeMergers := newGetMergers(mergers, getState)
		defer closeMergers()

		setStateOp, _, err := newSetStateOpFuncsWithLogger(1 << 9)
		t.NoError(err)

		initial, _, _, err := newFuncInitialValuesWorkerMerger(args, pargs)(ctx, 1<<3, 2, 1<<9, getMergers, setStateOp)
		t.NoError(err)

		insertLocal, doneLocal, err := initial(localWorkerName)
		t.NoError(err)
		defer doneLocal()

		ops := [][]StateMergeValue{
			t.randomMergeValues(1, 2, 3),
			t.randomMergeValues(1, 2, 3),
		}

		t.NoError(insertLocal(0, ops[0]))
		t.NoError(insertLocal(1, ops[1]))
		t.NoError(doneLocal())

		insertOther, doneOther, err := initial("w0")
		t.NoError(err)

		t.NoError(insertOther(0, ops[0]))
		t.NoError(insertOther(1, ops[1]))
		t.NoError(doneOther())

		vp.Traverse(func(opindex uint64, b []StateMergeValue) bool {
			a := ops[opindex]

			t.Equal(len(a), len(b))
			for i := range a {
				t.Equal(a[i].Key(), b[i].Key())
				t.True(base.IsEqualStateValue(a[i].Value(), b[i].Value()))
			}

			return true
		})
	})
}

func (t *testInitialValuesWorkerMerger) TestBroadcastDone() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, args, vp, mergers, _, getState := t.newInitialValuesArgs("local")

		getMergers, closeMergers := newGetMergers(mergers, getState)
		defer closeMergers()

		setStateOp, _, err := newSetStateOpFuncsWithLogger(1 << 9)
		t.NoError(err)

		workers := t.workers(2)
		workers = append(workers, localWorkerName)

		initial, workerDone, wait, err := newFuncInitialValuesWorkerMerger(args, pargs)(ctx, 1<<3, uint64(len(workers)), 1<<9, getMergers, setStateOp)
		t.NoError(err)

		ops := make([][]StateMergeValue, (len(workers))*3)
		for i := range ops {
			ops[i] = t.randomMergeValues(1, 2, 3)
		}

		workerOps := map[string][]uint64{}

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			p := make([]uint64, 3)
			for j := range p {
				p[j] = uint64((i * 3) + j)
			}

			workerOps[worker] = p

			return true, nil
		})

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			t.T().Log("initial:", worker, workerOps[worker])

			insert, done, err := initial(worker)
			t.NoError(err)
			defer done()

			return true, util.TraverseSliceV1(workerOps[worker], func(_ int, opindex uint64) (bool, error) {
				return true, insert(opindex, ops[opindex])
			})
		})

		go func() {
			err := util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
				if worker == localWorkerName {
					return true, nil
				}

				return true, workerDone(worker, nil)
			})
			if err != nil {
				panic(err)
			}
		}()

		t.NoError(wait(
			func(_ context.Context, newUnmerged []uint64) error {
				t.T().Log("broadcast phase done:", newUnmerged)

				return nil
			},
			func(unmerged []uint64, reasons map[uint64]string) error {
				t.T().Log("done:", unmerged, reasons)

				return nil
			},
		))

		vp.Traverse(func(opindex uint64, b []StateMergeValue) bool {
			a := ops[opindex]

			t.Equal(len(a), len(b))
			for i := range a {
				t.Equal(a[i].Key(), b[i].Key())
				t.True(base.IsEqualStateValue(a[i].Value(), b[i].Value()))
			}

			return true
		})
	})
}

func TestInitialValuesWorkerMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testInitialValuesWorkerMerger))
}
