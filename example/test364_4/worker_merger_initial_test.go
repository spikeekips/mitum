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

		pargs, wpargs, args, vp, mergers, _, _, getState := t.newInitialValuesArgs("local")

		getMergers, closeMergers := newGetMergersFunc(mergers, getState)
		defer closeMergers()

		initial, _, wait, err := newFuncInitialValuesWorkerMerger(args, wpargs, pargs)(ctx, 1, 1<<9, getMergers)
		t.NoError(err)

		defer wait(nil)

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

		pargs, wpargs, args, vp, mergers, _, _, getState := t.newInitialValuesArgs("local")

		getMergers, closeMergers := newGetMergersFunc(mergers, getState)
		defer closeMergers()

		initial, _, wait, err := newFuncInitialValuesWorkerMerger(args, wpargs, pargs)(ctx, 2, 1<<9, getMergers)
		t.NoError(err)

		defer wait(nil)

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

		pargs, wpargs, args, _, mergers, _, _, getState := t.newInitialValuesArgs("local")

		getMergers, closeMergers := newGetMergersFunc(mergers, getState)
		defer closeMergers()

		initial, _, wait, err := newFuncInitialValuesWorkerMerger(args, wpargs, pargs)(ctx, 1, 1<<9, getMergers)
		t.NoError(err)

		defer wait(nil)

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

		pargs, wpargs, args, vp, mergers, _, _, getState := t.newInitialValuesArgs("local")

		getMergers, closeMergers := newGetMergersFunc(mergers, getState)
		defer closeMergers()

		initial, _, wait, err := newFuncInitialValuesWorkerMerger(args, wpargs, pargs)(ctx, 2, 1<<9, getMergers)
		t.NoError(err)

		defer wait(nil)

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

func TestInitialValuesWorkerMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testInitialValuesWorkerMerger))
}
