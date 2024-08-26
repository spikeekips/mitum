package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testBaseWorkerMerger struct {
	baseTestWorkerMerger
}

func (t *testBaseWorkerMerger) TestCancel() {
	t.Run("before insert", func() {
		ctx, cancel := context.WithCancel(context.Background())

		_, _, args, _, mergers, _, getValues, getState := t.newArgs("local")

		getMergers, closeMergers := newGetMergersFunc(mergers, getState)
		defer closeMergers()

		extractKeys, closeExtractKeys := newExtractStateMergeValueKeysFunc(1 << 9)
		defer closeExtractKeys()

		insert, wait, err := newBaseWorkerMerger(ctx, args, 1, getValues, getMergers, extractKeys)
		t.NoError(err)

		cancel()

		err = insert(localWorkerName, nil)
		t.ErrorIs(err, context.Canceled)
		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after insert", func() {
		ctx, cancel := context.WithCancel(context.Background())

		_, _, args, _, mergers, _, getValues, getState := t.newArgs("local")

		getMergers, closeMergers := newGetMergersFunc(mergers, getState)
		defer closeMergers()

		extractKeys, closeExtractKeys := newExtractStateMergeValueKeysFunc(1 << 9)
		defer closeExtractKeys()

		insert, wait, err := newBaseWorkerMerger(ctx, args, 3, getValues, getMergers, extractKeys)
		t.NoError(err)

		t.NoError(insert(localWorkerName, nil))

		cancel()

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testBaseWorkerMerger) TestPhaseDone() {
	t.Run("1 min enough", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, args, vp, mergers, _, getValues, getState := t.newArgs("local")

		getMergers, closeMergers := newGetMergersFunc(mergers, getState)
		defer closeMergers()

		extractKeys, closeExtractKeys := newExtractStateMergeValueKeysFunc(1 << 9)
		defer closeExtractKeys()

		insert, wait, err := newBaseWorkerMerger(ctx, args, 1, getValues, getMergers, extractKeys)
		t.NoError(err)

		ops := make([]uint64, 3)
		for i := range ops {
			opindex := uint64(i)

			_ = t.randomMergeValuesVP(vp, opindex, 1)
			ops[i] = opindex
		}

		t.NoError(insert(localWorkerName, ops))
		t.NoError(wait(nil))
	})

	t.Run("over 1 min enough", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, args, vp, mergers, _, getValues, getState := t.newArgs("local")

		other := "w0"

		var insert workerMergerPhaseDoneFunc
		var wait workerMergerWaitFunc

		var err error

		getMergers, closeMergers := newGetMergersFunc(mergers, getState)
		defer closeMergers()

		extractKeys, closeExtractKeys := newExtractStateMergeValueKeysFunc(1 << 9)
		defer closeExtractKeys()

		insert, wait, err = newBaseWorkerMerger(ctx, args, 2, getValues, getMergers, extractKeys)
		t.NoError(err)

		ops := make([]uint64, 6)
		for i := range ops {
			opindex := uint64(i)

			_ = t.randomMergeValuesVP(vp, opindex, 1)
			ops[i] = opindex
		}

		t.NoError(insert(localWorkerName, ops[:3]))
		t.NoError(insert(other, ops[3:]))

		t.NoError(wait(nil))
	})
}

func TestBaseWorkerMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testBaseWorkerMerger))
}
