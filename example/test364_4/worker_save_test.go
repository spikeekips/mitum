package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorkerSave struct {
	baseTestWorkerMerger
}

func (t *testWorkerSave) newArgs(local string) (
	*phaseMergerArgs,
	*workerPhaseMergerArgs,
	*statesWorkerMergerArgs,
	*stateMergeValueExtractorArgs,
	*operationsWorkerMergerArgs,
	*workerSaveArgs,
	util.LockedMap[uint64, []StateMergeValue],
	util.LockedMap[string, StateMerger],
	util.LockedMap[string, base.State],
) {
	t.T().Helper()

	pargs, wpargs, sargs, vp, mergersPool, statesPool := t.newStatesArgs(local)

	eargs := newStateMergeValueExtractorArgs(local)
	oargs := newOperationsWorkerMergerArgs(sargs)

	args := newWorkerSaveArgs(oargs)

	return pargs, wpargs, sargs, eargs, oargs, args, vp, mergersPool, statesPool
}

func (t *testWorkerSave) TestInsertOps() {
	t.Run("no insert", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(nil), dummyStateFunc, dummySendMergeDoneFunc("local"), dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.NoError(err)

		t.NoError(insertOps(func(operationsWorkerLocalInsertOpFunc) error {
			return nil
		}))

		t.NoError(mergeDone())
		t.NoError(saveDone())
	})

	t.Run("insert, but all unmerged", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ops := make([]Operation, 33)
		expectedUnmerged := make([]uint64, len(ops))
		for i := range ops {
			ops[i] = t.randomOp(1)
			expectedUnmerged[i] = uint64(i)
		}

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		foundMergedStates := util.NewLocked(false)
		var unmerged []uint64

		sendMergeDone := func(_ context.Context, f workerSaveMergeDoneFunc) error {
			return f(
				func(statef workerSaveMergeStatesFuncs) error {
					return statef(func(stateKey string, h util.Hash) error {
						foundMergedStates.SetValue(true)

						return nil
					})
				},
				func(u []uint64) error {
					slices.Sort(u)
					unmerged = slices.Clone(u)

					return nil
				},
			)
		}

		gatheredOps := make([]Operation, len(ops))

		saveOperations := func(_ context.Context, f func(operationsWorkerSaveOperationsFunc) error) ([]blockFileChunk, error) {
			return nil, f(func(total uint64) operationsWorkerSaveOperationsCallbackFunc {
				fmt.Printf("save operations: total=%d\n", total)

				return func(tf func(operationsWorkerSaveOperationFunc) error) error {
					return tf(func(opindex uint64, op Operation) error {
						gatheredOps[opindex] = op

						return nil
					})
				}
			})
		}

		var gatheredReasons map[uint64]string

		saveReasons := func(reasons map[uint64]string) error {
			gatheredReasons = reasons

			return nil
		}

		saveDoneCh := make(chan bool, 1)
		sendSaveDone := func(context.Context) (workerSaveBlockFileChunksFunc, errorFunc, error) {
			saveDoneCh <- true

			return func(base.BlockItemType, []blockFileChunk) error { return nil }, func(err error) error { return err }, nil
		}

		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(ops), dummyStateFunc, sendMergeDone, saveOperations, dummySaveStatesFunc("local"), saveReasons, sendSaveDone)
		t.NoError(err)

		t.NoError(insertOps(func(f operationsWorkerLocalInsertOpFunc) error {
			for i := range ops {
				if err := f(uint64(i), nil, nil); err != nil {
					return err
				}
			}

			return nil
		}))

		t.NoError(mergeDone())
		t.NoError(saveDone())

		t.Equal(expectedUnmerged, unmerged)
		t.False(foundMergedStates.MustValue())

		for i := range ops {
			a := ops[i]
			b := gatheredOps[i]

			base.EqualOperation(t.Assert(), a, b)
		}

		t.Equal(len(ops), len(gatheredReasons))
		TraverseMap(gatheredReasons, func(opindex uint64, reason string) (bool, error) {
			t.True(opindex >= 0 && opindex < uint64(len(gatheredReasons)))
			t.Contains(reason, "insufficient")

			return true, nil
		})

		t.True(<-saveDoneCh)
	})

	t.Run("insert, ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		op := t.randomOp(1)
		ops := []Operation{op}
		issuer := op.Fact().(OpFact).Issuer
		receiver := op.Fact().(OpFact).Receiver
		issuerState := t.newState(issuer, 1)
		receiverState := t.newState(receiver, 1)

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		statesPool.SetValue(issuerState.Key(), issuerState)
		statesPool.SetValue(receiverState.Key(), receiverState)

		expectedStatesResult := map[string][2]interface{}{
			issuerState.Key():   {0, []util.Hash{op.Hash()}},
			receiverState.Key(): {2, []util.Hash{op.Hash()}},
		}

		newStateFunc := func() (newStateFunc, error) {
			return func(stateKey string, value base.StateValue, ophs []util.Hash) (base.State, error) {
				var prev util.Hash

				switch stateKey {
				case issuerState.Key():
					prev = issuerState.Hash()
				case receiverState.Key():
					prev = receiverState.Hash()
				default:
					return nil, util.ErrFound.Errorf("unknown state, %q", stateKey)
				}

				return base.NewBaseState(
					33,
					stateKey,
					value,
					prev,
					ophs,
				), nil
			}, nil
		}

		mergedStates := util.NewSingleLockedMap[string, util.Hash]()
		var unmerged []uint64

		sendMergeDone := func(_ context.Context, f workerSaveMergeDoneFunc) error {
			return f(
				func(statef workerSaveMergeStatesFuncs) error {
					if err := statef(func(stateKey string, h util.Hash) error {
						mergedStates.SetValue(stateKey, h)

						return nil
					}); err != nil {
						return err
					}

					return nil
				},
				func(u []uint64) error {
					slices.Sort(u)
					unmerged = slices.Clone(u)

					return nil
				},
			)
		}

		gatheredOps := make([]Operation, 1)

		saveOperations := func(_ context.Context, f func(operationsWorkerSaveOperationsFunc) error) ([]blockFileChunk, error) {
			return nil, f(func(total uint64) operationsWorkerSaveOperationsCallbackFunc {
				fmt.Printf("save operations: total=%d\n", total)

				return func(tf func(operationsWorkerSaveOperationFunc) error) error {
					return tf(func(opindex uint64, op Operation) error {
						gatheredOps[opindex] = op

						return nil
					})
				}
			})
		}

		var gatheredReasons map[uint64]string

		saveReasons := func(reasons map[uint64]string) error {
			gatheredReasons = reasons

			return nil
		}

		gatheredStates := util.NewSingleLockedMap[string, base.State]()

		saveStates := func(_ context.Context, f func(workerSaveStatesFunc) error) ([]blockFileChunk, error) {
			return nil, f(func(total uint64) workerSaveStatesCallbackFunc {
				fmt.Printf("save states: total=%d\n", total)

				return func(sf func(workerSaveStateFunc) error) error {
					return sf(func(st base.State) error {
						gatheredStates.SetValue(st.Key(), st)

						return nil
					})
				}
			})
		}

		saveDoneCh := make(chan bool, 1)
		sendSaveDone := func(context.Context) (workerSaveBlockFileChunksFunc, errorFunc, error) {
			saveDoneCh <- true

			return func(base.BlockItemType, []blockFileChunk) error { return nil }, func(err error) error { return err }, nil
		}

		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(ops), newStateFunc, sendMergeDone, saveOperations, saveStates, saveReasons, sendSaveDone)
		t.NoError(err)

		t.NoError(insertOps(func(f operationsWorkerLocalInsertOpFunc) error {
			return f(0, nil, nil)
		}))

		t.NoError(mergeDone())
		t.NoError(saveDone())

		t.Empty(unmerged)
		t.Equal(len(expectedStatesResult), mergedStates.Len())

		t.Equal(1, len(gatheredOps))
		base.EqualOperation(t.Assert(), op, gatheredOps[0])

		t.Empty(gatheredReasons)
		t.Equal(len(expectedStatesResult), gatheredStates.Len())
		for stateKey := range expectedStatesResult {
			a := expectedStatesResult[stateKey]
			b, found := gatheredStates.Value(stateKey)
			t.True(found)

			ab := a[0].(int)
			aophs := a[1].([]util.Hash)

			bb := int(b.Value().(AccountStateValue).Balance)
			bophs := b.Operations()

			t.Equal(len(aophs), len(bophs))
			for i := range aophs {
				t.True(aophs[i].Equal(bophs[i]))
			}

			t.Equal(ab, bb)
		}

		t.True(<-saveDoneCh)
	})
}

func (t *testWorkerSave) TestErrors() {
	t.Run("zero max ops size", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		_, _, _, _, _, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 0, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(nil), dummyStateFunc, dummySendMergeDoneFunc("local"), dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.ErrorContains(err, "empty size")
	})

	t.Run("merge wait, save wait again", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(nil), dummyStateFunc, dummySendMergeDoneFunc("local"), dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.NoError(err)

		t.NoError(insertOps(func(operationsWorkerLocalInsertOpFunc) error { return nil }))

		t.NoError(mergeDone())
		t.NoError(mergeDone())

		t.NoError(saveDone())
		t.NoError(saveDone())
	})

	t.Run("insert error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(nil), dummyStateFunc, dummySendMergeDoneFunc("local"), dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.NoError(err)

		err = insertOps(func(operationsWorkerLocalInsertOpFunc) error {
			return errors.Errorf("findme")
		})
		t.ErrorContains(err, "findme")

		err = mergeDone()
		t.ErrorContains(err, "findme")
		err = saveDone()
		t.ErrorContains(err, "findme")
	})

	t.Run("worker insert error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		insertOps, workerInsertOps, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(nil), dummyStateFunc, dummySendMergeDoneFunc("local"), dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.NoError(err)

		t.NoError(insertOps(func(operationsWorkerLocalInsertOpFunc) error { return nil }))
		_, _, err = workerInsertOps(localWorkerName)
		t.ErrorIs(err, util.ErrFound)

		err = mergeDone()
		t.ErrorIs(err, util.ErrFound)
		err = saveDone()
		t.ErrorIs(err, util.ErrFound)
	})

	t.Run("NewStateFunc error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		newStateFunc := func() (newStateFunc, error) {
			return nil, errors.Errorf("findme")
		}

		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(nil), newStateFunc, dummySendMergeDoneFunc("local"), dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.NoError(err)

		t.NoError(insertOps(func(operationsWorkerLocalInsertOpFunc) error { return nil }))

		err = mergeDone()
		t.ErrorContains(err, "findme")
		err = saveDone()
		t.ErrorContains(err, "findme")
	})

	t.Run("send merge done error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		sendMergeDone := func(context.Context, workerSaveMergeDoneFunc) error {
			return errors.Errorf("findme")
		}

		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(nil), dummyStateFunc, sendMergeDone, dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.NoError(err)

		t.NoError(insertOps(func(operationsWorkerLocalInsertOpFunc) error { return nil }))

		err = mergeDone()
		t.ErrorContains(err, "findme")
		err = saveDone()
		t.ErrorContains(err, "findme")
	})

	t.Run("send merge done, saveWorkerMergerSendStatesFuncs error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		sendMergeDone := func(_ context.Context, f workerSaveMergeDoneFunc) error {
			return f(
				func(statef workerSaveMergeStatesFuncs) error {
					return errors.Errorf("findme")
				},
				func(unmerged []uint64) error {
					return nil
				},
			)
		}

		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(nil), dummyStateFunc, sendMergeDone, dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.NoError(err)

		t.NoError(insertOps(func(operationsWorkerLocalInsertOpFunc) error { return nil }))

		err = mergeDone()
		t.ErrorContains(err, "findme")
		err = saveDone()
		t.ErrorContains(err, "findme")
	})

	t.Run("send merge done, saveWorkerMergerUnmergedFunc error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		sendMergeDone := func(_ context.Context, f workerSaveMergeDoneFunc) error {
			return f(
				func(statef workerSaveMergeStatesFuncs) error {
					return nil
				},
				func(unmerged []uint64) error {
					return errors.Errorf("findme")
				},
			)
		}

		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(nil), dummyStateFunc, sendMergeDone, dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.NoError(err)

		t.NoError(insertOps(func(operationsWorkerLocalInsertOpFunc) error { return nil }))

		err = mergeDone()
		t.ErrorContains(err, "findme")
		err = saveDone()
		t.ErrorContains(err, "findme")
	})

	t.Run("SaveStates error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		op := t.randomOp(1)
		ops := []Operation{op}

		issuerState := t.newState(op.Fact().(OpFact).Issuer, 1)
		receiverState := t.newState(op.Fact().(OpFact).Receiver, 1)

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		statesPool.SetValue(issuerState.Key(), issuerState)
		statesPool.SetValue(receiverState.Key(), receiverState)

		saveStates := func(_ context.Context, f func(workerSaveStatesFunc) error) ([]blockFileChunk, error) {
			return nil, f(func(total uint64) workerSaveStatesCallbackFunc {
				return func(func(workerSaveStateFunc) error) error {
					return errors.Errorf("findme")
				}
			})
		}

		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(ops), dummyStateFunc, dummySendMergeDoneFunc("local"), dummySaveOperationsFunc("local"), saveStates, dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.NoError(err)

		t.NoError(insertOps(func(f operationsWorkerLocalInsertOpFunc) error {
			return f(0, nil, nil)
		}))

		t.NoError(mergeDone())
		err = saveDone()
		t.ErrorContains(err, "findme")
	})

	t.Run("SaveReasons error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		op := t.randomOp(1)
		ops := []Operation{op}

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		saveReasons := func(reasons map[uint64]string) error {
			return errors.Errorf("findme")
		}

		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(ops), dummyStateFunc, dummySendMergeDoneFunc("local"), dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), saveReasons, dummySendSaveDoneFunc("local"))
		t.NoError(err)

		t.NoError(insertOps(func(f operationsWorkerLocalInsertOpFunc) error {
			return f(0, nil, nil)
		}))

		t.NoError(mergeDone())
		err = saveDone()
		t.ErrorContains(err, "findme")
	})

	t.Run("SaveDone error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		op := t.randomOp(1)
		ops := []Operation{op}

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		sendSaveDone := func(context.Context) (workerSaveBlockFileChunksFunc, errorFunc, error) {
			return func(base.BlockItemType, []blockFileChunk) error { return nil }, func(err error) error {
				if err != nil {
					return err
				}

				return errors.Errorf("findme")
			}, nil
		}

		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(ops), dummyStateFunc, dummySendMergeDoneFunc("local"), dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), sendSaveDone)
		t.NoError(err)

		t.NoError(insertOps(func(f operationsWorkerLocalInsertOpFunc) error {
			return f(0, nil, nil)
		}))

		t.NoError(mergeDone())
		err = saveDone()
		t.ErrorContains(err, "findme")
	})
}

func (t *testWorkerSave) TestCancel() {
	t.Run("before insertOps; empty ops", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(nil), dummyStateFunc, dummySendMergeDoneFunc("local"), dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.NoError(err)

		cancel()

		t.NoError(insertOps(func(f operationsWorkerLocalInsertOpFunc) error { return nil }))

		err = mergeDone()
		t.ErrorIs(err, context.Canceled)
		err = saveDone()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before insertOps", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		op := t.randomOp(1)
		ops := []Operation{op}

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(ops), dummyStateFunc, dummySendMergeDoneFunc("local"), dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.NoError(err)

		cancel()

		err = insertOps(func(f operationsWorkerLocalInsertOpFunc) error {
			return f(0, nil, nil)
		})
		t.ErrorIs(err, context.Canceled)

		err = mergeDone()
		t.ErrorIs(err, context.Canceled)
		err = saveDone()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before worker insert", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		canceledch := make(chan bool, 1)
		sendMergeDone := func(_ context.Context, f workerSaveMergeDoneFunc) error {
			return f(
				func(workerSaveMergeStatesFuncs) error { return nil },
				func([]uint64) error {
					<-canceledch

					return nil
				},
			)
		}

		insertOps, workerInsertOps, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(nil), dummyStateFunc, sendMergeDone, dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.NoError(err)

		t.NoError(insertOps(func(operationsWorkerLocalInsertOpFunc) error { return nil }))

		cancel()
		close(canceledch)

		insertW, _, err := workerInsertOps("w0")
		t.NoError(err)

		switch err := insertW(1, valuehash.RandomSHA256(), t.randomMergeValues(1)); {
		case errors.Is(err, context.Canceled),
			errors.Is(err, util.ErrJobWorkerDone):
		default:
			t.NoError(err)
		}

		err = mergeDone()
		t.ErrorIs(err, context.Canceled, err)
		err = saveDone()
		t.ErrorIs(err, context.Canceled, err)
	})

	t.Run("before worker phase done", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		insertOps, _, workerPhaseDone, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(nil), dummyStateFunc, dummySendMergeDoneFunc("local"), dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.NoError(err)

		t.NoError(insertOps(func(operationsWorkerLocalInsertOpFunc) error { return nil }))

		cancel()

		err = workerPhaseDone("w0", []uint64{1, 2})
		t.ErrorIs(err, context.Canceled)

		err = mergeDone()
		t.ErrorIs(err, context.Canceled)
		err = saveDone()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before merge done wait", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(nil), dummyStateFunc, dummySendMergeDoneFunc("local"), dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.NoError(err)

		t.NoError(insertOps(func(operationsWorkerLocalInsertOpFunc) error { return nil }))

		cancel()

		err = mergeDone()
		t.ErrorIs(err, context.Canceled)
		err = saveDone()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before save wait", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs("local")

		sendMergeDone := func(_ context.Context, f workerSaveMergeDoneFunc) error {
			return f(
				func(workerSaveMergeStatesFuncs) error {
					cancel() // NOTE cancel

					return nil
				},
				func([]uint64) error {
					return nil
				},
			)
		}
		insertOps, _, _, mergeDone, saveDone, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(ctx, 1, 1<<9, nil, newGetStateFunc(statesPool), dummyIsLocalStateValue, dummyBroadcastStateMergeValuesFuncs, newGetOperationFunc(nil), dummyStateFunc, sendMergeDone, dummySaveOperationsFunc("local"), dummySaveStatesFunc("local"), dummySaveReasonsFunc("local"), dummySendSaveDoneFunc("local"))
		t.NoError(err)

		t.NoError(insertOps(func(operationsWorkerLocalInsertOpFunc) error { return nil }))

		_ = mergeDone()

		err = saveDone()
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testWorkerSave) testMerger(
	workerValues map[ /* worker */ string][] /* state key */ string,
	workerOps map[ /* worker */ string][]uint64,
	minEnough uint64,
	sts []base.State,
	ops []Operation,
	expectedUnmerged []uint64,
	expectedReasons map[uint64]string,
	expectedStateOps map[string][]uint64,
	expectedStates map[string][2]interface{},
) {
	t.T().Helper()

	t.T().Logf("condition: values=%v ops=%v enough workers=%d", workerValues, workerOps, minEnough)

	workerNames := make([]string, len(workerValues))

	var i int
	for local := range workerValues {
		workerNames[i] = local
		i++
	}

	insertOps := map[string]operationsWorkerLocalInsertOpsFuncs{}
	workerInitialInsertOps := map[string]operationsworkerInitialInsertOpsFuncs{}
	phaseDones := map[string]workerMergerPhaseDoneFunc{}
	mergeWaits := map[string]func() error{}
	saveWaits := map[string]func() error{}

	for i := range workerNames {
		workerInitialInsertOps[workerNames[i]] = nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gatheredReasons := util.NewSingleLockedMap[uint64, string]()
	gatheredStates := util.NewSingleLockedMap[string, base.State]()
	mergedStates := util.NewSingleLockedMap[string, util.Hash]()
	gatheredUnmerged := util.EmptyLocked[[]uint64]()
	gatheredOps := util.NewSingleLockedMap[uint64, Operation]()
	gatheredBlockFileChunks := util.NewSingleLockedMap[string, [2]blockFileChunk]()

	for i := range workerNames {
		local := workerNames[i]

		wctx, wcancel := context.WithCancel(ctx)
		defer wcancel()

		pargs, wpargs, _, eargs, _, args, _, _, statesPool := t.newArgs(local)

		for i := range sts {
			st := sts[i]

			statesPool.SetValue(st.Key(), st)
		}

		//if local == "w1" {
		//	args.SetLogging(logging.TestLogging)
		//}

		args.local = local

		broadcastStateMergeValues := func(ctx context.Context) (
			operationsworkerInitialInsertOpFunc,
			func() error,
			error,
		) {
			f, err := newBroadcastStateMergeValuesFuncsFromInitialInsertOpsFunc(
				wctx,
				local,
				workerValues,
				workerInitialInsertOps,
			)
			if err != nil {
				return nil, nil, err
			}

			return f(ctx)
		}

		broadcastPhaseDone := func(_ context.Context, unmerged []uint64) error {
			gerrs := map[string]error{}

			TraverseMap(phaseDones, func(worker string, phaseDone workerMergerPhaseDoneFunc) (bool, error) {
				if worker == local {
					return true, nil
				}

				if err := phaseDone(local, unmerged); err != nil {
					gerrs[worker] = err
				}

				return true, nil
			})

			fmt.Printf("broadcast phase done: local=%q unmerged=%v errs=%+v\n", local, unmerged, gerrs)

			return nil
		}
		isLocalStateValue := func(stateKey string) bool {
			return slices.Index(workerValues[local], stateKey) >= 0
		}
		newStateFunc := func() (newStateFunc, error) {
			return func(stateKey string, value base.StateValue, ophs []util.Hash) (base.State, error) {
				return base.NewBaseState(33, stateKey, value, nil, ophs), nil
			}, nil
		}
		sendMergeDone := func(_ context.Context, f workerSaveMergeDoneFunc) error {
			return f(
				func(statef workerSaveMergeStatesFuncs) error {
					if err := statef(func(stateKey string, h util.Hash) error {
						mergedStates.SetValue(stateKey, h)

						return nil
					}); err != nil {
						return err
					}

					return nil
				},
				func(unmerged []uint64) error {
					gatheredUnmerged.Set(func(prev []uint64, _ bool) ([]uint64, error) {
						util.TraverseSliceV1(unmerged, func(_ int, opindex uint64) (bool, error) {
							if slices.Index(prev, opindex) < 0 {
								prev = append(prev, opindex)
							}

							return true, nil
						})

						return prev, nil
					})

					return nil
				},
			)
		}
		expectedOpsChunk := newBaseBlockFileChunk(util.UUID().String(), util.UUID().String(), "gz")
		saveOperations := func(_ context.Context, f func(operationsWorkerSaveOperationsFunc) error) ([]blockFileChunk, error) {
			return []blockFileChunk{expectedOpsChunk},
				f(func(total uint64) operationsWorkerSaveOperationsCallbackFunc {
					fmt.Printf("save operations: local=%q total=%d\n", local, total)

					return func(tf func(operationsWorkerSaveOperationFunc) error) error {
						return tf(func(opindex uint64, op Operation) error {
							gatheredOps.SetValue(opindex, op)

							return nil
						})
					}
				})
		}
		saveReasons := func(reasons map[uint64]string) error {
			TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
				gatheredReasons.SetValue(opindex, reason)

				return true, nil
			})

			return nil
		}
		expectedStatesChunk := newBaseBlockFileChunk(util.UUID().String(), util.UUID().String(), "gz")
		saveStates := func(_ context.Context, f func(workerSaveStatesFunc) error) ([]blockFileChunk, error) {
			return []blockFileChunk{expectedStatesChunk},
				f(func(total uint64) workerSaveStatesCallbackFunc {
					fmt.Printf("save states: local=%q total=%d\n", local, total)

					return func(sf func(workerSaveStateFunc) error) error {
						return sf(func(st base.State) error {
							gatheredStates.SetValue(st.Key(), st)

							return nil
						})
					}
				})
		}
		sendSaveDone := func(context.Context) (workerSaveBlockFileChunksFunc, errorFunc, error) {
			return func(it base.BlockItemType, bs []blockFileChunk) error {
					_, _, err := gatheredBlockFileChunks.Set(local, func(p [2]blockFileChunk, _ bool) ([2]blockFileChunk, error) {
						if len(bs) < 1 {
							return [2]blockFileChunk{}, util.ErrFound.Errorf("empty")
						}

						switch bs[0].URI() {
						case expectedOpsChunk.URI():
							if p[0] != nil {
								return [2]blockFileChunk{}, util.ErrFound.Errorf("operations already set")
							}

							p[0] = bs[0]
						case expectedStatesChunk.URI():
							if p[1] != nil {
								return [2]blockFileChunk{}, util.ErrFound.Errorf("states already set")
							}

							p[1] = bs[0]
						}

						return p, nil
					})

					return err
				},
				func(err error) error { return err },
				nil
		}

		insert, initialInsertOps, phaseDone, mergeWait, saveWait, err := newFuncWorkerSave(args, eargs, wpargs, pargs)(
			wctx,
			minEnough,
			1<<9,
			broadcastPhaseDone,
			newGetStateFunc(statesPool),
			isLocalStateValue,
			broadcastStateMergeValues,
			newGetOperationFunc(ops),
			newStateFunc,
			sendMergeDone,
			saveOperations,
			saveStates,
			saveReasons,
			sendSaveDone,
		)
		t.NoError(err)

		insertOps[local] = insert
		workerInitialInsertOps[local] = initialInsertOps
		phaseDones[local] = phaseDone
		mergeWaits[local] = mergeWait
		saveWaits[local] = saveWait
	}

	var initialWg sync.WaitGroup

	for local := range insertOps {
		local := local

		var ops []uint64

		for _, opindex := range workerOps[local] {
			ops = append(ops, opindex)
		}

		slices.Sort(ops)

		t.T().Logf("insert ops local=%q ops=%v", local, ops)

		initialWg.Add(1)

		go func() {
			defer initialWg.Done()

			insertOps[local](func(f operationsWorkerLocalInsertOpFunc) error {
				for _, opindex := range workerOps[local] {
					if err := f(opindex, nil, nil); err != nil {
						return err
					}
				}

				return nil
			})
		}()
	}

	initialWg.Wait()

	endch := make(chan struct{})
	endedWorkers := util.NewSingleLockedMap[string, struct{}]()

	var endOnce sync.Once

	endWait := func() {
		endOnce.Do(func() {
			close(endch)
		})
	}

	for local := range mergeWaits {
		local := local
		wait := mergeWaits[local]

		go func() {
			err := wait()
			if err != nil {
				fmt.Printf("merge wait error: local=%q err=%+v\n", local, err)
			}

			switch {
			case err == nil,
				errors.Is(err, util.ErrJobWorkerDone):
			case errors.Is(err, context.Canceled):
				return
			default:
				panic(fmt.Errorf("merge wait: local=%s %w", local, err))
			}
		}()
	}

	for local := range saveWaits {
		local := local
		wait := saveWaits[local]

		go func() {
			err := wait()
			if err != nil {
				fmt.Printf("save wait error: local=%q err=%+v\n", local, err)
			}

			switch {
			case err == nil,
				errors.Is(err, util.ErrJobWorkerDone):
			case errors.Is(err, context.Canceled):
				return
			default:
				panic(fmt.Errorf("save wait: local=%s %w", local, err))
			}

			endedWorkers.SetValue(local, struct{}{})
			if uint64(endedWorkers.Len()) >= minEnough {
				endWait()
			}
		}()
	}

	select {
	case <-time.After(time.Second * 2):
		t.Fail("wait")
	case <-ctx.Done():
	case <-endch:
	}

	gatheredUnmerged.Get(func(unmerged []uint64, _ bool) error {
		slices.Sort(unmerged)

		t.Equal(expectedUnmerged, unmerged)

		t.T().Log("unmerged ops:", unmerged)

		return nil
	})

	if expectedReasons != nil {
		t.Equal(len(expectedReasons), gatheredReasons.Len(), "reasons not matched; expected=%v gathered=%v", expectedReasons, gatheredReasons.Len())

		reasons := gatheredReasons.Map()

		TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
			r, found := expectedReasons[opindex]
			t.True(found)
			t.Equal(r, reason)

			return true, nil
		})
	}

	t.Equal(len(expectedStates), gatheredStates.Len(), "expected=%v gathered=%v", expectedStates, gatheredStates.Map())

	for stateKey := range expectedStates {
		a := expectedStates[stateKey]

		b, found := gatheredStates.Value(stateKey)
		t.True(found)

		t.Equal(a[0], int(b.Value().(AccountStateValue).Balance), stateKey)

		var aophs []util.Hash
		util.TraverseSliceV1(expectedStateOps[stateKey], func(_ int, opindex uint64) (bool, error) {
			aophs = append(aophs, ops[opindex].Hash())

			return true, nil
		})

		bophs := b.Operations()

		slices.SortFunc(aophs, func(a, b util.Hash) int { return strings.Compare(a.String(), b.String()) })
		slices.SortFunc(bophs, func(a, b util.Hash) int { return strings.Compare(a.String(), b.String()) })

		t.Equal(len(aophs), len(bophs))

		for i := range aophs {
			t.True(aophs[i].Equal(bophs[i]))
		}
	}

	endedWorkers.Traverse(func(local string, _ struct{}) bool {
		p, found := gatheredBlockFileChunks.Value(local)
		t.True(found)
		t.NotNil(p[0])
		t.NotNil(p[1])

		return true
	})
}

func (t *testWorkerSave) TestMergeNWorkers3States() {
	prv0, prv1, prv2 := base.NewDummyPrivatekey("0"), base.NewDummyPrivatekey("1"), base.NewDummyPrivatekey("2")
	pub0, pub1, pub2 := prv0.Publickey(), prv1.Publickey(), prv2.Publickey()

	sts := []base.State{
		t.newState(pub0, 1),
		t.newState(pub1, 1),
		t.newState(pub2, 1),
	}

	st0, st1, st2 := sts[0].Key(), sts[1].Key(), sts[2].Key()

	ops := []Operation{
		t.newOp(prv0, pub1, 1),
		t.newOp(prv0, pub2, 1), // 1: ignored
		t.newOp(prv1, pub0, 1),
		t.newOp(prv1, pub2, 1), // 3: ignored
		t.newOp(prv2, pub0, 1),
		t.newOp(prv2, pub1, 1), // 5: ignored
	}

	expectedUnmerged := []uint64{1, 3, 5}
	expectedReasons := map[uint64]string{
		1: "previous issuer found",
		3: "previous issuer found",
		5: "previous issuer found",
	}

	/*
		            | p0 | p1 | p2 |
		       base |  1 |  1 |  1 |
		p0 -> p1, 1 |  0 |  2 |    |
		p1 -> p0, 1 |  1 |  1 |    |
		p2 -> p0, 1 |  2 |    |  0 |
		            |  2 |  - |  0 |

		 state key | pub
		-----------+------
		   ta-0dpu | pub0
		   ta-1dpu | pub1
		   ta-2dpu | pub2
		-----------+------
	*/

	expectedStateOps := map[string][]uint64{
		"ta-0dpu": {0, 2, 4},
		"ta-2dpu": {4},
	}

	expectedStates := map[string][2]interface{}{
		"ta-0dpu": {2, []uint64{0, 2, 4}},
		"ta-2dpu": {0, []uint64{4}},
	}

	run := func(
		workerValues map[string][]string,
		workerOps map[string][]uint64,
		enough uint64,
	) func() {
		return func() {
			t.testMerger(
				workerValues,
				workerOps,
				enough,
				sts,
				ops,
				expectedUnmerged,
				expectedReasons,
				expectedStateOps,
				expectedStates,
			)
		}
	}

	t.Run("one worker", run(
		map[string][]string{
			"w0": {st0, st1, st2},
		},
		map[string][]uint64{
			"w0": {0, 1, 2, 3, 4, 5},
		},
		1,
	))

	t.Run("2 worker", run(
		map[string][]string{
			"w0": {st0, st1},
			"w1": {st1, st2},
		},
		map[string][]uint64{
			"w0": {0, 1, 2},
			"w1": {3, 4, 5},
		},
		2,
	))

	t.Run("3 worker", run(
		map[string][]string{
			"w0": {st0},
			"w1": {st1},
			"w2": {st2},
		},
		map[string][]uint64{
			"w0": {0, 1},
			"w1": {2, 3},
			"w2": {4, 5},
		},
		3,
	))

	t.Run("3 worker; w0 1 shared", run(
		map[string][]string{
			"w0": {st0, st1},
			"w1": {st1},
			"w2": {st2},
		},
		map[string][]uint64{
			"w0": {0, 1, 2, 3},
			"w1": {2, 3},
			"w2": {4, 5},
		},
		3,
	))

	t.Run("3 worker; 2 shared + 2 enough", run(
		map[string][]string{
			"w0": {st0, st1},
			"w1": {st1, st2},
			"w2": {st2, st0},
		},
		map[string][]uint64{
			"w0": {0, 1, 2, 3},
			"w1": {2, 3, 4, 5},
			"w2": {0, 1, 4, 5},
		},
		2,
	))

	t.Run("3 worker; 2 shared + 3 enough", run(
		map[string][]string{
			"w0": {st0, st1},
			"w1": {st1, st2},
			"w2": {st2, st0},
		},
		map[string][]uint64{
			"w0": {0, 1, 2, 3},
			"w1": {2, 3, 4, 5},
			"w2": {0, 1, 4, 5},
		},
		3,
	))

	t.Run("4 worker; 2 shared + 3 enough", run(
		map[string][]string{
			"w0": {st0, st1},
			"w1": {st1, st2},
			"w2": {st2, st0},
			"w3": {st0, st1},
		},
		map[string][]uint64{
			"w0": {0, 1, 2},
			"w1": {2, 3, 4},
			"w2": {3, 4, 5},
			"w3": {0, 5, 1},
		},
		3,
	))

	t.Run("6 worker; 2 shared = 5 enough", run(
		map[string][]string{
			"w0": {st0, st1},
			"w1": {st1, st2},
			"w2": {st2, st0},
			"w3": {st0, st1},
			"w4": {st1, st2},
			"w5": {st2, st0},
		},
		map[string][]uint64{
			"w0": {0, 1},
			"w1": {1, 2},
			"w2": {2, 3},
			"w3": {3, 4},
			"w4": {4, 5},
			"w5": {5, 0},
		},
		5,
	))

	t.Run("6 worker; 2 shared = 6 enough", run(
		map[string][]string{
			"w0": {st0, st1},
			"w1": {st1, st2},
			"w2": {st2, st0},
			"w3": {st0, st1},
			"w4": {st1, st2},
			"w5": {st2, st0},
		},
		map[string][]uint64{
			"w0": {0, 1},
			"w1": {1, 2},
			"w2": {2, 3},
			"w3": {3, 4},
			"w4": {4, 5},
			"w5": {5, 0},
		},
		6,
	))
}

func TestWorkerSave(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerSave))
}

func dummyStateFunc() (newStateFunc, error) {
	return func(string, base.StateValue, []util.Hash) (base.State, error) { return nil, nil }, nil
}

func dummySendMergeDoneFunc(local string) workerSaveSendMergeDoneFuncs {
	return func(_ context.Context, f workerSaveMergeDoneFunc) error {
		fmt.Printf("merge done: local=%q\n", local)

		return f(
			func(statef workerSaveMergeStatesFuncs) error {
				return statef(func(stateKey string, h util.Hash) error {
					fmt.Printf("merge done.state: local=%q state key=%q state=%q\n", local, stateKey, h)

					return nil
				})
			},
			func(unmerged []uint64) error {
				fmt.Printf("merge done.unmerged: local=%q unmerged=%v\n", local, unmerged)

				return nil
			},
		)
	}
}

func dummySaveOperationsFunc(local string) workerSaveSaveOperationsFunc {
	return func(_ context.Context, f func(operationsWorkerSaveOperationsFunc) error) ([]blockFileChunk, error) {
		return nil, f(func(total uint64) operationsWorkerSaveOperationsCallbackFunc {
			fmt.Printf("save operations: local=%q total=%d\n", local, total)

			return func(tf func(operationsWorkerSaveOperationFunc) error) error {
				return tf(func(opindex uint64, op Operation) error {
					fmt.Printf("save operations.op: local=%q opindex=%d op=%v\n", local, opindex, op.Hash())

					return nil
				})
			}
		})
	}
}

func dummySaveStatesFunc(local string) workerSaveStatesFuncs {
	return func(_ context.Context, f func(workerSaveStatesFunc) error) ([]blockFileChunk, error) {
		return nil, f(func(total uint64) workerSaveStatesCallbackFunc {
			fmt.Printf("save states: local=%q total=%d\n", local, total)

			return func(sf func(workerSaveStateFunc) error) error {
				return sf(func(st base.State) error {
					fmt.Printf("save states.state: local=%q state key=%q st=%v\n", local, st.Key(), st.Hash())

					return nil
				})
			}
		})
	}
}

func dummySaveReasonsFunc(local string) workerSaveReasonsFunc {
	return func(reasons map[uint64]string) error {
		fmt.Printf("save reasons: local=%q reasons=%v\n", local, reasons)

		return nil
	}
}

func dummySendSaveDoneFunc(local string) workerSaveSendSaveDoneFunc {
	return func(context.Context) (workerSaveBlockFileChunksFunc, errorFunc, error) {
		fmt.Printf("save done: local=%q\n", local)

		return func(base.BlockItemType, []blockFileChunk) error { return nil }, func(err error) error { return err }, nil
	}
}
