package main

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testOperationsWorkerMerger struct {
	baseTestWorkerMerger
}

func (t *testOperationsWorkerMerger) newArgs(enoughWorkers uint64, ops []Operation) (
	*PhaseMergerArgs,
	*workerPhaseMergerArgs,
	*statesWorkerMergerArgs,
	*operationsWorkerMergerArgs,
	util.LockedMap[uint64, []StateMergeValue],
	util.LockedMap[string, StateMerger],
	util.LockedMap[string, base.State],
) {
	pargs, wpargs, sargs, vp, mergersPool, statesPool := t.newStatesArgs(enoughWorkers)

	sargs.BroadcastPhaseDone = func(context.Context, []uint64) error {
		return nil
	}
	sargs.NewStateFunc = func() (newStateFunc, error) {
		return func(string, base.StateValue, []uint64) (base.State, error) { return nil, nil }, nil
	}

	eargs := newStateMergeValueExtractorArgs()
	eargs.GetOperation = func(_ context.Context, opindex uint64, _ util.Hash) (Operation, bool, error) {
		if opindex >= uint64(len(ops)) {
			return nil, false, nil
		}

		return ops[opindex], true, nil
	}

	args := newOperationsWorkerMergerArgs(sargs, eargs)
	args.SetLogging(logging.TestNilLogging)

	return pargs, wpargs, sargs, args, vp, mergersPool, statesPool
}

func (t *testOperationsWorkerMerger) TestInsertOp() {
	t.Run("no insert", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, _, args, _, _, _ := t.newArgs(1, nil)

		gatheredState := util.NewSingleLockedMap[string, [2]interface{}]()

		args.NewStateFunc = func() (newStateFunc, error) {
			return func(stateKey string, value base.StateValue, ops []uint64) (base.State, error) {
				gatheredState.SetValue(stateKey, [2]interface{}{value, ops})

				return nil, nil
			}, nil
		}

		insertOps, _, _, wait, err := newOperationsWorkerMerger(ctx, args)
		t.NoError(err)

		t.NoError(insertOps(func(operationsWorkerInsertOpFunc) error {
			return nil
		}))

		gatheredOps := util.NewSingleLockedMap[uint64, Operation]()

		t.NoError(wait(func(
			unmerged []uint64,
			reasons map[uint64]string,
			traverseStates func(func(base.State) error) error,
			traverseOperations func(func(uint64, Operation) error) error,
		) error {
			t.Empty(unmerged)
			t.Empty(reasons)

			t.NoError(traverseStates(nil))

			t.NoError(traverseOperations(func(opindex uint64, op Operation) error {
				gatheredOps.SetValue(opindex, op)

				return nil
			}))

			return nil
		}))

		t.Equal(0, gatheredState.Len())
		t.Equal(0, gatheredOps.Len())
	})

	t.Run("one", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		op := t.randomOp(1)
		issuerState := t.newState(op.Fact().(OpFact).Issuer, 1)
		receiverState := t.newState(op.Fact().(OpFact).Receiver, 1)

		_, _, _, args, _, _, statesPool := t.newArgs(1, []Operation{op})

		gatheredState := util.NewSingleLockedMap[string, [2]interface{}]()

		args.NewStateFunc = func() (newStateFunc, error) {
			return func(stateKey string, value base.StateValue, ops []uint64) (base.State, error) {
				gatheredState.SetValue(stateKey, [2]interface{}{value, ops})

				return nil, nil
			}, nil
		}

		insertOps, _, _, wait, err := newOperationsWorkerMerger(ctx, args)
		t.NoError(err)

		statesPool.SetValue(issuerState.Key(), issuerState)
		statesPool.SetValue(receiverState.Key(), receiverState)

		t.NoError(insertOps(func(f operationsWorkerInsertOpFunc) error {
			return f(0, nil, nil)
		}))

		gatheredOps := util.NewSingleLockedMap[uint64, Operation]()

		t.NoError(wait(func(
			unmerged []uint64,
			reasons map[uint64]string,
			traverseStates func(func(base.State) error) error,
			traverseOperations func(func(uint64, Operation) error) error,
		) error {
			t.Empty(unmerged)
			t.Empty(reasons)

			t.NoError(traverseStates(nil))

			t.NoError(traverseOperations(func(opindex uint64, op Operation) error {
				gatheredOps.SetValue(opindex, op)

				return nil
			}))

			return nil
		}))

		t.Equal(2, gatheredState.Len())
		{
			gsts, found := gatheredState.Value(issuerState.Key())
			t.True(found)

			gstv, gstops := gsts[0].(base.StateValue), gsts[1].([]uint64)
			t.Equal(issuerState.Value().(AccountStateValue).Balance-1, gstv.(AccountStateValue).Balance)
			t.Equal([]uint64{0}, gstops)
		}
		{
			gsts, found := gatheredState.Value(receiverState.Key())
			t.True(found)

			gstv, gstops := gsts[0].(base.StateValue), gsts[1].([]uint64)
			t.Equal(receiverState.Value().(AccountStateValue).Balance+1, gstv.(AccountStateValue).Balance)
			t.Equal([]uint64{0}, gstops)
		}

		t.Equal(1, gatheredOps.Len())
		gop, found := gatheredOps.Value(0)
		t.True(found)
		base.EqualOperation(t.Assert(), op, gop)
	})
}

func (t *testOperationsWorkerMerger) testMerger(
	workerValues map[ /* worker */ string][] /* state key */ string,
	workerOps map[ /* worker */ string][]uint64,
	enoughWorkers uint64,
	sts []base.State,
	ops []Operation,
	expectedUnmerged []uint64,
	expectedReasons map[uint64]string,
	expectedStateOps map[string][]uint64,
	expectedStates map[string][2]interface{},
) {
	t.T().Logf("condition: values=%v enough workers=%d", workerValues, enoughWorkers)

	workerNames := make([]string, len(workerValues))

	insertOps := map[string]operationsWorkerInsertOpsFuncs{}
	workerInitialInsertOps := map[string]workerInitialInsertOpsFuncs{}
	phaseDones := map[string]workerMergerPhaseDoneFunc{}
	waits := map[string]operationsWorkerMergerWaitFunc{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gatheredReasons := util.NewSingleLockedMap[uint64, string]()
	gatheredStates := util.NewSingleLockedMap[string, [2]interface{}]()

	for i := range workerNames {
		local := workerNames[i]

		wctx, wcancel := context.WithCancel(ctx)
		defer wcancel()

		_, _, _, args, _, _, statesPool := t.newArgs(enoughWorkers, ops)

		for i := range sts {
			st := sts[i]

			statesPool.SetValue(st.Key(), st)
		}

		//if local == "w1" {
		//	args.SetLogging(logging.TestLogging)
		//}

		args.local = local

		args.BroadcastPhaseDone = func(_ context.Context, unmerged []uint64) error {
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
		args.NewStateFunc = func() (newStateFunc, error) {
			return func(stateKey string, value base.StateValue, ops []uint64) (base.State, error) {
				slices.Sort(ops)

				gatheredStates.SetValue(
					stateKey,
					[2]interface{}{int(value.(AccountStateValue).Balance), ops},
				)

				return nil, nil
			}, nil
		}

		insert, initialInsertOps, phaseDone, wait, err := newOperationsWorkerMerger(wctx, args)
		t.NoError(err)

		insertOps[local] = insert
		workerInitialInsertOps[local] = initialInsertOps
		phaseDones[local] = phaseDone
		waits[local] = wait
	}

	var initialWg sync.WaitGroup

	for local := range insertOps {
		local := local

		var ops []uint64

		for _, opindex := range workerOps[local] {
			ops = append(ops, opindex)
		}

		slices.Sort(ops)

		t.T().Logf("insert ops worker=%q ops=%v", local, ops)

		initialWg.Add(1)

		go func() {
			defer initialWg.Done()

			insertOps[local](func(f operationsWorkerInsertOpFunc) error {
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

	gatheredUnmerged := util.EmptyLocked[[]uint64]()

	endch := make(chan struct{})
	endedWorkers := util.NewSingleLockedMap[string, struct{}]()

	var endOnce sync.Once

	endWait := func() {
		endOnce.Do(func() {
			close(endch)
		})
	}

	for local := range waits {
		local := local
		wait := waits[local]

		go func() {
			err := wait(func(
				unmerged []uint64,
				reasons map[uint64]string,
				traverseStates func(func(base.State) error) error,
				traverseOperations func(func(uint64, Operation) error) error,
			) error {
				fmt.Printf("wait done: worker=%q unmerged=%v reasons=%v\n", local, unmerged, reasons)

				gatheredUnmerged.Set(func(prev []uint64, _ bool) ([]uint64, error) {
					util.TraverseSliceV1(unmerged, func(_ int, opindex uint64) (bool, error) {
						if slices.Index(prev, opindex) < 0 {
							prev = append(prev, opindex)
						}

						return true, nil
					})

					return prev, nil
				})

				fmt.Printf("worker=%q reasons=%v\n", local, reasons)

				TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
					gatheredReasons.SetValue(opindex, reason)

					return true, nil
				})

				return traverseStates(func(base.State) error { return nil })
			})
			if err != nil {
				fmt.Printf("wait: local=%s err=%+v\n", local, err)
			}

			switch {
			case err == nil:
			case errors.Is(err, context.Canceled):
				return
			default:
				panic(fmt.Errorf("wait: local=%s %w", local, err))
			}

			endedWorkers.SetValue(local, struct{}{})
			if uint64(endedWorkers.Len()) >= enoughWorkers {
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
		t.Equal(len(expectedReasons), gatheredReasons.Len(), "reasons not matched")

		reasons := gatheredReasons.Map()

		TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
			r, found := expectedReasons[opindex]
			t.True(found)
			t.Equal(r, reason)

			return true, nil
		})
	}

	t.Equal(len(expectedStates), gatheredStates.Len())

	for stateKey := range expectedStates {
		a := expectedStates[stateKey]

		b, found := gatheredStates.Value(stateKey)
		t.True(found)

		t.Equal(a, b)
	}
}

func TestOperationsWorkerMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testOperationsWorkerMerger))
}
