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
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testOperationsWorkerMerger struct {
	baseTestWorkerMerger
}

func (t *testOperationsWorkerMerger) newArgs(local string, ops []Operation) (
	*phaseMergerArgs,
	*workerPhaseMergerArgs,
	*statesWorkerMergerArgs,
	*operationsWorkerMergerArgs,
	util.LockedMap[uint64, []StateMergeValue],
	util.LockedMap[string, StateMerger],
	util.LockedMap[string, base.State],
) {
	t.T().Helper()

	pargs, wpargs, sargs, vp, mergersPool, statesPool := t.newStatesArgs(local)

	sargs.BroadcastPhaseDone = func(context.Context, []uint64) error {
		return nil
	}

	eargs := newStateMergeValueExtractorArgs(local)
	eargs.GetOperation = func(_ context.Context, opindex uint64, _ util.Hash) (Operation, bool, error) {
		if opindex >= uint64(len(ops)) {
			return nil, false, nil
		}

		return ops[opindex], true, nil
	}

	args := newOperationsWorkerMergerArgs(sargs, eargs)
	args.BroadcastStateMergeValuesFuncs = func(context.Context) (
		operationsworkerInitialInsertOpFunc,
		func() error,
		error,
	) {
		return func(uint64, util.Hash, []StateMergeValue) error { return nil }, func() error { return nil }, nil
	}
	args.IsLocalStateValue = func(string) bool { return true }

	args.SetLogging(logging.TestNilLogging)

	return pargs, wpargs, sargs, args, vp, mergersPool, statesPool
}

func (t *testOperationsWorkerMerger) TestInsertOp() {
	t.Run("no insert", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, _, args, _, _, _ := t.newArgs("local", nil)

		gatheredState := util.NewSingleLockedMap[string, [2]interface{}]()

		insertOps, _, _, wait, err := newOperationsWorkerMerger(ctx, args, 1)
		t.NoError(err)

		t.NoError(insertOps(func(operationsWorkerLocalInsertOpFunc) error {
			return nil
		}))

		gatheredOps := util.NewSingleLockedMap[uint64, Operation]()

		t.NoError(wait(func(
			unmerged []uint64,
			reasons map[uint64]string,
			traverseMergers operationsWorkerMergerTraverseMergersFunc,
			traverseOperations func(operationsWorkerSaveOperationsFunc) error,
		) error {
			t.Empty(unmerged)
			t.Empty(reasons)

			t.NoError(traverseMergers(func(stateKey string, value base.StateValue, ops []util.Hash) error {
				gatheredState.SetValue(stateKey, [2]interface{}{value, ops})

				return nil
			}))

			t.NoError(traverseOperations(func(total uint64) operationsWorkerSaveOperationsCallbackFunc {
				fmt.Printf("traverse operations: total=%d\n", total)

				return func(f func(operationsWorkerSaveOperationFunc) error) error {
					return f(func(opindex uint64, op Operation) error {
						gatheredOps.SetValue(opindex, op)

						return nil
					})
				}
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

		_, _, _, args, _, _, statesPool := t.newArgs("local", []Operation{op})

		gatheredState := util.NewSingleLockedMap[string, [2]interface{}]()

		insertOps, _, _, wait, err := newOperationsWorkerMerger(ctx, args, 1)
		t.NoError(err)

		statesPool.SetValue(issuerState.Key(), issuerState)
		statesPool.SetValue(receiverState.Key(), receiverState)

		t.NoError(insertOps(func(f operationsWorkerLocalInsertOpFunc) error {
			return f(0, nil, nil)
		}))

		gatheredOps := util.NewSingleLockedMap[uint64, Operation]()

		t.NoError(wait(func(
			unmerged []uint64,
			reasons map[uint64]string,
			traverseMergers operationsWorkerMergerTraverseMergersFunc,
			traverseOperations func(operationsWorkerSaveOperationsFunc) error,
		) error {
			t.Empty(unmerged)
			t.Empty(reasons)

			t.NoError(traverseMergers(func(stateKey string, value base.StateValue, ops []util.Hash) error {
				gatheredState.SetValue(stateKey, [2]interface{}{value, ops})

				return nil
			}))

			t.NoError(traverseOperations(func(total uint64) operationsWorkerSaveOperationsCallbackFunc {
				fmt.Printf("traverse operations: total=%d\n", total)

				return func(f func(operationsWorkerSaveOperationFunc) error) error {
					return f(func(opindex uint64, op Operation) error {
						gatheredOps.SetValue(opindex, op)

						return nil
					})
				}
			}))

			return nil
		}))

		t.Equal(2, gatheredState.Len())
		{
			gsts, found := gatheredState.Value(issuerState.Key())
			t.True(found)

			gstv, gstops := gsts[0].(base.StateValue), gsts[1].([]util.Hash)
			t.Equal(issuerState.Value().(AccountStateValue).Balance-1, gstv.(AccountStateValue).Balance)
			t.Equal(1, len(gstops))
			t.True(op.Hash().Equal(gstops[0]))
		}
		{
			gsts, found := gatheredState.Value(receiverState.Key())
			t.True(found)

			gstv, gstops := gsts[0].(base.StateValue), gsts[1].([]util.Hash)
			t.Equal(receiverState.Value().(AccountStateValue).Balance+1, gstv.(AccountStateValue).Balance)
			t.Equal(1, len(gstops))
			t.True(op.Hash().Equal(gstops[0]))
		}

		t.Equal(1, gatheredOps.Len())
		gop, found := gatheredOps.Value(0)
		t.True(found)
		base.EqualOperation(t.Assert(), op, gop)
	})
}

func (t *testOperationsWorkerMerger) TestErrors() {
	t.Run("insert error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, _, args, _, _, _ := t.newArgs("local", []Operation{t.randomOp(1)})

		insertOps, _, _, wait, err := newOperationsWorkerMerger(ctx, args, 1)
		t.NoError(err)

		err = insertOps(func(operationsWorkerLocalInsertOpFunc) error {
			return errors.Errorf("showme")
		})
		t.ErrorContains(err, "showme")

		err = wait(nil)
		t.ErrorContains(err, "showme")
	})

	t.Run("extract error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		op := t.randomOp(1)

		_, _, _, args, _, _, _ := t.newArgs("local", []Operation{op})
		args.e.GetOperation = func(_ context.Context, opindex uint64, oph util.Hash) (Operation, bool, error) {
			return nil, false, errors.Errorf("showme")
		}

		insertOps, _, _, wait, err := newOperationsWorkerMerger(ctx, args, 1)
		t.NoError(err)

		t.NoError(insertOps(func(f operationsWorkerLocalInsertOpFunc) error {
			return f(0, nil, nil)
		}))

		err = wait(nil)
		t.ErrorContains(err, "showme")
	})

	t.Run("wait error: traverse mergers", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		op := t.randomOp(1)
		issuerState := t.newState(op.Fact().(OpFact).Issuer, 1)
		receiverState := t.newState(op.Fact().(OpFact).Receiver, 1)

		_, _, _, args, _, _, statesPool := t.newArgs("local", []Operation{op})
		statesPool.SetValue(issuerState.Key(), issuerState)
		statesPool.SetValue(receiverState.Key(), receiverState)

		insertOps, _, _, wait, err := newOperationsWorkerMerger(ctx, args, 1)
		t.NoError(err)

		t.NoError(insertOps(func(f operationsWorkerLocalInsertOpFunc) error {
			return f(0, nil, nil)
		}))

		err = wait(func(
			_ []uint64,
			_ map[uint64]string,
			traverseMergers operationsWorkerMergerTraverseMergersFunc,
			_ func(operationsWorkerSaveOperationsFunc) error,
		) error {
			return traverseMergers(func(string, base.StateValue, []util.Hash) error {
				return errors.Errorf("showme")
			})
		})

		t.ErrorContains(err, "showme")
	})

	t.Run("wait error: traverse operations", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		op := t.randomOp(1)

		_, _, _, args, _, _, _ := t.newArgs("local", []Operation{op})

		insertOps, _, _, wait, err := newOperationsWorkerMerger(ctx, args, 1)
		t.NoError(err)

		t.NoError(insertOps(func(f operationsWorkerLocalInsertOpFunc) error {
			return f(0, nil, nil)
		}))

		err = wait(func(
			_ []uint64,
			_ map[uint64]string,
			_ operationsWorkerMergerTraverseMergersFunc,
			traverseOperations func(operationsWorkerSaveOperationsFunc) error,
		) error {
			return traverseOperations(func(total uint64) operationsWorkerSaveOperationsCallbackFunc {
				return func(func(operationsWorkerSaveOperationFunc) error) error {
					return errors.Errorf("showme")
				}
			})
		})

		t.ErrorContains(err, "showme")
	})
}

func (t *testOperationsWorkerMerger) testMerger(
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
	waits := map[string]operationsWorkerMergerWaitFunc{}

	for i := range workerNames {
		workerInitialInsertOps[workerNames[i]] = nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gatheredReasons := util.NewSingleLockedMap[uint64, string]()
	gatheredStates := util.NewSingleLockedMap[string, [2]interface{}]()

	for i := range workerNames {
		local := workerNames[i]

		wctx, wcancel := context.WithCancel(ctx)
		defer wcancel()

		_, _, _, args, _, _, statesPool := t.newArgs(local, ops)

		for i := range sts {
			st := sts[i]

			statesPool.SetValue(st.Key(), st)
		}

		//if local == "w1" {
		//	args.SetLogging(logging.TestLogging)
		//}

		args.local = local

		args.BroadcastStateMergeValuesFuncs = func(ctx context.Context) (
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
		args.IsLocalStateValue = func(stateKey string) bool {
			return slices.Index(workerValues[local], stateKey) >= 0
		}

		insert, initialInsertOps, phaseDone, wait, err := newOperationsWorkerMerger(wctx, args, minEnough)
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
				traverseMergers operationsWorkerMergerTraverseMergersFunc,
				traverseOperations func(operationsWorkerSaveOperationsFunc) error,
			) error {
				fmt.Printf("wait done: local=%q unmerged=%v reasons=%v\n", local, unmerged, reasons)

				gatheredUnmerged.Set(func(prev []uint64, _ bool) ([]uint64, error) {
					util.TraverseSliceV1(unmerged, func(_ int, opindex uint64) (bool, error) {
						if slices.Index(prev, opindex) < 0 {
							prev = append(prev, opindex)
						}

						return true, nil
					})

					return prev, nil
				})

				TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
					gatheredReasons.SetValue(opindex, reason)

					return true, nil
				})

				return traverseMergers(func(stateKey string, value base.StateValue, ops []util.Hash) error {
					if slices.Index(workerValues[local], stateKey) < 0 {
						return util.ErrFound.Errorf("unknown state key for local, %q", stateKey)
					}

					slices.SortFunc(ops, func(a, b util.Hash) int { return strings.Compare(a.String(), b.String()) })

					balance := value.(AccountStateValue).Balance

					fmt.Printf("new state: local=%q state key=%q balance=%d ops=%v\n", local, stateKey, balance, ops)

					gatheredStates.SetValue(
						stateKey,
						[2]interface{}{int(balance), ops},
					)

					return nil
				})
			})
			if err != nil {
				fmt.Printf("wait error: local=%q err=%+v\n", local, err)
			}

			switch {
			case err == nil,
				errors.Is(err, util.ErrJobWorkerDone):
			case errors.Is(err, context.Canceled):
				return
			default:
				panic(fmt.Errorf("wait: local=%s %w", local, err))
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

	actualStates := map[string][2]interface{}{}
	gatheredStates.Traverse(func(stateKey string, i [2]interface{}) bool {
		if _, found := expectedStates[stateKey]; !found && i[0].(int) == 0 {
			return true
		}

		actualStates[stateKey] = i

		return true
	})

	t.Equal(len(expectedStates), len(actualStates), "expected=%v actual=%v gathered=%v", expectedStates, actualStates, gatheredStates.Map())

	for stateKey := range expectedStates {
		a := expectedStates[stateKey]

		var aophs []util.Hash

		util.TraverseSliceV1(a[1].([]uint64), func(i int, opindex uint64) (bool, error) {
			aophs = append(aophs, ops[opindex].Hash())

			return true, nil
		})

		b, found := actualStates[stateKey]
		t.True(found)

		t.Equal(a[0], b[0], stateKey)

		bophs := b[1].([]util.Hash)

		slices.SortFunc(aophs, func(a, b util.Hash) int { return strings.Compare(a.String(), b.String()) })
		slices.SortFunc(bophs, func(a, b util.Hash) int { return strings.Compare(a.String(), b.String()) })

		t.Equal(len(aophs), len(bophs), stateKey)

		for i := range bophs {
			t.True(aophs[i].Equal(bophs[i]), "a=%q b=%q", aophs[i], bophs[i])
		}
	}
}

func (t *testOperationsWorkerMerger) TestMergeNWorkers3States() {
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

func TestOperationsWorkerMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testOperationsWorkerMerger))
}

func newBroadcastStateMergeValuesFuncsFromInitialInsertOpsFunc(
	ctx context.Context,
	local string,
	stateOwners map[ /* worker */ string] /* state keys */ []string,
	funcs map[string]operationsworkerInitialInsertOpsFuncs,
) (
	operationsWorkerMergerBroadcastStateMergeValueFuncs,
	error,
) {
	var others []string

	TraverseMap(funcs, func(worker string, _ operationsworkerInitialInsertOpsFuncs) (bool, error) {
		if worker != local {
			others = append(others, worker)
		}

		return true, nil
	})

	if len(others) < 1 {
		return func(ctx context.Context) (
			operationsworkerInitialInsertOpFunc,
			func() error,
			error,
		) {
			return func(uint64, util.Hash, []StateMergeValue) error { return nil }, func() error { return nil }, nil
		}, nil
	}

	broadcast, broadcastDone, err := newBroadcastStateMergeValuesFuncs(
		ctx,
		local,
		others,
		func(ctx context.Context, worker string) (
			operationsworkerInitialInsertOpFunc,
			func() error,
			error,
		) {
			f, found := funcs[worker]
			if !found {
				return nil, nil, util.ErrNotFound.Errorf("worker, %q", worker)
			}

			return f(local)
		},
		func(worker string, err error) error {
			if err != nil {
				fmt.Sprintf("broadcast error: local=%q worker=%q err=%+v", local, worker, err)
			}

			return nil // NOTE ignore
		},
	)
	if err != nil {
		return nil, err
	}

	rStateOwners := map[ /* state keys */ string] /* workers */ []string{}
	TraverseMap(stateOwners, func(worker string, stateKeys []string) (bool, error) {
		util.TraverseSliceV1(stateKeys, func(_ int, stateKey string) (bool, error) {
			rStateOwners[stateKey] = append(rStateOwners[stateKey], worker)

			return true, nil
		})

		return true, nil
	})

	valuesOwners := newStateMergeValueOwnersFunc(
		local,
		func(stateKey string) []string {
			return rStateOwners[stateKey]
		},
	)

	return func(ctx context.Context) (
		operationsworkerInitialInsertOpFunc,
		func() error,
		error,
	) {
		return func(opindex uint64, oph util.Hash, values []StateMergeValue) error {
				return valuesOwners(values, func(worker string, values []StateMergeValue) error {
					return broadcast(worker, opindex, oph, values)
				})
			},
			func() error {
				switch err := broadcastDone(); {
				case errors.Is(err, context.Canceled),
					errors.Is(err, util.ErrJobWorkerDone):
					return nil
				default:
					return err
				}
			},
			nil
	}, nil
}
