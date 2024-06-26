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

type testWorkerMerger struct {
	baseTestMergeOp
}

func (t *testWorkerMerger) newArgs(workers uint64) (
	_ *PhaseMergerArgs,
	_ *workerPhaseMergerArgs,
	_ *workerMergerArgs,
	vp util.LockedMap[uint64, []StateMergeValue],
	mergers util.LockedMap[string, StateMerger],
	states util.LockedMap[string, base.State],
) {
	vp, mergers, states, _, getValues := t.baseTestMergeOp.getFuncs()

	getState := func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}

	pargs := NewPhaseMergerArgs()
	pargs.GetMergersFunc, _ = newGetMergersFunc(mergers, getState)
	pargs.GetValuesFunc = getValues
	extractKeys, _ := newExtractStateMergeValueKeysFunc(1 << 9)
	pargs.ExtractStateMergeValueKeysFunc = extractKeys

	wpargs := newWorkerPhaseMergerArgs()
	wpargs.NewPhaseMergerFuncs = func() (
		*PhaseMergerArgs,
		newPhaseMergerFunc,
		error,
	) {
		return pargs, NewPhaseMerger, nil
	}
	wpargs.EnoughWorkers = workers
	wpargs.FilterMergeOps = func(m, u []uint64) ([]uint64, []uint64) { return m, u }

	args := newWorkerMergerArgs()

	args.IsLocalOp = func(opindex uint64) bool { return vp.Exists(opindex) }
	args.NewWorkerPhaseMergerFuncs = func(phase int) (
		*workerPhaseMergerArgs,
		newWorkerPhaseMergerFunc,
		error,
	) {
		return wpargs, newWorkerPhaseMerger, nil
	}

	args.BroadcastPhaseDone = func(context.Context, int, []uint64, []uint64) error {
		return nil
	}
	args.SetReasons = func(int, map[uint64]string) error {
		return nil
	}

	args.SetLogging(logging.TestNilLogging)

	return pargs, wpargs, args, vp, mergers, states
}

func (t *testWorkerMerger) TestCancel() {
	t.Run("before insert", func() {
		ctx, cancel := context.WithCancel(context.Background())

		_, _, args, _, _, _ := t.newArgs(1)

		insert, wait, err := newWorkerMerger(ctx, args)
		t.NoError(err)

		cancel()

		err = insert(0, localWorkerName, nil, nil)
		t.ErrorIs(err, context.Canceled)
		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after insert", func() {
		ctx, cancel := context.WithCancel(context.Background())

		_, _, args, _, _, _ := t.newArgs(3)

		insert, wait, err := newWorkerMerger(ctx, args)
		t.NoError(err)

		t.NoError(insert(0, localWorkerName, nil, nil))

		cancel()

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after 0 phase", func() {
		ctx, cancel := context.WithCancel(context.Background())

		_, _, args, vp, _, _ := t.newArgs(3)

		nextPhaseCh := make(chan int, 1)

		newWorkerPhaseMergerFuncs := args.NewWorkerPhaseMergerFuncs
		args.NewWorkerPhaseMergerFuncs = func(phase int) (
			*workerPhaseMergerArgs,
			newWorkerPhaseMergerFunc,
			error,
		) {
			if phase == 1 {
				go func() {
					t.T().Log("new phase:", phase)

					nextPhaseCh <- phase
				}()
			}

			return newWorkerPhaseMergerFuncs(phase)
		}

		insert, wait, err := newWorkerMerger(ctx, args)
		t.NoError(err)

		ops := make([]uint64, 9)
		for i := range ops {
			opindex := uint64(i)

			_ = t.randomMergeValuesVP(vp, opindex, 1)
			ops[i] = opindex
		}

		t.NoError(insert(0, localWorkerName, ops[:3], nil))
		t.NoError(insert(0, "w0", ops[3:6], nil))
		t.NoError(insert(0, "w1", ops[6:], nil))

		select {
		case <-time.After(time.Second * 4):
			t.Fail("wait new next phase")
		case <-nextPhaseCh:
			cancel()

			t.T().Log("canceled")
		}

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testWorkerMerger) TestPhaseDone() {
	t.Run("1 EnoughWorkers", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, args, vp, _, _ := t.newArgs(1)

		insert, wait, err := newWorkerMerger(ctx, args)
		t.NoError(err)

		ops := make([]uint64, 3)
		for i := range ops {
			opindex := uint64(i)

			_ = t.randomMergeValuesVP(vp, opindex, 1)
			ops[i] = opindex
		}

		t.NoError(insert(0, localWorkerName, ops, nil))
		t.NoError(wait(nil))
	})

	t.Run("over 1 EnoughWorkers", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, args, vp, _, _ := t.newArgs(2)

		other := "w0"

		var insert workerMergerPhaseDoneFunc
		var wait workerMergerWaitFunc

		nextphasech := make(chan int, 1)
		args.BroadcastPhaseDone = func(_ context.Context, phase int, _, _ []uint64) error {
			if phase > 0 {
				t.NoError(insert(phase, other, nil, nil))

				nextphasech <- phase
			}

			return nil
		}

		var err error

		insert, wait, err = newWorkerMerger(ctx, args)
		t.NoError(err)

		ops := make([]uint64, 6)
		for i := range ops {
			opindex := uint64(i)

			_ = t.randomMergeValuesVP(vp, opindex, 1)
			ops[i] = opindex
		}

		t.NoError(insert(0, localWorkerName, ops[:3], nil))
		t.NoError(insert(0, other, ops[3:], nil))

		select {
		case <-time.After(time.Second):
			t.Fail("wait to enter next phase")
		case phase := <-nextphasech:
			t.Equal(1, phase)
		}

		t.NoError(wait(nil))
	})
}

func (t *testWorkerMerger) TestSetReasons() {
	t.Run("no reasons", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, args, vp, _, _ := t.newArgs(1)

		var insert workerMergerPhaseDoneFunc
		var wait workerMergerWaitFunc

		reasonsch := make(chan [2]interface{}, 1)
		args.SetReasons = func(phase int, reasons map[uint64]string) error {
			reasonsch <- [2]interface{}{phase, reasons}

			return nil
		}

		var err error

		insert, wait, err = newWorkerMerger(ctx, args)
		t.NoError(err)

		ops := make([]uint64, 3)
		for i := range ops {
			opindex := uint64(i)

			_ = t.randomMergeValuesVP(vp, opindex, 1)
			ops[i] = opindex
		}

		t.NoError(insert(0, localWorkerName, ops, nil))

		select {
		case <-time.After(time.Second):
		case i := <-reasonsch:
			phase := i[0].(int)
			reasons := i[1].(map[uint64]string)

			t.Fail("unexpected reasons", "phase=%d reasons=%v", phase, reasons)
		}

		t.NoError(wait(nil))
	})

	t.Run("reasons", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, args, vp, _, _ := t.newArgs(2)

		var insert workerMergerPhaseDoneFunc

		reasonsch := make(chan [2]interface{}, 1)
		args.SetReasons = func(phase int, reasons map[uint64]string) error {
			reasonsch <- [2]interface{}{phase, reasons}

			return nil
		}

		var err error

		insert, _, err = newWorkerMerger(ctx, args)
		t.NoError(err)

		ops := make([]uint64, 3)
		for i := range ops {
			opindex := uint64(i)

			_ = t.randomMergeValuesVP(vp, opindex, -1)
			ops[i] = opindex
		}

		t.NoError(insert(0, localWorkerName, ops, nil))
		t.NoError(insert(0, "w0", ops, nil))

		select {
		case <-time.After(time.Second):
			t.Fail("wait reasons")
		case i := <-reasonsch:
			phase := i[0].(int)
			reasons := i[1].(map[uint64]string)

			t.Equal(0, phase)
			t.Equal(
				map[uint64]string{0: "insufficient balance", 1: "insufficient balance", 2: "insufficient balance"},
				reasons,
			)
		}
	})
}

func (t *testWorkerMerger) testMergeNWorkers(
	workerValues map[ /* worker */ string][] /* state key */ string,
	enoughWorkers uint64,
	sts []base.State,
	allValues [][]StateMergeValue,
	expectedUnmergedOps []uint64,
	expectedStates map[string]int64,
) {
	t.T().Logf("condition: values=%v enough workers=%d", workerValues, enoughWorkers)

	getValues := func(stateKey string) map[uint64][]StateMergeValue {
		m := map[uint64][]StateMergeValue{}

		for i := range allValues {
			opindex := uint64(i)

			for j := range allValues[i] {
				v := allValues[i][j]

				if v.Key() == stateKey {
					m[opindex] = append(m[opindex], v)
				}
			}
		}

		return m
	}

	workerNames := make([]string, len(workerValues))
	wvalues := map[string]map[uint64][]StateMergeValue{}

	var i int
	for local := range workerValues {
		workerNames[i] = local
		i++

		m := map[uint64][]StateMergeValue{}

		for j := range workerValues[local] {
			TraverseMap(getValues(workerValues[local][j]), func(opindex uint64, vs []StateMergeValue) (bool, error) {
				m[opindex] = append(m[opindex], vs...)

				return true, nil
			})
		}

		wvalues[local] = m
	}

	inserts := map[string]workerMergerPhaseDoneFunc{}
	waits := map[string]workerMergerWaitFunc{}
	initials := map[string]func(map[uint64][]StateMergeValue) (mergedOps, unmergedOps []uint64, _ error){}
	mergers := map[string]util.LockedMap[string, StateMerger]{}
	saveReasonsDones := map[string]saveReasonsDoneFunc{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := range workerNames {
		local := workerNames[i]

		wctx, wcancel := context.WithCancel(ctx)
		defer wcancel()

		pargs, _, args, vp, workerMergers, states := t.newArgs(enoughWorkers)

		mergers[local] = workerMergers

		//if local == "w0" {
		//	args.SetLogging(logging.TestLogging)
		//}

		pargs.local = local
		args.local = local

		args.IsLocalOp = func(opindex uint64) bool { return vp.Exists(opindex) }
		args.BroadcastPhaseDone = func(_ context.Context, phase int, merged []uint64, unmerged []uint64) error {
			t.T().Logf("broadcast: local=%q phase=%d merged=%v unmerged=%v", local, phase, merged, unmerged)

			return TraverseMap(inserts, func(worker string, insert workerMergerPhaseDoneFunc) (bool, error) {
				if worker == local {
					return true, nil
				}

				err := insert(phase, local, merged, unmerged)
				if err != nil {
					t.T().Logf("broadcast: local=%q worker=%q phase=%d merged=%v unmerged=%v err=%+v", local, worker, phase, merged, unmerged, err)
				}

				return true, err
			})
		}

		setReasons, saveReasonDone := newSaveReasonsFuncs(wctx)
		args.SetReasons = setReasons
		saveReasonsDones[local] = saveReasonDone

		insert, wait, err := newWorkerMerger(wctx, args)
		t.NoError(err)

		inserts[local] = insert
		waits[local] = wait

		for i := range sts {
			st := sts[i]

			states.SetValue(st.Key(), st)
		}

		initials[local] = func(values map[uint64][]StateMergeValue) (mergedOps, unmergedOps []uint64, _ error) {
			pmerge, _, pdone, pwait, err := NewPhaseMerger(wctx, pargs)
			if err != nil {
				return nil, nil, err
			}

			for opindex := range values {
				vp.SetValue(opindex, values[opindex])
			}

			for opindex := range values {
				if err := pmerge(opindex); err != nil {
					return nil, nil, err
				}
			}

			pdone()

			if err := pwait(func(newlyMergedOps, newlyUnmergedOps []uint64, reasons map[uint64]string) error {
				mergedOps = slices.Clone(newlyMergedOps)
				unmergedOps = slices.Clone(newlyUnmergedOps)

				return nil
			}); err != nil {
				return nil, nil, err
			}

			return mergedOps, unmergedOps, nil
		}
	}

	var initialWg sync.WaitGroup

	for local := range initials {
		worker := local

		var ops []uint64

		for opindex := range wvalues[worker] {
			ops = append(ops, opindex)
		}

		slices.Sort(ops)

		merged, unmerged, err := initials[worker](wvalues[worker])
		t.NoError(err)
		t.T().Logf("initial worker=%q ops=%v merged=%v unmerged=%v", worker, ops, merged, unmerged)

		insert := inserts[worker]

		initialWg.Add(1)

		go func() {
			defer initialWg.Done()

			if err := insert(0, localWorkerName, merged, unmerged); err != nil {
				panic(fmt.Errorf("initial: worker=%s %w ", worker, err))
			}
		}()
	}

	initialWg.Wait()

	gatheredUnmerged := util.EmptyLocked[[]uint64]()
	workerReasons := util.NewSingleLockedMap[string, map[uint64]string]()

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
			err := wait(func(unmerged []uint64) error {
				gatheredUnmerged.Set(func(prev []uint64, _ bool) ([]uint64, error) {
					util.TraverseSliceV1(unmerged, func(_ int, opindex uint64) (bool, error) {
						if slices.Index(prev, opindex) < 0 {
							prev = append(prev, opindex)
						}

						return true, nil
					})

					return prev, nil
				})

				reasons := util.NewSingleLockedMap[uint64, string]()
				defer reasons.Close()

				if err := saveReasonsDones[local](unmerged, func(opindex uint64, reason string) error {
					reasons.SetValue(opindex, reason)

					return nil
				}); err != nil {
					return err
				}

				workerReasons.SetValue(local, reasons.Map())

				return nil
			})
			if err != nil {
				fmt.Sprintf("wait: local=%s err=%+v\n", local, err)
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
	case <-ctx.Done():
	case <-endch:
	}

	gatheredUnmerged.Get(func(unmerged []uint64, _ bool) error {
		slices.Sort(unmerged)

		t.Equal(expectedUnmergedOps, unmerged)

		t.T().Log("unmerged ops:", unmerged)

		return nil
	})

	gatheredMerged := map[string]int64{}

	for local := range mergers {
		if !endedWorkers.Exists(local) {
			continue
		}

		merged, err := t.closeMergers(mergers[local])
		t.NoError(err)
		t.T().Logf("merged: worker=%q expected=%v merged=%v", local, expectedStates, merged)

		for stateKey := range merged {
			switch i, found := gatheredMerged[stateKey]; {
			case found:
				t.Equal(i, merged[stateKey], stateKey)
			default:
				gatheredMerged[stateKey] = merged[stateKey]
			}
		}
	}

	t.Equal(expectedStates, gatheredMerged)

	workerReasons.Traverse(func(worker string, reasons map[uint64]string) bool {
		t.T().Logf("reasons: worker=%q reasons=%v", worker, reasons)

		return true
	})
}

func (t *testWorkerMerger) TestMergeNWorkersEmptyOps() {
	run := func(size, enough uint64) func() {
		return func() {
			t.Run(fmt.Sprintf("%d worker; %d enough; empty", size, enough), func() {
				workers := map[string][]string{}

				for i := uint64(0); i < size; i++ {
					workers[fmt.Sprintf("w%03d", i)] = nil
				}

				t.testMergeNWorkers(workers, enough, nil, nil, nil, nil)
			})
		}
	}

	for e := uint64(1); e < 9; e++ {
		run(e, e)()
	}

	for e := uint64(2); e < 7; e++ {
		run(6, e)()
	}

	t.Run("999 worker; empty", run(999, 999))
}

func (t *testWorkerMerger) TestMergeNWorkers3States() {
	pub0, pub1, pub2 := base.NewDummyPublickey("0"), base.NewDummyPublickey("1"), base.NewDummyPublickey("2")

	sts := []base.State{
		t.newState(pub0, 1),
		t.newState(pub1, 1),
		t.newState(pub2, 1),
	}

	st0, st1, st2 := sts[0].Key(), sts[1].Key(), sts[2].Key()

	allValues := [][]StateMergeValue{
		t.newMergeValues(pub0, -1, pub1, 1),
		t.newMergeValues(pub0, -1, pub2, 1), // 1: ignored
		t.newMergeValues(pub1, -1, pub0, 1),
		t.newMergeValues(pub1, -1, pub2, 1), // 3: ignored
		t.newMergeValues(pub2, -1, pub0, 1),
		t.newMergeValues(pub2, -1, pub1, 1), // 5: ignored
	}

	expectedUnmergedOps := []uint64{1, 3, 5}
	expectedStates := map[string]int64{
		st0: 2,
		st2: 0,
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

	run := func(values map[string][]string, enough uint64) func() {
		return func() {
			t.testMergeNWorkers(values, enough, sts, allValues, expectedUnmergedOps, expectedStates)
		}
	}

	t.Run("one worker", run(map[string][]string{
		"w0": {st0, st1, st2},
	}, 1))

	t.Run("2 worker", run(map[string][]string{
		"w0": {st0},
		"w1": {st1, st2},
	}, 2))

	t.Run("3 worker", run(map[string][]string{
		"w0": {st0},
		"w1": {st1},
		"w2": {st2},
	}, 3))

	t.Run("3 worker; w0 2 shared", run(map[string][]string{
		"w0": {st0, st1},
		"w1": {st1},
		"w2": {st2},
	}, 3))

	t.Run("3 worker; 2 shared", run(map[string][]string{
		"w0": {st0, st1},
		"w1": {st1, st2},
		"w2": {st2, st0},
	}, 3))

	t.Run("4 worker; 2 shared", run(map[string][]string{
		"w0": {st0, st1},
		"w1": {st1, st2},
		"w2": {st2, st0},
		"w3": {st0, st1},
	}, 3))

	t.Run("6 worker; 2 shared", run(map[string][]string{
		"w0": {st0, st1},
		"w1": {st1, st2},
		"w2": {st2, st0},
		"w3": {st0, st1},
		"w4": {st1, st2},
		"w5": {st2, st0},
	}, 5))
}

func (t *testWorkerMerger) TestMergeNWorkers3States3Values() {
	pub0, pub1, pub2 := base.NewDummyPublickey("0"), base.NewDummyPublickey("1"), base.NewDummyPublickey("2")

	sts := []base.State{
		t.newState(pub0, 2),
		t.newState(pub1, 2),
		t.newState(pub2, 2),
	}

	st0, st1, st2 := sts[0].Key(), sts[1].Key(), sts[2].Key()

	allValues := [][]StateMergeValue{
		t.newMergeValues(pub0, -2, pub1, 1, pub2, 1),
		t.newMergeValues(pub0, -2, pub1, 1, pub2, 1), // 1: ignored
		t.newMergeValues(pub1, -2, pub0, 1, pub2, 1),
		t.newMergeValues(pub1, -2, pub0, 1, pub2, 1), // 3: ignored
		t.newMergeValues(pub2, -2, pub0, 1, pub1, 1),
		t.newMergeValues(pub2, -2, pub0, 1, pub1, 1), // 5: ignored
	}

	expectedUnmergedOps := []uint64{1, 3, 5}

	/*
		                | p0 | p1 | p2 |
		           base |  2 |  2 |  2 |
		p0 -> p1, p2: 1 |  0 |  3 |  3 |
		p1 -> p0, p2: 1 |  1 |  1 |  4 |
		p2 -> p0, p1: 1 |  2 |  2 |  2 |
		                |  - |  - |  - |

		 state key | pub
		-----------+------
		    a-0dpu | pub0
		    a-1dpu | pub1
		    a-2dpu | pub2
		-----------+------
	*/

	run := func(values map[string][]string, enough uint64) func() {
		return func() {
			t.testMergeNWorkers(values, enough, sts, allValues, expectedUnmergedOps, map[string]int64{})
		}
	}

	t.Run("one worker", run(map[string][]string{
		"w0": {st0, st1, st2},
	}, 1))

	t.Run("2 worker", run(map[string][]string{
		"w0": {st0},
		"w1": {st1, st2},
	}, 2))

	t.Run("3 worker", run(map[string][]string{
		"w0": {st0},
		"w1": {st1},
		"w2": {st2},
	}, 3))

	t.Run("3 worker; w0 2 shared", run(map[string][]string{
		"w0": {st0, st1},
		"w1": {st1},
		"w2": {st2},
	}, 3))

	t.Run("3 worker; 2 shared", run(map[string][]string{
		"w0": {st0, st1},
		"w1": {st1, st2},
		"w2": {st2, st0},
	}, 3))

	t.Run("4 worker; 2 shared", run(map[string][]string{
		"w0": {st0, st1},
		"w1": {st1, st2},
		"w2": {st2, st0},
		"w3": {st0, st1},
	}, 3))

	t.Run("6 worker; 2 shared", run(map[string][]string{
		"w0": {st0, st1},
		"w1": {st1, st2},
		"w2": {st2, st0},
		"w3": {st0, st1},
		"w4": {st1, st2},
		"w5": {st2, st0},
	}, 5))
}

func (t *testWorkerMerger) TestMergeNWorkersNStates() {
	workers := make([]string, 3)
	for i := range workers {
		workers[i] = fmt.Sprintf("w%d", i)
	}

	pubs := make([]base.Publickey, 33)
	for i := range pubs {
		pubs[i] = base.NewDummyPublickey(fmt.Sprintf("%d", i))
	}

	workerValues := map[string][]string{}

	sts := make([]base.State, len(pubs))
	for i := range sts {
		sts[i] = t.newState(pubs[i], 1)

		worker := workers[i%len(workers)]
		workerValues[worker] = append(workerValues[worker], sts[i].Key())
	}

	values := make([][]StateMergeValue, len(pubs)*2)
	expectedUnmergedOps := []uint64{4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 63, 64, 65}

	for i := range pubs {
		j0 := (i + 1) % len(pubs)
		j1 := (i + 2) % len(pubs)

		n := (i * 2)

		values[i*2] = t.newMergeValues(pubs[i], -1, pubs[j0], 1)
		values[n+1] = t.newMergeValues(pubs[j1], -1, pubs[i], 1)
	}

	expectedStates := map[string]int64{
		"ta-1dpu":  2,
		"ta-2dpu":  2,
		"ta-31dpu": 0,
		"ta-32dpu": 0,
	}

	t.testMergeNWorkers(workerValues, uint64(len(workers)), sts, values, expectedUnmergedOps, expectedStates)
}

func TestWorkerMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerMerger))
}
