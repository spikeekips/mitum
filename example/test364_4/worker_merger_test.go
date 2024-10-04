package main

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type baseTestWorkerMerger struct {
	baseTestMergeOp
}

func (t *baseTestWorkerMerger) newArgs(local string) (
	_ *phaseMergerArgs,
	vp util.LockedMap[uint64, []StateMergeValue],
	mergers util.LockedMap[string, StateMerger],
	states util.LockedMap[string, base.State],
	getValues phaseMergerGetStateMergeValuesFunc,
	getState getStateFunc,
) {
	t.T().Helper()

	vp, mergers, states, _, getValues = t.baseTestMergeOp.getFuncs()

	getState = func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}

	pargs := newPhaseMergerArgs()

	pargs.Logging = LoggingWith(pargs.Logging, func(zctx zerolog.Context) zerolog.Context {
		return zctx.Str("local", local)
	})
	pargs.SetLogging(logging.TestNilLogging)

	return pargs, vp, mergers, states, getValues, getState
}

func (t *baseTestWorkerMerger) newInitialValuesArgs(local string) (
	*phaseMergerArgs,
	*initialValuesWorkerMergerArgs,
	util.LockedMap[uint64, []StateMergeValue],
	util.LockedMap[string, StateMerger],
	util.LockedMap[string, base.State],
	getStateFunc,
) {
	t.T().Helper()

	pargs, vp, mergers, states, _, getState := t.newArgs(local)

	args := newInitialValuesWorkerMergerArgs()
	args.NewValuesPool = func() (util.LockedMap[uint64, []StateMergeValue], error) {
		return vp, nil
	}

	return pargs, args, vp, mergers, states, getState
}

func (t *baseTestWorkerMerger) newStatesArgs(local string) (
	*phaseMergerArgs,
	*initialValuesWorkerMergerArgs,
	*statesWorkerMergerArgs,
	util.LockedMap[uint64, []StateMergeValue],
	util.LockedMap[string, StateMerger],
	util.LockedMap[string, base.State],
) {
	t.T().Helper()

	pargs, iargs, vp, mergers, states, _ := t.newInitialValuesArgs(local)

	args := newStatesWorkerMergerArgs()

	return pargs, iargs, args, vp, mergers, states
}

type testWorkerMerger struct {
	baseTestWorkerMerger
}

func (t *testWorkerMerger) testBaseWorkerMerger(
	workerValues map[ /* worker */ string][] /* state key */ string,
	minEnough uint64,
	sts []base.State,
	allValues [][]StateMergeValue,
	expectedUnmerged []uint64,
	expectedStates map[string]int64,
	expectedReasons map[uint64]string,
) {
	t.T().Helper()

	t.T().Logf("condition: values=%v min enough=%d", workerValues, minEnough)

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
	initials := map[string]func(map[uint64][]StateMergeValue) (merged, unmerged []uint64, _ error){}
	mergers := map[string]util.LockedMap[string, StateMerger]{}
	broadcasts := map[string]workerBroadcastPhaseDoneFunc{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gatheredReasons := util.NewSingleLockedMap[uint64, string]()

	for i := range workerNames {
		local := workerNames[i]

		wctx, wcancel := context.WithCancel(ctx)
		defer wcancel()

		pargs, vp, workerMergers, states, getValues, getState := t.newArgs(local)

		mergers[local] = workerMergers

		//if local == "w0" {
		//	args.SetLogging(logging.TestLogging)
		//}

		broadcasts[local] = func(_ context.Context, unmerged []uint64) error {
			gerrs := map[string]error{}

			TraverseMap(inserts, func(worker string, insert workerMergerPhaseDoneFunc) (bool, error) {
				if worker == local {
					return true, nil
				}

				if err := insert(local, unmerged); err != nil {
					gerrs[worker] = err
				}

				return true, nil
			})

			fmt.Printf("broadcast: local=%q unmerged=%v errs=%+v\n", local, unmerged, gerrs)

			return nil
		}

		getMergers, closeMergers := newGetMergers(workerMergers, getState)
		defer closeMergers()

		extractKeys, closeExtractKeys := newExtractStateMergeValueKeys(1 << 9)
		defer closeExtractKeys()

		insert, wait, err := newFuncBaseWorkerMerger(pargs, 1<<3, NewFuncBaseJobWorker(1<<6))(wctx, minEnough, getValues, getMergers, extractKeys, newWorkerMergerIsLocalOp(vp.Exists))
		t.NoError(err)

		inserts[local] = insert
		waits[local] = wait

		for i := range sts {
			st := sts[i]

			states.SetValue(st.Key(), st)
		}

		initials[local] = func(values map[uint64][]StateMergeValue) (merged, unmerged []uint64, _ error) {
			pmerge, _, pdone, pwait, err := newPhaseMerger(wctx, pargs, getValues, getMergers, extractKeys)
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

			if err := pwait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string) error {
				merged = slices.Clone(newMerged)
				unmerged = slices.Clone(newUnmerged)

				fmt.Printf("wait done: worker=%q merged=%v unmerged=%v reasons=%v\n", local, merged, unmerged, reasons)

				TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
					gatheredReasons.SetValue(opindex, reason)

					return true, nil
				})

				return nil
			}); err != nil {
				return nil, nil, err
			}

			return merged, unmerged, nil
		}
	}

	var initialWg sync.WaitGroup

	for local := range initials {
		local := local

		var ops []uint64

		for opindex := range wvalues[local] {
			ops = append(ops, opindex)
		}

		slices.Sort(ops)

		merged, unmerged, err := initials[local](wvalues[local])
		t.NoError(err)
		t.T().Logf("initial worker=%q ops=%v merged=%v unmerged=%v", local, ops, merged, unmerged)

		insert := inserts[local]
		broadcast := broadcasts[local]

		initialWg.Add(1)

		go func() {
			defer initialWg.Done()

			if err := insert(localWorkerName, unmerged); err != nil {
				panic(fmt.Errorf("initial: worker=%s %w ", local, err))
			}

			if err := broadcast(ctx, unmerged); err != nil {
				panic(fmt.Errorf("broadcast: worker=%s %w ", local, err))
			}
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

				return nil
			})
			if err != nil {
				fmt.Printf("wait error: local=%s err=%+v\n", local, err)
			}

			switch {
			case err == nil:
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
		t.Equal(len(expectedReasons), gatheredReasons.Len(), "reasons not matched")

		reasons := gatheredReasons.Map()

		TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
			r, found := expectedReasons[opindex]
			t.True(found)
			t.Equal(r, reason)

			return true, nil
		})
	}

	var gatheredMerged map[string]int64

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
				if gatheredMerged == nil {
					gatheredMerged = map[string]int64{}
				}

				gatheredMerged[stateKey] = merged[stateKey]
			}
		}
	}

	t.Equal(expectedStates, gatheredMerged)
}

func (t *testWorkerMerger) TestBaseMergeNWorkersEmptyOps() {
	run := func(size, enough uint64) func() {
		return func() {
			t.Run(fmt.Sprintf("%d worker; %d enough; empty", size, enough), func() {
				workers := map[string][]string{}

				for i := uint64(0); i < size; i++ {
					workers[fmt.Sprintf("w%03d", i)] = nil
				}

				t.testBaseWorkerMerger(workers, enough, nil, nil, nil, nil, map[uint64]string{})
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

func (t *testWorkerMerger) runMergeNWorkers3States(
	st0, st1, st2 string,
	run func(values map[string][]string, enough uint64) func(),
) {
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

	t.Run("3 worker; 2 shared + 2 enough", run(map[string][]string{
		"w0": {st0, st1},
		"w1": {st1, st2},
		"w2": {st2, st0},
	}, 2))

	t.Run("3 worker; 2 shared + 3 enough", run(map[string][]string{
		"w0": {st0, st1},
		"w1": {st1, st2},
		"w2": {st2, st0},
	}, 3))

	t.Run("4 worker; 2 shared + 3 enough", run(map[string][]string{
		"w0": {st0, st1},
		"w1": {st1, st2},
		"w2": {st2, st0},
		"w3": {st0, st1},
	}, 3))

	t.Run("4 worker; 2 shared + 4 enough", run(map[string][]string{
		"w0": {st0, st1},
		"w1": {st1, st2},
		"w2": {st2, st0},
		"w3": {st0, st1},
	}, 4))

	t.Run("6 worker; 2 shared + 5 enough", run(map[string][]string{
		"w0": {st0, st1},
		"w1": {st1, st2},
		"w2": {st2, st0},
		"w3": {st0, st1},
		"w4": {st1, st2},
		"w5": {st2, st0},
	}, 5))

	t.Run("6 worker; 2 shared + 6 enough", run(map[string][]string{
		"w0": {st0, st1},
		"w1": {st1, st2},
		"w2": {st2, st0},
		"w3": {st0, st1},
		"w4": {st1, st2},
		"w5": {st2, st0},
	}, 6))
}

func (t *testWorkerMerger) TestMergeNWorkers3StatesCase0() {
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

	expectedUnmerged := []uint64{1, 3, 5}
	expectedStates := map[string]int64{
		st0: 2,
		st2: 0,
	}
	expectedReasons := map[uint64]string{
		1: "previous issuer found",
		3: "previous issuer found",
		5: "previous issuer found",
	}

	/*
		            | p0 | p1 | p2 |
		       base |  1 |  1 |  1 |
		p0 -> p1, 1 |  0 |  2 |    | 0
		p1 -> p0, 1 |  1 |  1 |    | 2
		p2 -> p0, 1 |  2 |    |  0 | 4
		            |  2 |  - |  0 |

		 state key | pub
		-----------+------
		   ta-0dpu | pub0
		   ta-1dpu | pub1
		   ta-2dpu | pub2
		-----------+------
	*/

	t.Run("base", func() {
		run := func(values map[string][]string, enough uint64) func() {
			return func() {
				t.testBaseWorkerMerger(values, enough, sts, allValues, expectedUnmerged, expectedStates, expectedReasons)
			}
		}

		t.runMergeNWorkers3States(st0, st1, st2, run)
	})

	t.Run("initial values", func() {
		expectedStateOps := map[string][]uint64{
			"ta-0dpu": {0, 2, 4},
			"ta-2dpu": {4},
		}

		run := func(values map[string][]string, enough uint64) func() {
			return func() {
				t.testInitialValuesWorkerMerger(values, enough, sts, allValues, expectedUnmerged, expectedStates, expectedReasons, expectedStateOps)
			}
		}

		t.runMergeNWorkers3States(st0, st1, st2, run)
	})

	t.Run("states", func() {
		expectedStateOps := map[string][]uint64{
			"ta-0dpu": {0, 2, 4},
			"ta-2dpu": {4},
		}

		expectedStates := map[string][2]interface{}{
			"ta-0dpu": {2, []uint64{0, 2, 4}},
			"ta-2dpu": {0, []uint64{4}},
		}

		run := func(values map[string][]string, enough uint64) func() {
			return func() {
				t.T().Helper()

				t.testStatesWorkerMerger(values, enough, sts, allValues, expectedUnmerged, expectedReasons, expectedStateOps, expectedStates)
			}
		}

		t.runMergeNWorkers3States(st0, st1, st2, run)
	})
}

func (t *testWorkerMerger) TestMergeNWorkers3StatesCase2() {
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

	expectedUnmerged := []uint64{1, 3, 5}
	var expectedStates map[string]int64
	expectedReasons := map[uint64]string{
		1: "previous issuer found",
		3: "previous issuer found",
		5: "previous issuer found",
	}

	/*
		                   | p0 | p1 | p2 |
					  base |  2 |  2 |  2 |
		0: p0 -> p1, p2: 1 |  0 |  3 |  3 |
		2: p1 -> p0, p2: 1 |  1 |  1 |  4 |
		4: p2 -> p0, p1: 1 |  2 |  2 |  2 |
		                   |  - |  - |  - |

		 state key | pub
		-----------+------
		    a-0dpu | pub0
		    a-1dpu | pub1
		    a-2dpu | pub2
		-----------+------
	*/

	t.Run("base", func() {
		run := func(values map[string][]string, enough uint64) func() {
			return func() {
				t.testBaseWorkerMerger(values, enough, sts, allValues, expectedUnmerged, expectedStates, expectedReasons)
			}
		}

		t.runMergeNWorkers3States(st0, st1, st2, run)
	})

	t.Run("initial values", func() {
		run := func(values map[string][]string, enough uint64) func() {
			return func() {
				t.testInitialValuesWorkerMerger(values, enough, sts, allValues, expectedUnmerged, expectedStates, expectedReasons, nil)
			}
		}

		t.runMergeNWorkers3States(st0, st1, st2, run)
	})

	t.Run("states", func() {
		run := func(values map[string][]string, enough uint64) func() {
			return func() {
				t.testStatesWorkerMerger(values, enough, sts, allValues, expectedUnmerged, expectedReasons, nil, nil)
			}
		}

		t.runMergeNWorkers3States(st0, st1, st2, run)
	})
}

func (t *testWorkerMerger) TestMergeWorkerMergeNWorkersNStates() {
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
	expectedUnmerged := []uint64{4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 63, 64, 65}

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
	expectedReasons := map[uint64]string{
		4:  "previous issuer found",
		6:  "previous issuer found",
		8:  "previous issuer found",
		10: "previous issuer found",
		12: "previous issuer found",
		14: "previous issuer found",
		16: "previous issuer found",
		18: "previous issuer found",
		20: "previous issuer found",
		22: "previous issuer found",
		24: "previous issuer found",
		26: "previous issuer found",
		28: "previous issuer found",
		30: "previous issuer found",
		32: "previous issuer found",
		34: "previous issuer found",
		36: "previous issuer found",
		38: "previous issuer found",
		40: "previous issuer found",
		42: "previous issuer found",
		44: "previous issuer found",
		46: "previous issuer found",
		48: "previous issuer found",
		50: "previous issuer found",
		52: "previous issuer found",
		54: "previous issuer found",
		56: "previous issuer found",
		58: "previous issuer found",
		60: "previous issuer found",
		62: "previous issuer found",
		63: "previous issuer found",
		64: "previous issuer found",
		65: "previous issuer found",
	}

	t.testBaseWorkerMerger(workerValues, uint64(len(workers)), sts, values, expectedUnmerged, expectedStates, expectedReasons)
}

func (t *testWorkerMerger) TestMergeWorkerMergeNWorkersEmptyOps() {
	run := func(size, enough uint64) func() {
		return func() {
			t.Run(fmt.Sprintf("%d worker; %d enough; empty", size, enough), func() {
				workers := map[string][]string{}

				for i := uint64(0); i < size; i++ {
					workers[fmt.Sprintf("w%03d", i)] = nil
				}

				t.testBaseWorkerMerger(workers, enough, nil, nil, nil, nil, nil)
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

func (t *testWorkerMerger) testInitialValuesWorkerMerger(
	workerValues map[ /* worker */ string][] /* state key */ string,
	minEnough uint64,
	sts []base.State,
	allValues [][]StateMergeValue,
	expectedUnmerged []uint64,
	expectedStates map[string]int64,
	expectedReasons map[uint64]string,
	expectedStateOps map[string][]uint64,
) {
	t.T().Helper()

	t.T().Logf("condition: values=%v enough workers=%d", workerValues, minEnough)

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

	phaseDones := map[string]workerMergerPhaseDoneFunc{}
	waits := map[string]initialValuesWorkerMergerWaitFunc{}
	initials := map[string]workerInitialInsertOpsFuncs{}
	mergers := map[string]util.LockedMap[string, StateMerger]{}
	getStateOps := map[string]getStateOpsFunc{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gatheredReasons := util.NewSingleLockedMap[uint64, string]()

	for i := range workerNames {
		local := workerNames[i]

		wctx, wcancel := context.WithCancel(ctx)
		defer wcancel()

		pargs, args, _, workerMergers, states, getState := t.newInitialValuesArgs(local)

		setStateOp, getStateOpFuncs, err := newSetStateOpFuncsWithLogger(1 << 9)
		t.NoError(err)

		mergers[local] = workerMergers
		getStateOps[local], _ = getStateOpFuncs()

		for i := range sts {
			st := sts[i]

			states.SetValue(st.Key(), st)
		}

		//if local == "w1" {
		//	args.SetLogging(logging.TestLogging)
		//}

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

		getMergers, closeMergers := newGetMergers(workerMergers, getState)
		defer closeMergers()

		newInitialValuesWorkerMergerf := newFuncInitialValuesWorkerMerger(args, pargs)
		initial, phaseDone, wait, err := newInitialValuesWorkerMergerf(wctx, minEnough, 1<<9, getMergers, setStateOp, broadcastPhaseDone)
		t.NoError(err)

		initials[local] = initial
		phaseDones[local] = phaseDone
		waits[local] = wait
	}

	var initialWg sync.WaitGroup

	for local := range initials {
		local := local

		var ops []uint64

		for opindex := range wvalues[local] {
			ops = append(ops, opindex)
		}

		slices.Sort(ops)

		t.T().Logf("initial worker=%q ops=%v", local, ops)

		initialWg.Add(1)

		go func() {
			defer initialWg.Done()

			insert, done, err := initials[local](localWorkerName)
			if err != nil {
				panic(err)
			}

			for opindex := range wvalues[local] {
				values := wvalues[local][opindex]

				if err := insert(opindex, values); err != nil {
					panic(fmt.Errorf("initial: worker=%s %w ", local, err))
				}
			}

			if err := done(); err != nil {
				panic(err)
			}

			if err := util.TraverseSliceV1(workerNames, func(_ int, worker string) (bool, error) {
				if worker == local {
					return true, nil
				}

				_, done, err := initials[local](worker)
				if err != nil {
					return false, err
				}

				if done != nil {
					return true, done()
				}

				return true, nil
			}); err != nil {
				panic(err)
			}
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
			err := wait(func(unmerged []uint64, reasons map[uint64]string) error {
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

				TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
					gatheredReasons.SetValue(opindex, reason)

					return true, nil
				})

				return nil
			})
			if err != nil {
				fmt.Printf("wait error: local=%s err=%+v\n", local, err)
			}

			switch {
			case err == nil:
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
		t.Equal(len(expectedReasons), gatheredReasons.Len(), "reasons not matched")

		reasons := gatheredReasons.Map()

		TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
			r, found := expectedReasons[opindex]
			t.True(found)
			t.Equal(r, reason)

			return true, nil
		})
	}

	var gatheredMerged map[string]int64

	for local := range mergers {
		if !endedWorkers.Exists(local) {
			continue
		}

		merged, err := t.closeMergers(mergers[local])
		t.NoError(err)
		t.T().Logf("merged: worker=%q expected=%v merged=%v", local, expectedStates, merged)

		for stateKey := range merged {
			ops := getStateOps[local](stateKey)
			slices.Sort(ops)
			t.T().Logf("state ops: worker=%q state=%q ops=%v", local, stateKey, ops)

			t.Equal(len(expectedStateOps[stateKey]), len(ops))
			t.Equal(expectedStateOps[stateKey], ops)

			switch i, found := gatheredMerged[stateKey]; {
			case found:
				t.Equal(i, merged[stateKey], stateKey)
			default:
				if gatheredMerged == nil {
					gatheredMerged = map[string]int64{}
				}

				gatheredMerged[stateKey] = merged[stateKey]
			}
		}
	}

	t.Equal(expectedStates, gatheredMerged)
}

func (t *testWorkerMerger) testStatesWorkerMerger(
	workerValues map[ /* worker */ string][] /* state key */ string,
	minEnough uint64,
	sts []base.State,
	allValues [][]StateMergeValue,
	expectedUnmerged []uint64,
	expectedReasons map[uint64]string,
	expectedStateOps map[string][]uint64,
	expectedStates map[string][2]interface{},
) {
	t.T().Helper()

	t.T().Logf("condition: values=%v enough workers=%d", workerValues, minEnough)

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

	phaseDones := map[string]workerMergerPhaseDoneFunc{}
	waits := map[string]statesWorkerMergerWaitFunc{}
	initials := map[string]workerInitialInsertOpsFuncs{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gatheredReasons := util.NewSingleLockedMap[uint64, string]()
	gatheredStates := util.NewSingleLockedMap[string, [2]interface{}]()

	for i := range workerNames {
		local := workerNames[i]

		wctx, wcancel := context.WithCancel(ctx)
		defer wcancel()

		pargs, iargs, args, _, _, statesPool := t.newStatesArgs(local)

		for i := range sts {
			st := sts[i]

			statesPool.SetValue(st.Key(), st)
		}

		//if local == "w1" {
		//	args.SetLogging(logging.TestLogging)
		//}

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

		initial, phaseDone, wait, err := newFuncStatesWorkerMerger(args, iargs, pargs)(wctx, minEnough, 1<<9, broadcastPhaseDone, newGetStateFunc(statesPool))
		t.NoError(err)

		initials[local] = initial
		phaseDones[local] = phaseDone
		waits[local] = wait
	}

	var initialWg sync.WaitGroup

	for local := range initials {
		local := local

		var ops []uint64

		for opindex := range wvalues[local] {
			ops = append(ops, opindex)
		}

		slices.Sort(ops)

		t.T().Logf("initial worker=%q ops=%v", local, ops)

		initialWg.Add(1)

		go func() {
			defer initialWg.Done()

			insert, done, err := initials[local](localWorkerName)
			if err != nil {
				panic(err)
			}

			for opindex := range wvalues[local] {
				values := wvalues[local][opindex]

				if err := insert(opindex, values); err != nil {
					panic(fmt.Errorf("initial: worker=%s %w ", local, err))
				}
			}

			if err := done(); err != nil {
				panic(err)
			}

			if err := util.TraverseSliceV1(workerNames, func(_ int, worker string) (bool, error) {
				if worker == local {
					return true, nil
				}

				_, done, err := initials[local](worker)
				if err != nil {
					return false, err
				}

				if done != nil {
					return true, done()
				}

				return true, nil
			}); err != nil {
				panic(err)
			}
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
				traverseMergers statesWorkerMergerTraverseMergers,
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

				TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
					gatheredReasons.SetValue(opindex, reason)

					return true, nil
				})

				return traverseMergers(func(stateKey string, value base.StateValue, ops []uint64) error {
					slices.Sort(ops)

					balance := value.(AccountStateValue).Balance

					gatheredStates.SetValue(
						stateKey,
						[2]interface{}{int(balance), ops},
					)

					fmt.Printf("new state: local=%q state key=%q balance=%v ops=%v\n", local, stateKey, balance, ops)

					return nil
				})
			})
			if err != nil {
				fmt.Printf("wait error: local=%s err=%+v\n", local, err)
			}

			switch {
			case err == nil:
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
		t.Equal(len(expectedReasons), gatheredReasons.Len(), "reasons not matched: expected=%v gathered=%v", expectedReasons, gatheredReasons.Map())

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

		t.Equal(a, b)
	}
}

func TestWorkerMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerMerger))
}

func newGetStateFunc(states util.LockedMap[string, base.State]) getStateFunc {
	return func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}
}
