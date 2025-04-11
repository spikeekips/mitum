package dop5

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorkerMerger struct {
	dutil.BaseSuite
	baseTestMergeOp
	baseTestWorkers
}

func (t *testWorkerMerger) newFuncs() (
	mergers dutil.LockedMap[string, SVM],
	states dutil.LockedMap[string, base.State],
	getSVMs mergerGetSVMsFunc,
	traverseSVMs iter.Seq2[string, SVM],
) {
	t.T().Helper()

	_, mergers, states, _, _ = t.baseTestMergeOp.getFuncs()

	getState := func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}

	extractKeys, _ := t.extractSMVKeys()

	getSVMs, tr, err := newFuncSVMs(
		1<<9,
		getState,
		extractKeys,
	)
	t.IfErrPanic(err)
	_, traverseSVMs = tr()

	return mergers, states, getSVMs, traverseSVMs
}

func (t *testWorkerMerger) TestCancel() {
	local := "w0"
	opsWorkers := t.workers(2)

	t.Run("before new", func() {
		ctx, cancel := t.Ctx()

		cancel(nil)

		_, _, getSVMs, _ := t.newFuncs()

		insertf, unmerge, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		insert, insertDone, err := insertf(local)
		t.NoError(err)

		err = insert(0, t.randomMergeValues(1))
		t.ErrorIs(err, context.Canceled)
		err = insertDone(nil)
		t.ErrorIs(err, context.Canceled)

		err = unmerge(opsWorkers[1], nil)
		t.ErrorIs(err, context.Canceled)

		_, err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before insert", func() {
		ctx, cancel := t.Ctx()

		_, _, getSVMs, _ := t.newFuncs()

		insertf, unmerge, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		cancel(nil)

		insert, insertDone, err := insertf(local)
		t.NoError(err)

		err = insert(0, t.randomMergeValues(1))
		t.ErrorIs(err, context.Canceled)
		err = insertDone(nil)
		t.ErrorIs(err, context.Canceled)

		err = unmerge(opsWorkers[1], nil)
		t.ErrorIs(err, context.Canceled)

		_, err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before unmerge", func() {
		t.Run("before insert done", func() {
			ctx, cancel := t.Ctx()

			_, _, getSVMs, _ := t.newFuncs()

			insertf, unmerge, wait, err := newFuncWorkerMerger(
				ctx,
				local,
				dutil.NewFuncNewJobWorkerFunc(1<<9),
				getSVMs,
				1<<9, 1<<9,
				opsWorkers, 1, 1,
			)
			t.NoError(err)

			insert, insertDone, err := insertf(local)
			t.NoError(err)

			t.NoError(insert(0, t.randomMergeValues(1)))

			cancel(nil)

			err = insertDone(nil)
			t.ErrorIs(err, context.Canceled)

			err = unmerge(opsWorkers[1], nil)
			t.ErrorIs(err, context.Canceled)

			_, err = wait(nil)
			t.ErrorIs(err, context.Canceled)
		})

		t.Run("after insert done", func() {
			ctx, cancel := t.Ctx()

			_, _, getSVMs, _ := t.newFuncs()

			insertf, unmerge, wait, err := newFuncWorkerMerger(
				ctx,
				local,
				dutil.NewFuncNewJobWorkerFunc(1<<9),
				getSVMs,
				1<<9, 1<<9,
				opsWorkers, 1, 1,
			)
			t.NoError(err)

			insert, insertDone, err := insertf(local)
			t.NoError(err)

			t.NoError(insert(0, t.randomMergeValues(1)))
			t.NoError(insertDone(nil))

			cancel(nil)

			err = unmerge(opsWorkers[1], nil)
			t.ErrorIs(err, context.Canceled)

			_, err = wait(nil)
			t.ErrorIs(err, context.Canceled)
		})
	})

	t.Run("before wait", func() {
		ctx, cancel := t.Ctx()

		_, _, getSVMs, _ := t.newFuncs()

		insertf, unmerge, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		insert, insertDone, err := insertf(local)
		t.NoError(err)

		t.NoError(insert(0, t.randomMergeValues(1)))
		t.NoError(insertDone(nil))

		t.NoError(unmerge(opsWorkers[1], nil))

		cancel(nil)

		_, err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testWorkerMerger) TestError() {
	_, _, getSVMs, _ := t.newFuncs()

	t.Run("empty ops workers", func() {
		_, _, _, err := newFuncWorkerMerger(
			context.Background(),
			"w0",
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			nil, 1, 1,
		)
		t.ErrorContains(err, "empty size")
	})

	t.Run("zero ops share", func() {
		_, _, _, err := newFuncWorkerMerger(
			context.Background(),
			"w0",
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			t.workers(9), 0, 1,
		)
		t.ErrorContains(err, "zero share")
	})
}

func (t *testWorkerMerger) TestInsert() {
	local := "w0"
	opsWorkers := t.workers(3)

	t.Run("local insert error", func() {
		ctx, _ := t.Ctx()

		_, _, getSVMs, _ := t.newFuncs()

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		insert, insertDone, err := insertf(local)
		t.NoError(err)

		t.NoError(insert(0, t.randomMergeValues(1)))

		err = insertDone(errors.New("findme"))
		t.ErrorContains(err, "findme")

		_, err = wait(nil)
		t.ErrorContains(err, "findme")
	})

	t.Run("worker insert error", func() {
		ctx, _ := t.Ctx()

		_, _, getSVMs, _ := t.newFuncs()

		insertf, unmerge, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		_, insertDone, err := insertf(local)
		t.NoError(err)
		t.NoError(insertDone(nil))

		_, insertDone, err = insertf(opsWorkers[1])
		t.NoError(err)
		err = insertDone(errors.New("findme"))
		t.ErrorContains(err, "findme")

		err = unmerge(opsWorkers[1], nil)
		t.ErrorIs(err, context.Canceled)
		t.ErrorContains(err, "findme")

		_, err = wait(nil)
		t.ErrorIs(err, context.Canceled)
		t.ErrorContains(err, "findme")
	})

	t.Run("empty values; 1 worker", func() {
		ctx, _ := t.Ctx()

		opsWorkers := []string{local}

		_, _, getSVMs, _ := t.newFuncs()

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		_, insertDone, err := insertf(local)
		t.NoError(err)
		t.NoError(insertDone(nil))

		reasons, err := wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return nil, nil
			},
		)
		t.NoError(err)
		t.T().Logf("wait: reasons=%v", reasons)

		t.Empty(reasons)
	})

	t.Run("ok; 1 worker", func() {
		ctx, _ := t.Ctx()

		opsWorkers := []string{local}

		_, _, getSVMs, _ := t.newFuncs()

		values := make([][]SMV, 33)
		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs
		}

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		{
			insert, insertDone, err := insertf(local)
			t.NoError(err)

			for i := range values {
				t.IfErrPanic(insert(uint64(i), values[i]))
			}

			_ = insertDone(nil)
		}

		reasons, err := wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return []string{local}, nil
			},
		)
		t.NoError(err)
		t.T().Logf("wait: reasons=%v", reasons)

		t.Empty(reasons)
	})

	t.Run("empty values; 3 worker", func() {
		ctx, _ := t.Ctx()

		opsWorkers := t.workers(4)
		local = opsWorkers[0]

		_, _, getSVMs, _ := t.newFuncs()

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		go func() {
			for worker := range slices.Values(opsWorkers) {
				_, insertDone, err := insertf(worker)
				if err == nil {
					err = insertDone(err)
				}

				t.Print("insert worker: worker=%q err=%v", worker, err)
			}
		}()

		reasons, err := wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return nil, nil
			},
		)
		t.NoError(err)
		t.T().Logf("wait: reasons=%v", reasons)

		t.Empty(reasons)
	})

	t.Run("ok; 3 worker", func() {
		ctx, _ := t.Ctx()

		opsWorkers := t.workers(4)
		local = opsWorkers[0]
		valuesPerWorker := 3

		_, _, getSVMs, _ := t.newFuncs()

		workerValues := map[string][]uint64{}
		values := make([][]SMV, len(opsWorkers)*valuesPerWorker)

		expectedReasons := map[uint64]string{}

		for i := range values {
			opindex := uint64(i)

			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs

			l := len(opsWorkers)

			worker := opsWorkers[i%l]

			wv := workerValues[worker]
			wv = append(wv, opindex)
			workerValues[worker] = wv

			if worker != local {
				expectedReasons[opindex] = ""
			}
		}

		insertf, unmerge, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		go func() {
			for worker := range slices.Values(opsWorkers) {
				ops := workerValues[worker]

				insert, insertDone, err := insertf(worker)

				if err == nil {
					for i := range ops {
						if err = insert(ops[i], values[i]); err != nil {
							break
						}
					}

					err = insertDone(err)
				}

				t.Print("insert worker: worker=%q ops=%v err=%v", worker, ops, err)
			}

			for worker := range slices.Values(opsWorkers) {
				if worker == local {
					continue
				}

				ops := workerValues[worker]

				err := unmerge(worker, ops)
				t.Print("unmerge worker: worker=%q ops=%v err=%v", worker, ops, err)
			}
		}()

		reasons, err := wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return opsWorkers, nil
			},
		)
		t.NoError(err)
		t.T().Logf("wait: reasons=%v", reasons)

		t.Equal(expectedReasons, reasons)
	})

	t.Run("3 workers, but local not in ops workers", func() {
		ctx, _ := t.Ctx()

		opsWorkers := []string{"w1", "w2"}
		t.True(slices.Index(opsWorkers, local) < 0)

		_, _, getSVMs, _ := t.newFuncs()

		insertf, unmerge, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		go func() {
			for worker := range slices.Values(opsWorkers) {
				_, insertDone, err := insertf(worker)
				if err == nil {
					err = insertDone(nil)
				}

				t.Print("insert worker: worker=%q err=%v", worker, err)
			}

			for worker := range slices.Values(opsWorkers) {
				err := unmerge(worker, nil)
				t.Print("unmerge worker: worker=%q err=%v", worker, err)
			}
		}()

		reasons, err := wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return nil, nil
			},
		)
		t.NoError(err)
		t.T().Logf("wait: reasons=%v", reasons)

		t.Empty(reasons)
	})

	t.Run("unknown worker insert", func() {
		ctx, _ := t.Ctx()

		opsWorkers := []string{local}
		unknowns := []string{"u0", "u1", "u2"}

		_, _, getSVMs, _ := t.newFuncs()

		values := make([][]SMV, 33)
		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs
		}

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		for worker := range slices.Values(unknowns) {
			insert, insertDone, err := insertf(worker)

			if insert != nil && err == nil {
				for i := range values {
					if err = insert(uint64(i), values[i]); err != nil {
						break
					}
				}

				err = insertDone(err)
			}

			t.Print("insert unknown worker: worker=%q err=%v", worker, err)
		}

		{
			insert, insertDone, err := insertf(local)
			t.NoError(err)

			if err == nil {
				for i := range values {
					if err = insert(uint64(i), values[i]); err != nil {
						break
					}
				}

				t.NoError(insertDone(err))
			}
		}

		reasons, err := wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return []string{local}, nil
			},
		)
		t.NoError(err)
		t.T().Logf("wait: reasons=%v", reasons)

		t.Empty(reasons)
	})
}

type (
	testWorkerMergeReasonsFunc func(reasons map[uint64]string) error
	testWorkerMergerWaitsFunc  func(func(string) (workerMergerLocalPhaseMergerDoneFunc, testWorkerMergeReasonsFunc)) ([]string, error)

	testWorkerMergerTraverseMergersFunc func(func(string, iter.Seq2[string, SVM]) error) error
)

func (t *testWorkerMerger) workerMergers(
	ctx context.Context,
	workers []string,
	share uint64,
	states []base.State,
	values [][]SMV,
	subs map[string][]string,
) (
	func() error,
	testWorkerMergerWaitsFunc,
	testWorkerMergerTraverseMergersFunc,
	error,
) {
	wctx, wctxCancel := context.WithCancel(ctx)

	minEnough, maxFailed, err := minEnoughMaxFailed(uint64(len(workers)), share)
	if err != nil {
		return nil, nil, nil, err
	}
	t.T().Logf("min enough ops=%d max failed ops=%d", minEnough, maxFailed)
	t.T().Log("sub states owners:", t.MarshalIndent(subs))

	getStateOwners := t.stateOwnersFunc(subs)

	inserts := map[string]workerMergerInsertSMVsFunc{}
	unmerges := map[string]WorkerUnmergedFunc{}
	waits := map[string]workerMergerWaitFunc{}
	traverseMergers := map[string]iter.Seq2[string, SVM]{}

	for worker := range slices.Values(workers) {
		statesPool, insert, unmerge, wait, tr, err := t.workerMerger(
			wctx,
			worker,
			workers, share,
		)
		t.IfErrPanic(err)

		for st := range slices.Values(states) {
			statesPool.SetValue(st.Key(), st)
		}

		inserts[worker] = insert
		unmerges[worker] = unmerge
		waits[worker] = func(
			localPhaseDone workerMergerLocalPhaseMergerDoneFunc,
		) (map[uint64]string, error) {
			return wait(
				func(unmerged []uint64) ([]string, error) {
					var wg sync.WaitGroup

					for other := range slices.Values(workers) {
						if other == worker {
							continue
						}

						wg.Add(1)

						go func() {
							defer wg.Done()

							err := unmerges[other](worker, unmerged)
							t.Print("worker to other unmerge: worker=%q other=%q unemrged=%v err=%v",
								worker, other, unmerged, err)
						}()
					}

					wg.Wait()

					return localPhaseDone(unmerged)
				},
			)
		}

		traverseMergers[worker] = tr
	}

	workersValues, err := t.assignWorkersValues(values, getStateOwners)
	t.IfErrPanic(err)

	{
		for worker, vs := range maps.All(workersValues) {
			m := map[uint64][]string{}

			for opindex, vs := range maps.All(vs) {
				m[opindex] = extractSMVKeysFromValues(vs)
			}

			t.T().Logf("worker values: worker=%q values=%v", worker, m)
		}
	}

	workerStatesOwners := map[string][]string{}

	for worker := range slices.Values(workers) {
		workerStatesOwners[worker] = t.workersStatesOwners(
			values,
			workersValues[worker],
			getStateOwners,
		)

		t.T().Logf("worker active states owners: worker=%q workers=%v", worker, workerStatesOwners[worker])
	}

	return func() error {
			var wg sync.WaitGroup

			for worker, f := range maps.All(inserts) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					var ops []uint64

					opValues := workersValues[worker]

					insert, insertDone, err := f(worker)
					if insert != nil && err == nil {
						for opindex, vs := range maps.All(opValues) {
							ops = append(ops, opindex)

							if err = insert(opindex, vs); err != nil {
								break
							}
						}

						err = insertDone(err)
					}

					t.Print("worker insert: worker=%q ops=%v err=%v", worker, ops, err)

					for other, f := range maps.All(inserts) {
						if other == worker {
							continue
						}

						insert, insertDone, err := f(worker)
						if insert != nil && err == nil {
							err = insertDone(nil)
						}

						t.Print("worker to other insert: worker=%q other=%q err=%v", worker, other, err)
					}
				}()
			}

			wg.Wait()

			return nil
		},
		func(f func(worker string) (workerMergerLocalPhaseMergerDoneFunc, testWorkerMergeReasonsFunc)) ([]string, error) {
			donech := make(chan string, len(workers))

			var wg sync.WaitGroup

			for worker, wf := range maps.All(waits) {
				wg.Add(1)

				go func() {
					defer func() {
						wg.Done()
					}()

					var l workerMergerLocalPhaseMergerDoneFunc
					var c testWorkerMergeReasonsFunc
					if f != nil {
						l, c = f(worker)
					}

					reasons, err := wf(
						func(unmerged []uint64) ([]string, error) {
							subws := workerStatesOwners[worker]

							t.Print("worker local phase done: worker=%q unmerged=%v sub states owners=%v", worker, unmerged, subws)

							if l != nil {
								return l(unmerged)
							}

							return subws, nil
						},
					)

					t.Print("worker wait done: worker=%q reasons=%v, err=%v", worker, reasons, err)

					if err == nil && c != nil {
						err = c(reasons)
					}

					if err == nil {
						donech <- worker
					}
				}()
			}

			var doneWorkers []string

			var c uint64
			for {
				worker := <-donech
				doneWorkers = append(doneWorkers, worker)
				c++

				if c >= minEnough {
					break
				}
			}
			wctxCancel()

			wg.Wait()

			slices.Sort(doneWorkers)

			t.Print("all wait: done workers=%v", doneWorkers)

			return doneWorkers, nil
		},
		func(f func(string, iter.Seq2[string, SVM]) error) error {
			for worker, wf := range maps.All(traverseMergers) {
				if err := f(worker, wf); err != nil {
					return err
				}
			}

			return nil
		},
		nil
}

func (t *testWorkerMerger) workerMerger(
	ctx context.Context,
	local string,
	opsWorkers []string, share uint64,
) (
	dutil.LockedMap[string, base.State],
	workerMergerInsertSMVsFunc,
	WorkerUnmergedFunc,
	workerMergerWaitFunc,
	iter.Seq2[string, SVM],
	error,
) {
	_, statesPool, getSVMs, traverseSVMs := t.newFuncs()

	logger := dutil.LoadContextValueLogger(ctx).With().Str("local", local).Logger()
	ctx = dutil.SetContextValueLogger(ctx, &logger)

	i, u, m, err := newFuncWorkerMerger(
		ctx,
		local,
		dutil.NewFuncNewJobWorkerFunc(1<<9),
		getSVMs,
		1<<9, 1<<9,
		opsWorkers, share, share,
	)

	return statesPool, i, u, m, traverseSVMs, err
}

func (t *testWorkerMerger) stateOwnersFunc(
	subs map[string][]string,
) getStateOwnersFunc {
	return func(stateKey string) []string {
		return subs[stateKey]
	}
}

func (t *testWorkerMerger) assignWorkersValues(
	values [][]SMV,
	getStateOwners getStateOwnersFunc,
) (
	map[string]map[uint64][]SMV,
	error,
) {
	workersValues := map[string]map[uint64][]SMV{}

	for i, vs := range slices.All(values) {
		opindex := uint64(i)

		for value := range slices.Values(vs) {
			owners := getStateOwners(value.Key())
			if len(owners) < 1 {
				return nil, util.ErrNotFound.Errorf("state owners not found, %q(%d)", value.Key(), opindex)
			}

			for owner := range slices.Values(owners) {
				m, found := workersValues[owner]
				if !found {
					m = map[uint64][]SMV{}
				}

				m[opindex] = append(m[opindex], value)

				workersValues[owner] = m
			}
		}
	}

	return workersValues, nil
}

func (t *testWorkerMerger) workersStatesOwners(
	values [][]SMV,
	workerValues map[uint64][]SMV,
	getStateOwners getStateOwnersFunc,
) []string {
	m := map[string]struct{}{}
	defer clear(m)

	for opindex := range maps.Keys(workerValues) {
		for value := range slices.Values(values[opindex]) {
			if !isSubAccountSMV(value.(*AccountSMV)) {
				continue
			}

			for owner := range slices.Values(getStateOwners(value.Key())) {
				m[owner] = struct{}{}
			}
		}
	}

	return slices.Collect(maps.Keys(m))
}

func (t *testWorkerMerger) TestNoSubs() {
	t.Run("3 workers; empty values", func() {
		workers := t.workers(3)
		share := uint64(1)

		ctx, _ := t.Ctx()

		// ctx = dutil.SetContextValueLogger(ctx, testLogger)

		insert, waits, traverse, err := t.workerMergers(ctx, workers, share, nil, nil, nil)
		t.NoError(err)

		go func() {
			err := insert()

			t.Print("insert workers: err=%v", err)
			t.IfErrPanic(err)
		}()

		doneWorkers, err := waits(nil)
		t.NoError(err)
		t.Equal(workers, doneWorkers)

		var called int
		t.NoError(traverse(func(worker string, f iter.Seq2[string, SVM]) error {
			for range f {
				called++
			}

			return nil
		}))

		t.Equal(0, called)
	})

	t.Run("3 workers, 1 share", func() {
		workers := t.workers(3)
		share := uint64(1)
		subs := map[string][]string{}

		getStateOwners, getStateOwnersClose, err := stateOwnersFuncByCount(workers, share)
		t.NoError(err)
		defer getStateOwnersClose()

		var expectedValues int
		values := make([][]SMV, 3)
		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs

			for value := range slices.Values(vs) {
				subs[value.Key()] = getStateOwners(value.Key())
			}

			expectedValues += len(vs)
		}

		ctx, _ := t.Ctx()

		// ctx = dutil.SetContextValueLogger(ctx, testLogger)

		insert, waits, traverse, err := t.workerMergers(ctx, workers, share, nil, values, subs)
		t.NoError(err)

		go func() {
			err := insert()

			t.Print("insert workers: err=%v", err)
			t.IfErrPanic(err)
		}()

		doneWorkers, err := waits(nil)
		t.NoError(err)
		t.Equal(workers, doneWorkers)

		collectedStates := map[string]int64{}

		t.NoError(traverse(func(worker string, f iter.Seq2[string, SVM]) error {
			if slices.Index(doneWorkers, worker) < 0 {
				return nil
			}

			for stateKey, merger := range f {
				stv, err := merger.Close()
				if err != nil {
					return errors.WithMessage(err, worker)
				}

				var problem string

				if stv == nil {
					collectedStates[stateKey] = 0
				} else {
					v := stv.(*AccountSV)
					switch b, found := collectedStates[stateKey]; {
					case found:
						if v.Balance != b {
							problem = fmt.Sprintf("balance mismatch: key=%q prev=%d cur=%d", stateKey, b, v.Balance)
						}
					default:
						collectedStates[stateKey] = v.Balance
					}
				}

				t.Print("worker states: worker=%q state=%q value=%v problem=%q",
					worker, stateKey, t.printSMV(stv), problem)

				if problem != "" {
					return errors.Errorf("%s: worker=%q", problem, worker)
				}
			}

			return nil
		}))

		t.Equal(expectedValues, len(collectedStates))
	})

	t.Run("3 workers, 2 share", func() {
		workers := t.workers(3)
		share := uint64(2)
		subs := map[string][]string{}

		getStateOwners, getStateOwnersClose, err := stateOwnersFuncByCount(workers, share)
		t.NoError(err)
		defer getStateOwnersClose()

		var expectedValues int
		values := make([][]SMV, 3)
		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs

			for value := range slices.Values(vs) {
				subs[value.Key()] = getStateOwners(value.Key())
			}

			expectedValues += len(vs)
		}

		ctx, _ := t.Ctx()

		// ctx = dutil.SetContextValueLogger(ctx, testLogger)

		insert, waits, traverse, err := t.workerMergers(ctx, workers, share, nil, values, subs)
		t.NoError(err)

		go func() {
			err := insert()

			t.Print("insert workers: err=%v", err)
			t.IfErrPanic(err)
		}()

		doneWorkers, err := waits(nil)
		t.NoError(err)
		t.True(len(doneWorkers) >= 2)

		collectedStates := map[string]int64{}

		t.NoError(traverse(func(worker string, f iter.Seq2[string, SVM]) error {
			if slices.Index(doneWorkers, worker) < 0 {
				return nil
			}

			for stateKey, merger := range f {
				stv, err := merger.Close()
				if err != nil {
					return errors.WithMessage(err, worker)
				}

				var problem string

				v := stv.(*AccountSV)
				switch b, found := collectedStates[stateKey]; {
				case found:
					if v.Balance != b {
						problem = "balance mismatch"
					}
				default:
					collectedStates[stateKey] = b
				}

				t.Print("worker states: worker=%q state=%q value=%v problem=%q",
					worker, stateKey, t.printSMV(stv), problem)
			}

			return nil
		}))

		t.Equal(expectedValues, len(collectedStates))
	})
}

func (t *testWorkerMerger) TestSubs() {
	pub0 := base.NewDummyPublickey("p0")
	pub1 := base.NewDummyPublickey("p1")
	pub2 := base.NewDummyPublickey("p2")

	states := make([]base.State, 3)
	states[0] = t.newState(pub0, 1)
	states[1] = t.newState(pub1, 2)
	states[2] = t.newState(pub2, 3)

	nv, nnv := t.newMergeValue, t.newMergeValue

	values := [][]SMV{
		{nv(pub0, 1), nnv(pub1, -2)}, // 0
		{nv(pub1, -2), nv(pub2, 3)},  // 1: ignored
		{nv(pub0, -1), nv(pub1, 2)},  // 2
		{nv(pub0, -1), nv(pub1, 2)},  // 3: ignored
		{nv(pub0, 1), nnv(pub2, -3)}, // 4
		{nv(pub1, 1), nnv(pub2, 2)},  // 5
	}

	expectedUnmerged := []uint64{1, 3}
	expectedMergedStatesValues := map[string]int64{
		states[0].Key(): 2,
		states[1].Key(): 3,
		states[2].Key(): 2,
	}

	checkWaits := func(waits testWorkerMergerWaitsFunc) []string {
		addUnmerged, doneUnmerged, _ := dutil.NewLockAppendSlice[uint64](uint64(len(values)), 0)

		doneWorkers, err := waits(func(worker string) (workerMergerLocalPhaseMergerDoneFunc, testWorkerMergeReasonsFunc) {
			return nil,
				func(reasons map[uint64]string) error {
					unmerged := slices.Collect(maps.Keys(reasons))

					if len(unmerged) > 0 {
						addUnmerged(unmerged...)
					}

					return nil
				}
		})
		t.NoError(err)

		s := doneUnmerged()

		slices.Sort(s)
		s = slices.Compact(s)

		t.Equal(expectedUnmerged, s)

		return doneWorkers
	}

	checkMerged := func(doneWorkers []string, traverse testWorkerMergerTraverseMergersFunc) {
		collectedStates := map[string]int64{}

		t.NoError(traverse(func(worker string, f iter.Seq2[string, SVM]) error {
			if slices.Index(doneWorkers, worker) < 0 {
				return nil
			}

			for stateKey, merger := range f {
				stv, err := merger.Close()
				if err != nil {
					return errors.WithMessage(err, worker)
				}

				var problem string

				if stv == nil {
					collectedStates[stateKey] = 0
				} else {
					v := stv.(*AccountSV)
					switch b, found := collectedStates[stateKey]; {
					case found:
						if v.Balance != b {
							problem = fmt.Sprintf("balance mismatch: key=%q prev=%d cur=%d", stateKey, b, v.Balance)
						}
					default:
						collectedStates[stateKey] = v.Balance
					}
				}

				t.Print("worker states: worker=%q state=%q value=%v nil=%v problem=%q",
					worker, stateKey, t.printSMV(stv), stv == nil, problem)

				if problem != "" {
					return errors.Errorf("%s: worker=%q", problem, worker)
				}
			}

			return nil
		}))

		t.Equal(expectedMergedStatesValues, collectedStates)
	}

	run := func(n int, share uint64) func() {
		return func() {
			workers := t.workers(n)

			getStateOwners, getStateOwnersClose, err := stateOwnersFuncByCount(workers, share)
			t.NoError(err)
			defer getStateOwnersClose()

			subs := map[string][]string{}
			for i := range values {
				vs := values[i]

				for value := range slices.Values(vs) {
					subs[value.Key()] = getStateOwners(value.Key())
				}
			}

			t.T().Log("sub states owners:", subs)

			ctx, _ := t.Ctx()

			// ctx = dutil.SetContextValueLogger(ctx, testLogger)

			insert, waits, traverse, err := t.workerMergers(ctx, workers, share, states, values, subs)
			t.NoError(err)

			go func() {
				err := insert()

				t.Print("insert workers: err=%v", err)
				t.IfErrPanic(err)
			}()

			doneWorkers := checkWaits(waits)
			checkMerged(doneWorkers, traverse)
		}
	}

	t.Run("repeat", func() {
		maxWorkers := len(states)
		for workers := 1; workers <= maxWorkers; workers++ {
			for share := uint64(1); share <= uint64(workers); share++ {
				for i := range 1 << 8 {
					t.Run(fmt.Sprintf("%d: %d workers, %d share", i, workers, share), run(workers, share))
				}
			}
		}
	})
}

func (t *testWorkerMerger) TestWorkerManager() {
	local := "w0"

	t.Run("1 worker and then manager", func() {
		ctx, _ := t.Ctx()

		opsWorkers := []string{local}

		_, _, getSVMs, _ := t.newFuncs()

		values := make([][]SMV, 33)
		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs
		}

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		{
			insert, insertDone, err := insertf(local)
			t.NoError(err)

			for i := range values {
				t.IfErrPanic(insert(uint64(i), values[i]))
			}

			_ = insertDone(nil)

			_, insertDone, err = insertf(ManagerName)
			t.NoError(err)

			_ = insertDone(nil)
		}

		reasons, err := wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return []string{local}, nil
			},
		)
		t.NoError(err)
		t.T().Logf("wait: reasons=%v", reasons)

		t.Empty(reasons)
	})

	t.Run("manager before 1 worker", func() {
		ctx, _ := t.Ctx()

		opsWorkers := []string{local}

		_, _, getSVMs, _ := t.newFuncs()

		values := make([][]SMV, 33)
		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs
		}

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		{
			_, insertDone, err := insertf(ManagerName)
			t.NoError(err)

			_ = insertDone(nil)

			insert, insertDone, err := insertf(local)
			t.NoError(err)

			for i := range values {
				t.IfErrPanic(insert(uint64(i), values[i]))
			}

			_ = insertDone(nil)
		}

		reasons, err := wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return []string{local}, nil
			},
		)
		t.NoError(err)
		t.T().Logf("wait: reasons=%v", reasons)

		t.Empty(reasons)
	})

	t.Run("empty values; 2 worker and then manager", func() {
		ctx, _ := t.Ctx()

		opsWorkers := []string{local, "w0"}

		_, _, getSVMs, _ := t.newFuncs()

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		_, insertDone, err := insertf(local)
		t.NoError(err)
		t.NoError(insertDone(nil))

		_, insertDone, err = insertf(ManagerName)
		t.NoError(err)
		t.NoError(insertDone(nil))

		reasons, err := wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return nil, nil
			},
		)
		t.NoError(err)
		t.T().Logf("wait: reasons=%v", reasons)

		t.Empty(reasons)
	})

	t.Run("over workers and manager before unmerge", func() {
		ctx, _ := t.Ctx()

		opsWorkers := t.workers(4)
		local = opsWorkers[0]
		valuesPerWorker := 3

		_, _, getSVMs, _ := t.newFuncs()

		workerValues := map[string][]uint64{}
		values := make([][]SMV, len(opsWorkers)*valuesPerWorker)

		expectedReasons := map[uint64]string{}

		for i := range values {
			opindex := uint64(i)

			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs

			l := len(opsWorkers)

			worker := opsWorkers[i%l]

			wv := workerValues[worker]
			wv = append(wv, opindex)
			workerValues[worker] = wv

			if worker != local {
				expectedReasons[opindex] = ""
			}
		}

		overWorkers := slices.Clone(opsWorkers)
		overWorkers = append(overWorkers, "o0", "o1")

		insertf, unmerge, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			overWorkers, 1, 1,
		)
		t.NoError(err)

		go func() {
			for worker := range slices.Values(opsWorkers) {
				ops := workerValues[worker]

				insert, insertDone, err := insertf(worker)

				if err == nil {
					for i := range ops {
						if err = insert(ops[i], values[i]); err != nil {
							break
						}
					}

					err = insertDone(err)
				}

				t.Print("insert worker: worker=%q ops=%v err=%v", worker, ops, err)
			}

			_, insertDone, err := insertf(ManagerName)
			t.IfErrPanic(err)

			t.IfErrPanic(insertDone(nil))

			for worker := range slices.Values(opsWorkers) {
				if worker == local {
					continue
				}

				ops := workerValues[worker]

				err := unmerge(worker, ops)
				t.Print("unmerge worker: worker=%q ops=%v err=%v", worker, ops, err)
			}
		}()

		reasons, err := wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return opsWorkers, nil
			},
		)
		t.NoError(err)
		t.T().Logf("wait: reasons=%v", reasons)

		t.Equal(expectedReasons, reasons)
	})

	t.Run("over workers and manager", func() {
		ctx, _ := t.Ctx()

		opsWorkers := t.workers(4)
		local = opsWorkers[0]
		valuesPerWorker := 3

		_, _, getSVMs, _ := t.newFuncs()

		workerValues := map[string][]uint64{}
		values := make([][]SMV, len(opsWorkers)*valuesPerWorker)

		expectedReasons := map[uint64]string{}

		for i := range values {
			opindex := uint64(i)

			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs

			l := len(opsWorkers)

			worker := opsWorkers[i%l]

			wv := workerValues[worker]
			wv = append(wv, opindex)
			workerValues[worker] = wv

			if worker != local {
				expectedReasons[opindex] = ""
			}
		}

		overWorkers := slices.Clone(opsWorkers)
		overWorkers = append(overWorkers, "o0", "o1")

		insertf, unmerge, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			getSVMs,
			1<<9, 1<<9,
			overWorkers, 1, 1,
		)
		t.NoError(err)

		go func() {
			for worker := range slices.Values(opsWorkers) {
				ops := workerValues[worker]

				insert, insertDone, err := insertf(worker)

				if err == nil {
					for i := range ops {
						if err = insert(ops[i], values[i]); err != nil {
							break
						}
					}

					err = insertDone(err)
				}

				t.Print("insert worker: worker=%q ops=%v err=%v", worker, ops, err)
			}

			for worker := range slices.Values(opsWorkers) {
				if worker == local {
					continue
				}

				ops := workerValues[worker]

				err := unmerge(worker, ops)
				t.Print("unmerge worker: worker=%q ops=%v err=%v", worker, ops, err)
			}

			_, insertDone, err := insertf(ManagerName)
			t.IfErrPanic(err)

			t.IfErrPanic(insertDone(nil))
		}()

		reasons, err := wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return opsWorkers, nil
			},
		)
		t.NoError(err)
		t.T().Logf("wait: reasons=%v", reasons)

		t.Equal(expectedReasons, reasons)
	})
}

func TestWorkerMerger(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/spikeekips/mitum/example/test364_5/util.NewUnlimitedChans[...].func2"),
		goleak.IgnoreTopFunction("github.com/spikeekips/mitum/example/test364_5/util.NewUnlimitedChans[...].func4"),
	)

	suite.Run(t, new(testWorkerMerger))
}
