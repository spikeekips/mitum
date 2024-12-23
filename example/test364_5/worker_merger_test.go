package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorkerMerger struct {
	testSuite
	baseTestMergeOp
	baseTestWorkers
}

func (t *testWorkerMerger) newFuncs() (
	mergers util.LockedMap[string, StateMerger],
	states util.LockedMap[string, base.State],
	getState getStateFunc,
	getStateMergers mergerGetStateMergersFunc,
	traverseStateMergers traverseStateMergersFunc,
) {
	t.T().Helper()

	_, mergers, states, _, _ = t.baseTestMergeOp.getFuncs()

	getState = func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}

	extractKeys, _ := t.extractStateMergeValueKeys()

	switch g, t, err := newFuncStateMergers(
		1<<9,
		getState,
		extractKeys,
	); {
	case err != nil:
		panic(err)
	default:
		getStateMergers = g
		_, traverseStateMergers = t()
	}

	return mergers, states, getState, getStateMergers, traverseStateMergers
}

func (t *testWorkerMerger) TestCancel() {
	local := "w0"
	opsWorkers := t.workers(2)

	t.Run("before new", func() {
		ctx, cancel := t.Ctx()

		cancel()

		_, _, getState, getStateMergers, _ := t.newFuncs()

		insertf, unmerge, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9, 1<<9, 1<<9,
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

		err = wait(nil, nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before insert", func() {
		ctx, cancel := t.Ctx()

		_, _, getState, getStateMergers, _ := t.newFuncs()

		insertf, unmerge, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9, 1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		cancel()

		insert, insertDone, err := insertf(local)
		t.NoError(err)

		err = insert(0, t.randomMergeValues(1))
		t.ErrorIs(err, context.Canceled)
		err = insertDone(nil)
		t.ErrorIs(err, context.Canceled)

		err = unmerge(opsWorkers[1], nil)
		t.ErrorIs(err, context.Canceled)

		err = wait(nil, nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before unmerge", func() {
		t.Run("before insert done", func() {
			ctx, cancel := t.Ctx()

			_, _, getState, getStateMergers, _ := t.newFuncs()

			insertf, unmerge, wait, err := newFuncWorkerMerger(
				ctx,
				local,
				newFuncNewJobWorkerFunc(1<<9),
				getState,
				getStateMergers,
				1<<9, 1<<9, 1<<9,
				opsWorkers, 1, 1,
			)
			t.NoError(err)

			insert, insertDone, err := insertf(local)
			t.NoError(err)

			t.NoError(insert(0, t.randomMergeValues(1)))

			cancel()

			err = insertDone(nil)
			t.ErrorIs(err, context.Canceled)

			err = unmerge(opsWorkers[1], nil)
			t.ErrorIs(err, context.Canceled)

			err = wait(nil, nil)
			t.ErrorIs(err, context.Canceled)
		})

		t.Run("after insert done", func() {
			ctx, cancel := t.Ctx()

			_, _, getState, getStateMergers, _ := t.newFuncs()

			insertf, unmerge, wait, err := newFuncWorkerMerger(
				ctx,
				local,
				newFuncNewJobWorkerFunc(1<<9),
				getState,
				getStateMergers,
				1<<9, 1<<9, 1<<9,
				opsWorkers, 1, 1,
			)
			t.NoError(err)

			insert, insertDone, err := insertf(local)
			t.NoError(err)

			t.NoError(insert(0, t.randomMergeValues(1)))
			t.NoError(insertDone(nil))

			cancel()

			err = unmerge(opsWorkers[1], nil)
			t.ErrorIs(err, context.Canceled)

			err = wait(nil, nil)
			t.ErrorIs(err, context.Canceled)
		})
	})

	t.Run("before wait", func() {
		ctx, cancel := t.Ctx()

		_, _, getState, getStateMergers, _ := t.newFuncs()

		insertf, unmerge, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9, 1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		insert, insertDone, err := insertf(local)
		t.NoError(err)

		t.NoError(insert(0, t.randomMergeValues(1)))
		t.NoError(insertDone(nil))

		t.NoError(unmerge(opsWorkers[1], nil))

		cancel()

		err = wait(nil, nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testWorkerMerger) TestError() {
	_, _, getState, getStateMergers, _ := t.newFuncs()

	t.Run("empty ops workers", func() {
		_, _, _, err := newFuncWorkerMerger(
			context.Background(),
			"w0",
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9, 1<<9, 1<<9,
			nil, 1, 1,
		)
		t.ErrorContains(err, "zero workers")
	})

	t.Run("zero ops share", func() {
		_, _, _, err := newFuncWorkerMerger(
			context.Background(),
			"w0",
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1, 1<<9, 1<<9,
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

		_, _, getState, getStateMergers, _ := t.newFuncs()

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9, 1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		insert, insertDone, err := insertf(local)
		t.NoError(err)

		t.NoError(insert(0, t.randomMergeValues(1)))

		err = insertDone(errors.Errorf("findme"))
		t.ErrorContains(err, "findme")

		err = wait(nil, nil)
		t.ErrorContains(err, "findme")
	})

	t.Run("worker insert error", func() {
		ctx, _ := t.Ctx()

		_, _, getState, getStateMergers, _ := t.newFuncs()

		insertf, unmerge, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9, 1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		_, insertDone, err := insertf(local)
		t.NoError(err)
		t.NoError(insertDone(nil))

		_, insertDone, err = insertf(opsWorkers[1])
		t.NoError(err)
		err = insertDone(errors.Errorf("findme"))
		t.ErrorContains(err, "findme")

		err = unmerge(opsWorkers[1], nil)
		t.ErrorIs(err, context.Canceled)
		t.ErrorContains(err, "findme")

		err = wait(nil, nil)
		t.ErrorIs(err, context.Canceled)
		t.ErrorContains(err, "findme")
	})

	t.Run("empty values; 1 worker", func() {
		ctx, _ := t.Ctx()

		opsWorkers := []string{local}

		_, _, getState, getStateMergers, _ := t.newFuncs()

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9, 1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		_, insertDone, err := insertf(local)
		t.NoError(err)
		t.NoError(insertDone(nil))

		t.NoError(wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return nil, nil
			},
			func(reasons map[uint64]string) error {
				t.T().Logf("wait: reasons=%v", reasons)

				t.Empty(reasons)

				return nil
			},
		))
	})

	t.Run("ok; 1 worker", func() {
		ctx, _ := t.Ctx()

		opsWorkers := []string{local}

		_, _, getState, getStateMergers, _ := t.newFuncs()

		values := make([][]StateMergeValue, 33)
		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs
		}

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		{
			insert, insertDone, err := insertf(local)
			t.NoError(err)

			for i := range values {
				if err := insert(uint64(i), values[i]); err != nil {
					panic(err)
				}
			}

			_ = insertDone(nil)
		}

		t.NoError(wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return []string{local}, nil
			},
			func(reasons map[uint64]string) error {
				t.T().Logf("wait: reasons=%v", reasons)

				t.Empty(reasons)

				return nil
			},
		))
	})

	t.Run("empty values; 3 worker", func() {
		ctx, _ := t.Ctx()

		opsWorkers := t.workers(4)
		local = opsWorkers[0]

		_, _, getState, getStateMergers, _ := t.newFuncs()

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9, 1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		go func() {
			traverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
				_, insertDone, err := insertf(worker)
				if err == nil {
					err = insertDone(err)
				}

				t.println("insert worker: worker=%q err=%v", worker, err)

				return true, nil
			})
		}()

		t.NoError(wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return nil, nil
			},
			func(reasons map[uint64]string) error {
				t.T().Logf("wait: reasons=%v", reasons)

				t.Empty(reasons)

				return nil
			},
		))
	})

	t.Run("ok; 3 worker", func() {
		ctx, _ := t.Ctx()

		opsWorkers := t.workers(4)
		local = opsWorkers[0]
		valuesPerWorker := 3

		_, _, getState, getStateMergers, _ := t.newFuncs()

		workerValues := map[string][]uint64{}
		values := make([][]StateMergeValue, len(opsWorkers)*valuesPerWorker)

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
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9, 1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		go func() {
			traverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
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

				t.println("insert worker: worker=%q ops=%v err=%v", worker, ops, err)

				return true, nil
			})

			traverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
				if worker == local {
					return true, nil
				}

				ops := workerValues[worker]

				err := unmerge(worker, ops)
				t.println("unmerge worker: worker=%q ops=%v err=%v", worker, ops, err)

				return true, nil
			})
		}()

		t.NoError(wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return opsWorkers, nil
			},
			func(reasons map[uint64]string) error {
				t.T().Logf("wait: reasons=%v", reasons)

				t.Equal(expectedReasons, reasons)

				return nil
			},
		))
	})

	t.Run("3 workers, but local not in ops workers", func() {
		ctx, _ := t.Ctx()

		opsWorkers := []string{"w1", "w2"}
		t.True(slices.Index(opsWorkers, local) < 0)

		_, _, getState, getStateMergers, _ := t.newFuncs()

		insertf, unmerge, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9, 1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		go func() {
			traverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
				_, insertDone, err := insertf(worker)
				if err == nil {
					err = insertDone(nil)
				}

				t.println("insert worker: worker=%q err=%v", worker, err)

				return true, nil
			})

			traverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
				err := unmerge(worker, nil)
				t.println("unmerge worker: worker=%q err=%v", worker, err)

				return true, nil
			})
		}()

		t.NoError(wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return nil, nil
			},
			func(reasons map[uint64]string) error {
				t.T().Logf("wait: reasons=%v", reasons)

				t.Empty(reasons)

				return nil
			},
		))
	})

	t.Run("unknown worker insert", func() {
		ctx, _ := t.Ctx()

		opsWorkers := []string{local}
		unknowns := []string{"u0", "u1", "u2"}

		_, _, getState, getStateMergers, _ := t.newFuncs()

		values := make([][]StateMergeValue, 33)
		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs
		}

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		traverseSlice(unknowns, func(_ int, worker string) (bool, error) {
			insert, insertDone, err := insertf(worker)

			if insert != nil && err == nil {
				for i := range values {
					if err = insert(uint64(i), values[i]); err != nil {
						break
					}
				}

				err = insertDone(err)
			}

			t.println("insert unknown worker: worker=%q err=%v", worker, err)

			return true, nil
		})

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

		t.NoError(wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return []string{local}, nil
			},
			func(reasons map[uint64]string) error {
				t.T().Logf("wait: reasons=%v", reasons)

				t.Empty(reasons)

				return nil
			},
		))
	})
}

type (
	testWorkerMergerWaitsFunc           func(func(string) (workerMergerLocalPhaseMergerDoneFunc, workerMergerCallback)) ([]string, error)
	testWorkerMergerTraverseMergersFunc func(func(string, traverseStateMergersFunc) error) error
)

func (t *testWorkerMerger) workerMergers(
	ctx context.Context,
	workers []string,
	share uint64,
	states []base.State,
	values [][]StateMergeValue,
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
	t.T().Log("sub state owners:", t.MarshalIndent(subs))

	getStateOwners := t.stateOwnersFunc(subs)

	inserts := map[string]workerMergerInsertStateMergeValuesFunc{}
	unmerges := map[string]workerUnmergedFunc{}
	waits := map[string]workerMergerWaitFunc{}
	traverseMergers := map[string]traverseStateMergersFunc{}

	if err := traverseSlice(workers, func(_ int, worker string) (bool, error) {
		statesPool, insert, unmerge, wait, tr, err := t.workerMerger(
			wctx,
			worker,
			workers, share,
		)
		if err != nil {
			return false, err
		}

		traverseSlice(states, func(_ int, st base.State) (bool, error) {
			statesPool.SetValue(st.Key(), st)

			return true, nil
		})

		inserts[worker] = insert
		unmerges[worker] = unmerge
		waits[worker] = func(
			localPhaseDone workerMergerLocalPhaseMergerDoneFunc,
			done workerMergerCallback,
		) error {
			return wait(
				func(unmerged []uint64) ([]string, error) {
					var wg sync.WaitGroup

					if err := traverseSlice(workers, func(_ int, other string) (bool, error) {
						if other == worker {
							return true, nil
						}

						wg.Add(1)

						go func() {
							defer wg.Done()

							err := unmerges[other](worker, unmerged)
							t.println("worker to other unmerge: worker=%q other=%q unemrged=%v err=%v",
								worker, other, unmerged, err)
						}()

						return true, nil
					}); err != nil {
						return nil, err
					}

					wg.Wait()

					return localPhaseDone(unmerged)
				},
				done,
			)
		}

		traverseMergers[worker] = tr

		return true, nil
	}); err != nil {
		return nil, nil, nil, err
	}

	workersValues, err := t.assignWorkersValues(values, getStateOwners)
	if err != nil {
		panic(err)
	}

	{
		traverseMap(workersValues, func(worker string, vs map[uint64][]StateMergeValue) (bool, error) {
			m := map[uint64][]string{}

			traverseMap(vs, func(opindex uint64, vs []StateMergeValue) (bool, error) {
				m[opindex] = extractStateMergeValueKeysFromValues(vs)

				return true, nil
			})

			t.T().Logf("worker values: worker=%q values=%v", worker, m)

			return true, nil
		})
	}

	workerStateOwners := map[string][]string{}

	traverseSlice(workers, func(_ int, worker string) (bool, error) {
		workerStateOwners[worker] = t.workersStateOwners(
			values,
			workersValues[worker],
			getStateOwners,
		)

		t.T().Logf("worker active states workers: worker=%q workers=%v", worker, workerStateOwners[worker])

		return true, nil
	})

	return func() error {
			var wg sync.WaitGroup

			traverseMap(
				inserts,
				func(worker string, f workerMergerInsertStateMergeValuesFunc) (bool, error) {
					wg.Add(1)

					go func() {
						defer wg.Done()

						var ops []uint64

						opValues := workersValues[worker]

						insert, insertDone, err := f(worker)
						if insert != nil && err == nil {
							err = traverseMap(opValues, func(opindex uint64, vs []StateMergeValue) (bool, error) {
								ops = append(ops, opindex)

								return true, insert(opindex, vs)
							})

							err = insertDone(err)
						}

						t.println("worker insert: worker=%q ops=%v err=%v", worker, ops, err)

						traverseMap(
							inserts,
							func(other string, f workerMergerInsertStateMergeValuesFunc) (bool, error) {
								if other == worker {
									return true, nil
								}

								insert, insertDone, err := f(worker)
								if insert != nil && err == nil {
									err = insertDone(nil)
								}

								t.println("worker to other insert: worker=%q other=%q err=%v", worker, other, err)

								return true, err
							})
					}()

					return true, nil
				},
			)

			wg.Wait()

			return nil
		},
		func(f func(string) (workerMergerLocalPhaseMergerDoneFunc, workerMergerCallback)) ([]string, error) {
			donech := make(chan string, len(workers))

			var wg sync.WaitGroup

			traverseMap(waits, func(worker string, wf workerMergerWaitFunc) (bool, error) {
				wg.Add(1)

				go func() {
					defer func() {
						wg.Done()
					}()

					var l workerMergerLocalPhaseMergerDoneFunc
					var c workerMergerCallback
					if f != nil {
						l, c = f(worker)
					}

					err := wf(
						func(unmerged []uint64) ([]string, error) {
							subws := workerStateOwners[worker]

							t.println("worker local phase done: worker=%q unmerged=%v sub state owners=%v", worker, unmerged, subws)

							if l != nil {
								return l(unmerged)
							}

							return subws, nil
						},
						func(reasons map[uint64]string) error {
							t.println("worker wait done: worker=%q reasons=%v", worker, reasons)

							if c != nil {
								return c(reasons)
							}

							return nil
						},
					)

					t.println("worker wait: worker=%q err=%v", worker, err)

					if err == nil {
						donech <- worker
					}
				}()

				return true, nil
			})

			var doneWorkers []string

			var c uint64
			for {
				select {
				case worker := <-donech:
					doneWorkers = append(doneWorkers, worker)
					c++
				}

				if c >= minEnough {
					break
				}
			}
			wctxCancel()

			wg.Wait()

			slices.Sort(doneWorkers)

			t.println("all wait: done workers=%v", doneWorkers)

			return doneWorkers, nil
		},
		func(f func(string, traverseStateMergersFunc) error) error {
			return traverseMap(traverseMergers, func(worker string, wf traverseStateMergersFunc) (bool, error) {
				return true, f(worker, wf)
			})
		},
		nil
}

func (t *testWorkerMerger) workerMerger(
	ctx context.Context,
	local string,
	opsWorkers []string, share uint64,
) (
	util.LockedMap[string, base.State],
	workerMergerInsertStateMergeValuesFunc,
	workerUnmergedFunc,
	workerMergerWaitFunc,
	traverseStateMergersFunc,
	error,
) {
	_, statesPool, getState, getStateMergers, traverseStateMergers := t.newFuncs()

	logger := loadContextValueLogger(ctx).With().Str("local", local).Logger()
	ctx = setContextValueLogger(ctx, &logger)

	i, u, m, err := newFuncWorkerMerger(
		ctx,
		local,
		newFuncNewJobWorkerFunc(1<<9),
		getState,
		getStateMergers,
		1<<9, 1<<9, 1<<9,
		opsWorkers, share, share,
	)

	return statesPool, i, u, m, traverseStateMergers, err
}

func (t *testWorkerMerger) stateOwnersFunc(
	subs map[string][]string,
) getStateOwnersFunc {
	return func(stateKey string) []string {
		return subs[stateKey]
	}
}

func (t *testWorkerMerger) assignWorkersValues(
	values [][]StateMergeValue,
	getStateOwners getStateOwnersFunc,
) (
	map[string]map[uint64][]StateMergeValue,
	error,
) {
	workersValues := map[string]map[uint64][]StateMergeValue{}

	return workersValues,
		traverseSlice(values, func(i int, vs []StateMergeValue) (bool, error) {
			opindex := uint64(i)

			return true, traverseSlice(vs, func(_ int, value StateMergeValue) (bool, error) {
				owners := getStateOwners(value.Key())
				if len(owners) < 1 {
					return false, util.ErrNotFound.Errorf("state owners not found, %q(%d)", value.Key(), opindex)
				}

				traverseSlice(owners, func(i int, owner string) (bool, error) {
					m, found := workersValues[owner]
					if !found {
						m = map[uint64][]StateMergeValue{}
					}

					m[opindex] = append(m[opindex], value)

					workersValues[owner] = m

					return true, nil
				})

				return true, nil
			})
		})
}

func (t *testWorkerMerger) workersUnmerged(
	values [][]StateMergeValue,
	getStateOwners getStateOwnersFunc,
	unmerged []uint64,
) (map[string][]uint64, error) {
	m := map[string][]uint64{}

	return m, traverseSlice(
		unmerged,
		func(_ int, opindex uint64) (bool, error) {
			return true, traverseSlice(values[opindex], func(_ int, value StateMergeValue) (bool, error) {
				return true, traverseSlice(getStateOwners(value.Key()), func(_ int, owner string) (bool, error) {
					m[owner] = append(m[owner], opindex)

					return true, nil
				})
			})
		},
	)
}

func (t *testWorkerMerger) workersStateOwners(
	values [][]StateMergeValue,
	workerValues map[uint64][]StateMergeValue,
	getStateOwners getStateOwnersFunc,
) []string {
	m := map[string]struct{}{}
	defer clearMap(m)

	traverseMap(workerValues, func(opindex uint64, _ []StateMergeValue) (bool, error) {
		return true, traverseSlice(values[opindex], func(_ int, value StateMergeValue) (bool, error) {
			if !isSubAccountStateMergeValue(value.(*AccountStateMergeValue)) {
				return true, nil
			}

			return true, traverseSlice(getStateOwners(value.Key()), func(_ int, owner string) (bool, error) {
				m[owner] = struct{}{}

				return true, nil
			})
		})
	})

	return slices.Collect(maps.Keys(m))
}

func (t *testWorkerMerger) TestNoSubs() {
	t.Run("3 workers; empty values", func() {
		workers := t.workers(3)
		share := uint64(1)

		ctx, _ := t.Ctx()

		// ctx = setContextValueLogger(ctx, testLogger)

		insert, waits, traverse, err := t.workerMergers(ctx, workers, share, nil, nil, nil)
		t.NoError(err)

		go func() {
			err := insert()

			t.println("insert workers: err=%v", err)
			if err != nil {
				panic(err)
			}
		}()

		doneWorkers, err := waits(nil)
		t.NoError(err)
		t.Equal(workers, doneWorkers)

		var called int
		t.NoError(traverse(func(worker string, f traverseStateMergersFunc) error {
			return f(func(stateKey string, merger StateMerger) (bool, error) {
				called++

				return true, nil
			})
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
		values := make([][]StateMergeValue, 3)
		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs

			traverseSlice(vs, func(_ int, value StateMergeValue) (bool, error) {
				subs[value.Key()] = getStateOwners(value.Key())

				return true, nil
			})

			expectedValues += len(vs)
		}

		ctx, _ := t.Ctx()

		// ctx = setContextValueLogger(ctx, testLogger)

		insert, waits, traverse, err := t.workerMergers(ctx, workers, share, nil, values, subs)
		t.NoError(err)

		go func() {
			err := insert()

			t.println("insert workers: err=%v", err)
			if err != nil {
				panic(err)
			}
		}()

		doneWorkers, err := waits(nil)
		t.NoError(err)
		t.Equal(workers, doneWorkers)

		collectedStates := map[string]int64{}

		t.NoError(traverse(func(worker string, f traverseStateMergersFunc) error {
			if slices.Index(doneWorkers, worker) < 0 {
				return nil
			}

			return f(func(stateKey string, merger StateMerger) (bool, error) {
				stv, err := merger.Close()
				if err != nil {
					return false, errors.WithMessage(err, worker)
				}

				var problem string

				if stv == nil {
					collectedStates[stateKey] = 0
				} else {
					v := stv.(*AccountStateValue)
					switch b, found := collectedStates[stateKey]; {
					case found:
						if v.Balance != b {
							problem = fmt.Sprintf("balance mismatch: key=%q prev=%d cur=%d", stateKey, b, v.Balance)
						}
					default:
						collectedStates[stateKey] = v.Balance
					}
				}

				t.println("worker states: worker=%q state=%q value=%v problem=%q",
					worker, stateKey, t.printStateMergeValue(stv), problem)

				if problem != "" {
					return false, errors.Errorf("%s: worker=%q", problem, worker)
				}

				return true, nil
			})
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
		values := make([][]StateMergeValue, 3)
		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs

			traverseSlice(vs, func(_ int, value StateMergeValue) (bool, error) {
				subs[value.Key()] = getStateOwners(value.Key())

				return true, nil
			})

			expectedValues += len(vs)
		}

		ctx, _ := t.Ctx()

		// ctx = setContextValueLogger(ctx, testLogger)

		insert, waits, traverse, err := t.workerMergers(ctx, workers, share, nil, values, subs)
		t.NoError(err)

		go func() {
			err := insert()

			t.println("insert workers: err=%v", err)
			if err != nil {
				panic(err)
			}
		}()

		doneWorkers, err := waits(nil)
		t.True(len(doneWorkers) >= 2)

		collectedStates := map[string]int64{}

		t.NoError(traverse(func(worker string, f traverseStateMergersFunc) error {
			if slices.Index(doneWorkers, worker) < 0 {
				return nil
			}

			return f(func(stateKey string, merger StateMerger) (bool, error) {
				stv, err := merger.Close()
				if err != nil {
					return false, errors.WithMessage(err, worker)
				}

				var problem string

				v := stv.(*AccountStateValue)
				switch b, found := collectedStates[stateKey]; {
				case found:
					if v.Balance != b {
						problem = "balance mismatch"
					}
				default:
					collectedStates[stateKey] = b
				}

				t.println("worker states: worker=%q state=%q value=%v problem=%q",
					worker, stateKey, t.printStateMergeValue(stv), problem)

				return true, nil
			})
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

	values := [][]StateMergeValue{
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
		addUnmerged, doneUnmerged, _ := appendSlice[uint64](uint64(len(values)), 0)

		doneWorkers, err := waits(func(worker string) (workerMergerLocalPhaseMergerDoneFunc, workerMergerCallback) {
			return nil,
				func(reasons map[uint64]string) error {
					unmerged := slices.Collect(maps.Keys(reasons))

					if len(unmerged) > 0 {
						addUnmerged(unmerged[0], unmerged[1:]...)
					}

					return nil
				}
		})
		t.NoError(err)

		doneUnmerged(func(s []uint64) error {
			slices.Sort(s)
			s = slices.Compact(s)

			t.Equal(expectedUnmerged, s)

			return nil
		})

		return doneWorkers
	}

	checkMerged := func(doneWorkers []string, traverse testWorkerMergerTraverseMergersFunc) {
		collectedStates := map[string]int64{}

		t.NoError(traverse(func(worker string, f traverseStateMergersFunc) error {
			if slices.Index(doneWorkers, worker) < 0 {
				return nil
			}

			return f(func(stateKey string, merger StateMerger) (bool, error) {
				stv, err := merger.Close()
				if err != nil {
					return false, errors.WithMessage(err, worker)
				}

				var problem string

				if stv == nil {
					collectedStates[stateKey] = 0
				} else {
					v := stv.(*AccountStateValue)
					switch b, found := collectedStates[stateKey]; {
					case found:
						if v.Balance != b {
							problem = fmt.Sprintf("balance mismatch: key=%q prev=%d cur=%d", stateKey, b, v.Balance)
						}
					default:
						collectedStates[stateKey] = v.Balance
					}
				}

				t.println("worker states: worker=%q state=%q value=%v nil=%v problem=%q",
					worker, stateKey, t.printStateMergeValue(stv), stv == nil, problem)

				if problem != "" {
					return false, errors.Errorf("%s: worker=%q", problem, worker)
				}

				return true, nil
			})
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

				traverseSlice(vs, func(_ int, value StateMergeValue) (bool, error) {
					subs[value.Key()] = getStateOwners(value.Key())

					return true, nil
				})
			}

			t.T().Log("sub state owners:", subs)

			ctx, _ := t.Ctx()

			// ctx = setContextValueLogger(ctx, testLogger)

			insert, waits, traverse, err := t.workerMergers(ctx, workers, share, states, values, subs)
			t.NoError(err)

			go func() {
				err := insert()

				t.println("insert workers: err=%v", err)
				if err != nil {
					panic(err)
				}
			}()

			doneWorkers := checkWaits(waits)
			checkMerged(doneWorkers, traverse)
		}
	}

	t.Run("repeat", func() {
		maxWorkers := len(states)
		for workers := 1; workers <= maxWorkers; workers++ {
			for share := uint64(1); share <= uint64(maxWorkers); share++ {
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

		_, _, getState, getStateMergers, _ := t.newFuncs()

		values := make([][]StateMergeValue, 33)
		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs
		}

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		{
			insert, insertDone, err := insertf(local)
			t.NoError(err)

			for i := range values {
				if err := insert(uint64(i), values[i]); err != nil {
					panic(err)
				}
			}

			_ = insertDone(nil)

			_, insertDone, err = insertf(managerName)
			t.NoError(err)

			_ = insertDone(nil)
		}

		t.NoError(wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return []string{local}, nil
			},
			func(reasons map[uint64]string) error {
				t.T().Logf("wait: reasons=%v", reasons)

				t.Empty(reasons)

				return nil
			},
		))
	})

	t.Run("manager before 1 worker", func() {
		ctx, _ := t.Ctx()

		opsWorkers := []string{local}

		_, _, getState, getStateMergers, _ := t.newFuncs()

		values := make([][]StateMergeValue, 33)
		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs
		}

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9,
			1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		{
			_, insertDone, err := insertf(managerName)
			t.NoError(err)

			_ = insertDone(nil)

			insert, insertDone, err := insertf(local)
			t.NoError(err)

			for i := range values {
				if err := insert(uint64(i), values[i]); err != nil {
					panic(err)
				}
			}

			_ = insertDone(nil)
		}

		t.NoError(wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return []string{local}, nil
			},
			func(reasons map[uint64]string) error {
				t.T().Logf("wait: reasons=%v", reasons)

				t.Empty(reasons)

				return nil
			},
		))
	})

	t.Run("empty values; 2 worker and then manager", func() {
		ctx, _ := t.Ctx()

		opsWorkers := []string{local, "w0"}

		_, _, getState, getStateMergers, _ := t.newFuncs()

		insertf, _, wait, err := newFuncWorkerMerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9, 1<<9, 1<<9,
			opsWorkers, 1, 1,
		)
		t.NoError(err)

		_, insertDone, err := insertf(local)
		t.NoError(err)
		t.NoError(insertDone(nil))

		_, insertDone, err = insertf(managerName)
		t.NoError(err)
		t.NoError(insertDone(nil))

		t.NoError(wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return nil, nil
			},
			func(reasons map[uint64]string) error {
				t.T().Logf("wait: reasons=%v", reasons)

				t.Empty(reasons)

				return nil
			},
		))
	})

	t.Run("over workers and manager before unmerge", func() {
		ctx, _ := t.Ctx()

		opsWorkers := t.workers(4)
		local = opsWorkers[0]
		valuesPerWorker := 3

		_, _, getState, getStateMergers, _ := t.newFuncs()

		workerValues := map[string][]uint64{}
		values := make([][]StateMergeValue, len(opsWorkers)*valuesPerWorker)

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
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9, 1<<9, 1<<9,
			overWorkers, 1, 1,
		)
		t.NoError(err)

		go func() {
			traverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
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

				t.println("insert worker: worker=%q ops=%v err=%v", worker, ops, err)

				return true, nil
			})

			_, insertDone, err := insertf(managerName)
			if err != nil {
				panic(err)
			}

			if err := insertDone(nil); err != nil {
				panic(err)
			}

			traverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
				if worker == local {
					return true, nil
				}

				ops := workerValues[worker]

				err := unmerge(worker, ops)
				t.println("unmerge worker: worker=%q ops=%v err=%v", worker, ops, err)

				return true, nil
			})
		}()

		t.NoError(wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return opsWorkers, nil
			},
			func(reasons map[uint64]string) error {
				t.T().Logf("wait: reasons=%v", reasons)

				t.Equal(expectedReasons, reasons)

				return nil
			},
		))
	})

	t.Run("over workers and manager", func() {
		ctx, _ := t.Ctx()

		opsWorkers := t.workers(4)
		local = opsWorkers[0]
		valuesPerWorker := 3

		_, _, getState, getStateMergers, _ := t.newFuncs()

		workerValues := map[string][]uint64{}
		values := make([][]StateMergeValue, len(opsWorkers)*valuesPerWorker)

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
			newFuncNewJobWorkerFunc(1<<9),
			getState,
			getStateMergers,
			1<<9, 1<<9, 1<<9,
			overWorkers, 1, 1,
		)
		t.NoError(err)

		go func() {
			traverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
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

				t.println("insert worker: worker=%q ops=%v err=%v", worker, ops, err)

				return true, nil
			})

			traverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
				if worker == local {
					return true, nil
				}

				ops := workerValues[worker]

				err := unmerge(worker, ops)
				t.println("unmerge worker: worker=%q ops=%v err=%v", worker, ops, err)

				return true, nil
			})

			_, insertDone, err := insertf(managerName)
			if err != nil {
				panic(err)
			}

			if err := insertDone(nil); err != nil {
				panic(err)
			}
		}()

		t.NoError(wait(
			func(unmerged []uint64) ([]string, error) {
				t.T().Logf("local phase done: unmerged=%v", unmerged)

				t.Empty(unmerged)

				return opsWorkers, nil
			},
			func(reasons map[uint64]string) error {
				t.T().Logf("wait: reasons=%v", reasons)

				t.Equal(expectedReasons, reasons)

				return nil
			},
		))
	})
}

func TestWorkerMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerMerger))
}
