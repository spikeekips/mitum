package main

import (
	"context"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorkerProcess struct {
	dutil.BaseSuite
	baseTestMergeOp
	baseTestWorkers
}

func (t *testWorkerProcess) newf(
	ctx context.Context,
	local string,
	getState getStateFunc,
	newState newStateFunc,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
) (
	workerProcessInitialInsertFunc,
	workerProcessWorkerInsertStateMergeValuesFunc,
	workerUnmergedFunc,
	func(
		dutil.In3ErrorFunc[context.Context, string /* worker */, []uint64 /* unmerged */],
		workerProcessReportResult,
	) error,
	util.LockedMap[uint64, Operation],
	util.LockedMap[string, base.State],
) {
	initial, insert, unmerged, wait, err := newFuncWorkerProcess(
		ctx,
		local,
		dutil.NewFuncNewJobWorkerFunc(1<<9),
		getState,
		newState,
		1<<9,
		workers, opsWorkers, opsShare, statesShare,
		1<<9,
	)
	t.NoError(err)

	defaultGetStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, opsShare)
	t.NoError(err)

	defaultDistribute := newFuncDistributeStateMergeValues(
		func(_ context.Context, worker string) (workerProcessInsertStateMergeValuesFunc, dutil.In2ErrorFunc[[]string, []string], error) {
			return func(opindex uint64, fact util.Hash, values []StateMergeValue) error {
					t.Println("worker distribute: local=%q worker=%q opindex=%d fact=%q values=%d", local, worker, opindex, fact, len(values))

					return nil
				},
				func(subws []string, affected []string) error {
					t.Println("worker distribute: local=%q worker=%q subs=%v affected=%v", local, worker, subws, affected)

					return nil
				},
				nil
		},
	)

	savedOps := util.NewSingleLockedMap[uint64, Operation]()
	savedStates := util.NewSingleLockedMap[string, base.State]()

	return func(
			getStateOwners getStateOwnersFunc,
			distribute workerProcessDistributeStateMergeValuesFunc,
			getOperation getOperationFunc,
			insertOp dutil.InErrorFunc[opindexOpHashesFunc],
			reportDone dutil.In2ErrorFunc[[]string, map[uint64]string],
		) error {
			if getStateOwners == nil {
				getStateOwners = defaultGetStateOwners
			}

			if distribute == nil {
				distribute = defaultDistribute
			}

			return initial(
				getStateOwners,
				distribute,
				getOperation,
				insertOp,
				reportDone,
			)
		},
		insert,
		unmerged,
		func(
			b dutil.In3ErrorFunc[context.Context, string, []uint64],
			d workerProcessReportResult,
		) error {
			defer func() {
				stateOwnersClose()
			}()

			traverseOps, err := wait(
				func(ctx context.Context, worker string, unmerged []uint64) error {
					t.Println("wait: broadcast unmerged: worker=%q unmerged=%v", worker, unmerged)

					if b != nil {
						return b(ctx, worker, unmerged)
					}

					return nil
				},
				func(
					ctx context.Context,
					reasons map[uint64]string,
					statesSize uint64,
					states dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
				) error {
					t.Println("wait: report result: reasons=%v states=%d", reasons, statesSize)

					var called bool

					if d != nil {
						if err := d(ctx, reasons, statesSize,
							func(f dutil.InErrorFunc[base.State]) error {
								called = true

								return states(func(st base.State) error {
									savedStates.SetValue(st.Key(), st)

									return f(st)
								})
							},
						); err != nil {
							return err
						}
					}

					if !called {
						return states(func(st base.State) error {
							savedStates.SetValue(st.Key(), st)

							return nil
						})
					}

					return nil
				},
			)
			if err != nil {
				return err
			}

			return traverseOps(func(opindex uint64, op Operation) (bool, error) {
				_ = savedOps.SetValue(opindex, op)

				return true, nil
			})
		},
		savedOps,
		savedStates
}

func (t *testWorkerProcess) defaultFuncInsertOp(ops []Operation) dutil.InErrorFunc[opindexOpHashesFunc] {
	return func(f opindexOpHashesFunc) error {
		return dutil.TraverseSlice(ops, func(i int, op Operation) (bool, error) {
			return true, f(uint64(i), op.Hash(), op.Fact().Hash())
		})
	}
}

func (t *testWorkerProcess) newStateFunc() newStateFunc {
	return func(stateKey string, v base.StateValue, ops []util.Hash) (base.State, error) {
		return base.NewBaseState(
			3,
			stateKey,
			v,
			valuehash.NewBytes(v.HashBytes()),
			ops,
		), nil
	}
}

func (t *testWorkerProcess) TestCancel() {
	local := "w0"

	newf := func(ctx context.Context, local string) (
		dutil.ErrorFunc,
		dutil.ErrorFunc,
		dutil.ErrorFunc,
		dutil.ErrorFunc,
		error,
	) {
		workers := t.workers(3)

		insertOps, insertValues, unmerged, wait, err := newFuncWorkerProcess(
			ctx,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			func(string) (base.State, bool, error) { return nil, false, nil },
			func(string, base.StateValue, []util.Hash) (base.State, error) { return nil, nil },
			1<<9,
			workers, workers, 1, 1, 1<<9,
		)

		return func() error {
				return insertOps(
					func(string) []string { return workers },
					func(context.Context, string, []string) (
						workerDistributeWorkerStateMergeValuesFunc,
						workerStateOwnersSetFunc,
						dutil.ErrorFunc,
						error,
					) {
						return func(string, uint64, util.Hash, []StateMergeValue) error { return nil },
							func(string, []string, []string) error { return nil },
							func() error { return nil },
							nil
					},
					func(context.Context, uint64, util.Hash, util.Hash) (Operation, bool, error) { return nil, false, nil },
					func(opindexOpHashesFunc) error { return nil },
					func([]string, map[uint64]string) error { return nil },
				)
			},
			func() error {
				return dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
					if worker == local {
						return true, nil
					}

					insert, done, err := insertValues(worker)
					if err != nil {
						return false, err
					}

					if err := insert(0, valuehash.RandomSHA256(), t.randomMergeValues(1, 2, 3)); err != nil {
						return false, err
					}

					return true, done(workers, nil)
				})
			},
			func() error {
				return dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
					if worker == local {
						return true, nil
					}

					return true, unmerged(worker, []uint64{0})
				})
			},
			func() error {
				_, err := wait(nil, nil)

				return err
			},
			err
	}

	t.Run("before insert ops; local not in ops workers", func() {
		ctx, cancel := t.Ctx()

		cancel(nil)

		_, _, _, _, err := newf(ctx, "local")
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before insert ops", func() {
		ctx, cancel := t.Ctx()

		cancel(nil)

		insert, _, _, _, err := newf(ctx, local)
		t.NoError(err)

		err = insert()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before insert values", func() {
		ctx, cancel := t.Ctx()

		insertOps, insertValues, _, _, err := newf(ctx, local)
		t.NoError(err)

		t.NoError(insertOps())

		cancel(nil)

		err = insertValues()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before unmerged", func() {
		ctx, cancel := t.Ctx()

		insertOps, insertValues, unmerged, _, err := newf(ctx, local)
		t.NoError(err)

		t.NoError(insertOps())
		t.NoError(insertValues())

		cancel(nil)

		err = unmerged()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before wait", func() {
		ctx, cancel := t.Ctx()

		insertOps, insertValues, unmerged, wait, err := newf(ctx, local)
		t.NoError(err)

		t.NoError(insertOps())
		t.NoError(insertValues())
		t.NoError(unmerged())

		cancel(nil)

		err = wait()
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testWorkerProcess) newFuncGetOperation(ops []Operation) getOperationFunc {
	return func(_ context.Context, opindex uint64, _, _ util.Hash) (Operation, bool, error) {
		if opindex >= uint64(len(ops)) {
			return nil, false, nil
		}

		return ops[opindex], true, nil
	}
}

func (t *testWorkerProcess) TestLocal() {
	t.Run("local only; inside ops workers", func() {
		local := "local"
		workers := []string{local}

		ops := make([]Operation, 33)
		for i := range ops {
			ops[i] = t.randomAddOp(1)
		}

		ctx, _ := t.Ctx()

		initial, _, _, wait, savedOps, savedStates := t.newf(
			ctx,
			local,
			func(string) (base.State, bool, error) { return nil, false, nil },
			t.newStateFunc(),
			workers, workers, 1, 1,
		)

		t.NoError(initial(nil, nil,
			t.newFuncGetOperation(ops),
			t.defaultFuncInsertOp(ops),
			func(owners []string, reasons map[uint64]string) error {
				t.Equal(workers, owners)

				return nil
			},
		))

		var calledBroadcastUnmerged bool
		var reportedStates atomic.Value

		t.NoError(wait(
			func(_ context.Context, worker string, unmerged []uint64) error {
				calledBroadcastUnmerged = true

				return nil
			},
			func(
				_ context.Context,
				reasons map[uint64]string,
				statesSize uint64,
				states dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
			) error {
				reportedStates.Store(reasons)

				t.Equal(uint64(len(ops)), statesSize)

				return nil
			},
		))

		t.False(calledBroadcastUnmerged)

		t.Equal(len(ops), savedOps.Len())

		{
			reported := reportedStates.Load().(map[uint64]string)
			t.Empty(reported)
		}

		t.Equal(len(ops), savedStates.Len())

		dutil.TraverseSlice(ops, func(i int, op Operation) (bool, error) {
			s, found := savedOps.Value(uint64(i))
			t.True(found)

			base.EqualOperation(t.Assert(), op, s)

			return true, nil
		})
	})

	t.Run("local only; outside ops workers", func() {
		local := "local"
		workers := t.workers(1)

		ops := make([]Operation, 33)
		for i := range ops {
			ops[i] = t.randomAddOp(1)
		}

		ctx, _ := t.Ctx()

		_, insertf, unmerge, wait, savedOps, savedStates := t.newf(
			ctx,
			local,
			func(string) (base.State, bool, error) { return nil, false, nil },
			t.newStateFunc(),
			workers, workers, 1, 1,
		)

		var calledBroadcastUnmerged bool
		var reportedStates atomic.Value

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			insert, done, err := insertf(worker)
			if err != nil {
				return false, err
			}

			if err := dutil.TraverseSlice(ops, func(i int, op Operation) (bool, error) {
				subv, addvs, reason, err := op.StateMergeValues(ctx)
				t.IfErrPanic(err)
				t.IfTruePanic(reason != "", "not empty reason, %q", reason)

				var values []StateMergeValue

				if subv != nil {
					values = append(values, subv)
				}

				values = append(values, addvs...)

				return true, insert(uint64(i), op.Fact().Hash(), values)
			}); err != nil {
				return false, err
			}

			return true, done(workers, workers)
		})

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			return true, unmerge(worker, nil)
		})

		t.NoError(wait(
			func(_ context.Context, worker string, unmerged []uint64) error {
				calledBroadcastUnmerged = true

				return nil
			},
			func(
				_ context.Context,
				reasons map[uint64]string,
				statesSize uint64,
				states dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
			) error {
				reportedStates.Store(reasons)

				t.Equal(uint64(len(ops)), statesSize)

				return nil
			},
		))

		t.False(calledBroadcastUnmerged)

		t.Equal(0, savedOps.Len())

		{
			reported := reportedStates.Load().(map[uint64]string)
			t.Empty(reported)
		}

		t.Equal(len(ops), savedStates.Len())
	})

	t.Run("report reasons", func() {
		local := "local"
		workers := []string{local}

		ops := make([]Operation, 33)
		for i := range ops {
			ops[i] = t.randomAddOp(1)
		}

		ctx, _ := t.Ctx()

		initial, _, _, wait, savedOps, savedStates := t.newf(
			ctx,
			local,
			func(string) (base.State, bool, error) { return nil, false, nil },
			t.newStateFunc(),
			workers, workers, 1, 1,
		)

		expectedReasonsAfterExtracted := map[uint64]string{}
		dutil.TraverseSlice(make([]int, 3), func(i int, _ int) (bool, error) {
			expectedReasonsAfterExtracted[uint64(i+len(ops))] = reasonOperationNotFound

			return true, nil
		})
		expectedUnmergedAfterExtracted := slices.Collect(maps.Keys(expectedReasonsAfterExtracted))
		slices.Sort(expectedUnmergedAfterExtracted)

		t.NoError(initial(nil, nil,
			t.newFuncGetOperation(ops),
			func(f opindexOpHashesFunc) error {
				if err := dutil.TraverseSlice(ops, func(i int, op Operation) (bool, error) {
					return true, f(uint64(i), op.Hash(), op.Fact().Hash())
				}); err != nil {
					return err
				}

				return dutil.TraverseMap(expectedReasonsAfterExtracted, func(opindex uint64, _ string) (bool, error) {
					return true, f(opindex, valuehash.RandomSHA256(), valuehash.RandomSHA256())
				})
			},
			func(owners []string, reasons map[uint64]string) error {
				t.Equal([]string{local}, owners)

				return nil
			},
		))

		var calledBroadcastUnmerged bool
		var reportedStates atomic.Value

		t.NoError(wait(
			func(_ context.Context, worker string, unmerged []uint64) error {
				calledBroadcastUnmerged = true

				return nil
			},
			func(
				_ context.Context,
				reasons map[uint64]string,
				statesSize uint64,
				states dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
			) error {
				reportedStates.Store(reasons)

				t.Equal(uint64(len(ops)), statesSize)

				return nil
			},
		))

		t.False(calledBroadcastUnmerged)
		t.Equal(len(ops), savedOps.Len())

		{
			reported := reportedStates.Load().(map[uint64]string)
			t.Equal(expectedReasonsAfterExtracted, reported)
		}

		t.Equal(len(ops), savedStates.Len())

		dutil.TraverseSlice(ops, func(i int, op Operation) (bool, error) {
			s, found := savedOps.Value(uint64(i))
			t.True(found)

			base.EqualOperation(t.Assert(), op, s)

			return true, nil
		})
	})

	t.Run("local inside ops workers; empty state merge values", func() {
		local := "local"
		other := "w0"
		workers := []string{local, other}

		ops := make([]Operation, 3)
		for i := range ops {
			ops[i] = t.randomAddOp(1)
		}

		ctx, _ := t.Ctx()

		initial, insertf, _, wait, savedOps, savedStates := t.newf(
			ctx,
			local,
			func(string) (base.State, bool, error) { return nil, false, nil },
			t.newStateFunc(),
			workers, workers, 1, 1,
		)

		t.NoError(initial(
			func(string) []string { return []string{other} },
			nil,
			t.newFuncGetOperation(ops),
			t.defaultFuncInsertOp(ops),
			func(owners []string, reasons map[uint64]string) error {
				t.Equal([]string{other}, owners)

				return nil
			},
		))

		go func() {
			_, insertDone, err := insertf(other)
			t.IfErrPanic(err)
			t.IfErrPanic(insertDone(nil, nil))
		}()

		var calledBroadcastUnmerged bool
		var reportedStates atomic.Value

		t.NoError(wait(
			func(_ context.Context, worker string, unmerged []uint64) error {
				calledBroadcastUnmerged = true

				return nil
			},
			func(
				_ context.Context,
				reasons map[uint64]string,
				statesSize uint64,
				states dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
			) error {
				reportedStates.Store(reasons)

				t.Equal(uint64(len(ops)), statesSize)

				return nil
			},
		))

		t.False(calledBroadcastUnmerged)
		t.Equal(len(ops), savedOps.Len())

		{
			reported := reportedStates.Load().(map[uint64]string)
			t.Empty(reported)
		}

		t.Equal(len(ops), savedStates.Len())

		dutil.TraverseSlice(ops, func(i int, op Operation) (bool, error) {
			s, found := savedOps.Value(uint64(i))
			t.True(found)

			base.EqualOperation(t.Assert(), op, s)

			return true, nil
		})
	})

	t.Run("local broadcasts insert done to outside ops workers", func() {
		local := "local"
		opsWorkers := []string{local}
		workers := []string{local, "w0"}

		ops := make([]Operation, 3)
		for i := range ops {
			ops[i] = t.randomAddOp(1)
		}

		ctx, _ := t.Ctx()

		initial, _, _, wait, savedOps, savedStates := t.newf(
			ctx,
			local,
			func(string) (base.State, bool, error) { return nil, false, nil },
			t.newStateFunc(),
			workers, opsWorkers, 1, 1,
		)

		var otherInserted, otherInsertedDone bool

		t.NoError(initial(
			func(string) []string { return []string{local} },
			newFuncDistributeStateMergeValues(
				func(_ context.Context, worker string) (workerProcessInsertStateMergeValuesFunc, dutil.In2ErrorFunc[[]string, []string], error) {
					return func(opindex uint64, fact util.Hash, values []StateMergeValue) error {
							otherInserted = true

							return nil
						},
						func(subws []string, affected []string) error {
							otherInsertedDone = true

							t.Println("worker distribute: local=%q worker=%q subs=%v affected=%v", local, worker, subws, affected)

							return nil
						},
						nil
				},
			),

			t.newFuncGetOperation(ops),
			t.defaultFuncInsertOp(ops),
			func(owners []string, reasons map[uint64]string) error {
				t.Equal(opsWorkers, owners)

				return nil
			},
		))

		var calledBroadcastUnmerged bool
		var reportedStates atomic.Value

		t.NoError(wait(
			func(_ context.Context, worker string, unmerged []uint64) error {
				calledBroadcastUnmerged = true

				return nil
			},
			func(
				_ context.Context,
				reasons map[uint64]string,
				statesSize uint64,
				states dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
			) error {
				reportedStates.Store(reasons)

				t.Equal(uint64(len(ops)), statesSize)

				return nil
			},
		))

		t.False(otherInserted)
		t.True(otherInsertedDone)

		t.False(calledBroadcastUnmerged)
		t.Equal(len(ops), savedOps.Len())

		{
			reported := reportedStates.Load().(map[uint64]string)
			t.Empty(reported)
		}

		t.Equal(len(ops), savedStates.Len())

		dutil.TraverseSlice(ops, func(i int, op Operation) (bool, error) {
			s, found := savedOps.Value(uint64(i))
			t.True(found)

			base.EqualOperation(t.Assert(), op, s)

			return true, nil
		})
	})
}

func (t *testWorkerProcess) TestWorkerManager() {
	t.Run("insert and then manager", func() {
		local := "local"
		workers := []string{local, "w0"}

		ctx, _ := t.Ctx()

		initial, insertf, _, wait, savedOps, savedStates := t.newf(
			ctx,
			local,
			func(string) (base.State, bool, error) { return nil, false, nil },
			t.newStateFunc(),
			workers, workers, 1, 1,
		)

		go func() {
			t.IfErrPanic(initial(nil, nil, nil, t.defaultFuncInsertOp(nil), nil))

			_, insertDone, err := insertf(managerName)
			t.IfErrPanic(err)
			t.IfErrPanic(insertDone(nil, nil))
		}()

		var calledBroadcastUnmerged bool

		t.NoError(wait(
			func(_ context.Context, worker string, unmerged []uint64) error {
				calledBroadcastUnmerged = true

				return nil
			},
			func(
				_ context.Context,
				reasons map[uint64]string,
				statesSize uint64,
				states dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
			) error {
				t.Empty(reasons)
				t.Equal(uint64(0), statesSize)

				return nil
			},
		))

		t.False(calledBroadcastUnmerged)
		t.Equal(0, savedOps.Len())
		t.Equal(0, savedStates.Len())
	})

	t.Run("manager and then insert", func() {
		local := "local"
		workers := []string{local, "w0"}

		ctx, _ := t.Ctx()

		initial, insertf, _, wait, savedOps, savedStates := t.newf(
			ctx,
			local,
			func(string) (base.State, bool, error) { return nil, false, nil },
			t.newStateFunc(),
			workers, workers, 1, 1,
		)

		go func() {
			_, insertDone, err := insertf(managerName)
			t.IfErrPanic(err)
			t.IfErrPanic(insertDone(nil, nil))

			switch err := initial(nil, nil, nil, t.defaultFuncInsertOp(nil), nil); {
			case errors.Is(err, context.Canceled):
			case err == nil:
				t.IfErrPanic(err)
			}
		}()

		var calledBroadcastUnmerged bool

		t.NoError(wait(
			func(_ context.Context, worker string, unmerged []uint64) error {
				calledBroadcastUnmerged = true

				return nil
			},
			func(
				_ context.Context,
				reasons map[uint64]string,
				statesSize uint64,
				states dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
			) error {
				t.Empty(reasons)
				t.Equal(uint64(0), statesSize)

				return nil
			},
		))

		t.False(calledBroadcastUnmerged)
		t.Equal(0, savedOps.Len())
		t.Equal(0, savedStates.Len())
	})
}

func (t *testWorkerProcess) newProcessWorker(
	ctx context.Context,
	local string,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
) (
	workerProcessInitialInsertFunc,
	workerProcessWorkerInsertStateMergeValuesFunc,
	workerUnmergedFunc,
	workerProcessWaitFunc,
) {
	if slices.Index(workers, local) < 0 {
		t.Fail("local not in workers")
	}

	initial, insert, unmerged, wait, err := newFuncWorkerProcess(
		ctx,
		local,
		dutil.NewFuncNewJobWorkerFunc(1<<9),
		func(string) (base.State, bool, error) { return nil, false, nil },
		t.newStateFunc(),
		1<<9,
		workers, opsWorkers, opsShare, statesShare,
		1<<9,
	)
	t.NoError(err)

	return initial, insert, unmerged, wait
}

func (t *testWorkerProcess) newProcessWorkers(
	ctx context.Context,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
	ops []Operation,
) {
	dutil.TraverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
		if slices.Index(workers, worker) < 0 {
			t.Fail("unknown op worker, %q", worker)
		}

		return true, nil
	})

	initials := map[string]workerProcessInitialInsertFunc{}
	workerInserts := map[string]workerProcessWorkerInsertStateMergeValuesFunc{}
	workerUnmerges := map[string]workerUnmergedFunc{}
	workerWaits := map[string]workerProcessWaitFunc{}

	dutil.TraverseSlice(workers, func(_ int, local string) (bool, error) {
		initials[local], workerInserts[local], workerUnmerges[local], workerWaits[local] = t.newProcessWorker(
			ctx,
			local, workers, opsWorkers,
			opsShare, statesShare,
		)

		return true, nil
	})

	getOperation := t.newFuncGetOperation(ops)
	getStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, opsShare)
	t.NoError(err)
	defer stateOwnersClose()

	distributeValues := func(local string) workerProcessDistributeStateMergeValuesFunc {
		return newFuncDistributeStateMergeValues(func(_ context.Context, worker string) (
			workerProcessInsertStateMergeValuesFunc,
			dutil.In2ErrorFunc[[]string, []string],
			error,
		) {
			return workerInserts[worker](local)
		})
	}

	go func() {
		ophs := make([][2]util.Hash, len(ops))
		dutil.TraverseSlice(ops, func(i int, op Operation) (bool, error) {
			ophs[i] = [2]util.Hash{op.Hash(), op.Fact().Hash()}

			return true, nil
		})

		_, opWorkers, err := opsNShareWorkers(opsWorkers, opsShare, ophs)
		t.IfErrPanic(err)

		var wg sync.WaitGroup

		dutil.TraverseSlice(opsWorkers, func(_ int, local string) (bool, error) {
			wg.Add(1)

			go func() {
				defer wg.Done()

				err := initials[local](
					getStateOwners,
					distributeValues(local),
					getOperation,
					func(f opindexOpHashesFunc) error {
						return opWorkers(local, f)
					},
					func(owners []string, reasons map[uint64]string) error {
						t.Println("local done: local=%q owners=%v reasons=%v", local, owners, reasons)

						return nil
					},
				)

				t.Println("initial: worker=%q err=%v", local, err)
			}()

			return true, nil
		})

		wg.Wait()
	}()

	var wg sync.WaitGroup

	dutil.TraverseSlice(workers, func(_ int, local string) (bool, error) {
		wg.Add(1)

		go func() {
			defer wg.Done()

			traverseOps, err := workerWaits[local](
				func(ctx context.Context, worker string, unmerged []uint64) error {
					t.Println("worker unmerge: local=%q worker=%q unmerged=%v", local, worker, unmerged)

					return workerUnmerges[worker](local, unmerged)
				},
				func(
					ctx context.Context,
					reasons map[uint64]string,
					statesSize uint64,
					f dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
				) error {
					t.Println("wait states: local=%q reasons=%v states=%d", local, reasons, statesSize)

					return f(func(st base.State) error {
						t.Println("\tstate: local=%q st=%q", local, st.Key())

						return nil
					})
				},
			)

			if err == nil {
				err = traverseOps(func(opindex uint64, op Operation) (bool, error) {
					switch {
					case opindex >= uint64(len(ops)):
						return false, errors.Errorf("unknown opindex=%d", opindex)
					case !op.Hash().Equal(ops[opindex].Hash()):
						return false, errors.Errorf("hash mismatch, %d", opindex)
					default:
						return true, nil
					}
				})
			}

			t.Println("wait done: worker=%q err=%v", local, err)
		}()

		return true, nil
	})

	wg.Wait()
}

func (t *testWorkerProcess) TestWorkers() {
	t.Run("local only", func() {
		ctx, _ := t.Ctx()

		workers := t.workers(1)
		ops := make([]Operation, 33)
		for i := range ops {
			ops[i] = t.randomAddOp(1)
		}

		t.newProcessWorkers(
			ctx,
			workers, workers,
			1, 1,
			ops,
		)
	})

	t.Run("2 workers", func() {
		ctx, _ := t.Ctx()

		workers := t.workers(2)
		ops := make([]Operation, 33)
		for i := range ops {
			ops[i] = t.randomAddOp(1)
		}

		t.newProcessWorkers(
			ctx,
			workers, workers,
			1, 1,
			ops,
		)
	})
}

func TestWorkerProcess(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerProcess))
}
