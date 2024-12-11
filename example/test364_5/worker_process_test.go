package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorkerProcess struct {
	suite.Suite
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
	workerProcessWaitFunc,
	util.LockedMap[uint64, Operation],
	util.LockedMap[string, base.State],
) {
	initial, insert, unmerged, wait, err := newFuncWorkerProcess(
		ctx,
		local,
		newFuncNewJobWorkerFunc(1<<9),
		getState,
		newState,
		1<<9,
		util.UUID().String(),
		workers, opsWorkers, opsShare, statesShare,
		1<<9,
	)
	t.NoError(err)

	defaultGetStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, opsShare)
	t.NoError(err)

	defaultDistribute := newFuncDistributeStateMergeValues(
		func(workID, worker string) (inErrorFunc[[]StateMergeValue], in2ErrorFunc[[]string, []string], error) {
			return func(values []StateMergeValue) error {
					t.println("worker distribute: worker=%q values=%d", worker, len(values))

					return nil
				},
				func(subws []string, affected []string) error {
					t.println("worker distribute: worker=%q subs=%v affected=%v", worker, subws, affected)

					return nil
				},
				nil
		},
	)

	savedOps := util.NewSingleLockedMap[uint64, Operation]()
	savedStates := util.NewSingleLockedMap[string, base.State]()

	return func(
			getStateOwners getStateOwnersFunc,
			distribute workerDistributeStateMergeValuesFunc,
			getOperation getOperationFunc,
			insertOp inErrorFunc[opindexHashFunc],
			saveOperation in2ErrorFunc[uint64, Operation],
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
				func(opindex uint64, op Operation) error {
					if saveOperation != nil {
						return saveOperation(opindex, op)
					}

					return nil
				},
			)
		},
		insert,
		unmerged,
		func(
			a in2Func[uint64, Operation, bool],
			b in3ErrorFunc[context.Context, string, []uint64],
			c in2ErrorFunc[context.Context, []string],
			d workerProcessReportStates,
		) error {
			defer func() {
				stateOwnersClose()
			}()

			return wait(
				func(opindex uint64, op Operation) bool {
					savedOps.SetValue(opindex, op)

					if a != nil {
						return a(opindex, op)
					}

					return true
				},
				func(ctx context.Context, worker string, unmerged []uint64) error {
					t.println("wait: broadcast unmerged: worker=%q unmerged=%v", worker, unmerged)

					if b != nil {
						return b(ctx, worker, unmerged)
					}

					return nil
				},
				func(ctx context.Context, owners []string) error {
					t.println("wait: report state owners: owners=%v", owners)

					if c != nil {
						return c(ctx, owners)
					}

					return nil
				},
				func(
					ctx context.Context,
					unmerged []uint64,
					reasons map[uint64]string,
					states inErrorFunc[inErrorFunc[base.State]],
				) error {
					t.println("wait: report result: unmerged=%v reasons=%v", unmerged, reasons)

					var called bool

					if d != nil {
						if err := d(ctx, unmerged, reasons,
							func(f inErrorFunc[base.State]) error {
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
		},
		savedOps,
		savedStates
}

func (t *testWorkerProcess) defaultFuncInsertOp(ops []Operation) inErrorFunc[opindexHashFunc] {
	return func(f opindexHashFunc) error {
		return traverseSlice(ops, func(i int, op Operation) (bool, error) {
			return true, f(uint64(i), op.Hash(), op.Fact().Hash())
		})
	}
}

func (t *testWorkerProcess) TestCancel() {
	local := "w0"

	newf := func(ctx context.Context, local string) (
		errorFunc,
		errorFunc,
		errorFunc,
		errorFunc,
		error,
	) {
		workers := t.workers(3)

		insertOps, insertValues, unmerged, wait, err := newFuncWorkerProcess(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			func(string) (base.State, bool, error) { return nil, false, nil },
			func(string, base.StateValue, []util.Hash) (base.State, error) { return nil, nil },
			1<<9,
			util.UUID().String(),
			workers, workers, 1, 1, 1<<9,
		)

		return func() error {
				return insertOps(
					func(string) []string { return workers },
					func(string, uint64) (
						workerStateMergeValuesFunc,
						workerStateOwnersSetFunc,
						func(),
						error,
					) {
						return func(string, []StateMergeValue) error { return nil },
							func(string, []string, []string) error { return nil },
							func() {},
							nil
					},
					func(context.Context, uint64, util.Hash, util.Hash) (Operation, bool, error) { return nil, false, nil },
					func(opindexHashFunc) error { return nil },
					func(uint64, Operation) error { return nil },
				)
			},
			func() error {
				return traverseSlice(workers, func(_ int, worker string) (bool, error) {
					if worker == local {
						return true, nil
					}

					return true, insertValues(
						worker,
						func(
							insert workerProcessInsertStateMergeValuesFunc,
							owners in2ErrorFunc[[]string, []string],
						) error {
							if err := insert(0, valuehash.RandomSHA256(), t.randomMergeValues(1, 2, 3)); err != nil {
								return err
							}

							return owners(workers, nil)
						},
					)
				})
			},
			func() error {
				return traverseSlice(workers, func(_ int, worker string) (bool, error) {
					if worker == local {
						return true, nil
					}

					return true, unmerged(worker, []uint64{0})
				})
			},
			func() error {
				return wait(nil, nil, nil, nil)
			},
			err
	}

	t.Run("before insert ops; local not in ops workers", func() {
		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		_, _, _, _, err := newf(ctx, "local")
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before insert ops", func() {
		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		insert, _, _, _, err := newf(ctx, local)
		t.NoError(err)

		err = insert()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before insert values", func() {
		ctx, cancel := context.WithCancel(context.Background())

		insertOps, insertValues, _, _, err := newf(ctx, local)
		t.NoError(err)

		t.NoError(insertOps())

		cancel()

		err = insertValues()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before unmerged", func() {
		ctx, cancel := context.WithCancel(context.Background())

		insertOps, insertValues, unmerged, _, err := newf(ctx, local)
		t.NoError(err)

		t.NoError(insertOps())
		t.NoError(insertValues())

		cancel()

		err = unmerged()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before wait", func() {
		ctx, cancel := context.WithCancel(context.Background())

		insertOps, insertValues, unmerged, wait, err := newf(ctx, local)
		t.NoError(err)

		t.NoError(insertOps())
		t.NoError(insertValues())
		t.NoError(unmerged())

		cancel()

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

func (t *testWorkerProcess) TestInitialInsert() {
	t.Run("local only; inside ops workers", func() {
		local := "local"
		workers := []string{local}

		ops := make([]Operation, 33)
		for i := range ops {
			ops[i] = t.randomAddOp(1)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		initial, _, _, wait, savedOps, savedStates := t.newf(
			ctx,
			local,
			func(string) (base.State, bool, error) { return nil, false, nil },
			func(stateKey string, v base.StateValue, ops []util.Hash) (base.State, error) {
				return base.NewBaseState(
					3,
					stateKey,
					v,
					valuehash.NewBytes(v.HashBytes()),
					ops,
				), nil
			},
			workers, workers, 1, 1,
		)

		t.NoError(initial(nil, nil,
			t.newFuncGetOperation(ops),
			t.defaultFuncInsertOp(ops),
			nil,
		))

		var calledBroadcastUnmerged bool
		var reportedStateOwners atomic.Value
		var reportedStates atomic.Value

		t.NoError(wait(
			nil,
			func(_ context.Context, worker string, unmerged []uint64) error {
				calledBroadcastUnmerged = true

				return nil
			},
			func(_ context.Context, owners []string) error {
				reportedStateOwners.Store(slices.Clone(owners))

				return nil
			},
			func(
				_ context.Context,
				unmerged []uint64,
				reasons map[uint64]string,
				states inErrorFunc[inErrorFunc[base.State]],
			) error {
				reportedStates.Store([2]interface{}{
					slices.Clone(unmerged),
					maps.Clone(reasons),
				})

				return nil
			},
		))

		t.False(calledBroadcastUnmerged)

		t.Equal(len(ops), savedOps.Len())
		t.Empty(reportedStateOwners.Load().([]string))

		{
			reported := reportedStates.Load().([2]interface{})
			t.Empty(reported[0].([]uint64))
			t.Empty(reported[1].(map[uint64]string))
		}

		t.Equal(len(ops), savedStates.Len())

		traverseSlice(ops, func(i int, op Operation) (bool, error) {
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

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		initial, insert, _, wait, savedOps, savedStates := t.newf(
			ctx,
			local,
			func(string) (base.State, bool, error) { return nil, false, nil },
			func(stateKey string, v base.StateValue, ops []util.Hash) (base.State, error) {
				return base.NewBaseState(
					3,
					stateKey,
					v,
					valuehash.NewBytes(v.HashBytes()),
					ops,
				), nil
			},
			workers, workers, 1, 1,
		)

		t.NoError(initial(nil, nil, nil, nil, nil))

		var calledBroadcastUnmerged bool
		var reportedStateOwners atomic.Value
		var reportedStates atomic.Value

		traverseSlice(workers, func(_ int, worker string) (bool, error) {
			return true, insert(
				worker,
				func(
					f workerProcessInsertStateMergeValuesFunc,
					g in2ErrorFunc[[]string, []string],
				) error {
					if err := traverseSlice(ops, func(i int, op Operation) (bool, error) {
						subv, addvs, reason, err := op.StateMergeValues(ctx)
						if err != nil {
							panic(err)
						}

						if reason != "" {
							panic(fmt.Sprintf("not empty reason, %q", reason))
						}

						var values []StateMergeValue

						if subv != nil {
							values = append(values, subv)
						}

						values = append(values, addvs...)

						return true, f(uint64(i), op.Fact().Hash(), values)
					}); err != nil {
						return err
					}

					return g(workers, workers)
				},
			)
		})

		t.NoError(wait(
			nil,
			func(_ context.Context, worker string, unmerged []uint64) error {
				calledBroadcastUnmerged = true

				return nil
			},
			func(_ context.Context, owners []string) error {
				reportedStateOwners.Store(slices.Clone(owners))

				return nil
			},
			func(
				_ context.Context,
				unmerged []uint64,
				reasons map[uint64]string,
				states inErrorFunc[inErrorFunc[base.State]],
			) error {
				reportedStates.Store([2]interface{}{
					slices.Clone(unmerged),
					maps.Clone(reasons),
				})

				return nil
			},
		))

		t.False(calledBroadcastUnmerged)

		t.Equal(0, savedOps.Len())
		t.Equal(workers, reportedStateOwners.Load().([]string))

		{
			reported := reportedStates.Load().([2]interface{})
			t.Empty(reported[0].([]uint64))
			t.Empty(reported[1].(map[uint64]string))
		}

		t.Equal(len(ops), savedStates.Len())
	})
}

func TestWorkerProcess(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerProcess))
}
