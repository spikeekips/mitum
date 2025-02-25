package dop5

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
	createState createStateFunc,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
) (
	workerProcessInitialInsertFunc,
	workerProcessWorkerInsertSMVsFunc,
	workerUnmergedFunc,
	func(
		dutil.In3ErrorFunc[context.Context, string, []uint64],
		workerProcessReportResult,
	) error,
	dutil.LockedMap[uint64, operation],
	dutil.LockedMap[string, base.State],
) {
	initial, insert, unmerged, wait, err := newFuncWorkerProcess(
		ctx,
		local,
		dutil.NewFuncNewJobWorkerFunc(1<<9),
		getState,
		createState,
		1<<9,
		workers, opsWorkers, opsShare, statesShare,
		1<<9,
	)
	t.NoError(err)

	defaultGetStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, opsShare)
	t.NoError(err)

	defaultDistribute := func(_ context.Context, worker string) (workerProcessInsertSMVsFunc, dutil.In2ErrorFunc[[]string, []string], error) {
		return func(opindex uint64, fact util.Hash, values []SMV) error {
				t.Print("worker distribute: local=%q worker=%q opindex=%d fact=%q values=%d", local, worker, opindex, fact, len(values))

				return nil
			},
			func(subws []string, affected []string) error {
				t.Print("worker distribute: local=%q worker=%q subs=%v affected=%v", local, worker, subws, affected)

				return nil
			},
			nil
	}

	savedOps := dutil.NewSingleLockedMap[uint64, operation]()
	savedStates := dutil.NewSingleLockedMap[string, base.State]()

	return func(
			getStateOwners getStateOwnersFunc,
			distribute newWorkerProcessDistributeSMVsFunc,
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
					t.Print("wait: broadcast unmerged: worker=%q unmerged=%v", worker, unmerged)

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
					t.Print("wait: report result: reasons=%v states=%d", reasons, statesSize)

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

			for opindex, op := range traverseOps {
				_ = savedOps.SetValue(opindex, op)
			}

			return nil
		},
		savedOps,
		savedStates
}

func (t *testWorkerProcess) defaultFuncInsertOp(ops []operation) dutil.InErrorFunc[opindexOpHashesFunc] {
	return func(f opindexOpHashesFunc) error {
		for i, op := range slices.All(ops) {
			if err := f(uint64(i), op.Hash(), op.Fact().Hash()); err != nil {
				return err
			}
		}

		return nil
	}
}

func (t *testWorkerProcess) createStateFunc() createStateFunc {
	return func(stateKey string, v base.StateValue, prev util.Hash, ops []util.Hash) (base.State, error) {
		return base.NewBaseState(
			3,
			stateKey,
			v,
			prev,
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
			func(string, base.StateValue, util.Hash, []util.Hash) (base.State, error) { return nil, nil },
			1<<9,
			workers, workers, 1, 1, 1<<9,
		)

		return func() error {
				return insertOps(
					func(string) []string { return workers },
					func(context.Context, string) (
						workerProcessInsertSMVsFunc,
						dutil.In2ErrorFunc[[]string, []string],
						error,
					) {
						return func(uint64, util.Hash, []SMV) error { return nil },
							func([]string, []string) error { return nil },
							nil
					},
					func(context.Context, uint64, util.Hash, util.Hash) (operation, bool, string, error) {
						return nil, false, "", nil
					},
					func(opindexOpHashesFunc) error { return nil },
					func([]string, map[uint64]string) error { return nil },
				)
			},
			func() error {
				for worker := range slices.Values(workers) {
					if worker == local {
						continue
					}

					insert, done, err := insertValues(worker)
					if err != nil {
						return err
					}

					if err := insert(0, valuehash.RandomSHA256(), t.randomMergeValues(1, 2, 3)); err != nil {
						return err
					}

					if err := done(workers, nil); err != nil {
						return err
					}
				}

				return nil
			},
			func() error {
				for worker := range slices.Values(workers) {
					if worker == local {
						continue
					}

					if err := unmerged(worker, []uint64{0}); err != nil {
						return err
					}
				}

				return nil
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

func (t *testWorkerProcess) newFuncGetOperation(ops []operation) getOperationFunc {
	return func(_ context.Context, opindex uint64, _, _ util.Hash) (operation, bool, string, error) {
		if opindex >= uint64(len(ops)) {
			return nil, false, "", nil
		}

		return ops[opindex], true, "", nil
	}
}

func (t *testWorkerProcess) TestLocal() {
	t.Run("local only; inside ops workers", func() {
		local := "local"
		workers := []string{local}

		ops := make([]operation, 33)
		for i := range ops {
			ops[i] = t.randomAddOp(1)
		}

		ctx, _ := t.Ctx()

		initial, _, _, wait, savedOps, savedStates := t.newf(
			ctx,
			local,
			func(string) (base.State, bool, error) { return nil, false, nil },
			t.createStateFunc(),
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

		for i, op := range slices.All(ops) {
			s, found := savedOps.Value(uint64(i))
			t.True(found)

			base.EqualOperation(t.Assert(), op, s)
		}
	})

	t.Run("local only; outside ops workers", func() {
		local := "local"
		workers := t.workers(1)

		ops := make([]operation, 33)
		for i := range ops {
			ops[i] = t.randomAddOp(1)
		}

		ctx, _ := t.Ctx()

		_, insertf, unmerge, wait, savedOps, savedStates := t.newf(
			ctx,
			local,
			func(string) (base.State, bool, error) { return nil, false, nil },
			t.createStateFunc(),
			workers, workers, 1, 1,
		)

		var calledBroadcastUnmerged bool
		var reportedStates atomic.Value

		for worker := range slices.Values(workers) {
			insert, done, err := insertf(worker)
			t.NoError(err)

			for i, op := range slices.All(ops) {
				subv, addvs, reason, err := op.SMVs(ctx)
				t.IfErrPanic(err)
				t.IfTruePanic(reason != "", "not empty reason, %q", reason)

				var values []SMV

				if subv != nil {
					values = append(values, subv)
				}

				values = append(values, addvs...)

				t.NoError(insert(uint64(i), op.Fact().Hash(), values))
			}

			t.NoError(done(workers, workers))
		}

		for worker := range slices.Values(workers) {
			t.NoError(unmerge(worker, nil))
		}

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

		t.True(calledBroadcastUnmerged)

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

		ops := make([]operation, 33)
		for i := range ops {
			ops[i] = t.randomAddOp(1)
		}

		ctx, _ := t.Ctx()

		initial, _, _, wait, savedOps, savedStates := t.newf(
			ctx,
			local,
			func(string) (base.State, bool, error) { return nil, false, nil },
			t.createStateFunc(),
			workers, workers, 1, 1,
		)

		expectedReasonsAfterExtracted := map[uint64]string{}
		for i := range 3 {
			expectedReasonsAfterExtracted[uint64(i+len(ops))] = reasonOperationNotFound
		}
		expectedUnmergedAfterExtracted := slices.Collect(maps.Keys(expectedReasonsAfterExtracted))
		slices.Sort(expectedUnmergedAfterExtracted)

		t.NoError(initial(nil, nil,
			t.newFuncGetOperation(ops),
			func(f opindexOpHashesFunc) error {
				for i, op := range slices.All(ops) {
					if err := f(uint64(i), op.Hash(), op.Fact().Hash()); err != nil {
						return err
					}
				}

				for opindex := range maps.Keys(expectedReasonsAfterExtracted) {
					if err := f(opindex, valuehash.RandomSHA256(), valuehash.RandomSHA256()); err != nil {
						return err
					}
				}

				return nil
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

		for i, op := range slices.All(ops) {
			s, found := savedOps.Value(uint64(i))
			t.True(found)

			base.EqualOperation(t.Assert(), op, s)
		}
	})

	t.Run("local inside ops workers; empty state merge values", func() {
		local := "local"
		other := "w0"
		workers := []string{local, other}

		ops := make([]operation, 3)
		for i := range ops {
			ops[i] = t.randomAddOp(1)
		}

		ctx, _ := t.Ctx()

		initial, insertf, _, wait, savedOps, savedStates := t.newf(
			ctx,
			local,
			func(string) (base.State, bool, error) { return nil, false, nil },
			t.createStateFunc(),
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

		var calledBroadcastUnmerged, calledReportStates bool

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
				calledReportStates = true

				return nil
			},
		))

		t.False(calledBroadcastUnmerged)
		t.True(calledReportStates)

		t.EqualValues(0, savedStates.Len())

		for i, op := range slices.All(ops) {
			s, found := savedOps.Value(uint64(i))
			t.True(found)

			base.EqualOperation(t.Assert(), op, s)
		}
	})

	t.Run("local broadcasts insert done to outside ops workers", func() {
		local := "local"
		opsWorkers := []string{local}
		workers := []string{local, "w0"}

		ops := make([]operation, 3)
		for i := range ops {
			ops[i] = t.randomAddOp(1)
		}

		ctx, _ := t.Ctx()

		initial, _, _, wait, savedOps, savedStates := t.newf(
			ctx,
			local,
			func(string) (base.State, bool, error) { return nil, false, nil },
			t.createStateFunc(),
			workers, opsWorkers, 1, 1,
		)

		var otherInserted, otherInsertedDone bool

		t.NoError(initial(
			func(string) []string { return []string{local} },
			func(_ context.Context, worker string) (workerProcessInsertSMVsFunc, dutil.In2ErrorFunc[[]string, []string], error) {
				return func(opindex uint64, fact util.Hash, values []SMV) error {
						otherInserted = true

						return nil
					},
					func(subws []string, affected []string) error {
						otherInsertedDone = true

						t.Print("worker distribute: local=%q worker=%q subs=%v affected=%v", local, worker, subws, affected)

						return nil
					},
					nil
			},
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

		for i, op := range slices.All(ops) {
			s, found := savedOps.Value(uint64(i))
			t.True(found)

			base.EqualOperation(t.Assert(), op, s)
		}
	})

	t.Run("not inserted; don't report result", func() {
		workers := t.workers(3)
		local := workers[0]

		ctx, _ := t.Ctx()

		initial, insertf, _, wait, savedOps, savedStates := t.newf(
			ctx,
			local,
			func(string) (base.State, bool, error) { return nil, false, nil },
			t.createStateFunc(),
			workers, workers, 1, 1,
		)

		t.NoError(initial(
			func(string) []string { return workers },
			nil,
			nil,
			t.defaultFuncInsertOp(nil),
			func([]string, map[uint64]string) error { return nil },
		))

		go func() {
			for worker := range slices.Values(workers) {
				if worker == local {
					continue
				}

				_, insertDone, err := insertf(worker)
				t.IfErrPanic(err)
				t.IfErrPanic(insertDone(nil, nil))
			}
		}()

		var calledBroadcastUnmerged, calledReportStates bool

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
				calledReportStates = true

				return nil
			},
		))

		t.False(calledBroadcastUnmerged)
		t.False(calledReportStates)

		t.EqualValues(0, savedStates.Len())
		t.EqualValues(0, savedOps.Len())
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
			t.createStateFunc(),
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
			t.createStateFunc(),
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
	workerProcessWorkerInsertSMVsFunc,
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
		t.createStateFunc(),
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
	ops []operation,
) {
	for worker := range slices.Values(opsWorkers) {
		if slices.Index(workers, worker) < 0 {
			t.Fail("unknown op worker, %q", worker)
		}
	}

	initials := map[string]workerProcessInitialInsertFunc{}
	workerInserts := map[string]workerProcessWorkerInsertSMVsFunc{}
	workerUnmerges := map[string]workerUnmergedFunc{}
	workerWaits := map[string]workerProcessWaitFunc{}

	for local := range slices.Values(workers) {
		initials[local], workerInserts[local], workerUnmerges[local], workerWaits[local] = t.newProcessWorker(
			ctx,
			local, workers, opsWorkers,
			opsShare, statesShare,
		)
	}

	getOperation := t.newFuncGetOperation(ops)
	getStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, opsShare)
	t.NoError(err)
	defer stateOwnersClose()

	distributeValues := func(local string) newWorkerProcessDistributeSMVsFunc {
		return func(_ context.Context, worker string) (
			workerProcessInsertSMVsFunc,
			dutil.In2ErrorFunc[[]string, []string],
			error,
		) {
			return workerInserts[worker](local)
		}
	}

	go func() {
		ophs := make([][2]util.Hash, len(ops))
		for i, op := range slices.All(ops) {
			ophs[i] = [2]util.Hash{op.Hash(), op.Fact().Hash()}
		}

		_, opWorkers, err := opsNShareWorkers(opsWorkers, opsShare, ophs)
		t.IfErrPanic(err)

		var wg sync.WaitGroup

		for local := range slices.Values(opsWorkers) {
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
						t.Print("local done: local=%q owners=%v reasons=%v", local, owners, reasons)

						return nil
					},
				)

				t.Print("initial: worker=%q err=%v", local, err)
			}()
		}

		wg.Wait()
	}()

	var wg sync.WaitGroup

	for local := range slices.Values(workers) {
		wg.Add(1)

		go func() {
			defer wg.Done()

			traverseOps, err := workerWaits[local](
				func(ctx context.Context, worker string, unmerged []uint64) error {
					t.Print("worker unmerge: local=%q worker=%q unmerged=%v", local, worker, unmerged)

					return workerUnmerges[worker](local, unmerged)
				},
				func(
					ctx context.Context,
					reasons map[uint64]string,
					statesSize uint64,
					f dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
				) error {
					t.Print("wait states: local=%q reasons=%v states=%d", local, reasons, statesSize)

					return f(func(st base.State) error {
						t.Print("\tstate: local=%q st=%q", local, st.Key())

						return nil
					})
				},
			)

			if err == nil {
				for opindex, op := range traverseOps {
					switch {
					case opindex >= uint64(len(ops)):
						err = errors.Errorf("unknown opindex=%d", opindex)
					case !op.Hash().Equal(ops[opindex].Hash()):
						err = errors.Errorf("hash mismatch, %d", opindex)
					}

					if err != nil {
						break
					}
				}
			}

			t.Print("wait done: worker=%q err=%v", local, err)
		}()
	}

	wg.Wait()
}

func (t *testWorkerProcess) TestWorkers() {
	t.Run("local only", func() {
		ctx, _ := t.Ctx()

		workers := t.workers(1)
		ops := make([]operation, 33)
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
		ops := make([]operation, 33)
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

func (t *testWorkerProcess) TestWorkerProcessInitialInsert() {
	for range 33 {
		t.Run("subs and adds", func() {
			pub0 := base.NewDummyPublickey("p0")
			pub1 := base.NewDummyPublickey("p1")
			pub2 := base.NewDummyPublickey("p2")

			getStateOwners := func(stateKey string) (owners []string) { // NOTE comment
				var owner string

				switch stateKey {
				case accountStateKey(pub0):
					owner = "w0"
				case accountStateKey(pub1):
					owner = "w1"
				case accountStateKey(pub2):
					owner = "w2"
				default:
					t.Panicf(nil, "unknown state, %q", stateKey)
				}

				return []string{owner}
			}

			workers := t.workers(3)

			ops := []operation{
				t.newDummyOp([]byte("0"), pub0, 1, pub1, -2), // 0
				t.newDummyOp([]byte("1"), pub1, -2, pub2, 3), // 1: ignored
				t.newDummyOp([]byte("2"), pub0, -1, pub1, 2), // 2
				t.newDummyOp([]byte("3"), pub0, -1, pub1, 2), // 3: ignored
				t.newDummyOp([]byte("4"), pub0, 1, pub2, -3), // 4
				t.newDummyOp([]byte("5"), pub1, 1, pub2, 2),  // 5
			}

			expectedOwners := map[string]map[string][][]string{
				"w0": {
					"w1": {
						{"w0"},
						{"w0", "w2"},
					},
					"w2": {
						{"w1"},
						{"w0"},
					},
				},
				"w1": {
					"w0": {
						{"w1", "w2"},
						{"w1"},
					},
					"w2": {
						{"w1"},
						{"w0"},
					},
				},
				"w2": {
					"w0": {
						{"w1", "w2"},
						{"w1"},
					},
					"w1": {
						{"w0"},
						{"w0", "w2"},
					},
				},
			}

			for local := range slices.Values(workers) {
				ctx, cancel := t.Ctx()
				defer cancel(nil)

				var localExtractedStatesOwners localExtractedStatesOwnersFunc

				localExtractedStatesOwners, _, _, err := newFuncWorkerStatesOwners(ctx, local, uint64(len(ops)))
				t.NoError(err)

				initial, err := newFuncWorkerProcessInitialInsert(
					ctx,
					local,
					dutil.NewFuncNewJobWorkerFunc(1<<9),
					workers,
					uint64(len(ops)),
					localExtractedStatesOwners,
					func(worker string) (insertSMVsFunc, dutil.InErrorFunc[error], error) {
						return func(uint64, []SMV) error { return nil },
							dutil.DummyErrorInFunc,
							nil
					},
					func(uint64, util.Hash, []string) bool { return true },
				)
				t.NoError(err)

				t.NoError(initial(
					getStateOwners,
					func(_ context.Context, worker string) (
						workerProcessInsertSMVsFunc,
						dutil.In2ErrorFunc[[]string, []string],
						error,
					) {
						return func(uint64, util.Hash, []SMV) error { return nil },
							func(subws, affected []string) error {
								t.Print("local=%q worker=%q sub=%v affected=%v", local, worker, subws, affected)

								a := expectedOwners[local][worker]
								asubws := a[0]
								aaffected := a[1]

								if !slices.Equal(asubws, subws) {
									t.Panicf(nil, "local=%q worker=%q expected=%v result=%v", local, worker, asubws, subws)
								}

								if !slices.Equal(aaffected, affected) {
									t.Panicf(nil, "local=%q worker=%q expected=%v result=%v", local, worker, aaffected, affected)
								}

								return nil
							},
							nil
					},
					t.getOperationFunc(ops),
					func(f opindexOpHashesFunc) error {
						for opindex, op := range slices.All(ops) {
							if err := f(uint64(opindex), op.Hash(), op.Fact().Hash()); err != nil {
								return err
							}
						}

						return nil
					},
					func(uint64, operation) error { return nil },
					func(owners []string, reasons map[uint64]string) error {
						t.Print("local=%q owners=%v reasons=%v", local, owners, reasons)

						slices.Sort(owners)

						if !slices.Equal(workers, owners) {
							t.Panicf(nil, "local=%q expected=%v result=%v", local, workers, owners)
						}

						return nil
					},
				))
			}
		})
	}
}

func TestWorkerProcess(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerProcess))
}
