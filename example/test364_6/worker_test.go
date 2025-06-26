package dop6

import (
	"context"
	"iter"
	"maps"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorker struct {
	baseMergerSuite
}

func (t *testWorker) TestNew() {
	local, workers := t.workers(3)

	newGetSVM := t.newGetSVM(nil)

	t.Run("zero max ops size", func() {
		_, _, _, _, err := NewWorker(
			context.Background(),
			local,
			workers, workers,
			1, 1,
			0,
			nil,
			newGetSVM,
			nil, nil, nil,
			nil, nil, nil,
		)
		t.ErrorContains(err, "empty size")
	})

	t.Run("empty workers", func() {
		_, _, _, _, err := NewWorker(
			context.Background(),
			local,
			nil, workers,
			1, 1,
			1,
			nil,
			newGetSVM,
			func(context.Context) (mergerUnmergedFunc, func()) { return nil, nil },
			func(context.Context, string) (mergerUnmergedFunc, dutil.InErrorFunc[error], error) {
				return nil, nil, nil
			},
			nil,
			nil, nil, nil,
		)
		t.ErrorContains(err, "under 1")
	})

	t.Run("empty ops workers", func() {
		_, _, _, _, err := NewWorker(
			context.Background(),
			local,
			workers, nil,
			1, 1,
			1,
			nil,
			newGetSVM,
			func(context.Context) (mergerUnmergedFunc, func()) { return nil, nil },
			func(context.Context, string) (mergerUnmergedFunc, dutil.InErrorFunc[error], error) {
				return nil, nil, nil
			},
			nil,
			nil, nil, nil,
		)
		t.ErrorContains(err, "zero workers")
	})
}

func (t *testWorker) mangerReportUnmerged() (mergerReportUnmergedFunc, func() map[uint64]string) {
	m := dutil.NewSingleLockedMap[uint64, string]()

	var v atomic.Value

	return func(context.Context) (mergerUnmergedFunc, func()) {
			return func(opindex uint64, reason string) error {
					m.SetValue(opindex, reason)

					return nil
				},
				func() {
					v.CompareAndSwap(nil, m.Map())
				}
		},
		func() map[uint64]string {
			if i := v.Load(); i != nil {
				return i.(map[uint64]string)
			}

			return nil
		}
}

func (t *testWorker) workerReportUnmerged() (newWorkerReportUnmergedToWorkersFunc, func() map[string]map[uint64]string) {
	m := dutil.NewSingleLockedMap[string, map[uint64]string]()

	var v atomic.Value

	return func(_ context.Context, worker string) (mergerUnmergedFunc, dutil.InErrorFunc[error], error) {
			return func(opindex uint64, reason string) error {
					m.Set(worker, func(i map[uint64]string, found bool) (map[uint64]string, error) {
						if i == nil {
							i = map[uint64]string{}
						}

						i[opindex] = reason

						return i, nil
					})

					return nil
				},
				func(err error) error {
					v.CompareAndSwap(nil, m.Map())

					return err
				},
				nil
		},
		func() map[string]map[uint64]string {
			if i := v.Load(); i != nil {
				return i.(map[string]map[uint64]string)
			}

			return nil
		}
}

func (t *testWorker) reportStates() (reportStatesFunc, func() ([]base.State, bool, error)) {
	var addState dutil.AppendSliceAddFunc[base.State]
	var doneState dutil.OutFunc[[]base.State]

	addState, doneState, err := dutil.NewLockAppendSlice[base.State](1<<9, 1<<9)
	t.NoError(err)

	var v atomic.Value

	return func(context.Context) (dutil.InErrorFunc[base.State], dutil.InErrorFunc[error], error) {
			return func(st base.State) error {
					addState(st)

					return nil
				},
				func(err error) error {
					if err != nil {
						v.CompareAndSwap(nil, err)

						return err
					}

					v.CompareAndSwap(nil, doneState())

					return nil
				},
				nil
		},
		func() ([]base.State, bool, error) {
			switch t := v.Load().(type) {
			case error:
				return nil, true, t
			case []base.State:
				return t, true, nil
			default:
				return nil, false, nil
			}
		}
}

func (t *testWorker) reportOwners() (reportStateOwnersFunc, func() [2][]string) {
	var v atomic.Value

	return func(_ context.Context, subs, affected []string) error {
			v.CompareAndSwap(nil, [2][]string{subs, affected})

			return nil
		},
		func() [2][]string {
			if i := v.Load(); i != nil {
				return i.([2][]string)
			}

			return [2][]string{}
		}
}

func (t *testWorker) newWorkerDistributeSMVs() (
	NewWorkerDistributeSMVsFunc,
	iter.Seq2[string, iter.Seq2[uint64, [2][]SMV]],
	iter.Seq2[string, [2][]string],
) {
	smvs := dutil.NewSingleLockedMap[string, map[uint64][2][]SMV]()
	owners := dutil.NewSingleLockedMap[string, [2][]string]()

	return func(_ context.Context, worker string) (
			mergerMergeSMVsFunc,
			workerWorkerStateOwnersFunc,
			error,
		) {
			return func(opindex uint64, subv SMV, addvs []SMV) error {
					_, err := smvs.Set(worker, func(i map[uint64][2][]SMV, _ bool) (map[uint64][2][]SMV, error) {
						if i == nil {
							i = map[uint64][2][]SMV{}
						}

						if _, found := i[opindex]; found {
							return nil, errors.Errorf("already set, worker=%q opindex=%d", worker, opindex)
						}

						i[opindex] = [2][]SMV{{subv}, addvs}

						return i, nil
					})

					return err
				},
				func(subs, affected []string) error {
					_, err := owners.Set(worker, func(_ [2][]string, found bool) ([2][]string, error) {
						if found {
							return [2][]string{}, errors.Errorf("already set, worker=%q", worker)
						}

						return [2][]string{subs, affected}, nil
					})

					return err
				},
				nil
		},
		func(y0 func(string, iter.Seq2[uint64, [2][]SMV]) bool) {
			for worker, vs := range smvs.Seq2 {
				if !y0(worker, func(y1 func(uint64, [2][]SMV) bool) {
					for opindex, v := range maps.All(vs) {
						if !y1(opindex, v) {
							return
						}
					}
				}) {
					return
				}
			}
		},
		func(yield func(string, [2][]string) bool) {
			for worker, os := range owners.Seq2 {
				if !yield(worker, os) {
					return
				}
			}
		}
}

func (t *testWorker) newMakeState() makeStateFunc {
	return func(stateKey string, value base.StateValue, ops []util.Hash) (base.State, error) {
		return dop5.NewBaseState(
				33,
				stateKey,
				value,
				nil,
				ops,
			),
			nil
	}
}

func (t *testWorker) opsSeq(facts []OperationFact) iter.Seq2[uint64, [2]util.Hash] {
	return func(yield func(uint64, [2]util.Hash) bool) {
		for i, fact := range slices.All(facts) {
			if !yield(uint64(i), [2]util.Hash{fact.Hash(), fact.Hash()}) {
				return
			}
		}
	}
}

func (t *testWorker) TestInsertOps() {
	t.Run("single worker", func() {
		t.Run("local is ops worker", func() {
			local, workers := t.workers(1)

			facts := t.randomAddFacts(3, 1)
			of := newFuncGetOperationFact(facts)
			newGetSVM := t.newGetSVM(nil)

			reportUnmergedToManager, unmergedToManager := t.mangerReportUnmerged()
			reportUnmergedToWorker, unmergedToWorker := t.workerReportUnmerged()
			reportStates, reportedStates := t.reportStates()
			newWorkerDistribute, _, _ := t.newWorkerDistributeSMVs()
			reportStateOwners, reportedStateOwners := t.reportOwners()
			makeState := t.newMakeState()

			insertOps, _, _, wait, err := NewWorker(
				context.Background(),
				local,
				workers, workers,
				1, 1,
				uint64(len(facts)),
				of,
				newGetSVM,
				reportUnmergedToManager,
				reportUnmergedToWorker,
				reportStates,
				oneWorkerGetFactStateOwners(local),
				newWorkerDistribute,
				reportStateOwners,
			)
			t.NoError(err)

			t.NoError(insertOps(t.opsSeq(facts)))

			t.NoError(wait(makeState))

			unmergedM := unmergedToManager()
			t.Empty(unmergedM)
			unmergedW := unmergedToWorker()
			t.Empty(unmergedW)
			rsts, called, err := reportedStates()
			t.NoError(err)
			t.NotEmpty(rsts)
			t.True(called)

			rowners := reportedStateOwners()
			t.Empty(rowners[0])
			t.Equal(workers, rowners[1])
		})

		t.Run("local is not ops worker", func() {
			local := "local"
			_, workers := t.workers(1)

			facts := t.randomAddFacts(3, 1)
			of := newFuncGetOperationFact(facts)
			newGetSVM := t.newGetSVM(nil)

			reportUnmergedToManager, unmergedToManager := t.mangerReportUnmerged()
			reportUnmergedToWorker, unmergedToWorker := t.workerReportUnmerged()
			reportStates, reportedStates := t.reportStates()
			newWorkerDistribute, _, _ := t.newWorkerDistributeSMVs()
			reportStateOwners, reportedStateOwners := t.reportOwners()
			makeState := t.newMakeState()

			_, workerInsertOps, _, wait, err := NewWorker(
				context.Background(),
				local,
				workers, workers,
				1, 1,
				uint64(len(facts)),
				of,
				newGetSVM,
				reportUnmergedToManager,
				reportUnmergedToWorker,
				reportStates,
				oneWorkerGetFactStateOwners(local),
				newWorkerDistribute,
				reportStateOwners,
			)
			t.NoError(err)

			t.NoError(workerInsertOps(
				workers[0],
				func(f workerWorkerMergeOpFunc) error {
					for i, fact := range slices.All(facts) {
						opindex := uint64(i)
						subv, addvs, reason, err := fact.SMVs(context.Background())
						if err != nil {
							return err
						}
						if reason != "" {
							return errors.New(reason)
						}

						if err := f(opindex, fact.Hash(), subv, addvs, []string{local}); err != nil {
							return err
						}
					}

					return nil
				},
				func(f workerWorkerStateOwnersFunc) error {
					return f(nil, []string{local})
				},
			))

			t.NoError(wait(makeState))

			unmergedM := unmergedToManager()
			t.Empty(unmergedM)
			unmergedW := unmergedToWorker()
			t.Empty(unmergedW)
			rsts, called, err := reportedStates()
			t.NoError(err)
			t.NotEmpty(rsts)
			t.True(called)

			rowners := reportedStateOwners()
			t.Empty(rowners[0])
			t.Equal([]string{local}, rowners[1])
		})

		t.Run("local no ops and smvs", func() {
			local := "local"
			_, workers := t.workers(1)

			facts := t.randomAddFacts(3, 1)
			of := newFuncGetOperationFact(facts)
			newGetSVM := t.newGetSVM(nil)

			reportUnmergedToManager, unmergedToManager := t.mangerReportUnmerged()
			reportUnmergedToWorker, unmergedToWorker := t.workerReportUnmerged()
			reportStates, reportedStates := t.reportStates()
			newWorkerDistribute, _, _ := t.newWorkerDistributeSMVs()
			reportStateOwners, reportedStateOwners := t.reportOwners()
			makeState := t.newMakeState()

			_, workerInsertOps, _, wait, err := NewWorker(
				context.Background(),
				local,
				workers, workers,
				1, 1,
				uint64(len(facts)),
				of,
				newGetSVM,
				reportUnmergedToManager,
				reportUnmergedToWorker,
				reportStates,
				oneWorkerGetFactStateOwners(local),
				newWorkerDistribute,
				reportStateOwners,
			)
			t.NoError(err)

			t.NoError(workerInsertOps(
				managerName,
				func(workerWorkerMergeOpFunc) error { return nil },
				func(f workerWorkerStateOwnersFunc) error {
					return f(nil, nil)
				},
			))

			t.NoError(wait(makeState))

			unmergedM := unmergedToManager()
			t.Nil(unmergedM)
			unmergedW := unmergedToWorker()
			t.Nil(unmergedW)
			rsts, called, err := reportedStates()
			t.NoError(err)
			t.Nil(rsts)
			t.False(called)

			rowners := reportedStateOwners()
			t.Nil(rowners[0])
			t.Nil(rowners[1])
		})

		t.Run("insert ops from not ops worker", func() {})
		t.Run("unmerge ops from not ops worker", func() {})
	})

	t.Run("multiple workers", func() {
		t.Run("local is not state owner", func() {})
	})
}

func TestWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorker))
}

func oneWorkerGetFactStateOwners(worker string) getFactStateOwnersFunc {
	return func(opindex uint64, subv SMV, addvs []SMV) ([]string, [][]string) {
		var s []string
		var a [][]string

		if subv != nil {
			s = []string{worker}
		}

		if len(addvs) > 0 {
			a = make([][]string, len(addvs))
			for i := range a {
				a[i] = []string{worker}
			}
		}

		return s, a
	}
}
