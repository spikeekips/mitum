package dop6

import (
	"context"
	"iter"
	"maps"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerDistributeOps struct {
	baseMergerSuite
}

func (t *testManagerDistributeOps) TestNew() {
	t.Run("empty workers", func() {
		_, _, err := newManagerDistributeOps(
			context.Background(),
			nil,
			3,
			t.randomOphs(3),
		)
		t.ErrorContains(err, "empty workers")
	})

	t.Run("zero share", func() {
		_, workers := t.workers(3)

		_, _, err := newManagerDistributeOps(
			context.Background(),
			workers,
			0,
			t.randomOphs(3),
		)
		t.ErrorContains(err, "zero share")
	})

	t.Run("empty ophs", func() {
		_, workers := t.workers(3)

		_, _, err := newManagerDistributeOps(
			context.Background(),
			workers,
			3,
			nil,
		)
		t.ErrorContains(err, "empty ophs")
	})
}

func (t *testManagerDistributeOps) newFuncManagerDistributeOpsWorker() (
	util.LockedMap[string, [][2]any],
	newManagerDistributeOpsWorkerFunc,
) {
	pool := util.NewSingleLockedMap[string, [][2]any]()

	return pool, func(worker string) (managerDistributeOpsWorkerFunc, error) {
		return func(items uint64, f dutil.InErrorFunc[opindexOpHashesFunc]) error {
				c := make([][2]any, 0, items)

				if err := f(func(opindex uint64, oph, facth util.Hash) error {
					c = append(c, [2]any{opindex, [2]util.Hash{oph, facth}})

					return nil
				}); err != nil {
					return err
				}

				pool.SetValue(worker, c)

				return nil
			},
			nil
	}
}

func (t *testManagerDistributeOps) TestDistribute() {
	t.Run("1 ophs", func() {
		_, workers := t.workers(3)
		ophs := t.randomOphs(1)

		collected, distribute := t.newFuncManagerDistributeOpsWorker()

		ow, wait, err := newManagerDistributeOps(
			context.Background(),
			workers,
			1,
			ophs,
		)
		t.NoError(err)

		t.Equal(workers[:1], ow)
		t.NoError(wait(distribute, func(string, error) error { return nil }))

		t.Equal(1, collected.Len())

		j, found := collected.Value(workers[0])
		t.True(found)

		t.Equal(1, len(j))

		t.EqualValues(0, j[0][0].(uint64))
		t.EqualValues(ophs[0], j[0][1].([2]util.Hash))
	})

	t.Run("3 ophs", func() {
		_, workers := t.workers(3)
		ophs := t.randomOphs(3)
		var share uint64 = 1

		collected, distribute := t.newFuncManagerDistributeOpsWorker()

		ow, wait, err := newManagerDistributeOps(
			context.Background(),
			workers,
			share,
			ophs,
		)
		t.NoError(err)

		t.Equal(workers, ow)
		t.NoError(wait(distribute, func(string, error) error { return nil }))

		t.Equal(len(workers), collected.Len())

		co := map[uint64]uint64{}

		for i, worker := range slices.All(workers) {
			j, found := collected.Value(worker)
			t.True(found)

			t.Equal(1, len(j))

			opindex := j[0][0].(uint64)
			co[opindex]++

			t.EqualValues(i, opindex)
			t.EqualValues(ophs[i], j[0][1].([2]util.Hash))
		}

		for i := range maps.Values(co) {
			t.EqualValues(share, i)
		}
	})

	t.Run("async ophs", func() {
		_, workers := t.workers(33)
		ophs := t.randomOphs(3333)
		var share uint64 = 3

		collected, distribute := t.newFuncManagerDistributeOpsWorker()

		ow, wait, err := newManagerDistributeOps(
			context.Background(),
			workers,
			share,
			ophs,
		)
		t.NoError(err)

		t.Equal(workers, ow)
		t.NoError(wait(distribute, func(string, error) error { return nil }))

		t.Equal(len(workers), collected.Len())

		co := map[uint64]uint64{}

		for worker := range slices.Values(workers) {
			j, found := collected.Value(worker)
			t.True(found)
			t.T().Logf("worker=%q len=%d", worker, len(j))
			t.Equal((len(ophs)*3)/len(workers), len(j))

			co[j[0][0].(uint64)]++
		}

		for i := range maps.Values(co) {
			t.EqualValues(share, i)
		}
	})
}

func (t *testManagerDistributeOps) TestCheckFailed() {
	t.Run("all workers ok", func() {
		_, workers := t.workers(3)
		ophs := t.randomOphs(3)
		var share uint64 = 1

		collected, distribute := t.newFuncManagerDistributeOpsWorker()

		ow, wait, err := newManagerDistributeOps(
			context.Background(),
			workers,
			share,
			ophs,
		)
		t.NoError(err)

		t.Equal(workers, ow)
		t.NoError(wait(distribute, func(string, error) error { return nil }))

		t.Equal(len(workers), collected.Len())

		co := map[uint64]uint64{}

		for i, worker := range slices.All(workers) {
			j, found := collected.Value(worker)
			t.True(found)

			t.Equal(1, len(j))

			opindex := j[0][0].(uint64)
			co[opindex]++

			t.EqualValues(i, opindex)
			t.EqualValues(ophs[i], j[0][1].([2]util.Hash))
		}

		for i := range maps.Values(co) {
			t.EqualValues(share, i)
		}
	})

	t.Run("over failed; new distribute error", func() {
		_, workers := t.workers(3)
		ophs := t.randomOphs(3)
		var share uint64 = 1

		_, distribute := t.newFuncManagerDistributeOpsWorker()

		ow, wait, err := newManagerDistributeOps(
			context.Background(),
			workers,
			share,
			ophs,
		)
		t.NoError(err)

		t.Equal(workers, ow)
		err = wait(func(worker string) (managerDistributeOpsWorkerFunc, error) {
			if worker == workers[2] {
				return nil, errors.New("showme")
			}

			return distribute(worker)
		}, func(_ string, err error) error { return err })
		t.ErrorIs(err, dop5.ErrWorkersFailed)
	})

	t.Run("over failed; distribute error", func() {
		_, workers := t.workers(3)
		ophs := t.randomOphs(33)
		var share uint64 = 1

		_, distribute := t.newFuncManagerDistributeOpsWorker()

		ow, wait, err := newManagerDistributeOps(
			context.Background(),
			workers,
			share,
			ophs,
		)
		t.NoError(err)

		t.Equal(workers, ow)

		var errcount int64
		err = wait(func(worker string) (managerDistributeOpsWorkerFunc, error) {
			if worker == workers[2] {
				return func(_ uint64, f dutil.InErrorFunc[opindexOpHashesFunc]) error {
						return f(func(opindex uint64, oph, facth util.Hash) error {
							if atomic.AddInt64(&errcount, 1) > 3 {
								return errors.New("showme")
							}

							return nil
						})
					},
					nil
			}

			return distribute(worker)
		}, func(_ string, err error) error { return err })
		t.ErrorIs(err, dop5.ErrWorkersFailed)
	})
}

func (t *testManagerDistributeOps) TestWorkerErr() {
	t.Run("over failed", func() {
		_, workers := t.workers(3)
		ophs := t.randomOphs(33)
		var share uint64 = 1

		_, distribute := t.newFuncManagerDistributeOpsWorker()

		ow, wait, err := newManagerDistributeOps(
			context.Background(),
			workers,
			share,
			ophs,
		)
		t.NoError(err)

		t.Equal(workers, ow)

		var errcount int64
		err = wait(func(worker string) (managerDistributeOpsWorkerFunc, error) {
			if worker == workers[2] {
				return func(_ uint64, f dutil.InErrorFunc[opindexOpHashesFunc]) error {
						return f(func(opindex uint64, oph, facth util.Hash) error {
							if atomic.AddInt64(&errcount, 1) > 3 {
								return errors.New("showme")
							}

							return nil
						})
					},
					nil
			}

			return distribute(worker)
		}, func(_ string, err error) error { return err })
		t.ErrorIs(err, dop5.ErrWorkersFailed)
	})
}

func TestManagerDistributeOps(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerDistributeOps))
}

func nilFuncManagerDistributeOpsWorker() newManagerDistributeOpsWorkerFunc {
	return func(string) (managerDistributeOpsWorkerFunc, error) {
		return func(_ uint64, f dutil.InErrorFunc[opindexOpHashesFunc]) error {
				return f(func(opindex uint64, oph, facth util.Hash) error {
					return nil
				})
			},
			nil
	}
}

type testManagerCollectUnmergedOps struct {
	baseMergerSuite
}

func (t *testManagerCollectUnmergedOps) TestCanceled() {
	t.Run("context canceled", func() {
		ctx, cancel := t.Ctx()

		_, wait, err := newManagerCollectUnmergedOps(
			ctx,
			3,
			func(string, error) (bool, bool, error) { return false, false, nil },
		)
		t.NoError(err)

		cancel(nil)

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testManagerCollectUnmergedOps) TestEnough() {
	_, workers := t.workers(2)

	t.Run("failed", func() {
		workerUnmerged, wait, err := newManagerCollectUnmergedOps(
			context.Background(),
			3,
			func(worker string, _ error) (bool, bool, error) {
				if worker == workers[1] {
					return true, false, errors.New("findme")
				}

				return false, false, nil
			},
		)
		t.NoError(err)

		go func() {
			for worker := range slices.Values(workers) {
				_ = workerUnmerged(worker, nil)
			}
		}()

		err = wait(func(seq iter.Seq2[uint64, string]) error {
			return nil
		})
		t.ErrorContains(err, "findme")
	})

	t.Run("ok", func() {
		var count int64

		workerUnmerged, wait, err := newManagerCollectUnmergedOps(
			context.Background(),
			3,
			func(string, error) (bool, bool, error) {
				return true, atomic.AddInt64(&count, 1) == int64(len(workers)), nil
			},
		)
		t.NoError(err)

		go func() {
			for worker := range slices.Values(workers) {
				t.IfErrPanicf(workerUnmerged(worker, nil), "unmerged: worker=%q", worker)
			}
		}()

		t.NoError(wait(func(seq iter.Seq2[uint64, string]) error {
			return nil
		}))
	})

	t.Run("unmerged", func() {
		_, workers := t.workers(3)

		var count int64
		var opsSize uint64 = 3
		workerUnmerged, wait, err := newManagerCollectUnmergedOps(
			context.Background(),
			opsSize,
			func(string, error) (bool, bool, error) {
				return true, atomic.AddInt64(&count, 1) == int64(len(workers)), nil
			},
		)
		t.NoError(err)

		t.NoError(workerUnmerged(workers[0], nil))
		t.NoError(workerUnmerged(workers[1], nil))
		t.NoError(workerUnmerged(workers[2], map[uint64]string{
			1: "showme",
		}))

		expected := map[uint64]string{}
		{
			for i := range opsSize {
				expected[uint64(i)] = ""
			}
		}
		expected[1] = "showme"

		m := map[uint64]string{}
		t.NoError(wait(func(seq iter.Seq2[uint64, string]) error {
			for opindex, reason := range seq {
				m[opindex] = reason
			}

			t.T().Logf("ops: %v", m)

			return nil
		}))
		t.Equal(expected, m)
	})

	t.Run("unmerged after enough", func() {
		_, workers := t.workers(3)

		var count int64
		var opsSize uint64 = 3
		workerUnmerged, wait, err := newManagerCollectUnmergedOps(
			context.Background(),
			opsSize,
			func(string, error) (bool, bool, error) {
				i := atomic.AddInt64(&count, 1)
				j := i == int64(len(workers))-1

				return i <= int64(len(workers))-1, j, nil
			},
		)
		t.NoError(err)

		t.NoError(workerUnmerged(workers[0], nil))
		t.NoError(workerUnmerged(workers[1], nil))
		t.NoError(workerUnmerged(workers[2], map[uint64]string{
			1: "showme",
		}))

		expected := map[uint64]string{}
		{
			for i := range opsSize {
				expected[uint64(i)] = ""
			}
		}

		m := map[uint64]string{}
		t.NoError(wait(func(seq iter.Seq2[uint64, string]) error {
			for opindex, reason := range seq {
				m[opindex] = reason
			}

			t.T().Logf("ops: %v", m)

			return nil
		}))
		t.Equal(expected, m)
	})
}

func TestManagerCollectUnmergedOps(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerCollectUnmergedOps))
}
