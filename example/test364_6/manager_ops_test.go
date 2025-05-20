package dop6

import (
	"context"
	"maps"
	"slices"
	"testing"

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
			nilFuncManagerDistributeOpsWorker(),
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
			nilFuncManagerDistributeOpsWorker(),
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
			nilFuncManagerDistributeOpsWorker(),
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
			distribute,
		)
		t.NoError(err)

		t.Equal(workers[:1], ow)
		t.NoError(wait())

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
			distribute,
		)
		t.NoError(err)

		t.Equal(workers, ow)
		t.NoError(wait())

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
			distribute,
		)
		t.NoError(err)

		t.Equal(workers, ow)
		t.NoError(wait())

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
