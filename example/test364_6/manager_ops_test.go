package dop6

import (
	"context"
	"maps"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
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

type testManagerUnmergedOps struct {
	baseMergerSuite
}

func (t *testManagerUnmergedOps) TestCanceled() {
	t.Run("context canceled", func() {
		ctx, cancel := t.Ctx()
		ophs := t.randomOphs(3)

		_, wait, err := newManagerUnmergedOps(
			ctx,
			ophs,
			func(string, error) (bool, bool, error) { return false, false, nil },
		)
		t.NoError(err)

		cancel(nil)

		_, err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testManagerUnmergedOps) ophs(n int, unmerged map[uint64]string) (
	[][2]util.Hash,
	*fixedtree.Tree,
	fixedtree.GenerateNodesHashFunc,
) {
	ophs := t.randomOphs(3)

	nodes := make([]fixedtree.Node, n)

	for i, hs := range slices.All(ophs) {
		reason := unmerged[uint64(i)]
		n, err := dop5.NewOperationTreeNode(hs[0], hs[1], reason)
		t.NoError(err)

		nodes[i] = n
	}

	g := t.generateNodesHashFunc()
	t.NoError(g(context.Background(), nodes, nil))

	return ophs, dop5.NewStatesTree(nodes), g
}

func (t *testManagerUnmergedOps) generateNodesHashFunc() fixedtree.GenerateNodesHashFunc {
	return fixedtree.NewFuncAsyncGenerateNodesHashByFollowingParent(1 << 9)
}

func (t *testManagerUnmergedOps) TestEnough() {
	_, workers := t.workers(2)
	ophs, expectedTree, g := t.ophs(3, nil)

	t.Run("failed", func() {
		workerUnmerged, wait, err := newManagerUnmergedOps(
			context.Background(),
			ophs,
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

		_, err = wait(g)
		t.ErrorContains(err, "findme")
	})

	t.Run("ok", func() {
		var count int64

		workerUnmerged, wait, err := newManagerUnmergedOps(
			context.Background(),
			ophs,
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

		tr, err := wait(g)
		t.NoError(err)

		t.True(fixedtree.IsEqualTree(expectedTree, tr))
	})

	t.Run("unmerged", func() {
		unmerged := map[uint64]string{
			1: "showme",
		}
		ophs, expectedTree, g := t.ophs(3, unmerged)

		_, workers := t.workers(3)

		var count int64
		workerUnmerged, wait, err := newManagerUnmergedOps(
			context.Background(),
			ophs,
			func(string, error) (bool, bool, error) {
				return true, atomic.AddInt64(&count, 1) == int64(len(workers)), nil
			},
		)
		t.NoError(err)

		t.NoError(workerUnmerged(workers[0], nil))
		t.NoError(workerUnmerged(workers[1], nil))
		t.NoError(workerUnmerged(workers[2], unmerged))

		tr, err := wait(g)
		t.NoError(err)

		t.True(fixedtree.IsEqualTree(expectedTree, tr))
	})

	t.Run("unmerged after enough", func() {
		_, workers := t.workers(3)

		var count int64
		workerUnmerged, wait, err := newManagerUnmergedOps(
			context.Background(),
			ophs,
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

		tr, err := wait(g)
		t.NoError(err)

		t.True(fixedtree.IsEqualTree(expectedTree, tr))
	})
}

func TestManagerUnmergedOps(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerUnmergedOps))
}
