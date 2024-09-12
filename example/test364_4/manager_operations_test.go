package main

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerWorkersOperations struct {
	suite.Suite
	baseTestManager
}

func (t *testManagerWorkersOperations) newArgs() *managerWorkersOperationsArgs {
	args := newManagerWorkersOperationsArgs()
	args.NewCheckWorkersFunc = func(minEnough, maxFailed uint64) (
		checkEnoughFailedWorkerFunc,
		func(),
		error,
	) {
		return newCheckEnoughFailedWorkers(minEnough, maxFailed)
	}
	args.NewWorkersOpsFunc = serialNoShareWorkerOps

	_ = args.SetLogging(logging.TestNilLogging)

	return args
}

func (t *testManagerWorkersOperations) TestError() {
	t.Run("tree done error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		args := t.newArgs()
		workers := t.workers(3)
		ophs := t.ophs(3)

		opf, u, tf, err := newManagerWorkersOperations(ctx, args, workers, 0, ophs)
		t.NoError(err)

		t.NoError(opf(func(context.Context, string, func(managerWorkerOperationFunc) error) error {
			return nil
		}))

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			return u(worker, nil)
		})

		err = tf(func(tr fixedtree.Tree) error {
			t.Equal(len(ophs), tr.Len())

			return errors.Errorf("findme")
		})
		t.ErrorContains(err, "findme")

		var called bool

		err = tf(func(tr fixedtree.Tree) error {
			called = true

			return nil
		})
		t.ErrorContains(err, "findme")
		t.False(called)
	})
}

func (t *testManagerWorkersOperations) TestOps() {
	newGatherOpsFunc := func() (
		func(worker string, opindex uint64, oph, fact util.Hash),
		func(worker string, opindex uint64, oph, fact util.Hash) error,
	) {
		gathersOps := util.NewSingleLockedMap[string, map[uint64][2]util.Hash]()

		return func(worker string, opindex uint64, oph, fact util.Hash) {
				gathersOps.Set(worker, func(p map[uint64][2]util.Hash, found bool) (map[uint64][2]util.Hash, error) {
					if !found {
						p = map[uint64][2]util.Hash{}
					}

					p[opindex] = [2]util.Hash{oph, fact}

					return p, nil
				})
			},
			func(worker string, opindex uint64, oph, fact util.Hash) error {
				var gops map[uint64][2]util.Hash

				if i, found := gathersOps.Value(worker); found {
					gops = i
				}

				hs, found := gops[opindex]
				if !found {
					return errors.Errorf("unknown opindex=%d", opindex)
				}

				if !hs[0].Equal(oph) {
					return errors.Errorf("oph mismatch opindex=%d", opindex)
				}
				if !hs[1].Equal(fact) {
					return errors.Errorf("fact mismatch opindex=%d", opindex)
				}

				return nil
			}
	}

	t.Run("empty ophs", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		args := t.newArgs()
		workers := t.workers(1)

		opf, u, tf, err := newManagerWorkersOperations(ctx, args, workers, 0, nil)
		t.NoError(err)

		t.NoError(opf(func(ctx context.Context, worker string, _ func(managerWorkerOperationFunc) error) error {
			return nil
		}))

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			return u(worker, nil)
		})

		t.NoError(tf(func(tr fixedtree.Tree) error {
			t.Equal(0, tr.Len())

			return nil
		}))
	})

	t.Run("1 worker", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ophs := t.ophs(3)
		workers := t.workers(1)

		args := t.newArgs()

		g, gdone := newGatherOpsFunc()

		opf, u, tf, err := newManagerWorkersOperations(ctx, args, workers, 0, ophs)
		t.NoError(err)
		t.NoError(opf(func(ctx context.Context, worker string, f func(managerWorkerOperationFunc) error) error {
			if err := f(func(opindex uint64, oph, fact util.Hash) error {
				g(worker, opindex, oph, fact)

				return nil
			}); err != nil {
				return err
			}

			return nil
		}))

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			return u(worker, []uint64{0})
		})

		t.NoError(tf(func(tr fixedtree.Tree) error {
			t.Equal(3, tr.Len())

			tr.Traverse(func(index uint64, node fixedtree.Node) (bool, error) {
				n := node.(base.OperationFixedtreeNode)

				switch index {
				case 0:
					t.False(n.InState())
				default:
					t.True(n.InState())
				}

				return true, nil
			})

			return nil
		}))

		for i := range workers {
			util.TraverseSliceV1(ophs, func(j int, hs [2]util.Hash) (bool, error) {
				t.NoError(gdone(workers[i], uint64(j), hs[0], hs[1]))

				return true, nil
			})
		}

		var called bool

		err = tf(func(tr fixedtree.Tree) error {
			called = true

			return nil
		})
		t.ErrorIs(err, context.Canceled)
		t.False(called)
	})

	t.Run("under failed", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ophs := t.ophs(3)
		workers := t.workers(3)

		args := t.newArgs()

		opf, u, tf, err := newManagerWorkersOperations(ctx, args, workers, 1, ophs)
		t.NoError(err)
		err = opf(func(ctx context.Context, worker string, f func(managerWorkerOperationFunc) error) error {
			if worker == workers[0] {
				return errors.Errorf("showme")
			}

			return f(nil)
		})
		t.ErrorContains(err, "showme")

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			added, err := u(worker, []uint64{0})
			t.ErrorContains(err, "showme")
			t.False(added)

			return true, nil
		})

		var called bool

		err = tf(func(tr fixedtree.Tree) error {
			called = true

			return nil
		})
		t.ErrorContains(err, "showme")
		t.False(called)
	})

	t.Run("under max failed; distribute", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ophs := t.ophs(3)
		workers := t.workers(3)

		args := t.newArgs()

		opf, u, tf, err := newManagerWorkersOperations(ctx, args, workers, 2, ophs)
		t.NoError(err)
		t.NoError(opf(func(ctx context.Context, worker string, f func(managerWorkerOperationFunc) error) error {
			if worker == workers[0] {
				return errors.Errorf("showme")
			}

			return f(nil)
		}))

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			added, err := u(worker, []uint64{0})
			t.NoError(err)
			t.True(added)

			return true, nil
		})

		t.NoError(tf(func(tr fixedtree.Tree) error {
			t.Equal(3, tr.Len())

			tr.Traverse(func(index uint64, node fixedtree.Node) (bool, error) {
				n := node.(base.OperationFixedtreeNode)

				switch index {
				case 0:
					t.False(n.InState())
				default:
					t.True(n.InState())
				}

				return true, nil
			})

			return nil
		}))
	})

	t.Run("under max failed", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ophs := t.ophs(3)
		workers := t.workers(2)

		args := t.newArgs()

		opf, u, tf, err := newManagerWorkersOperations(ctx, args, workers, 0, ophs)
		t.NoError(err)
		t.NoError(opf(func(ctx context.Context, worker string, f func(managerWorkerOperationFunc) error) error {
			return f(nil)
		}))

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			added, err := u(worker, []uint64{0})
			t.NoError(err)
			t.True(added)

			return true, nil
		})

		t.NoError(tf(func(tr fixedtree.Tree) error {
			t.Equal(3, tr.Len())

			tr.Traverse(func(index uint64, node fixedtree.Node) (bool, error) {
				n := node.(base.OperationFixedtreeNode)

				switch index {
				case 0:
					t.False(n.InState())
				default:
					t.True(n.InState())
				}

				return true, nil
			})

			return nil
		}))
	})

	t.Run("3 worker", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ophs := t.ophs(5)
		workers := t.workers(3)

		workersOps := util.NewSingleLockedMap[string, [][3]interface{}]()

		args := t.newArgs()
		args.NewWorkersOpsFunc = func(workers []string, ophs [][2]util.Hash) (managerWorkersOperationsWorkerOpsFunc, error) {
			c, err := serialNoShareWorkerOps(workers, ophs)
			if err != nil {
				return nil, err
			}

			return func(worker string, f managerWorkersOperationsOpsFunc) error {
					return c(worker, func(of func(managerWorkerOperationFunc) error) error {
						return f(func(off managerWorkerOperationFunc) error {
							var add util.CompactAppendSliceAddFunc[[3]interface{}]
							var done util.CompactAppendSliceDoneFunc[[3]interface{}]

							switch a, d, err := util.CompactAppendSliceV1[[3]interface{}](1<<9, 1<<9); {
							case err != nil:
								return err
							default:
								add = a
								done = d
							}

							if err := of(func(opindex uint64, oph, fact util.Hash) error {
								add([3]interface{}{opindex, oph, fact})

								return off(opindex, oph, fact)
							}); err != nil {
								return err
							}

							_ = done(func(s [][3]interface{}) error {
								workersOps.SetValue(worker, slices.Clone(s))

								return nil
							})

							return nil
						})
					})
				},
				nil
		}

		g, gdone := newGatherOpsFunc()

		opf, u, tf, err := newManagerWorkersOperations(ctx, args, workers, 0, ophs)
		t.NoError(err)
		t.NoError(opf(func(ctx context.Context, worker string, f func(managerWorkerOperationFunc) error) error {
			if err := f(func(opindex uint64, oph, fact util.Hash) error {
				g(worker, opindex, oph, fact)

				return nil
			}); err != nil {
				return err
			}

			return nil
		}))

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			return u(worker, nil)
		})

		t.NoError(tf(func(tr fixedtree.Tree) error {
			t.Equal(5, tr.Len())

			return nil
		}))

		for i := range workers {
			worker := workers[i]
			wops, found := workersOps.Value(worker)
			t.True(found)

			util.TraverseSliceV1(wops, func(_ int, v [3]interface{}) (bool, error) {
				t.NoError(gdone(worker, v[0].(uint64), v[1].(util.Hash), v[2].(util.Hash)), worker)

				return true, nil
			})
		}
	})
}

func TestManagerWorkersOperations(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerWorkersOperations))
}

type testNewDistributeItemsSerialNShareFunc struct {
	suite.Suite
}

func (t *testNewDistributeItemsSerialNShareFunc) TestCallback() {
	t.Run("zero items", func() {
		workers := uint64(3)

		f, err := newDistributeItemsSerialNShare(workers, 0, 1)
		t.NoError(err)

		for w := uint64(0); w < workers; w++ {
			called := make(chan int, 1)

			t.NoError(f(w, func(of func(func(uint64) error) error) error {
				var count int

				if err := of(func(item uint64) error {
					count++

					return nil
				}); err != nil {
					return err
				}

				called <- count
				return nil
			}))
			t.Equal(0, <-called)
		}
	})

	t.Run("worker has zero items", func() {
		workers := uint64(3)

		f, err := newDistributeItemsSerialNShare(workers, 2, 1)
		t.NoError(err)

		expected := map[uint64]int{
			0: 1,
			1: 1,
			2: 0,
		}

		for w := uint64(0); w < workers; w++ {
			called := make(chan int, 1)

			t.NoError(f(w, func(of func(func(uint64) error) error) error {
				var count int

				var ritems []uint64
				if err := of(func(item uint64) error {
					ritems = append(ritems, item)

					count++

					return nil
				}); err != nil {
					return err
				}
				t.T().Log("collected:", ritems)

				called <- count

				return nil
			}))

			t.Equal(expected[w], <-called)
		}
	})
}

func (t *testNewDistributeItemsSerialNShareFunc) TestError() {
	t.Run("zero workers", func() {
		_, err := newDistributeItemsSerialNShare(0, 1, 1)
		t.ErrorContains(err, "zero workers")
	})

	t.Run("zero share", func() {
		_, err := newDistributeItemsSerialNShare(1, 1, 0)
		t.ErrorContains(err, "zero share")
	})

	t.Run("unknown worker", func() {
		f, err := newDistributeItemsSerialNShare(1, 1, 1)
		t.NoError(err)
		t.NoError(f(0, nil))
		err = f(1, nil)
		t.ErrorContains(err, "unknown worker")
	})
}

func TestNewDistributeItemsSerialNShareFunc(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testNewDistributeItemsSerialNShareFunc))
}

func TestNewDistributeItemsSerialNShareFuncCases(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	cases := []struct {
		workers  uint64
		items    uint64
		share    uint64
		expected map[ /* worker */ uint64][] /* index */ uint64
	}{
		{
			workers: 1, items: 0, share: 1,
			expected: map[uint64][]uint64{},
		},
		{
			workers: 1, items: 1, share: 1,
			expected: map[uint64][]uint64{
				0: {0},
			},
		},
		{
			workers: 1, items: 1, share: 2,
			expected: map[uint64][]uint64{
				0: {0},
			},
		},
		{
			workers: 2, items: 1, share: 9,
			expected: map[uint64][]uint64{
				0: {0},
				1: {0},
			},
		},
		{
			workers: 2, items: 1, share: 1,
			expected: map[uint64][]uint64{
				0: {0},
			},
		},
		{
			workers: 2, items: 1, share: 2,
			expected: map[uint64][]uint64{
				0: {0},
				1: {0},
			},
		},
		{
			workers: 3, items: 3, share: 1,
			expected: map[uint64][]uint64{
				0: {0},
				1: {1},
				2: {2},
			},
		},
		{
			workers: 3, items: 2, share: 1,
			expected: map[uint64][]uint64{
				0: {0},
				1: {1},
				2: nil,
			},
		},
		{
			workers: 3, items: 3, share: 3,
			expected: map[uint64][]uint64{
				0: {0, 1, 2},
				1: {0, 1, 2},
				2: {0, 1, 2},
			},
		},
		{
			workers: 3, items: 4, share: 1,
			expected: map[uint64][]uint64{
				0: {0, 3},
				1: {1},
				2: {2},
			},
		},
		{
			workers: 4, items: 8, share: 2,
			expected: map[uint64][]uint64{
				0: {0, 2, 4, 6},
				1: {0, 2, 4, 6},
				2: {1, 3, 5, 7},
				3: {1, 3, 5, 7},
			},
		},
		{
			workers: 3, items: 8, share: 1,
			expected: map[uint64][]uint64{
				0: {0, 3, 6},
				1: {1, 4, 7},
				2: {2, 5},
			},
		},
		{
			workers: 4, items: 8, share: 1,
			expected: map[uint64][]uint64{
				0: {0, 4},
				1: {1, 5},
				2: {2, 6},
				3: {3, 7},
			},
		},
		{
			workers: 4, items: 7, share: 1,
			expected: map[uint64][]uint64{
				0: {0, 4},
				1: {1, 5},
				2: {2, 6},
				3: {3},
			},
		},
		{
			workers: 4, items: 7, share: 2,
			expected: map[uint64][]uint64{
				0: {0, 2, 4, 6},
				1: {0, 2, 4, 6},
				2: {1, 3, 5},
				3: {1, 3, 5},
			},
		},
		{
			workers: 4, items: 3, share: 2,
			expected: map[uint64][]uint64{
				0: {0, 2},
				1: {0, 2},
				2: {1},
				3: {1},
			},
		},
		{
			workers: 4, items: 3, share: 1,
			expected: map[uint64][]uint64{
				0: {0},
				1: {1},
				2: {2},
				3: nil,
			},
		},
		{
			workers: 4, items: 33, share: 3,
			expected: map[uint64][]uint64{
				0: {0, 1, 2, 4, 5, 6, 8, 9, 10, 12, 13, 14, 16, 17, 18, 20, 21, 22, 24, 25, 26, 28, 29, 30, 32},
				1: {0, 1, 3, 4, 5, 7, 8, 9, 11, 12, 13, 15, 16, 17, 19, 20, 21, 23, 24, 25, 27, 28, 29, 31, 32},
				2: {0, 2, 3, 4, 6, 7, 8, 10, 11, 12, 14, 15, 16, 18, 19, 20, 22, 23, 24, 26, 27, 28, 30, 31, 32},
				3: {1, 2, 3, 5, 6, 7, 9, 10, 11, 13, 14, 15, 17, 18, 19, 21, 22, 23, 25, 26, 27, 29, 30, 31},
			},
		},
	}

	for i, c := range cases {
		i := i
		c := c
		name := fmt.Sprintf("workers=%d items=%d share=%d", c.workers, c.items, c.share)

		t.Run(name, func() {
			f, err := newDistributeItemsSerialNShare(
				c.workers,
				c.items,
				c.share,
			)
			t.NoError(err)

			itemCount := map[uint64]uint64{}
			collected := map[uint64][]uint64{}

			for w := uint64(0); w < c.workers; w++ {
				var ritems []uint64
				t.NoError(f(w, func(of func(func(uint64) error) error) error {
					if of == nil {
						return nil
					}

					return of(func(index uint64) error {
						ritems = append(ritems, index)

						itemCount[index]++

						return nil
					})
				}))

				collected[w] = ritems
			}

			t.T().Log("collected:", collected)

			for w, ritems := range collected {
				items := c.expected[w]

				slices.Sort(items)
				slices.Sort(ritems)

				t.Equal(items, ritems, "%d: %v worker=%d", i, name, w)
			}

			share := c.share
			if share > c.workers {
				share = c.workers
			}

			t.Equal(c.items, uint64(len(itemCount)))

			for index, count := range itemCount {
				if count != share {
					t.Fail("over shared", "%d: %v index=%d items=%d share=%d",
						i, name, index, count, share)
				}
			}
		})
	}
}

func TestNewDistributeItemsSerialNShareFuncCompound(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	maxWorkers := uint64(33)
	maxItems := uint64(33)
	maxShare := uint64(33)

	run := func(workers, items, share uint64) {
		name := fmt.Sprintf("workers=%d items=%d share=%d", workers, items, share)

		t.Run(name, func() {
			f, err := newDistributeItemsSerialNShare(
				workers,
				items,
				share,
			)
			t.NoError(err)

			itemCount := map[uint64]uint64{}

			for w := uint64(0); w < workers; w++ {
				var ritems []uint64
				t.NoError(f(w, func(of func(func(uint64) error) error) error {
					if of == nil {
						return nil
					}

					return of(func(index uint64) error {
						ritems = append(ritems, index)

						itemCount[index]++

						return nil
					})
				}))

				t.T().Logf("collected: worker=%d items=%v", w, ritems)

				if count := uint64(len(ritems)); count > items {
					t.Fail("over items", "%v worker=%d items=%d; %v",
						name, w, count, ritems)
				}
			}

			rshare := share
			if rshare > workers {
				rshare = workers
			}

			t.Equal(items, uint64(len(itemCount)), "%v items=%d counted=%d", name, items, len(itemCount))

			for index, count := range itemCount {
				if count != rshare {
					t.Fail("over shared", "%v index=%d items=%d share=%d",
						name, index, count, rshare)
				}
			}
		})
	}

	for workers := uint64(1); workers < maxWorkers; workers++ {
		for items := uint64(0); items < maxItems; items++ {
			for share := uint64(1); share < maxShare; share++ {
				run(workers, items, share)
			}
		}
	}
}
