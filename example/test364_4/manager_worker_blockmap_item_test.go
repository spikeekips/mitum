package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerWorkerBlockMapItem struct {
	baseTestManager
}

func (t *testManagerWorkerBlockMapItem) newArgs() *managerWorkerBlockMapItemsArgs {
	args := newManagerWorkerBlockMapItemsArgs()

	_ = args.SetLogging(logging.TestNilLogging)

	return args
}

func (t *testManagerWorkerBlockMapItem) randomBlockMapItemChunks(n int) []blockMapItemChunk {
	chunks := make([]blockMapItemChunk, n)

	for i := 0; i < n; i++ {
		chunks[i] = newBaseBlockMapItemChunk(util.UUID().String(), util.UUID().String(), "gz")
	}

	return chunks
}

func (t *testManagerWorkerBlockMapItem) TestError() {
	t.Run("cancel before receive", func() {
		args := t.newArgs()
		f := newFuncManagerWorkerBlockMapItems(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())

		workers := t.workers(3)

		cancel()

		receive, done, err := f(ctx, uint64(len(workers)), 0, func(worker string, items map[base.BlockItemType][]blockMapItemChunk) error {
			fmt.Printf("receive: worker=%q items=%v\n", worker, items)

			return nil
		})
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			err := receive(context.Background(), worker, func(f workerSaveBlockMapItemChunksFunc) error {
				return f(base.BlockItemOperations, t.randomBlockMapItemChunks(3))
			})
			t.ErrorIs(err, context.Canceled)

			return true, nil
		})

		err = done()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel during receive", func() {
		args := t.newArgs()
		f := newFuncManagerWorkerBlockMapItems(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		receive, done, err := f(ctx, uint64(len(workers)), 0, func(worker string, items map[base.BlockItemType][]blockMapItemChunk) error {
			fmt.Printf("receive: worker=%q items=%v\n", worker, items)

			return nil
		})
		t.NoError(err)

		var canceled bool

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			if i == 1 {
				cancel()
				canceled = true
			}

			err := receive(context.Background(), worker, func(f workerSaveBlockMapItemChunksFunc) error {
				return f(base.BlockItemOperations, t.randomBlockMapItemChunks(3))
			})

			switch {
			case canceled:
				t.ErrorIs(err, context.Canceled)
			default:
				t.NoError(err)
			}

			return true, nil
		})

		err = done()
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testManagerWorkerBlockMapItem) TestEnoughFailed() {
	t.Run("no failed workers", func() {
		args := t.newArgs()
		f := newFuncManagerWorkerBlockMapItems(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		receive, done, err := f(ctx, uint64(len(workers)), 0, func(worker string, items map[base.BlockItemType][]blockMapItemChunk) error {
			fmt.Printf("receive: worker=%q items=%v\n", worker, items)

			return nil
		})
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			t.NoError(receive(context.Background(), worker, func(f workerSaveBlockMapItemChunksFunc) error {
				return f(base.BlockItemOperations, t.randomBlockMapItemChunks(3))
			}))

			return true, nil
		})

		t.NoError(done())
	})

	t.Run("under max failed", func() {
		args := t.newArgs()
		f := newFuncManagerWorkerBlockMapItems(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		receive, done, err := f(ctx, 2, 2, func(worker string, items map[base.BlockItemType][]blockMapItemChunk) error {
			fmt.Printf("receive: worker=%q items=%v\n", worker, items)

			return nil
		})
		t.NoError(err)

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			t.NoError(receive(context.Background(), worker, func(f workerSaveBlockMapItemChunksFunc) error {
				if i == 1 {
					return errors.Errorf("showme")
				}

				return f(base.BlockItemOperations, t.randomBlockMapItemChunks(3))
			}))

			return true, nil
		})

		t.NoError(done())
	})

	t.Run("over max failed", func() {
		args := t.newArgs()
		f := newFuncManagerWorkerBlockMapItems(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		receive, done, err := f(ctx, 2, 1, func(worker string, items map[base.BlockItemType][]blockMapItemChunk) error {
			fmt.Printf("receive: worker=%q items=%v\n", worker, items)

			return nil
		})
		t.NoError(err)

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			err := receive(context.Background(), worker, func(f workerSaveBlockMapItemChunksFunc) error {
				if i == 1 {
					return errors.Errorf("showme")
				}

				return f(base.BlockItemOperations, t.randomBlockMapItemChunks(3))
			})

			switch {
			case i >= 1:
				t.ErrorContains(err, "showme")
			default:
				t.NoError(err)
			}

			return true, nil
		})

		err = done()
		t.ErrorContains(err, "showme")
	})

	t.Run("under min enough workers", func() {
		args := t.newArgs()
		f := newFuncManagerWorkerBlockMapItems(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		receive, done, err := f(ctx, 4, 0, func(worker string, items map[base.BlockItemType][]blockMapItemChunk) error {
			fmt.Printf("receive: worker=%q items=%v\n", worker, items)

			return nil
		})
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			t.NoError(receive(context.Background(), worker, func(f workerSaveBlockMapItemChunksFunc) error {
				return f(base.BlockItemOperations, t.randomBlockMapItemChunks(3))
			}))

			return true, nil
		})

		donech := make(chan error, 1)
		go func() {
			donech <- done()
		}()

		select {
		case <-time.After(time.Second):
		case err := <-donech:
			t.Fail("unexpected wait done", err)
		}
	})
}

func (t *testManagerWorkerBlockMapItem) TestReceive() {
	t.Run("ok", func() {
		args := t.newArgs()
		f := newFuncManagerWorkerBlockMapItems(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)
		items := t.randomBlockMapItemChunks(9)

		workersItems := map[string]map[base.BlockItemType][]blockMapItemChunk{}

		for i := range workers {
			m := map[base.BlockItemType][]blockMapItemChunk{}
			util.TraverseSliceV1(items[i*3:(i+1)*3], func(_ int, item blockMapItemChunk) (bool, error) {
				m[base.BlockItemType(util.UUID().String())] = []blockMapItemChunk{item}

				return true, nil
			})

			workersItems[workers[i]] = m
		}

		received := util.NewSingleLockedMap[string, map[base.BlockItemType][]blockMapItemChunk]()

		receive, done, err := f(ctx, uint64(len(workers)), 0, func(worker string, items map[base.BlockItemType][]blockMapItemChunk) error {
			fmt.Printf("receive: worker=%q items=%v\n", worker, items)

			received.SetValue(worker, items)

			return nil
		})
		t.NoError(err)

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			t.NoError(receive(context.Background(), worker, func(f workerSaveBlockMapItemChunksFunc) error {
				return TraverseMap(workersItems[worker], func(it base.BlockItemType, items []blockMapItemChunk) (bool, error) {
					return true, f(it, items)
				})
			}))

			return true, nil
		})

		t.NoError(done())

		t.Equal(workersItems, received.Map())
	})

	t.Run("empty items", func() {
		args := t.newArgs()
		f := newFuncManagerWorkerBlockMapItems(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)
		items := t.randomBlockMapItemChunks(9)

		workersItems := map[string]map[base.BlockItemType][]blockMapItemChunk{}

		for i := range workers {
			m := map[base.BlockItemType][]blockMapItemChunk{}
			util.TraverseSliceV1(items[i*3:(i+1)*3], func(_ int, item blockMapItemChunk) (bool, error) {
				m[base.BlockItemType(util.UUID().String())] = []blockMapItemChunk{item}

				return true, nil
			})

			workersItems[workers[i]] = m
		}

		received := util.NewSingleLockedMap[string, map[base.BlockItemType][]blockMapItemChunk]()

		receive, done, err := f(ctx, uint64(len(workers)), 0, func(worker string, items map[base.BlockItemType][]blockMapItemChunk) error {
			fmt.Printf("receive: worker=%q items=%v\n", worker, items)

			if len(items) < 1 {
				return nil
			}

			received.SetValue(worker, items)

			return nil
		})
		t.NoError(err)

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			t.NoError(receive(context.Background(), worker, func(f workerSaveBlockMapItemChunksFunc) error {
				return TraverseMap(workersItems[worker], func(it base.BlockItemType, items []blockMapItemChunk) (bool, error) {
					return true, f(it, items)
				})
			}))

			return true, nil
		})

		t.NoError(done())

		t.Equal(workersItems, received.Map())
	})
}

func TestManagerWorkerBlockMapItem(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerWorkerBlockMapItem))
}
