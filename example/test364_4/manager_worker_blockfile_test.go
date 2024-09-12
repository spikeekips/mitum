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

type testManagerWorkerBlockFiles struct {
	suite.Suite
	baseTestManager
}

func (t *testManagerWorkerBlockFiles) newArgs() *managerWorkerBlockFilesArgs {
	args := newManagerWorkerBlockFilesArgs()

	_ = args.SetLogging(logging.TestNilLogging)

	return args
}

func (t *testManagerWorkerBlockFiles) randomChecksums(n int) []string {
	cs := make([]string, n)

	for i := 0; i < n; i++ {
		cs[i] = util.UUID().String()
	}

	return cs
}

func (t *testManagerWorkerBlockFiles) TestError() {
	t.Run("cancel before receive", func() {
		args := t.newArgs()
		f := newFuncManagerWorkerBlockFiles(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())

		workers := t.workers(3)

		cancel()

		receive, done, err := f(ctx, uint64(len(workers)), 0, func(worker string, checksums map[base.BlockItemType][]string) error {
			fmt.Printf("receive: worker=%q checksums=%v\n", worker, checksums)

			return nil
		})
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			err := receive(context.Background(), worker, func(f managerWorkerBlockFileReceiveChecksumsFunc) error {
				return f(base.BlockItemOperations, t.randomChecksums(3))
			})
			t.ErrorIs(err, context.Canceled)

			return true, nil
		})

		err = done()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel during receive", func() {
		args := t.newArgs()
		f := newFuncManagerWorkerBlockFiles(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		receive, done, err := f(ctx, uint64(len(workers)), 0, func(worker string, checksums map[base.BlockItemType][]string) error {
			fmt.Printf("receive: worker=%q checksums=%v\n", worker, checksums)

			return nil
		})
		t.NoError(err)

		var canceled bool

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			if i == 1 {
				cancel()
				canceled = true
			}

			err := receive(context.Background(), worker, func(f managerWorkerBlockFileReceiveChecksumsFunc) error {
				return f(base.BlockItemOperations, t.randomChecksums(3))
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

func (t *testManagerWorkerBlockFiles) TestEnoughFailed() {
	t.Run("no failed workers", func() {
		args := t.newArgs()
		f := newFuncManagerWorkerBlockFiles(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		receive, done, err := f(ctx, uint64(len(workers)), 0, func(worker string, checksums map[base.BlockItemType][]string) error {
			fmt.Printf("receive: worker=%q checksums=%v\n", worker, checksums)

			return nil
		})
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			t.NoError(receive(context.Background(), worker, func(f managerWorkerBlockFileReceiveChecksumsFunc) error {
				return f(base.BlockItemOperations, t.randomChecksums(3))
			}))

			return true, nil
		})

		t.NoError(done())
	})

	t.Run("under max failed", func() {
		args := t.newArgs()
		f := newFuncManagerWorkerBlockFiles(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		receive, done, err := f(ctx, 2, 2, func(worker string, checksums map[base.BlockItemType][]string) error {
			fmt.Printf("receive: worker=%q checksums=%v\n", worker, checksums)

			return nil
		})
		t.NoError(err)

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			t.NoError(receive(context.Background(), worker, func(f managerWorkerBlockFileReceiveChecksumsFunc) error {
				if i == 1 {
					return errors.Errorf("showme")
				}

				return f(base.BlockItemOperations, t.randomChecksums(3))
			}))

			return true, nil
		})

		t.NoError(done())
	})

	t.Run("over max failed", func() {
		args := t.newArgs()
		f := newFuncManagerWorkerBlockFiles(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		receive, done, err := f(ctx, 2, 1, func(worker string, checksums map[base.BlockItemType][]string) error {
			fmt.Printf("receive: worker=%q checksums=%v\n", worker, checksums)

			return nil
		})
		t.NoError(err)

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			err := receive(context.Background(), worker, func(f managerWorkerBlockFileReceiveChecksumsFunc) error {
				if i == 1 {
					return errors.Errorf("showme")
				}

				return f(base.BlockItemOperations, t.randomChecksums(3))
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
		f := newFuncManagerWorkerBlockFiles(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		receive, done, err := f(ctx, 4, 0, func(worker string, checksums map[base.BlockItemType][]string) error {
			fmt.Printf("receive: worker=%q checksums=%v\n", worker, checksums)

			return nil
		})
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			t.NoError(receive(context.Background(), worker, func(f managerWorkerBlockFileReceiveChecksumsFunc) error {
				return f(base.BlockItemOperations, t.randomChecksums(3))
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

func (t *testManagerWorkerBlockFiles) TestReceive() {
	t.Run("ok", func() {
		args := t.newArgs()
		f := newFuncManagerWorkerBlockFiles(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)
		checksums := t.randomChecksums(9)

		workersChecksums := map[string]map[base.BlockItemType][]string{}

		for i := range workers {
			m := map[base.BlockItemType][]string{}
			util.TraverseSliceV1(checksums[i*3:(i+1)*3], func(_ int, c string) (bool, error) {
				m[base.BlockItemType(util.UUID().String())] = []string{c}

				return true, nil
			})

			workersChecksums[workers[i]] = m
		}

		received := util.NewSingleLockedMap[string, map[base.BlockItemType][]string]()

		receive, done, err := f(ctx, uint64(len(workers)), 0, func(worker string, checksums map[base.BlockItemType][]string) error {
			fmt.Printf("receive: worker=%q checksums=%v\n", worker, checksums)

			received.SetValue(worker, checksums)

			return nil
		})
		t.NoError(err)

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			t.NoError(receive(context.Background(), worker, func(f managerWorkerBlockFileReceiveChecksumsFunc) error {
				return TraverseMap(workersChecksums[worker], func(it base.BlockItemType, checksums []string) (bool, error) {
					return true, f(it, checksums)
				})
			}))

			return true, nil
		})

		t.NoError(done())

		t.Equal(workersChecksums, received.Map())
	})

	t.Run("empty checksums", func() {
		args := t.newArgs()
		f := newFuncManagerWorkerBlockFiles(args, newCheckEnoughFailedWorkers)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)
		checksums := t.randomChecksums(9)

		workersChecksums := map[string]map[base.BlockItemType][]string{}

		for i := range workers {
			m := map[base.BlockItemType][]string{}
			util.TraverseSliceV1(checksums[i*3:(i+1)*3], func(_ int, c string) (bool, error) {
				m[base.BlockItemType(util.UUID().String())] = []string{c}

				return true, nil
			})

			workersChecksums[workers[i]] = m
		}

		received := util.NewSingleLockedMap[string, map[base.BlockItemType][]string]()

		receive, done, err := f(ctx, uint64(len(workers)), 0, func(worker string, checksums map[base.BlockItemType][]string) error {
			fmt.Printf("receive: worker=%q checksums=%v\n", worker, checksums)

			if len(checksums) < 1 {
				return nil
			}

			received.SetValue(worker, checksums)

			return nil
		})
		t.NoError(err)

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			t.NoError(receive(context.Background(), worker, func(f managerWorkerBlockFileReceiveChecksumsFunc) error {
				return TraverseMap(workersChecksums[worker], func(it base.BlockItemType, checksums []string) (bool, error) {
					return true, f(it, checksums)
				})
			}))

			return true, nil
		})

		t.NoError(done())

		t.Equal(workersChecksums, received.Map())
	})
}

func TestManagerWorkerBlockFiles(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerWorkerBlockFiles))
}
