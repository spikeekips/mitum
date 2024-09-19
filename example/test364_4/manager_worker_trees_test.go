package main

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerWorkersTrees struct {
	suite.Suite
	baseTestManager
}

func (t *testManagerWorkersTrees) newArgs() *managerWorkersTreesArgs {
	args := newManagerWorkersTreesArgs()
	args.NewCheckWorkersFunc = func(minEnough, maxFailed uint64) (
		checkEnoughFailedWorkerFunc,
		func(),
		error,
	) {
		return newCheckEnoughFailedWorkers(minEnough, maxFailed)
	}
	args.NewWorkersOpsFunc = distributeNShareWorkerOps
	args.NewOperationsTreeWriterFuncs = newFuncOperationsTreeWriter()
	args.NewStatesTreeWriterFuncs = newFuncStatesTreeWriter(1<<9, 1<<9)
	args.DistributeWorkerOps = func(context.Context, string, func(managerWorkerOperationFunc) error) error {
		return nil
	}

	return args
}

func (t *testManagerWorkersTrees) TestError() {
	t.Run("canceled before new", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		workers := t.workers(3)

		f := newFuncManagerWorkersTrees(t.newArgs())

		u, s, tree, err := f(ctx, workers, 1, 1, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, []uint64{0})
			t.ErrorIs(err, context.Canceled)

			err = s(ctx, worker, nil)
			t.ErrorIs(err, context.Canceled)

			return true, nil
		})

		ops, states, err := tree()
		t.ErrorIs(err, context.Canceled)
		t.Equal(0, ops.Len())
		t.Equal(0, states.Len())
		t.Nil(ops.Root())
		t.Nil(states.Root())
	})

	t.Run("canceled during distribute", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		args := t.newArgs()

		args.DistributeWorkerOps = func(context.Context, string, func(managerWorkerOperationFunc) error) error {
			cancel()

			return nil
		}

		f := newFuncManagerWorkersTrees(args)
		u, s, tree, err := f(ctx, workers, 1, 1, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		<-ctx.Done()

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			if _, err := u(worker, []uint64{0}); err != nil {
				t.ErrorIs(err, context.Canceled)
			}

			err = s(ctx, worker, nil)
			t.ErrorIs(err, context.Canceled)

			return true, nil
		})

		ops, states, err := tree()
		t.ErrorIs(err, context.Canceled)
		t.Equal(0, ops.Len())
		t.Equal(0, states.Len())
		t.Nil(ops.Root())
		t.Nil(states.Root())
	})

	t.Run("canceled after distribute", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		args := t.newArgs()
		var count int64

		args.DistributeWorkerOps = func(_ context.Context, worker string, _ func(managerWorkerOperationFunc) error) error {
			atomic.AddInt64(&count, 1)
			if atomic.LoadInt64(&count) == int64(len(workers)) {
				cancel()
			}

			return nil
		}

		f := newFuncManagerWorkersTrees(args)
		u, s, tree, err := f(ctx, workers, 1, 1, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		<-ctx.Done()
		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, []uint64{0})
			if err != nil {
				t.ErrorIs(err, context.Canceled)
			}

			err = s(ctx, worker, nil)
			if err != nil {
				t.ErrorIs(err, context.Canceled)
			}

			return true, nil
		})

		ops, states, err := tree()
		t.ErrorIs(err, context.Canceled)
		t.Equal(0, ops.Len())
		t.Equal(0, states.Len())
		t.Nil(ops.Root())
		t.Nil(states.Root())
	})

	t.Run("canceled after unmerge", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		args := t.newArgs()

		f := newFuncManagerWorkersTrees(args)
		u, s, tree, err := f(ctx, workers, 1, 1, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, []uint64{0})
			t.NoError(err)

			return true, nil
		})

		cancel()

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			err := s(ctx, worker, nil)
			t.ErrorIs(err, context.Canceled)

			return true, nil
		})

		ops, states, err := tree()
		t.ErrorIs(err, context.Canceled)
		t.Equal(0, ops.Len())
		t.Equal(0, states.Len())
		t.Nil(ops.Root())
		t.Nil(states.Root())
	})

	t.Run("canceled after state keys", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		args := t.newArgs()

		f := newFuncManagerWorkersTrees(args)
		u, s, tree, err := f(ctx, workers, 1, 1, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, []uint64{0})
			t.NoError(err)

			t.NoError(s(ctx, worker, nil))

			return true, nil
		})

		cancel()

		ops, states, err := tree()
		t.ErrorIs(err, context.Canceled)
		t.Equal(0, ops.Len())
		t.Equal(0, states.Len())
		t.Nil(ops.Root())
		t.Nil(states.Root())
	})
}

func (t *testManagerWorkersTrees) TestEnoughFailed() {
	t.Run("over min enough", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		f := newFuncManagerWorkersTrees(t.newArgs())
		u, s, tree, err := f(ctx, workers, 1, 1, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, []uint64{0})
			t.NoError(err)

			t.NoError(s(ctx, worker, nil))

			return true, nil
		})

		ops, states, err := tree()
		t.NoError(err)

		t.NotEqual(0, ops.Len())
		t.Equal(0, states.Len())
		t.NotNil(ops.Root())
		t.Nil(states.Root())
	})

	t.Run("over min enough; one failed", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		args := t.newArgs()
		args.DistributeWorkerOps = func(_ context.Context, worker string, _ func(managerWorkerOperationFunc) error) error {
			if worker == workers[2] {
				return errors.Errorf("showme")
			}

			return nil
		}

		f := newFuncManagerWorkersTrees(args)
		u, s, tree, err := f(ctx, workers, 2, 1, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, []uint64{0})
			t.NoError(err)

			t.NoError(s(ctx, worker, nil))

			return true, nil
		})

		ops, states, err := tree()
		t.NoError(err)

		t.NotEqual(0, ops.Len())
		t.Equal(0, states.Len())
		t.NotNil(ops.Root())
		t.Nil(states.Root())
	})

	t.Run("over max failed", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		args := t.newArgs()
		args.DistributeWorkerOps = func(_ context.Context, worker string, _ func(managerWorkerOperationFunc) error) error {
			if worker == workers[2] {
				return errors.Errorf("showme")
			}

			return nil
		}

		f := newFuncManagerWorkersTrees(args)
		u, s, tree, err := f(ctx, workers, 1, 1, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, _ = u(worker, []uint64{0})

			_ = s(ctx, worker, nil)

			return true, nil
		})

		_, _, err = tree()
		t.ErrorContains(err, "showme")
	})
}

func (t *testManagerWorkersTrees) newOpsBlockFileWriter() blockFileWriterTreeFunc {
	createWriter, _ := t.newCreateWriter()

	return newFuncBlockFileOperationsTreeWriter(t.enc, createWriter, nil, nil)
}

func (t *testManagerWorkersTrees) newStatesBlockFileWriter() blockFileWriterTreeFunc {
	createWriter, _ := t.newCreateWriter()
	return newFuncBlockFileStatesTreeWriter(t.enc, createWriter, nil, nil)
}

func (t *testManagerWorkersTrees) TestTree() {
	t.Run("empty ophs", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		args := t.newArgs()

		f := newFuncManagerWorkersTrees(args)
		u, s, tree, err := f(ctx, workers, 1, 1, util.UUID().String(), nil)
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, nil)
			t.NoError(err)

			err = s(ctx, worker, nil)
			t.NoError(err)

			return true, nil
		})

		ops, states, err := tree()
		t.NoError(err)

		t.Equal(0, ops.Len())
		t.Equal(0, states.Len())
		t.Nil(ops.Root())
		t.Nil(states.Root())
	})

	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		args := t.newArgs()

		f := newFuncManagerWorkersTrees(args)
		u, s, tree, err := f(ctx, workers, 1, 1, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		statesKeys := t.stateKeys(3)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, []uint64{0})
			t.NoError(err)

			err = s(ctx, worker, func(f func(stateKey string) error) error {
				for i := range statesKeys {
					if err := f(statesKeys[i]); err != nil {
						return err
					}
				}

				return nil
			})
			t.NoError(err)

			return true, nil
		})

		ops, states, err := tree()
		t.NoError(err)

		t.NotEqual(0, ops.Len())
		t.NotEqual(0, states.Len())
		t.NotNil(ops.Root())
		t.NotNil(states.Root())
	})

	t.Run("no states", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		args := t.newArgs()

		f := newFuncManagerWorkersTrees(args)
		u, s, tree, err := f(ctx, workers, 1, 1, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, []uint64{0})
			t.NoError(err)

			err = s(ctx, worker, nil)
			t.NoError(err)

			return true, nil
		})

		ops, states, err := tree()
		t.NoError(err)

		t.NotEqual(0, ops.Len())
		t.Equal(0, states.Len())
		t.NotNil(ops.Root())
		t.Nil(states.Root())
	})
}

func TestManagerWorkersTrees(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerWorkersTrees))
}
