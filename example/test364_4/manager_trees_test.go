package main

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testNewManagerWorkersTreesFuncs struct {
	baseTestManager
}

func (t *testNewManagerWorkersTreesFuncs) newArgs() *managerWorkersTreesArgs {
	args := newManagerWorkersTreesArgs()
	args.NewCheckWorkersFunc = func(minEnough, maxFailed uint64) (
		checkEnoughFailedWorkerFunc,
		func(),
		error,
	) {
		return newCheckEnoughFailedWorkersFunc(minEnough, maxFailed)
	}
	args.NewWorkersOpsFunc = serialNoShareWorkerOps
	args.NewOperationsTreeWriterFuncs = newOperationsTreeWriterFunc()
	args.NewStatesTreeWriterFuncs = newStatesTreeWriterFunc(1<<9, 1<<9)
	args.DistributeWorkerOps = func(context.Context, string, func(managerWorkerOperationFunc) error) error {
		return nil
	}
	args.WriteBlockItemOperationsTree = func(context.Context, string, fixedtree.Tree) (base.BlockMapItem, error) {
		return nil, nil
	}
	args.WriteBlockItemStatesTree = func(context.Context, string, fixedtree.Tree) (base.BlockMapItem, error) {
		return nil, nil
	}

	return args
}

func (t *testNewManagerWorkersTreesFuncs) TestError() {
	t.Run("canceled before new", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		workers := t.workers(3)
		u, s, tree, err := newManagerWorkersTreesFuncs(ctx, t.newArgs(), workers, 3, 0, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, []uint64{0})
			t.ErrorIs(err, context.Canceled)

			err = s(ctx, worker, nil)
			t.ErrorIs(err, context.Canceled)

			return true, nil
		})

		opsR, statesR, save, err := tree()
		t.ErrorIs(err, context.Canceled)
		t.Nil(opsR)
		t.Nil(statesR)
		t.Nil(save)
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

		u, s, tree, err := newManagerWorkersTreesFuncs(ctx, args, workers, 3, 0, util.UUID().String(), t.ophs(3))
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

		opsR, statesR, save, err := tree()
		t.ErrorIs(err, context.Canceled)
		t.Nil(opsR)
		t.Nil(statesR)
		t.Nil(save)
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

		u, s, tree, err := newManagerWorkersTreesFuncs(ctx, args, workers, 3, 0, util.UUID().String(), t.ophs(3))
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

		opsR, statesR, save, err := tree()
		t.ErrorIs(err, context.Canceled)
		t.Nil(opsR)
		t.Nil(statesR)
		t.Nil(save)
	})

	t.Run("canceled after unmerge", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		args := t.newArgs()

		u, s, tree, err := newManagerWorkersTreesFuncs(ctx, args, workers, 3, 0, util.UUID().String(), t.ophs(3))
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

		opsR, statesR, save, err := tree()
		t.ErrorIs(err, context.Canceled)
		t.Nil(opsR)
		t.Nil(statesR)
		t.Nil(save)
	})

	t.Run("canceled after state keys", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		args := t.newArgs()

		u, s, tree, err := newManagerWorkersTreesFuncs(ctx, args, workers, 3, 0, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, []uint64{0})
			t.NoError(err)

			t.NoError(s(ctx, worker, nil))

			return true, nil
		})

		cancel()

		opsR, statesR, save, err := tree()
		t.ErrorIs(err, context.Canceled)
		t.Nil(opsR)
		t.Nil(statesR)
		t.Nil(save)
	})

	t.Run("canceled before save", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		args := t.newArgs()

		u, s, tree, err := newManagerWorkersTreesFuncs(ctx, args, workers, 3, 0, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, []uint64{0})
			t.NoError(err)

			t.NoError(s(ctx, worker, nil))

			return true, nil
		})

		opsR, statesR, save, err := tree()
		t.NoError(err)
		t.NotNil(opsR)
		t.Nil(statesR)

		cancel()

		opsB, statesB, err := save()
		t.ErrorIs(err, context.Canceled)
		t.Nil(opsB)
		t.Nil(statesB)
	})
}

func (t *testNewManagerWorkersTreesFuncs) TestEnoughFailed() {
	t.Run("over min enough", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)
		u, s, tree, err := newManagerWorkersTreesFuncs(ctx, t.newArgs(), workers, 3, 0, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, []uint64{0})
			t.NoError(err)

			t.NoError(s(ctx, worker, nil))

			return true, nil
		})

		opsR, statesR, save, err := tree()
		t.NoError(err)
		t.NotNil(opsR)
		t.Nil(statesR)

		_, _, err = save()
		t.NoError(err)
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

		u, s, tree, err := newManagerWorkersTreesFuncs(ctx, args, workers, 2, 2, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, []uint64{0})
			t.NoError(err)

			t.NoError(s(ctx, worker, nil))

			return true, nil
		})

		opsR, statesR, save, err := tree()
		t.NoError(err)
		t.NotNil(opsR)
		t.Nil(statesR)

		_, _, err = save()
		t.NoError(err)
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

		u, s, tree, err := newManagerWorkersTreesFuncs(ctx, args, workers, 3, 0, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, _ = u(worker, []uint64{0})

			_ = s(ctx, worker, nil)

			return true, nil
		})

		_, _, _, err = tree()
		t.ErrorContains(err, "showme")
	})
}

func (t *testNewManagerWorkersTreesFuncs) newOpsBlockItemWriter() blockItemWriterNewWriteTreeFunc {
	createWriter, _ := t.newCreateWriter()

	return newBlockItemOperationsTreeWriterFunc(t.enc, createWriter)
}

func (t *testNewManagerWorkersTreesFuncs) newStatesBlockItemWriter() blockItemWriterNewWriteTreeFunc {
	createWriter, _ := t.newCreateWriter()
	return newBlockItemStatesTreeWriterFunc(t.enc, createWriter)
}

func (t *testNewManagerWorkersTreesFuncs) TestTree() {
	t.Run("empty ophs", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		args := t.newArgs()

		args.WriteBlockItemOperationsTree = t.newOpsBlockItemWriter()
		args.WriteBlockItemStatesTree = t.newStatesBlockItemWriter()

		u, s, tree, err := newManagerWorkersTreesFuncs(ctx, args, workers, 3, 0, util.UUID().String(), nil)
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, nil)
			t.NoError(err)

			err = s(ctx, worker, nil)
			t.NoError(err)

			return true, nil
		})

		opsR, statesR, save, err := tree()
		t.NoError(err)
		t.Nil(opsR)
		t.Nil(statesR)

		opsB, statesB, err := save()
		t.NoError(err)
		t.Nil(opsB)
		t.Nil(statesB)
	})

	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		args := t.newArgs()
		args.WriteBlockItemOperationsTree = t.newOpsBlockItemWriter()
		args.WriteBlockItemStatesTree = t.newStatesBlockItemWriter()

		u, s, tree, err := newManagerWorkersTreesFuncs(ctx, args, workers, 3, 0, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		statesKeys := t.states(3)

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

		opsR, statesR, save, err := tree()
		t.NoError(err)
		t.NotNil(opsR)
		t.NotNil(statesR)

		opsB, statesB, err := save()
		t.NoError(err)
		t.NotNil(opsB)
		t.NotNil(statesB)
	})

	t.Run("no states", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		args := t.newArgs()
		args.WriteBlockItemOperationsTree = t.newOpsBlockItemWriter()
		args.WriteBlockItemStatesTree = t.newStatesBlockItemWriter()

		u, s, tree, err := newManagerWorkersTreesFuncs(ctx, args, workers, 3, 0, util.UUID().String(), t.ophs(3))
		t.NoError(err)

		util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			_, err := u(worker, []uint64{0})
			t.NoError(err)

			err = s(ctx, worker, nil)
			t.NoError(err)

			return true, nil
		})

		opsR, statesR, save, err := tree()
		t.NoError(err)
		t.NotNil(opsR)
		t.Nil(statesR)

		opsB, statesB, err := save()
		t.NoError(err)
		t.NotNil(opsB)
		t.Nil(statesB)
	})
}

func TestNewManagerWorkersTreesFuncs(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testNewManagerWorkersTreesFuncs))
}
