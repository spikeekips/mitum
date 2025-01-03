package main

import (
	"context"
	"slices"
	"testing"

	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerProcess struct {
	dutil.BaseSuite
	baseTestWorkers
	baseTestManager
}

func (t *testManagerProcess) TestCancel() {
	newf := func(ctx context.Context) (
		[]string,
		dutil.InErrorFunc[newWorkerSendOpFunc],
		dutil.In2ErrorFunc[string, []string],
		managerInsertWorkerResultFunc,
		managerProcessWaitFunc,
		error,
	) {
		workers := t.workers(3)

		s, wo, wr, w, err := newManagerProcess(
			ctx,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			workers,
			1, 1,
			t.randomOphs(33),
		)

		return workers, s, wo, wr, w, err
	}

	t.Run("before new", func() {
		ctx, cancel := t.Ctx()
		cancel(nil)

		workers, s, wo, wr, w, err := newf(ctx)
		t.NoError(err)

		err = s(nil)
		t.ErrorIs(err, context.Canceled)

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			err := wo(worker, workers)
			t.ErrorIs(err, context.Canceled)

			return true, nil
		})

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			r, s, d, err := wr(worker)
			t.ErrorIs(err, context.Canceled)
			t.Nil(r)
			t.Nil(s)
			t.Nil(d)

			return true, nil
		})

		_, _, err = w(nil, nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before send", func() {
		ctx, cancel := t.Ctx()

		workers, s, wo, wr, w, err := newf(ctx)
		t.NoError(err)

		cancel(nil)

		err = s(nil)
		t.ErrorIs(err, context.Canceled)

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			err := wo(worker, workers)
			t.ErrorIs(err, context.Canceled)

			return true, nil
		})

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			r, s, d, err := wr(worker)
			t.ErrorIs(err, context.Canceled)
			t.Nil(r)
			t.Nil(s)
			t.Nil(d)

			return true, nil
		})

		_, _, err = w(nil, nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("during send", func() {
		ctx, cancel := t.Ctx()

		workers, s, wo, wr, w, err := newf(ctx)
		t.NoError(err)

		err = s(func(_ context.Context, worker string) (opindexOpHashesFunc, dutil.InErrorFunc[error], error) {
			if worker == workers[1] {
				cancel(nil)
			}

			return func(uint64, util.Hash, util.Hash) error { return nil },
				func(err error) error { return err },
				nil
		})
		if err != nil {
			t.ErrorIs(err, context.Canceled)
		}

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			err := wo(worker, workers)
			if err != nil {
				t.ErrorIs(err, context.Canceled)
			}

			return true, nil
		})

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			r, s, d, err := wr(worker)
			if err != nil {
				t.ErrorIs(err, context.Canceled)
				t.Nil(r)
				t.Nil(s)
				t.Nil(d)
			} else {
				err = d(nil)
				if err != nil {
					t.ErrorIs(err, context.Canceled)
				}
			}

			return true, nil
		})

		_, _, err = w(nil, nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before worker owners", func() {
		ctx, cancel := t.Ctx()

		workers, s, wo, wr, w, err := newf(ctx)
		t.NoError(err)

		t.NoError(s(func(_ context.Context, worker string) (opindexOpHashesFunc, dutil.InErrorFunc[error], error) {
			return func(uint64, util.Hash, util.Hash) error { return nil },
				func(err error) error { return err },
				nil
		}))

		cancel(nil)

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			err := wo(worker, workers)
			t.ErrorIs(err, context.Canceled)

			return true, nil
		})

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			r, s, d, err := wr(worker)
			t.ErrorIs(err, context.Canceled)
			t.Nil(r)
			t.Nil(s)
			t.Nil(d)

			return true, nil
		})

		_, _, err = w(nil, nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("during worker owners", func() {
		ctx, cancel := t.Ctx()

		workers, s, wo, wr, w, err := newf(ctx)
		t.NoError(err)

		t.NoError(s(func(_ context.Context, worker string) (opindexOpHashesFunc, dutil.InErrorFunc[error], error) {
			return func(uint64, util.Hash, util.Hash) error { return nil },
				func(err error) error { return err },
				nil
		}))

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			if worker == workers[1] {
				cancel(nil)
			}

			err := wo(worker, workers)
			if err != nil {
				t.ErrorIs(err, context.Canceled)
			}

			return true, nil
		})

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			r, s, d, err := wr(worker)
			if err != nil {
				t.ErrorIs(err, context.Canceled)
				t.Nil(r)
				t.Nil(s)
				t.Nil(d)
			} else {
				err = d(nil)
				if err != nil {
					t.ErrorIs(err, context.Canceled)
				}
			}

			return true, nil
		})

		_, _, err = w(nil, nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before worker result", func() {
		ctx, cancel := t.Ctx()

		workers, s, wo, wr, w, err := newf(ctx)
		t.NoError(err)

		t.NoError(s(func(_ context.Context, worker string) (opindexOpHashesFunc, dutil.InErrorFunc[error], error) {
			return func(uint64, util.Hash, util.Hash) error { return nil },
				func(err error) error { return err },
				nil
		}))

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			t.NoError(wo(worker, workers))

			return true, nil
		})

		cancel(nil)

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			r, s, d, err := wr(worker)
			t.ErrorIs(err, context.Canceled)
			t.Nil(r)
			t.Nil(s)
			t.Nil(d)

			return true, nil
		})

		_, _, err = w(nil, nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("during worker result", func() {
		ctx, cancel := t.Ctx()

		workers, s, wo, wr, w, err := newf(ctx)
		t.NoError(err)

		t.NoError(s(func(_ context.Context, worker string) (opindexOpHashesFunc, dutil.InErrorFunc[error], error) {
			return func(uint64, util.Hash, util.Hash) error { return nil },
				func(err error) error { return err },
				nil
		}))

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			t.NoError(wo(worker, workers))

			return true, nil
		})

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			if worker == workers[1] {
				cancel(nil)
			}

			r, s, d, err := wr(worker)
			if err != nil {
				t.ErrorIs(err, context.Canceled)
				t.Nil(r)
				t.Nil(s)
				t.Nil(d)
			} else {
				err = d(nil)
				if err != nil {
					t.ErrorIs(err, context.Canceled)
				}
			}

			return true, nil
		})

		_, _, err = w(nil, nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before wait", func() {
		ctx, cancel := t.Ctx()

		workers, s, wo, wr, w, err := newf(ctx)
		t.NoError(err)

		t.NoError(s(func(_ context.Context, worker string) (opindexOpHashesFunc, dutil.InErrorFunc[error], error) {
			return func(uint64, util.Hash, util.Hash) error { return nil },
				func(err error) error { return err },
				nil
		}))

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			t.NoError(wo(worker, workers))

			return true, nil
		})

		dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
			_, _, d, err := wr(worker)
			t.NoError(err)
			t.NoError(d(nil))

			return true, nil
		})

		cancel(nil)

		_, _, err = w(nil, nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testManagerProcess) TestOpsTree() {
	t.Run("ok", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		workers := t.workers(11)
		ophs := t.randomOphs(333)
		var opsShare uint64 = 3

		workersOwners := map[string][]string{}
		var expectedOwners []string
		{
			w, _, err := opsNShareWorkers(workers, opsShare, ophs)
			t.NoError(err)
			opsWorkers := w()
			t.T().Log("ops workers:", opsWorkers)

			_, f, err := newDistributeSerialNShare(uint64(len(opsWorkers)), uint64(len(workers)), opsShare)
			dutil.TraverseSlice(opsWorkers, func(i int, worker string) (bool, error) {
				var owners []string
				f(uint64(i), func(index uint64) error {
					owners = append(owners, workers[index])

					return nil
				})

				workersOwners[worker] = owners
				expectedOwners = append(expectedOwners, owners...)

				return true, nil
			})

			slices.Sort(expectedOwners)
			expectedOwners = slices.Compact(expectedOwners)
		}
		t.T().Log("expected owners:", expectedOwners)

		s, wo, wr, w, err := newManagerProcess(
			ctx,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			workers,
			opsShare, 3,
			ophs,
		)

		t.T().Logf("workers=%d ophs=%d ops share=%d states share=%d",
			len(workers), len(ophs), opsShare, 3)

		go func() {
			t.IfErrPanic(s(func(_ context.Context, worker string) (opindexOpHashesFunc, dutil.InErrorFunc[error], error) {
				return func(uint64, util.Hash, util.Hash) error { return nil },
					func(err error) error {
						t.IfErrPanic(err)

						return err
					},
					nil
			}))

			dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
				owners, found := workersOwners[worker]
				if !found {
					return true, nil
				}

				go func() {
					err := wo(worker, owners)
					t.Println("worker owners: worker=%q, owners=%v err=%v", worker, owners, err)
				}()

				return true, nil
			})

			dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
				go func() {
					_, _, d, err := wr(worker)
					t.IfErrPanic(err)

					if d != nil {
						err := d(nil)
						t.Println("worker result: worker=%q err=%v", worker, err)
					} else {
						t.Println("worker result: worker=%q nil", worker)
					}
				}()

				return true, nil
			})
		}()

		opsTree, statesTree, err := w(
			t.baseGenerateNodesHashFunc(),
			func(owners []string) error {
				slices.Sort(owners)

				t.Equal(expectedOwners, owners)
				return nil
			},
		)
		t.NoError(err)

		t.Equal(len(ophs), opsTree.Len())
		t.Equal(0, statesTree.Len())

		t.NoError(t.checkOpsTree(ophs, nil, opsTree))
		t.NoError(t.checkStatesTree(nil, statesTree))

		t.T().Log("ops tree:", opsTree.Root())
		t.T().Log("states tree:", statesTree.Root())
	})
}

func TestManagerProcess(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerProcess))
}
