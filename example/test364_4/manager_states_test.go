package main

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerWorkersStates struct {
	suite.Suite
	baseTestManager
}

func (t *testManagerWorkersStates) newArgs() *managerWorkersStatesArgs {
	args := newManagerWorkersStatesArgs()
	args.NewCheckWorkersFunc = func(minEnough, maxFailed uint64) (
		checkEnoughFailedWorkerFunc,
		func(),
		error,
	) {
		return newCheckEnoughFailedWorkers(minEnough, maxFailed)
	}
	args.NewTreeWriterFuncs = newFuncStatesTreeWriter(1<<9, 1<<9)

	_ = args.SetLogging(logging.TestNilLogging)

	return args
}

func (t *testManagerWorkersStates) TestError() {
	t.Run("ok", func() {
		ctx := context.Background()

		workers := t.workers(1)
		s, tf, err := newManagerWorkersStates(ctx, t.newArgs(), workers, 1, 0)
		t.NoError(err)

		sts := t.stateKeys(3)

		trch := make(chan fixedtree.Tree, 1)
		donech := make(chan error, 1)

		go func() {
			donech <- tf(func(tr fixedtree.Tree) error {
				trch <- tr

				return nil
			})
		}()

		t.NoError(util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			return true, s(ctx, worker, func(f func(stateKey string) error) error {
				for i := range sts {
					if err := f(sts[i]); err != nil {
						return err
					}
				}

				return nil
			})
		}))

		select {
		case <-time.After(time.Second):
			t.Fail("wait tree")
		case tr := <-trch:
			t.Equal(len(sts), tr.Len())

			n := tr.Nodes()
			for i := range sts {
				t.Equal(sts[i], n[i].Key())
			}
		}

		select {
		case <-time.After(time.Second):
			t.Fail("wait done")
		case err := <-donech:
			if err != nil {
				panic(err)
			}
		}

		var called bool

		// NOTE again
		err = tf(func(tr fixedtree.Tree) error {
			called = true

			return nil
		})
		t.ErrorIs(err, context.Canceled)
		t.False(called)
	})

	t.Run("cancel before add states", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)
		s, tf, err := newManagerWorkersStates(ctx, t.newArgs(), workers, 1, 0)
		t.NoError(err)

		cancel()

		_ = util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			sts := t.stateKeys(3)

			err = s(ctx, worker, func(f func(stateKey string) error) error {
				for i := range sts {
					if err := f(sts[i]); err != nil {
						return err
					}
				}

				return nil
			})
			t.ErrorIs(err, context.Canceled)

			return true, nil
		})

		var called bool

		err = tf(func(tr fixedtree.Tree) error {
			called = true

			return nil
		})
		t.ErrorIs(err, context.Canceled)
		t.False(called)
	})

	t.Run("cancel before tree", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)
		s, tf, err := newManagerWorkersStates(ctx, t.newArgs(), workers, 1, 0)
		t.NoError(err)

		sts := t.stateKeys(3)

		_ = util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			return true, s(ctx, worker, func(f func(stateKey string) error) error {
				for i := range sts {
					if err := f(sts[i]); err != nil {
						return err
					}
				}

				return nil
			})
		})

		cancel()

		var called bool

		err = tf(func(tr fixedtree.Tree) error {
			called = true

			return nil
		})
		t.ErrorIs(err, context.Canceled)
		t.False(called)
	})

	t.Run("add error; over max failed", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)
		s, tf, err := newManagerWorkersStates(ctx, t.newArgs(), workers, 3, 0)
		t.NoError(err)

		sts := t.stateKeys(3)

		jw, err := NewBaseJobWorker(ctx, 1<<9)
		t.NoError(err)

		t.NoError(util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			return true, jw.NewJob(func(context.Context) error {
				_ = s(ctx, worker, func(f func(stateKey string) error) error {
					if worker == workers[2] {
						return errors.Errorf("showme")
					}

					for i := range sts {
						if err := f(sts[i]); err != nil {
							return err
						}
					}

					return nil
				})

				return nil
			})
		}))

		jw.JobDone()
		t.NoError(jw.Wait())

		var called bool

		err = tf(func(tr fixedtree.Tree) error {
			called = true

			return nil
		})
		t.ErrorContains(err, "showme")
		t.False(called)
	})

	t.Run("add error; under max failed", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)
		s, tf, err := newManagerWorkersStates(ctx, t.newArgs(), workers, 2, 2)
		t.NoError(err)

		sts := t.stateKeys(3)

		jw, err := NewBaseJobWorker(ctx, 1<<9)
		t.NoError(err)

		t.NoError(util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			return true, jw.NewJob(func(context.Context) error {
				_ = s(ctx, worker, func(f func(stateKey string) error) error {
					if worker == workers[0] {
						return errors.Errorf("showme")
					}

					for i := range sts {
						if err := f(sts[i]); err != nil {
							return err
						}
					}

					return nil
				})

				return nil
			})
		}))

		jw.JobDone()
		t.NoError(jw.Wait())

		var called bool

		t.NoError(tf(func(tr fixedtree.Tree) error {
			called = true

			t.Equal(len(sts), tr.Len())

			n := tr.Nodes()
			for i := range sts {
				t.Equal(sts[i], n[i].Key())
			}

			return nil
		}))
		t.True(called)
	})

	t.Run("tree done error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)
		s, tf, err := newManagerWorkersStates(ctx, t.newArgs(), workers, 3, 0)
		t.NoError(err)

		sts := t.stateKeys(3)

		t.NoError(util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			return true, s(ctx, worker, func(f func(stateKey string) error) error {
				for i := range sts {
					if err := f(sts[i]); err != nil {
						return err
					}
				}

				return nil
			})
		}))

		err = tf(func(tr fixedtree.Tree) error {
			return errors.Errorf("showme")
		})
		t.ErrorContains(err, "showme")

		var called bool

		err = tf(func(tr fixedtree.Tree) error {
			called = true

			return nil
		})
		t.ErrorContains(err, "showme")
		t.False(called)
	})
}

func (t *testManagerWorkersStates) TestAdd() {
	t.Run("async", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(33)
		s, tf, err := newManagerWorkersStates(ctx, t.newArgs(), workers, 33, 0)
		t.NoError(err)

		sts := t.stateKeys(99)

		trch := make(chan fixedtree.Tree, 1)

		go func() {
			if err := tf(func(tr fixedtree.Tree) error {
				trch <- tr

				return nil
			}); err != nil {
				panic(err)
			}
		}()

		jw, err := NewBaseJobWorker(ctx, 1<<9)
		t.NoError(err)
		defer jw.Close()

		t.NoError(util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
			return true, jw.NewJob(func(context.Context) error {
				_ = s(ctx, worker, func(f func(stateKey string) error) error {
					for i := range sts {
						if err := f(sts[i]); err != nil {
							return err
						}
					}

					return nil
				})

				return nil
			})
		}))

		select {
		case <-time.After(time.Second):
			t.Fail("wait tree")
		case tr := <-trch:
			t.Equal(len(sts), tr.Len())

			n := tr.Nodes()
			for i := range sts {
				t.Equal(sts[i], n[i].Key())
			}
		}
	})
}

func TestManagerWorkersStates(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerWorkersStates))
}
