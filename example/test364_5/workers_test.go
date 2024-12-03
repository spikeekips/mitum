package main

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type baseTestWorkers struct{}

func (t *baseTestWorkers) workers(n int) []string {
	w := make([]string, n)

	for i := 0; i < n; i++ {
		w[i] = fmt.Sprintf("w%d", i)
	}

	return w
}

func (t *baseTestWorkers) toCtxCancel(f func()) inErrorFunc[error] {
	return func(err error) error {
		f()

		return err
	}
}

type testCheckEnoughFailedWorkers struct {
	suite.Suite
}

func (t *testCheckEnoughFailedWorkers) TestNew() {
	t.Run("1 min enough + 0 max failed", func() {
		_, _, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
	})

	t.Run("0 min enough + 0 max failed", func() {
		_, _, err := newFuncCheckEnoughFailedWorkers(0, 0)
		t.ErrorContains(err, "zero min enough")
	})
}

func (t *testCheckEnoughFailedWorkers) TestEnough() {
	t.Run("enough", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.NoError(err)
		t.True(ok)
		t.True(enough)
		t.False(failed)
	})

	t.Run("same worker", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.True(ok)
		t.True(enough)
		t.False(failed)

		ok, enough, failed, err = w("w0", func() error { return nil })
		t.False(ok)
		t.False(enough)
		t.False(failed)
	})

	t.Run("ok, but after closed", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.True(ok)
		t.True(enough)
		t.False(failed)

		ok, enough, failed, err = w("w1", func() error { return nil })
		t.False(ok)
		t.True(enough)
		t.False(failed)
	})

	t.Run("error, but after closed", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.True(ok)
		t.True(enough)
		t.False(failed)

		ok, enough, failed, err = w("w1", func() error { return errors.Errorf("showme") })
		t.False(ok)
		t.True(enough)
		t.False(failed)
	})
}

func (t *testCheckEnoughFailedWorkers) TestEnoughUntil() {
	t.Run("enough, but not ready", func() {
		w, c, err := newFuncBaseCheckEnoughFailedWorkers(1, 0, func(worker string) bool { return worker == "w1" })
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.NoError(err)
		t.True(ok)
		t.False(enough)
		t.False(failed)

		ok, enough, failed, err = w("w1", func() error { return nil })
		t.NoError(err)
		t.True(ok)
		t.True(enough)
		t.False(failed)
	})

	t.Run("enough, not ready", func() {
		w, c, err := newFuncBaseCheckEnoughFailedWorkers(1, 0, func(worker string) bool { return worker == "w0" })
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.NoError(err)
		t.True(ok)
		t.True(enough)
		t.False(failed)
	})

	t.Run("failed and ready", func() {
		w, c, err := newFuncBaseCheckEnoughFailedWorkers(1, 0, func(worker string) bool { return worker == "w1" })
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.ErrorContains(err, "showme")
		t.True(ok)
		t.False(enough)
		t.True(failed)

		ok, enough, failed, err = w("w1", func() error { return nil })
		t.ErrorIs(err, errWorkersFailed)
		t.False(ok)
		t.False(enough)
		t.True(failed)
	})

	t.Run("ready, but failed", func() {
		w, c, err := newFuncBaseCheckEnoughFailedWorkers(1, 0, func(worker string) bool { return worker == "w1" })
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.NoError(err)
		t.True(ok)
		t.False(enough)
		t.False(failed)

		ok, enough, failed, err = w("w1", func() error { return errors.Errorf("showme") })
		t.ErrorContains(err, "showme")
		t.True(ok)
		t.False(enough)
		t.True(failed)
	})
}

func (t *testCheckEnoughFailedWorkers) TestFailed() {
	t.Run("failed", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)
		t.ErrorContains(err, "showme")
	})

	t.Run("error after failed", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)
		t.ErrorContains(err, "showme")

		ok, enough, failed, err = w("w1", func() error { return nil })
		t.False(ok)
		t.False(enough)
		t.True(failed)
		t.ErrorIs(err, errWorkersFailed)
	})

	t.Run("failed, over zero", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 1)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)
	})

	t.Run("error, but under max failed", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 2)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.False(failed)

		ok, enough, failed, err = w("w1", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)
	})

	t.Run("ok, but after closed", func() {
		w, c, err := newFuncCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)

		ok, enough, failed, err = w("w1", func() error { return nil })
		t.False(ok)
		t.False(enough)
		t.True(failed)
	})
}

func TestCheckEnoughFailedWorkersFunc(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testCheckEnoughFailedWorkers))
}

func TestMinEnoughMaxFailed(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	cases := []struct {
		workers   uint64
		share     uint64
		err       string
		minEnough uint64
		maxFailed uint64
	}{
		{workers: 0, share: 1, err: "zero workers"},
		{workers: 1, share: 0, err: "zero share"},
		{workers: 1, share: 1, minEnough: 1, maxFailed: 0},
		{workers: 1, share: 2, minEnough: 1, maxFailed: 0},
		{workers: 2, share: 1, minEnough: 2, maxFailed: 0},
		{workers: 2, share: 2, minEnough: 1, maxFailed: 1},
	}

	for i, c := range cases {
		index := i
		c := c

		name := fmt.Sprintf("workers=%d share=%d", c.workers, c.share)

		t.Run(name, func() {
			me, mf, err := minEnoughMaxFailed(c.workers, c.share)

			if c.err != "" {
				if err == nil {
					t.Fail("no error", `%d: expected=%q`, index, c.err)

					return
				}

				t.ErrorContains(err, c.err, "%d", index)

				return
			} else if err != nil {
				t.Fail("unexpected error", `%d: error=%+v`, index, err)

				return
			}

			t.Equal(c.minEnough, me, "%s(%d): min enough; expected=%d result=%d", name, index, c.minEnough, me)
			t.Equal(c.maxFailed, mf, "%s(%d): max failed; expected=%d result=%d", name, index, c.maxFailed, mf)
		})
	}
}

func fixedStateOwnersFunc(owners map[string][]string) (getStateOwnersFunc, func(), error) {
	return func(stateKey string) []string {
			return owners[stateKey]
		},
		func() {},
		nil
}
