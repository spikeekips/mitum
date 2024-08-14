package main

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type bsaeTestManager struct {
	suite.Suite
}

func (t *bsaeTestManager) workers(n int) []string {
	w := make([]string, n)

	for i := 0; i < n; i++ {
		w[i] = fmt.Sprintf("w%d", i)
	}

	return w
}

type testCheckEnoughFailedWorkersFunc struct {
	suite.Suite
}

func (t *testCheckEnoughFailedWorkersFunc) TestNew() {
	t.Run("1 min enough + 0 max failed", func() {
		_, _, err := newCheckEnoughFailedWorkersFunc(1, 0)
		t.NoError(err)
	})

	t.Run("0 min enough + 0 max failed", func() {
		_, _, err := newCheckEnoughFailedWorkersFunc(0, 0)
		t.ErrorContains(err, "zero min enough")
	})
}

func (t *testCheckEnoughFailedWorkersFunc) TestEnough() {
	t.Run("enough", func() {
		w, c, err := newCheckEnoughFailedWorkersFunc(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.NoError(err)
		t.True(ok)
		t.True(enough)
		t.False(failed)
	})

	t.Run("same worker", func() {
		w, c, err := newCheckEnoughFailedWorkersFunc(1, 0)
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
		w, c, err := newCheckEnoughFailedWorkersFunc(1, 0)
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
		w, c, err := newCheckEnoughFailedWorkersFunc(1, 0)
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

func (t *testCheckEnoughFailedWorkersFunc) TestFailed() {
	t.Run("failed", func() {
		w, c, err := newCheckEnoughFailedWorkersFunc(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)
	})

	t.Run("failed, over zero", func() {
		w, c, err := newCheckEnoughFailedWorkersFunc(1, 1)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)
	})

	t.Run("error, but under max failed", func() {
		w, c, err := newCheckEnoughFailedWorkersFunc(1, 2)
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
		w, c, err := newCheckEnoughFailedWorkersFunc(1, 0)
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

func TestNewCheckEnoughFailedWorkersFunc(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testCheckEnoughFailedWorkersFunc))
}
