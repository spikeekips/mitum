package main

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testCheckEnoughFailedWorkersFunc struct {
	suite.Suite
}

func (t *testCheckEnoughFailedWorkersFunc) TestNew() {
	t.Run("1 min enough + 0 max failed", func() {
		_, _, err := newCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
	})

	t.Run("0 min enough + 0 max failed", func() {
		_, _, err := newCheckEnoughFailedWorkers(0, 0)
		t.ErrorContains(err, "zero min enough")
	})
}

func (t *testCheckEnoughFailedWorkersFunc) TestEnough() {
	t.Run("enough", func() {
		w, c, err := newCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.NoError(err)
		t.True(ok)
		t.True(enough)
		t.False(failed)
	})

	t.Run("same worker", func() {
		w, c, err := newCheckEnoughFailedWorkers(1, 0)
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
		w, c, err := newCheckEnoughFailedWorkers(1, 0)
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
		w, c, err := newCheckEnoughFailedWorkers(1, 0)
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
		w, c, err := newCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)
	})

	t.Run("failed, over zero", func() {
		w, c, err := newCheckEnoughFailedWorkers(1, 1)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)
	})

	t.Run("error, but under max failed", func() {
		w, c, err := newCheckEnoughFailedWorkers(1, 2)
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
		w, c, err := newCheckEnoughFailedWorkers(1, 0)
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

func TestDistributeByCoun(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	cases := []struct {
		name     string
		total    uint64
		share    uint64
		ks       []string
		expected map[string][]uint64
		err      string
	}{
		{
			name:  "ok",
			total: 3, share: 1,
			ks: []string{"a", "b", "c"},
			expected: map[string][]uint64{
				"a": {0},
				"b": {1},
				"c": {2},
			},
		},
		{
			name:  "zero total",
			total: 0, share: 1,
			err: "empty size",
		},
		{
			name:  "share == total",
			total: 3, share: 3,
			ks: []string{"a", "b", "c"},
			expected: map[string][]uint64{
				"a": {0, 1, 2},
				"b": {0, 1, 2},
				"c": {0, 1, 2},
			},
		},
		{
			name:  "share > total",
			total: 3, share: 4,
			ks: []string{"a", "b", "c"},
			expected: map[string][]uint64{
				"a": {0, 1, 2},
				"b": {0, 1, 2},
				"c": {0, 1, 2},
			},
		},
		{
			name:  "1 total",
			total: 1, share: 1,
			ks: []string{"a", "b", "c"},
			expected: map[string][]uint64{
				"a": {0},
				"b": {0},
				"c": {0},
			},
		},
		{
			name:  "9 total, 3 share",
			total: 9, share: 3,
			ks: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"},
			expected: map[string][]uint64{
				"a": {0, 1, 2},
				"b": {3, 4, 5},
				"c": {6, 7, 8},
				"d": {0, 1, 2},
				"e": {3, 4, 5},
				"f": {6, 7, 8},
				"g": {0, 1, 2},
				"h": {3, 4, 5},
				"i": {6, 7, 8},
			},
		},
		{
			name:  "9 total, 3 share",
			total: 8, share: 3,
			ks: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"},
			expected: map[string][]uint64{
				"a": {0, 1, 2},
				"b": {3, 4, 5},
				"c": {6, 7, 0},
				"d": {1, 2, 3},
				"e": {4, 5, 6},
				"f": {7, 0, 1},
				"g": {2, 3, 4},
				"h": {5, 6, 7},
				"i": {0, 1, 2},
			},
		},
	}

	for i, c := range cases {
		index := i
		c := c

		name := fmt.Sprintf("%s: total=%d share=%d", c.name, c.total, c.share)

		t.Run(name, func() {
			f, closef, err := distributeByCount(
				c.total,
				c.share,
			)
			if closef != nil {
				defer closef()
			}

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

			for i := range c.ks {
				k := c.ks[i]

				poses := f(k)

				t.Equal(c.expected[k], poses,
					"%d: key=%q expected=%v result=%v",
					index,
					k,
					c.expected[k],
					poses,
				)
			}
		})
	}
}
