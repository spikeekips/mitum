package main

import (
	"context"
	"slices"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/stretchr/testify/suite"
)

type testStatesTreeWriter struct {
	baseTestBaseBlockFileWriter
}

func (t *testStatesTreeWriter) newFunc(size uint64, grow ...uint64) newStatesTreeFuncs {
	return newFuncStatesTreeWriter(size, grow...)
}

func (t *testStatesTreeWriter) TestAdd() {
	t.Run("empty state keys; nil error", func() {
		_, j, d, err := t.newFunc(0)(context.Background())
		t.NoError(err)

		t.NoError(j(nil))

		tr, err := d(nil)
		t.Equal(0, tr.Len())
	})

	t.Run("empty state keys; trying to add", func() {
		a, j, d, err := t.newFunc(0)(context.Background())
		t.NoError(err)
		defer d(nil)

		added, closed, err := a(util.UUID().String())
		t.False(added)
		t.False(closed)
		t.ErrorContains(err, "zero size")

		err = j(nil)
		t.ErrorContains(err, "zero size")

		_, err = d(nil)
		t.ErrorContains(err, "zero size")
	})

	t.Run("canceled context", func() {
		ctx, cancel := context.WithCancel(context.Background())

		a, j, d, err := t.newFunc(1)(ctx)
		t.NoError(err)

		cancel()

		_, closed, err := a(util.UUID().String())
		t.True(closed)
		t.NoError(err)

		err = j(nil)
		t.ErrorIs(err, context.Canceled)

		_, err = d(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("canceled context; before done", func() {
		ctx, cancel := context.WithCancel(context.Background())

		a, j, d, err := t.newFunc(1)(ctx)
		t.NoError(err)
		defer d(nil)

		added, closed, err := a(util.UUID().String())
		t.True(added)
		t.False(closed)
		t.NoError(err)

		cancel()

		err = j(nil)
		t.ErrorIs(err, context.Canceled)

		_, err = d(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("duplicated state keys", func() {
		a, j, d, err := t.newFunc(3)(context.Background())
		t.NoError(err)
		defer d(nil)

		stateKeys := []string{
			util.UUID().String(),
			util.UUID().String(),
		}

		added, closed, err := a(stateKeys[0])
		t.True(added)
		t.False(closed)
		t.NoError(err)
		added, closed, err = a(stateKeys[1])
		t.True(added)
		t.False(closed)
		t.NoError(err)

		added, closed, err = a(stateKeys[0])
		t.False(added)
		t.False(closed)
		t.NoError(err)

		t.NoError(j(nil))

		tr, err := d(nil)
		t.Equal(2, tr.Len())

		slices.Sort(stateKeys)

		tr.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
			t.Equal(stateKeys[index], n.Key())

			return true, nil
		})
	})

	t.Run("add ok, but done error", func() {
		a, j, d, err := t.newFunc(1)(context.Background())
		t.NoError(err)
		defer d(nil)

		added, closed, err := a(util.UUID().String())
		t.True(added)
		t.False(closed)
		t.NoError(err)

		t.NoError(j(nil))

		_, err = d(errors.Errorf("showme"))
		t.ErrorContains(err, "showme")
	})

	t.Run("add ok, no grow", func() {
		a, j, d, err := t.newFunc(1)(context.Background())
		t.NoError(err)
		defer d(nil)

		added, closed, err := a(util.UUID().String())
		t.True(added)
		t.False(closed)
		t.NoError(err)

		added, closed, err = a(util.UUID().String())
		t.False(added)
		t.False(closed)
		t.NoError(err)

		t.NoError(j(nil))

		tr, err := d(nil)
		t.Equal(1, tr.Len())
	})

	t.Run("add after done", func() {
		a, j, d, err := t.newFunc(1)(context.Background())
		t.NoError(err)
		defer d(nil)

		added, closed, err := a(util.UUID().String())
		t.True(added)
		t.False(closed)
		t.NoError(err)

		t.NoError(j(nil))

		added, closed, err = a(util.UUID().String())
		t.False(added)
		t.True(closed)
		t.NoError(err)

		tr, err := d(nil)
		t.Equal(1, tr.Len())

		added, closed, err = a(util.UUID().String())
		t.False(added)
		t.True(closed)
		t.NoError(err)
	})

	t.Run("add after done; async", func() {
		a, j, d, err := t.newFunc(1, 1)(context.Background())
		t.NoError(err)
		defer d(nil)

		added, closed, err := a(util.UUID().String())
		t.True(added)
		t.False(closed)
		t.NoError(err)

		added, closed, err = a(util.UUID().String())
		t.True(added)
		t.False(closed)
		t.NoError(err)

		ch := make(chan struct{}, 1)
		donech := make(chan error, 1)

		go func() {
			<-ch

			tr, err := d(nil)
			t.True(tr.Len() > 2)

			donech <- err
		}()

		for i := 0; i < 9999; i++ {
			if i == 33 {
				t.NoError(j(nil))

				ch <- struct{}{}
			}

			switch _, _, err = a(util.UUID().String()); {
			case err == nil:
			default:
				panic(err)
			}
		}

		t.NoError(<-donech)
	})

	t.Run("single", func() {
		a, j, d, err := t.newFunc(1)(context.Background())
		t.NoError(err)
		defer d(nil)

		stateKey := util.UUID().String()
		added, closed, err := a(stateKey)
		t.True(added)
		t.False(closed)
		t.NoError(err)

		t.NoError(j(nil))

		tr, err := d(nil)
		t.Equal(1, tr.Len())
		t.Equal(stateKey, tr.Nodes()[0].Key())
	})

	t.Run("async", func() {
		var n uint64 = 3333

		a, j, d, err := t.newFunc(n)(context.Background())
		t.NoError(err)
		defer d(nil)

		jw, err := NewBaseJobWorker(context.Background(), int64(n))
		t.NoError(err)

		for i := uint64(0); i < n; i++ {
			t.NoError(jw.NewJob(func(context.Context) error {
				switch added, closed, err := a(util.UUID().String()); {
				case err != nil:
					return err
				case !added, closed:
					if added {
						return errors.Errorf("failed to add")
					}

					if closed {
						return errors.Errorf("closed")
					}
				}

				return nil
			}))
		}

		jw.JobDone()
		t.NoError(jw.Wait())

		t.NoError(j(nil))

		tr, err := d(nil)
		t.Equal(n, uint64(tr.Len()))
	})
}

func TestStatesTreeWriter(t *testing.T) {
	suite.Run(t, new(testStatesTreeWriter))
}
