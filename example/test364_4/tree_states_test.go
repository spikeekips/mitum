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

type testNewStatesTreeWriterFunc struct {
	baseTestBaseBlockItemWriterFunc
}

func (t *testNewStatesTreeWriterFunc) newFunc() newStatesTreeFuncs {
	return newStatesTreeWriterFunc()
}

func (t *testNewStatesTreeWriterFunc) TestAdd() {
	t.Run("empty state keys; nil error", func() {
		_, d, err := t.newFunc()(context.Background(), 0)
		t.NoError(err)

		tr, err := d(nil)
		t.Equal(0, tr.Len())
	})

	t.Run("empty state keys; trying to add", func() {
		a, d, err := t.newFunc()(context.Background(), 0)
		t.NoError(err)
		defer d(nil)

		added, err := a(util.UUID().String())
		t.False(added)
		t.ErrorContains(err, "zero size")

		_, err = d(nil)
		t.ErrorContains(err, "zero size")
	})

	t.Run("canceled context", func() {
		ctx, cancel := context.WithCancel(context.Background())

		a, d, err := t.newFunc()(ctx, 1)
		t.NoError(err)

		cancel()

		_, err = a(util.UUID().String())
		t.ErrorIs(err, context.Canceled)

		_, err = d(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("canceled context; before done", func() {
		ctx, cancel := context.WithCancel(context.Background())

		a, d, err := t.newFunc()(ctx, 1)
		t.NoError(err)
		defer d(nil)

		added, err := a(util.UUID().String())
		t.True(added)
		t.NoError(err)

		cancel()

		_, err = d(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("duplicated state keys", func() {
		a, d, err := t.newFunc()(context.Background(), 3)
		t.NoError(err)
		defer d(nil)

		stateKeys := []string{
			util.UUID().String(),
			util.UUID().String(),
		}

		added, err := a(stateKeys[0])
		t.True(added)
		t.NoError(err)
		added, err = a(stateKeys[1])
		t.True(added)
		t.NoError(err)

		added, err = a(stateKeys[0])
		t.False(added)
		t.NoError(err)

		tr, err := d(nil)
		t.Equal(2, tr.Len())

		slices.Sort(stateKeys)

		tr.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
			t.Equal(stateKeys[index], n.Key())

			return true, nil
		})
	})

	t.Run("add ok, but done error", func() {
		a, d, err := t.newFunc()(context.Background(), 1)
		t.NoError(err)
		defer d(nil)

		added, err := a(util.UUID().String())
		t.True(added)
		t.NoError(err)

		_, err = d(errors.Errorf("showme"))
		t.ErrorContains(err, "showme")
	})

	t.Run("add ok, no grow", func() {
		a, d, err := t.newFunc()(context.Background(), 1)
		t.NoError(err)
		defer d(nil)

		added, err := a(util.UUID().String())
		t.True(added)
		t.NoError(err)

		added, err = a(util.UUID().String())
		t.False(added)
		t.NoError(err)

		tr, err := d(nil)
		t.Equal(1, tr.Len())
	})

	t.Run("add during done", func() {
		a, d, err := t.newFunc()(context.Background(), 1)
		t.NoError(err)
		defer d(nil)

		added, err := a(util.UUID().String())
		t.True(added)
		t.NoError(err)

		tr, err := d(nil)
		added, err = a(util.UUID().String())
		t.False(added)
		t.ErrorIs(err, context.Canceled)

		t.Equal(1, tr.Len())
	})

	t.Run("add after done", func() {
		a, d, err := t.newFunc()(context.Background(), 1)
		t.NoError(err)
		defer d(nil)

		added, err := a(util.UUID().String())
		t.True(added)
		t.NoError(err)

		tr, err := d(nil)
		t.Equal(1, tr.Len())

		added, err = a(util.UUID().String())
		t.False(added)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("add after done; async", func() {
		a, d, err := t.newFunc()(context.Background(), 1, 1)
		t.NoError(err)
		defer d(nil)

		added, err := a(util.UUID().String())
		t.True(added)
		t.NoError(err)

		added, err = a(util.UUID().String())
		t.True(added)
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
				ch <- struct{}{}
			}

			switch _, err = a(util.UUID().String()); {
			case err == nil:
			case errors.Is(err, context.Canceled):
			default:
				panic(err)
			}
		}

		t.NoError(<-donech)
	})

	t.Run("single", func() {
		a, d, err := t.newFunc()(context.Background(), 1)
		t.NoError(err)
		defer d(nil)

		stateKey := util.UUID().String()
		added, err := a(stateKey)
		t.True(added)
		t.NoError(err)

		tr, err := d(nil)
		t.Equal(1, tr.Len())
		t.Equal(stateKey, tr.Nodes()[0].Key())
	})

	t.Run("async", func() {
		var n uint64 = 3333

		a, d, err := t.newFunc()(context.Background(), n)
		t.NoError(err)
		defer d(nil)

		jw, err := NewBaseJobWorker(context.Background(), int64(n))
		t.NoError(err)

		for i := uint64(0); i < n; i++ {
			t.NoError(jw.NewJob(func(context.Context) error {
				switch added, err := a(util.UUID().String()); {
				case err != nil:
					return err
				case !added:
					return errors.Errorf("failed to add")
				default:
					return nil
				}
			}))
		}

		jw.JobDone()
		t.NoError(jw.Wait())

		tr, err := d(nil)
		t.Equal(n, uint64(tr.Len()))
	})
}

func TestNewStatesTreeWriterFunc(t *testing.T) {
	suite.Run(t, new(testNewStatesTreeWriterFunc))
}
