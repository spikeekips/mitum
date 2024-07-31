package main

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testNewOperationsTreeWriterFunc struct {
	baseTestBaseBlockItemWriterFunc
}

func (t *testNewOperationsTreeWriterFunc) newFunc() newOperationsTreeFuncs {
	return newOperationsTreeWriterFunc(func(ctx context.Context) (JobWorker, error) {
		return NewBaseJobWorker(ctx, 1<<9)
	})
}

func (t *testNewOperationsTreeWriterFunc) randomOps(n uint64) [][2]util.Hash {
	ops := make([][2]util.Hash, n)

	for i := uint64(0); i < n; i++ {
		ops[i] = [2]util.Hash{valuehash.RandomSHA512(), valuehash.RandomSHA512()}
	}

	return ops
}

func (t *testNewOperationsTreeWriterFunc) TestAdd() {
	t.Run("empty ops; nil error", func() {
		_, d, err := t.newFunc()(context.Background(), nil)
		t.NoError(err)

		tr, err := d(nil)
		t.NoError(err)
		t.Equal(0, tr.Len())
	})

	t.Run("empty ops; trying to 'not in state'", func() {
		a, d, err := t.newFunc()(context.Background(), nil)
		t.NoError(err)
		defer d(nil)

		err = a([]uint64{0})
		t.ErrorContains(err, "empty ops")

		_, err = d(nil)
		t.ErrorContains(err, "empty ops")
	})

	t.Run("canceled context", func() {
		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		_, _, err := t.newFunc()(ctx, t.randomOps(1))
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("canceled context; before not in state", func() {
		ctx, cancel := context.WithCancel(context.Background())

		a, d, err := t.newFunc()(ctx, t.randomOps(1))
		t.NoError(err)
		defer d(nil)

		cancel()

		err = a([]uint64{0})
		t.ErrorIs(err, context.Canceled)

		_, err = d(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("canceled context; before done", func() {
		ctx, cancel := context.WithCancel(context.Background())

		a, d, err := t.newFunc()(ctx, t.randomOps(1))
		t.NoError(err)
		defer d(nil)

		t.NoError(a([]uint64{0}))

		cancel()

		_, err = d(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("add error", func() {
		a, d, err := t.newFunc()(context.Background(), t.randomOps(1))
		t.NoError(err)
		defer d(nil)

		t.NoError(a([]uint64{0}))
		err = a([]uint64{2})
		t.ErrorContains(err, "out of range")

		_, err = d(nil)
		t.ErrorContains(err, "out of range")
	})

	t.Run("add ok, but done error", func() {
		a, d, err := t.newFunc()(context.Background(), t.randomOps(1))
		t.NoError(err)
		defer d(nil)

		t.NoError(a([]uint64{0}))

		_, err = d(errors.Errorf("showme"))
		t.ErrorContains(err, "showme")
	})

	t.Run("add after done", func() {
		op := t.randomOps(1)[0]
		fact := op[1]

		a, d, err := t.newFunc()(context.Background(), [][2]util.Hash{op})
		t.NoError(err)
		defer d(nil)

		tr, err := d(nil)
		t.NoError(err)
		t.Equal(1, tr.Len())

		n := tr.Nodes()[0].(base.OperationFixedtreeNode)
		t.True(fact.Equal(n.Operation()))
		t.True(n.InState())

		err = a([]uint64{0})
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("add after done; async", func() {
		ops := t.randomOps(2)
		fact := ops[0][1]

		a, d, err := t.newFunc()(context.Background(), ops)
		t.NoError(err)
		defer d(nil)

		ch := make(chan struct{}, 1)
		donech := make(chan error, 1)

		go func() {
			<-ch

			tr, err := d(nil)
			t.NoError(err)
			t.Equal(2, tr.Len())

			n := tr.Nodes()[0].(base.OperationFixedtreeNode)
			t.True(fact.Equal(n.Operation()))
			t.True(n.InState())

			donech <- err
		}()

		for i := 0; i < 9999; i++ {
			if i == 33 {
				ch <- struct{}{}
			}

			switch err = a([]uint64{1}); {
			case err == nil:
			case errors.Is(err, context.Canceled):
			default:
				panic(err)
			}
		}

		t.NoError(<-donech)
	})

	t.Run("single", func() {
		op := t.randomOps(1)[0]
		fact := op[1]

		a, d, err := t.newFunc()(context.Background(), [][2]util.Hash{op})
		t.NoError(err)
		defer d(nil)

		t.NoError(a([]uint64{0}))

		tr, err := d(nil)
		t.NoError(err)
		t.Equal(1, tr.Len())

		n := tr.Nodes()[0].(base.OperationFixedtreeNode)
		t.True(fact.Equal(n.Operation()))
		t.False(n.InState())
	})

	t.Run("async", func() {
		var n uint64 = 3333

		ops := t.randomOps(n)

		a, d, err := t.newFunc()(context.Background(), ops)
		t.NoError(err)
		defer d(nil)

		jw, err := NewBaseJobWorker(context.Background(), int64(n))
		t.NoError(err)

		for i := uint64(0); i < n; i++ {
			i := i

			t.NoError(jw.NewJob(func(context.Context) error {
				return a([]uint64{i})
			}))
		}

		jw.JobDone()
		t.NoError(jw.Wait())

		tr, err := d(nil)
		t.NoError(err)
		t.Equal(n, uint64(tr.Len()))
	})
}

func TestNewOperationsTreeWriterFunc(t *testing.T) {
	suite.Run(t, new(testNewOperationsTreeWriterFunc))
}
