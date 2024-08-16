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
	return newOperationsTreeWriterFunc()
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
		_, j, d, err := t.newFunc()(context.Background(), nil)
		t.NoError(err)

		t.NoError(j(nil))

		tr, err := d(nil)
		t.NoError(err)
		t.Equal(0, tr.Len())
	})

	t.Run("empty ops; trying to 'not in state'", func() {
		a, j, d, err := t.newFunc()(context.Background(), nil)
		t.NoError(err)
		defer d(nil)

		err = a([]uint64{0})
		t.ErrorContains(err, "empty ops")

		err = j(nil)
		t.ErrorContains(err, "empty ops")

		_, err = d(nil)
		t.ErrorContains(err, "empty ops")
	})

	t.Run("canceled context", func() {
		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		_, j, d, _ := t.newFunc()(ctx, t.randomOps(1))

		err := j(nil)
		t.ErrorIs(err, context.Canceled)

		_, err = d(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("canceled context; before not in state", func() {
		ctx, cancel := context.WithCancel(context.Background())

		a, j, d, err := t.newFunc()(ctx, t.randomOps(1))
		t.NoError(err)
		defer d(nil)

		cancel()

		t.NoError(a([]uint64{0}))

		err = j(nil)
		t.ErrorIs(err, context.Canceled)

		_, err = d(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("canceled context; before done", func() {
		ctx, cancel := context.WithCancel(context.Background())

		a, j, d, err := t.newFunc()(ctx, t.randomOps(1))
		t.NoError(err)
		defer d(nil)

		t.NoError(a([]uint64{0}))

		cancel()

		err = j(nil)
		t.ErrorIs(err, context.Canceled)

		_, err = d(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("add error", func() {
		a, j, d, err := t.newFunc()(context.Background(), t.randomOps(1))
		t.NoError(err)
		defer d(nil)

		t.NoError(a([]uint64{0}))
		err = a([]uint64{2})
		t.ErrorContains(err, "out of range")

		err = j(nil)
		t.ErrorContains(err, "out of range")

		_, err = d(nil)
		t.ErrorContains(err, "out of range")
	})

	t.Run("add ok, but done error", func() {
		a, j, d, err := t.newFunc()(context.Background(), t.randomOps(1))
		t.NoError(err)
		defer d(nil)

		t.NoError(a([]uint64{0}))

		t.NoError(j(nil))

		_, err = d(errors.Errorf("showme"))
		t.ErrorContains(err, "showme")
	})

	t.Run("add after done", func() {
		op := t.randomOps(1)[0]
		fact := op[1]

		a, j, d, err := t.newFunc()(context.Background(), [][2]util.Hash{op})
		t.NoError(err)
		defer d(nil)

		t.NoError(j(nil))

		tr, err := d(nil)
		t.NoError(err)
		t.Equal(1, tr.Len())

		n := tr.Nodes()[0].(base.OperationFixedtreeNode)
		t.True(fact.Equal(n.Operation()))
		t.True(n.InState())

		t.NoError(a([]uint64{0}))
	})

	t.Run("add after done; async", func() {
		ops := t.randomOps(2)
		fact := ops[0][1]

		a, j, d, err := t.newFunc()(context.Background(), ops)
		t.NoError(err)
		defer d(nil)

		ch := make(chan struct{}, 1)
		donech := make(chan error, 1)

		go func() {
			<-ch

			tr, err := d(nil)
			if err != nil {
				panic(err)
			}

			if l := tr.Len(); l != 2 {
				panic(errors.Errorf("tr.Len(); %d != 2", l))
			}

			n := tr.Nodes()[0].(base.OperationFixedtreeNode)
			if !fact.Equal(n.Operation()) {
				panic(errors.Errorf("!fact.Equal(n.Operation())"))
			}
			if !n.InState() {
				panic(errors.Errorf("!n.InState()"))
			}

			donech <- err
		}()

		for i := 0; i < 9999; i++ {
			if i == 33 {
				t.NoError(j(nil))

				ch <- struct{}{}
			}

			switch err = a([]uint64{1}); {
			case err == nil,
				errors.Is(err, context.Canceled),
				errors.Is(err, util.ErrJobWorkerDone):
			default:
				panic(err)
			}
		}

		t.NoError(<-donech)
	})

	t.Run("single", func() {
		op := t.randomOps(1)[0]
		fact := op[1]

		a, j, d, err := t.newFunc()(context.Background(), [][2]util.Hash{op})
		t.NoError(err)
		defer d(nil)

		t.NoError(a([]uint64{0}))

		t.NoError(j(nil))

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

		a, j, d, err := t.newFunc()(context.Background(), ops)
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

		t.NoError(j(nil))

		tr, err := d(nil)
		t.NoError(err)
		t.Equal(n, uint64(tr.Len()))
	})
}

func TestNewOperationsTreeWriterFunc(t *testing.T) {
	suite.Run(t, new(testNewOperationsTreeWriterFunc))
}
