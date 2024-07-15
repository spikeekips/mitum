package main

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testStateMergeValueExtractor struct {
	baseTestMergeOp
}

func (t *testStateMergeValueExtractor) newArgs(ops []Operation) *stateMergeValueExtractorArgs {
	t.T().Helper()

	args := newStateMergeValueExtractorArgs("local")
	args.SetLogging(logging.TestNilLogging)

	args.GetOperation = func(_ context.Context, opindex uint64, _ util.Hash) (Operation, bool, error) {
		if opindex >= uint64(len(ops)) {
			return nil, false, nil
		}

		return ops[opindex], ops[opindex] != nil, nil
	}
	args.Done = func(context.Context, uint64, Operation, []StateMergeValue) error {
		return nil
	}

	return args
}

func (t *testStateMergeValueExtractor) TestCancel() {
	t.Run("before insert", func() {
		ctx, cancel := context.WithCancel(context.Background())

		ops := []Operation{t.randomOp(2)}
		args := t.newArgs(ops)

		insert, _, wait, err := newStateMergeValueExtractor(ctx, args)
		t.NoError(err)

		cancel()

		err = insert(0, ops[0].Hash())
		t.ErrorIs(err, context.Canceled)

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after insert", func() {
		ctx, cancel := context.WithCancel(context.Background())

		ops := []Operation{t.randomOp(2)}
		args := t.newArgs(ops)

		insert, done, wait, err := newStateMergeValueExtractor(ctx, args)
		t.NoError(err)

		t.NoError(insert(0, ops[0].Hash()))

		cancel()

		done()
		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after wait", func() {
		ctx, cancel := context.WithCancel(context.Background())

		ops := []Operation{t.randomOp(2)}
		args := t.newArgs(ops)

		insert, done, wait, err := newStateMergeValueExtractor(ctx, args)
		t.NoError(err)

		t.NoError(insert(0, ops[0].Hash()))

		done()
		t.NoError(wait(nil))

		t.NoError(wait(nil))

		cancel()

		err = wait(nil)
		t.NoError(wait(nil))
	})
}

func (t *testStateMergeValueExtractor) TestGetOperation() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ops := []Operation{t.randomOp(2)}
		args := t.newArgs(ops)

		insert, done, wait, err := newStateMergeValueExtractor(ctx, args)
		t.NoError(err)

		t.NoError(insert(0, ops[0].Hash()))

		done()
		t.NoError(wait(nil))
	})

	t.Run("not found", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ops := []Operation{t.randomOp(2)}
		args := t.newArgs(ops)

		insert, done, wait, err := newStateMergeValueExtractor(ctx, args)
		t.NoError(err)

		t.NoError(insert(1, valuehash.RandomSHA256()))

		done()
		t.NoError(wait(func(reasons map[uint64]string) error {
			t.NotEmpty(reasons)

			_, found := reasons[1]
			t.True(found)

			return nil
		}))
	})

	t.Run("error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ops := []Operation{
			t.randomOp(1),
			t.randomOp(2),
			t.randomOp(3),
		}
		args := t.newArgs(ops)

		getOperation := args.GetOperation
		args.GetOperation = func(ctx context.Context, opindex uint64, oph util.Hash) (Operation, bool, error) {
			if opindex == 1 {
				return nil, false, errors.Errorf("showme")
			}

			return getOperation(ctx, opindex, oph)
		}
		args.Done = func(ctx context.Context, opindex uint64, _ Operation, _ []StateMergeValue) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second * 3):
				return nil
			}
		}

		insert, done, wait, err := newStateMergeValueExtractor(ctx, args)
		t.NoError(err)

		for i := range ops {
			err := insert(uint64(i), ops[i].Hash())
			if err != nil {
				t.ErrorContains(err, "showme")
			}
		}

		done()
		err = wait(nil)
		t.ErrorContains(err, "showme")
	})
}

func (t *testStateMergeValueExtractor) TestExtract() {
	nOps := func(n int) func() {
		t.T().Helper()

		return func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ops := make([]Operation, n)
			opsValues := make([][]StateMergeValue, len(ops))

			for i := range ops {
				ops[i] = t.randomOp(int64(i + 1))

				values, _, err := ops[i].StateMergeValues(ctx)
				t.NoError(err)
				opsValues[i] = values
			}

			args := t.newArgs(ops)

			extracted := util.NewSingleLockedMap[uint64, [2]interface{}]()

			args.Done = func(_ context.Context, opindex uint64, op Operation, values []StateMergeValue) error {
				extracted.SetValue(opindex, [2]interface{}{op, values})

				return nil
			}

			insert, done, wait, err := newStateMergeValueExtractor(ctx, args)
			t.NoError(err)

			for i := range ops {
				t.NoError(insert(uint64(i), ops[i].Hash()))
			}

			done()

			reasonsch := make(chan map[uint64]string, 1)
			t.NoError(wait(func(reasons map[uint64]string) error {
				reasonsch <- reasons

				return nil
			}))

			select {
			case <-time.After(time.Millisecond):
			case reasons := <-reasonsch:
				t.Fail("unexpected reasons", reasons)
			}

			t.Equal(len(ops), extracted.Len())
			for i := range ops {
				v, found := extracted.Value(uint64(i))
				t.True(found)
				t.True(ops[i].Hash().Equal(v[0].(Operation).Hash()))

				values := opsValues[i]

				pvalues := v[1].([]StateMergeValue)
				t.Equal(len(values), len(pvalues))

				for i := range values {
					a := values[i]
					b := pvalues[i]

					t.Equal(a.Key(), b.Key())
					t.True(base.IsEqualStateValue(a.Value(), b.Value()))
				}
			}
		}
	}

	t.Run("1 operation", nOps(1))
	t.Run("over 1 operation", nOps(33))

	t.Run("extract error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		op := t.randomOp(0)

		args := t.newArgs([]Operation{op})

		args.Done = func(context.Context, uint64, Operation, []StateMergeValue) error {
			return errors.Errorf("findme")
		}

		extract, done, wait, err := newStateMergeValueExtractor(ctx, args)
		t.NoError(err)

		t.NoError(extract(0, op.Hash()))

		done()
		err = wait(nil)
		t.ErrorContains(err, "findme")
	})
}

func TestStateMergeValueExtractor(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testStateMergeValueExtractor))
}
