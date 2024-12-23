package main

import (
	"cmp"
	"context"
	"slices"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testStateMergeValueExtractor struct {
	testSuite
	baseTestMergeOp
}

func (t *testStateMergeValueExtractor) newf(ctx context.Context, ops []Operation) (
	in3ErrorFunc[getOperationFunc, inErrorFunc[opindexOpHashesFunc], stateMergeValueExtractorExtractedFunc],
	inErrorFunc[inErrorFunc[map[uint64]string /* reason */]],
	error,
) {
	insert, wait, err := newFuncStateMergeValueExtractor(ctx, t.newJobWorker(), 1<<9)

	return func(
			getOperation getOperationFunc,
			f inErrorFunc[opindexOpHashesFunc],
			extracted stateMergeValueExtractorExtractedFunc,
		) error {
			of := newGetOperationFunc(ops)
			if getOperation != nil {
				of = getOperation
			}

			inf := func(f opindexOpHashesFunc) error {
				return traverseSlice(ops, func(opindex int, op Operation) (bool, error) {
					t.T().Logf("insert: opindex=%d op=%q", opindex, op.Hash())

					return true, f(uint64(opindex), op.Hash(), op.Fact().Hash())
				})
			}
			if f != nil {
				inf = f
			}

			return insert(
				of,
				inf,
				func(opindex uint64, op Operation, subv StateMergeValue, addvs []StateMergeValue) error {
					t.T().Logf("extracted: opindex=%d op=%v subv=%v addvs=%v", opindex, op == nil, subv == nil, len(addvs))

					if extracted == nil {
						return nil
					}

					return extracted(opindex, op, subv, addvs)
				},
			)
		},
		func(f inErrorFunc[map[uint64]string]) error {
			return wait(func(reasons map[uint64]string) error {
				t.T().Logf("reasons: %v", reasons)

				if f == nil {
					return nil
				}

				return f(reasons)
			})
		},
		err
}

func (t *testStateMergeValueExtractor) TestCancel() {
	t.Run("before insert", func() {
		ctx, cancel := t.Ctx()

		ops := []Operation{t.randomOp(2)}

		insert, wait, err := t.newf(ctx, ops)
		t.NoError(err)

		cancel()

		err = insert(nil, nil, nil)
		t.ErrorIs(err, context.Canceled)

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after insert", func() {
		ctx, cancel := t.Ctx()

		ops := []Operation{t.randomOp(2)}

		insert, wait, err := t.newf(ctx, ops)
		t.NoError(err)

		t.NoError(insert(nil, nil, nil))

		cancel()

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after wait", func() {
		ctx, _ := t.Ctx()

		ops := []Operation{t.randomOp(2)}

		insert, wait, err := t.newf(ctx, ops)
		t.NoError(err)

		t.NoError(insert(nil, nil, nil))
		t.NoError(wait(nil))

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testStateMergeValueExtractor) TestGetOperation() {
	t.Run("ok", func() {
		ctx, _ := t.Ctx()

		ops := []Operation{t.randomOp(2)}

		insert, wait, err := t.newf(ctx, ops)
		t.NoError(err)

		collected := make([]Operation, len(ops))

		t.NoError(insert(nil, nil, func(
			opindex uint64,
			op Operation,
			subv StateMergeValue,
			addvs []StateMergeValue,
		) error {
			esubv, eaddvs, reason, err := op.StateMergeValues(context.Background())
			t.NoError(err)
			t.Empty(reason)

			t.True(IsEqualStateMergeValue(esubv, subv))

			slices.SortFunc(eaddvs, func(a, b StateMergeValue) int {
				return cmp.Compare(a.Key(), b.Key())
			})

			slices.SortFunc(addvs, func(a, b StateMergeValue) int {
				return cmp.Compare(a.Key(), b.Key())
			})

			t.Equal(len(eaddvs), len(addvs))
			traverseSlice(eaddvs, func(i int, value StateMergeValue) (bool, error) {
				t.True(IsEqualStateMergeValue(value, addvs[i]))

				return true, nil
			})

			collected[opindex] = op

			return nil
		}))

		t.NoError(wait(func(reasons map[uint64]string) error {
			t.Empty(reasons)

			return nil
		}))
	})

	t.Run("insert op error", func() {
		ctx, _ := t.Ctx()

		ops := []Operation{t.randomOp(2)}

		insert, wait, err := t.newf(ctx, ops)
		t.NoError(err)

		err = insert(nil, func(f opindexOpHashesFunc) error {
			return errors.Errorf("findme")
		}, nil)
		t.ErrorContains(err, "findme")

		t.NoError(wait(func(map[uint64]string) error { return nil }))
	})

	t.Run("get operation error", func() {
		ctx, _ := t.Ctx()

		ops := []Operation{t.randomOp(2)}

		insert, wait, err := t.newf(ctx, ops)
		t.NoError(err)

		t.NoError(insert(
			func(context.Context, uint64, util.Hash, util.Hash) (Operation, bool, error) {
				return nil, false, errors.Errorf("showme")
			}, nil, nil,
		))

		err = wait(func(map[uint64]string) error {
			return nil
		})
		t.ErrorContains(err, "showme")
	})

	t.Run("extracted error", func() {
		ctx, _ := t.Ctx()

		ops := []Operation{t.randomOp(2)}

		insert, wait, err := t.newf(ctx, ops)
		t.NoError(err)

		t.NoError(insert(nil, nil,
			func(uint64, Operation, StateMergeValue, []StateMergeValue) error {
				return errors.Errorf("showme")
			}))

		err = wait(func(map[uint64]string) error {
			return nil
		})
		t.ErrorContains(err, "showme")
	})

	t.Run("get operation; not found", func() {
		ctx, _ := t.Ctx()

		insert, wait, err := t.newf(ctx, nil)
		t.NoError(err)

		expectedReasons := map[uint64]string{}

		t.NoError(insert(nil,
			func(f opindexOpHashesFunc) error {
				for i := range 3 {
					_ = f(uint64(i), valuehash.RandomSHA256(), valuehash.RandomSHA256())

					expectedReasons[uint64(i)] = reasonOperationNotFound
				}

				return nil
			},
			nil))

		var called bool

		t.NoError(wait(func(reasons map[uint64]string) error {
			t.Equal(expectedReasons, reasons)

			called = true

			return nil
		}))
		t.True(called)
	})
}

func (t *testStateMergeValueExtractor) TestExtract() {
	t.Run("empty operation", func() {
		ctx, _ := t.Ctx()

		insert, wait, err := t.newf(ctx, nil)
		t.NoError(err)

		var called int

		t.NoError(insert(nil, nil,
			func(uint64, Operation, StateMergeValue, []StateMergeValue) error {
				called++

				return errors.Errorf("showme")
			}))

		t.NoError(wait(func(reasons map[uint64]string) error {
			called++

			return nil
		}))

		t.Equal(1, called)
	})

	nOps := func(n int) func() {
		t.T().Helper()

		return func() {
			ctx, _ := t.Ctx()

			ops := make([]Operation, n)
			opsValues := make([][2][]StateMergeValue, len(ops))

			for i := range ops {
				ops[i] = t.randomOp(int64(i + 1))

				subv, addvs, _, err := ops[i].StateMergeValues(ctx)
				t.NoError(err)
				opsValues[i] = [2][]StateMergeValue{{subv}, addvs}
			}

			extracted := util.NewSingleLockedMap[uint64, [3]interface{}]()

			insert, wait, err := t.newf(ctx, ops)
			t.NoError(err)

			t.NoError(insert(nil, nil,
				func(
					opindex uint64,
					op Operation,
					subv StateMergeValue,
					addvs []StateMergeValue,
				) error {
					extracted.SetValue(opindex, [3]interface{}{
						op,
						subv,
						addvs,
					})

					return nil
				}))

			reasonsch := make(chan map[uint64]string, 1)
			t.NoError(wait(func(reasons map[uint64]string) error {
				reasonsch <- reasons

				return nil
			}))

			select {
			case <-time.After(time.Millisecond):
			case reasons := <-reasonsch:
				if len(reasons) > 0 {
					t.Fail("unexpected reasons", reasons)
				}
			}

			t.Equal(len(ops), extracted.Len())
			for i := range ops {
				v, found := extracted.Value(uint64(i))
				t.True(found)

				base.EqualOperation(t.Assert(), ops[i], v[0].(Operation))

				asubv := opsValues[i][0][0]
				bsubv := v[1].(StateMergeValue)
				t.True(IsEqualStateMergeValue(asubv, bsubv))

				aaddvs := opsValues[i][1]
				baddvs := v[2].([]StateMergeValue)
				t.Equal(len(aaddvs), len(baddvs))

				slices.SortFunc(aaddvs, func(a, b StateMergeValue) int { return cmp.Compare(a.Key(), b.Key()) })
				slices.SortFunc(baddvs, func(a, b StateMergeValue) int { return cmp.Compare(a.Key(), b.Key()) })

				for i := range aaddvs {
					t.True(IsEqualStateMergeValue(aaddvs[i], baddvs[i]))
				}
			}
		}
	}

	t.Run("1 operation", nOps(1))
	t.Run("over 1 operation", nOps(33))
}

func TestStateMergeValueExtractor(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testStateMergeValueExtractor))
}

func newGetOperationFunc(ops []Operation) getOperationFunc {
	return func(_ context.Context, opindex uint64, _, _ util.Hash) (Operation, bool, error) {
		if opindex >= uint64(len(ops)) {
			return nil, false, nil
		}

		return ops[opindex], ops[opindex] != nil, nil
	}
}
