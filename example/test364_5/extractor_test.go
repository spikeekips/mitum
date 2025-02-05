package main

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testSMVExtractor struct {
	dutil.BaseSuite
	baseTestMergeOp
}

func (t *testSMVExtractor) newf(ctx context.Context, ops []operation) (
	dutil.In3ErrorFunc[getOperationFunc, dutil.InErrorFunc[opindexOpHashesFunc], smvExtractorExtractedFunc],
	dutil.InErrorFunc[dutil.InErrorFunc[map[uint64]string]],
	error,
) {
	insert, wait, err := newFuncSMVExtractor(ctx, t.newJobWorker(), 1<<9)

	return func(
			getOperation getOperationFunc,
			f dutil.InErrorFunc[opindexOpHashesFunc],
			extracted smvExtractorExtractedFunc,
		) error {
			of := newGetOperationFunc(ops)
			if getOperation != nil {
				of = getOperation
			}

			inf := func(f opindexOpHashesFunc) error {
				for opindex, op := range slices.All(ops) {
					t.Println("insert: opindex=%d op=%q", opindex, op.Hash())

					if err := f(uint64(opindex), op.Hash(), op.Fact().Hash()); err != nil {
						return err
					}
				}

				return nil
			}
			if f != nil {
				inf = f
			}

			return insert(
				of,
				inf,
				func(opindex uint64, op operation, subv SMV, addvs []SMV) error {
					t.Println("extracted: opindex=%d op=%v subv=%v addvs=%v", opindex, op == nil, subv == nil, len(addvs))

					if extracted == nil {
						return nil
					}

					return extracted(opindex, op, subv, addvs)
				},
			)
		},
		func(f dutil.InErrorFunc[map[uint64]string]) error {
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

func (t *testSMVExtractor) TestCancel() {
	t.Run("before insert", func() {
		ctx, cancel := t.Ctx()

		ops := []operation{t.randomOp(2)}

		insert, wait, err := t.newf(ctx, ops)
		t.NoError(err)

		cancel(nil)

		err = insert(nil, nil, nil)
		t.ErrorIs(err, context.Canceled)

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after insert", func() {
		ctx, cancel := t.Ctx()

		ops := []operation{t.randomOp(2)}

		insert, wait, err := t.newf(ctx, ops)
		t.NoError(err)

		t.NoError(insert(nil, nil, nil))

		cancel(nil)

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after wait", func() {
		ctx, _ := t.Ctx()

		ops := []operation{t.randomOp(2)}

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

func (t *testSMVExtractor) TestGetOperation() {
	t.Run("ok", func() {
		ctx, _ := t.Ctx()

		ops := []operation{t.randomOp(2)}

		insert, wait, err := t.newf(ctx, ops)
		t.NoError(err)

		collected := make([]operation, len(ops))

		t.NoError(insert(nil, nil, func(
			opindex uint64,
			op operation,
			subv SMV,
			addvs []SMV,
		) error {
			esubv, eaddvs, reason, err := op.SMVs(context.Background())
			t.NoError(err)
			t.Empty(reason)

			t.True(isEqualSMV(esubv, subv))

			slices.SortFunc(eaddvs, func(a, b SMV) int {
				return cmp.Compare(a.Key(), b.Key())
			})

			slices.SortFunc(addvs, func(a, b SMV) int {
				return cmp.Compare(a.Key(), b.Key())
			})

			t.Equal(len(eaddvs), len(addvs))
			for i, value := range slices.All(eaddvs) {
				t.True(isEqualSMV(value, addvs[i]))
			}

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

		ops := []operation{t.randomOp(2)}

		insert, wait, err := t.newf(ctx, ops)
		t.NoError(err)

		err = insert(nil, func(f opindexOpHashesFunc) error {
			return errors.Errorf("findme")
		}, nil)
		t.ErrorContains(err, "findme")

		err = wait(func(map[uint64]string) error { return nil })
		t.ErrorContains(err, "findme")
	})

	t.Run("get operation error", func() {
		ctx, _ := t.Ctx()

		ops := []operation{t.randomOp(2)}

		insert, wait, err := t.newf(ctx, ops)
		t.NoError(err)

		t.NoError(insert(
			func(context.Context, uint64, util.Hash, util.Hash) (operation, bool, string, error) {
				return nil, false, "", errors.Errorf("showme")
			}, nil, nil,
		))

		err = wait(func(map[uint64]string) error {
			return nil
		})
		t.ErrorContains(err, "showme")
	})

	t.Run("extracted error", func() {
		ctx, _ := t.Ctx()

		ops := []operation{t.randomOp(2)}

		insert, wait, err := t.newf(ctx, ops)
		t.NoError(err)

		t.NoError(insert(nil, nil,
			func(uint64, operation, SMV, []SMV) error {
				return errors.Errorf("showme")
			}))

		err = wait(func(map[uint64]string) error {
			return nil
		})
		t.ErrorContains(err, "showme")
	})

	t.Run("get operation; reason", func() {
		ctx, _ := t.Ctx()

		insert, wait, err := t.newf(ctx, nil)
		t.NoError(err)

		expectedReasons := dutil.NewSingleLockedMap[uint64, string]()

		t.NoError(insert(
			func(_ context.Context, opindex uint64, _, _ util.Hash) (operation, bool, string, error) {
				if opindex < 2 {
					reason := fmt.Sprintf("reason:%d", opindex)
					expectedReasons.SetValue(opindex, reason)

					return nil, true, reason, nil
				}

				return t.randomOp(1), true, "", nil
			},
			func(f opindexOpHashesFunc) error {
				for i := range 3 {
					_ = f(uint64(i), valuehash.RandomSHA256(), valuehash.RandomSHA256())
				}

				return nil
			},
			nil))

		var called bool

		t.NoError(wait(func(reasons map[uint64]string) error {
			t.Equal(expectedReasons.Map(), reasons)

			called = true

			return nil
		}))
		t.True(called)
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

func (t *testSMVExtractor) TestExtract() {
	t.Run("empty operation", func() {
		ctx, _ := t.Ctx()

		insert, wait, err := t.newf(ctx, nil)
		t.NoError(err)

		var called int

		t.NoError(insert(nil, nil,
			func(uint64, operation, SMV, []SMV) error {
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

			ops := make([]operation, n)
			opsValues := make([][2][]SMV, len(ops))

			for i := range ops {
				ops[i] = t.randomOp(int64(i + 1))

				subv, addvs, _, err := ops[i].SMVs(ctx)
				t.NoError(err)
				opsValues[i] = [2][]SMV{{subv}, addvs}
			}

			extracted := dutil.NewSingleLockedMap[uint64, [3]interface{}]()

			insert, wait, err := t.newf(ctx, ops)
			t.NoError(err)

			t.NoError(insert(nil, nil,
				func(
					opindex uint64,
					op operation,
					subv SMV,
					addvs []SMV,
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

				base.EqualOperation(t.Assert(), ops[i], v[0].(operation))

				asubv := opsValues[i][0][0]
				bsubv := v[1].(SMV)
				t.True(isEqualSMV(asubv, bsubv))

				aaddvs := opsValues[i][1]
				baddvs := v[2].([]SMV)
				t.Equal(len(aaddvs), len(baddvs))

				slices.SortFunc(aaddvs, func(a, b SMV) int { return cmp.Compare(a.Key(), b.Key()) })
				slices.SortFunc(baddvs, func(a, b SMV) int { return cmp.Compare(a.Key(), b.Key()) })

				for i := range aaddvs {
					t.True(isEqualSMV(aaddvs[i], baddvs[i]))
				}
			}
		}
	}

	t.Run("1 operation", nOps(1))
	t.Run("over 1 operation", nOps(33))
}

func TestSMVExtractor(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testSMVExtractor))
}

func newGetOperationFunc(ops []operation) getOperationFunc {
	return func(_ context.Context, opindex uint64, _, _ util.Hash) (operation, bool, string, error) {
		if opindex >= uint64(len(ops)) {
			return nil, false, "", nil
		}

		return ops[opindex], ops[opindex] != nil, "", nil
	}
}
