package dop6

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testSMVExtractor struct {
	baseMergerSuite
}

func (t *testSMVExtractor) newf(
	ctx context.Context,
	facts []OperationFact,
	getOperationFact getOperationFactFunc,
	reportUnmerged mergerUnmergedFunc,
) (
	smvExtractorInsertFunc,
	func() (map[uint64]string, error),
	error,
) {
	of := newFuncGetOperationFact(facts)
	if getOperationFact != nil {
		of = getOperationFact
	}

	reasons := dutil.NewSingleLockedMap[uint64, string]()

	insert, wait, err := newSMVExtractor(ctx, 1<<9, of, func(opindex uint64, reason string) error {
		reasons.SetValue(opindex, reason)

		if reportUnmerged != nil {
			return reportUnmerged(opindex, reason)
		}

		return nil
	})

	return func(
			f dutil.InErrorFunc[opindexOpHashesFunc],
			extracted smvExtractorExtractedFunc,
		) error {
			inf := func(f opindexOpHashesFunc) error {
				for opindex, fact := range slices.All(facts) {
					t.Print("insert: opindex=%d fact=%q", opindex, fact.Hash())

					if err := f(uint64(opindex), fact.Hash(), fact.Hash()); err != nil {
						return err
					}
				}

				return nil
			}
			if f != nil {
				inf = f
			}

			return insert(
				inf,
				func(opindex uint64, subv SMV, addvs []SMV) error {
					t.Print("extracted: opindex=%d subv=%v addvs=%v", opindex, subv == nil, len(addvs))

					if extracted == nil {
						return nil
					}

					return extracted(opindex, subv, addvs)
				},
			)
		},
		func() (map[uint64]string, error) {
			defer reasons.Close()

			err := wait()
			t.T().Logf("wait: %+v", err)

			r := maps.Clone(reasons.Map())
			t.T().Logf("reasons: %v", r)

			return r, err
		},
		err
}

func (t *testSMVExtractor) TestCancel() {
	t.Run("before insert", func() {
		ctx, cancel := t.Ctx()

		facts := []OperationFact{t.randomFact(2)}

		insert, wait, err := t.newf(ctx, facts, nil, nil)
		t.NoError(err)

		cancel(nil)

		err = insert(nil, nil)
		t.ErrorIs(err, context.Canceled)

		_, err = wait()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after insert", func() {
		ctx, cancel := t.Ctx()

		facts := []OperationFact{t.randomFact(2)}

		insert, wait, err := t.newf(ctx, facts, nil, nil)
		t.NoError(err)

		t.NoError(insert(nil, nil))

		cancel(nil)

		_, err = wait()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after wait", func() {
		ctx, _ := t.Ctx()

		facts := []OperationFact{t.randomFact(2)}

		insert, wait, err := t.newf(ctx, facts, nil, nil)
		t.NoError(err)

		t.NoError(insert(nil, nil))
		_, err = wait()
		t.NoError(err)

		_, err = wait()
		t.ErrorIs(err, context.Canceled)

		_, err = wait()
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testSMVExtractor) TestGetOperation() {
	t.Run("ok", func() {
		ctx, _ := t.Ctx()

		facts := []OperationFact{t.randomFact(2)}

		insert, wait, err := t.newf(ctx, facts, nil, nil)
		t.NoError(err)

		collected := make([]util.Hash, len(facts))

		t.NoError(insert(nil, func(opindex uint64, subv SMV, addvs []SMV) error {
			esubv, eaddvs, reason, err := facts[opindex].SMVs(context.Background())
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

			collected[opindex] = facts[opindex].Hash()

			return nil
		}))

		unmerged, err := wait()
		t.NoError(err)

		t.Empty(unmerged)

		for i, fact := range slices.All(facts) {
			t.True(fact.Hash().Equal(collected[i]))
		}
	})

	t.Run("insert op error", func() {
		ctx, _ := t.Ctx()

		facts := []OperationFact{t.randomFact(2)}

		insert, wait, err := t.newf(ctx, facts, nil, nil)
		t.NoError(err)

		err = insert(func(opindexOpHashesFunc) error {
			return errors.New("findme")
		}, nil)
		t.ErrorContains(err, "findme")

		_, err = wait()
		t.ErrorContains(err, "findme")
	})

	t.Run("get operation error", func() {
		ctx, _ := t.Ctx()

		facts := []OperationFact{t.randomFact(2)}

		insert, wait, err := t.newf(ctx, facts,
			func(context.Context, uint64, util.Hash, util.Hash) (OperationFact, bool, string, error) {
				return nil, false, "", errors.New("showme")
			},
			nil)
		t.NoError(err)

		t.NoError(insert(nil, nil))

		_, err = wait()
		t.ErrorContains(err, "showme")
	})

	t.Run("extracted error", func() {
		ctx, _ := t.Ctx()

		facts := []OperationFact{t.randomFact(2)}

		insert, wait, err := t.newf(ctx, facts, nil, nil)
		t.NoError(err)

		t.NoError(insert(nil,
			func(uint64, SMV, []SMV) error {
				return errors.New("showme")
			}))

		_, err = wait()
		t.ErrorContains(err, "showme")
	})

	t.Run("get operation; reason", func() {
		ctx, _ := t.Ctx()

		facts := []OperationFact{t.randomFact(2)}

		expectedReasons := dutil.NewSingleLockedMap[uint64, string]()
		expectedReported := dutil.NewSingleLockedMap[uint64, string]()

		insert, wait, err := t.newf(ctx, facts,
			func(_ context.Context, opindex uint64, oph, facth util.Hash) (
				_ OperationFact, found bool, reason string, _ error,
			) {
				if opindex < 2 {
					reason := fmt.Sprintf("reason:%d", opindex)
					expectedReasons.SetValue(opindex, reason)

					return nil, true, reason, nil
				}

				return facts[opindex], true, "", nil
			},
			func(opindex uint64, reason string) error {
				expectedReported.SetValue(opindex, reason)

				return nil
			},
		)
		t.NoError(err)

		t.NoError(insert(nil, nil))

		reasons, err := wait()
		t.NoError(err)

		t.Equal(expectedReasons.Map(), reasons)
		t.Equal(expectedReported.Map(), reasons)
	})

	t.Run("get operation; not found", func() {
		ctx, _ := t.Ctx()

		facts := []OperationFact{t.randomFact(2)}
		expectedReasons := dutil.NewSingleLockedMap[uint64, string]()
		expectedReported := dutil.NewSingleLockedMap[uint64, string]()

		insert, wait, err := t.newf(ctx, facts,
			func(_ context.Context, opindex uint64, oph, facth util.Hash) (
				_ OperationFact, found bool, reason string, _ error,
			) {
				if opindex < 1 {
					expectedReasons.SetValue(opindex, reasonOperationNotFound)

					return nil, false, "", nil
				}

				return facts[opindex], true, "", nil
			},
			func(opindex uint64, reason string) error {
				expectedReported.SetValue(opindex, reason)

				return nil
			},
		)
		t.NoError(err)

		t.NoError(insert(nil, nil))

		reasons, err := wait()
		t.NoError(err)

		t.Equal(expectedReasons.Map(), reasons)
		t.Equal(expectedReported.Map(), reasons)
	})
}

func (t *testSMVExtractor) TestExtract() {
	t.Run("empty operation", func() {
		ctx, _ := t.Ctx()

		insert, wait, err := t.newf(ctx, nil, nil, nil)
		t.NoError(err)

		var called int

		t.NoError(insert(nil,
			func(uint64, SMV, []SMV) error {
				called++

				return errors.New("showme")
			}))

		_, err = wait()
		t.NoError(err)

		t.Equal(0, called)
	})

	nfacts := func(n int) func() {
		t.T().Helper()

		return func() {
			ctx, _ := t.Ctx()

			facts := make([]OperationFact, n)
			factsValues := make([][2][]SMV, len(facts))

			for i := range facts {
				facts[i] = t.randomFact(int64(i + 1))

				subv, addvs, _, err := facts[i].SMVs(ctx)
				t.NoError(err)
				factsValues[i] = [2][]SMV{{subv}, addvs}
			}

			extracted := dutil.NewSingleLockedMap[uint64, [2]interface{}]()

			insert, wait, err := t.newf(ctx, facts, nil, nil)
			t.NoError(err)

			t.NoError(insert(nil,
				func(
					opindex uint64,
					subv SMV,
					addvs []SMV,
				) error {
					extracted.SetValue(opindex, [2]interface{}{
						subv,
						addvs,
					})

					return nil
				}))

			reasons, err := wait()
			t.NoError(err)
			if len(reasons) > 0 {
				t.Fail("unexpected reasons", reasons)
			}

			t.Equal(len(facts), extracted.Len())
			for i := range facts {
				v, found := extracted.Value(uint64(i))
				t.True(found)

				asubv := factsValues[i][0][0]
				bsubv := v[0].(SMV)
				t.True(isEqualSMV(asubv, bsubv))

				aaddvs := factsValues[i][1]
				baddvs := v[1].([]SMV)
				t.Equal(len(aaddvs), len(baddvs))

				slices.SortFunc(aaddvs, func(a, b SMV) int { return cmp.Compare(a.Key(), b.Key()) })
				slices.SortFunc(baddvs, func(a, b SMV) int { return cmp.Compare(a.Key(), b.Key()) })

				for i := range aaddvs {
					t.True(isEqualSMV(aaddvs[i], baddvs[i]))
				}
			}
		}
	}

	t.Run("1 operation", nfacts(1))
	t.Run("over 1 operation", nfacts(33))
}

func TestSMVExtractor(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testSMVExtractor))
}

func newFuncGetOperationFact(facts []OperationFact) getOperationFactFunc {
	return func(_ context.Context, opindex uint64, _, _ util.Hash) (OperationFact, bool, string, error) {
		if opindex >= uint64(len(facts)) {
			return nil, false, "", nil
		}

		return facts[opindex], facts[opindex] != nil, "", nil
	}
}
