package isaac

import (
	"context"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testProposalMaker struct {
	suite.Suite
}

func (t *testProposalMaker) newMaker(
	getOperations func(context.Context, base.Height) ([][2]util.Hash, error),
	lastBlockMap func() (base.BlockMap, bool, error),
) *ProposalMaker {
	return NewProposalMaker(
		base.RandomLocalNode(),
		util.UUID().Bytes(),
		getOperations,
		newDummyProposalPool(10),
		lastBlockMap,
	)
}

func (t *testProposalMaker) TestPreferEmpty() {
	point := base.RawPoint(33, 3)

	t.Run("ok", func() {
		maker := t.newMaker(nil, nil)

		pr, err := maker.PreferEmpty(context.Background(), point, valuehash.RandomSHA256())
		t.NoError(err)
		t.Empty(pr.ProposalFact().Operations())
	})

	t.Run("not empty in pool", func() {
		maker := t.newMaker(
			func(context.Context, base.Height) ([][2]util.Hash, error) {
				return [][2]util.Hash{
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
				}, nil
			},
			nil,
		)

		prev := valuehash.RandomSHA256()
		prevpr, err := maker.Make(context.Background(), point, prev)
		t.NoError(err)
		t.NotEmpty(prevpr.ProposalFact().Operations())

		pr, err := maker.PreferEmpty(context.Background(), point, prev)
		t.NoError(err)
		t.Equal(2, len(pr.ProposalFact().Operations()))
	})

	t.Run("old point", func() {
		maker := t.newMaker(
			nil,
			func() (base.BlockMap, bool, error) {
				return base.NewDummyBlockMap(base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())), true, nil
			},
		)

		_, err := maker.PreferEmpty(context.Background(), point.PrevHeight().PrevHeight(), valuehash.RandomSHA256())
		t.Error(err)
		t.ErrorContains(err, "too old")
	})

	t.Run("unreachable point", func() {
		maker := t.newMaker(
			func(context.Context, base.Height) ([][2]util.Hash, error) {
				return [][2]util.Hash{
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
				}, nil
			},
			func() (base.BlockMap, bool, error) {
				return base.NewDummyBlockMap(base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())), true, nil
			},
		)

		pr, err := maker.PreferEmpty(context.Background(), point.NextHeight().NextHeight(), valuehash.RandomSHA256())
		t.NoError(err)
		t.Empty(pr.ProposalFact().Operations())
	})
}

func (t *testProposalMaker) TestMake() {
	point := base.RawPoint(33, 3)

	t.Run("ok", func() {
		ops := [][2]util.Hash{
			{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
			{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
		}

		maker := t.newMaker(
			func(context.Context, base.Height) ([][2]util.Hash, error) {
				return ops, nil
			},
			nil,
		)

		prev := valuehash.RandomSHA256()

		pr, err := maker.Make(context.Background(), point, prev)
		t.NoError(err)
		t.NotEmpty(pr.ProposalFact().Operations())

		rops := pr.ProposalFact().Operations()
		t.Equal(len(ops), len(rops))
		for i := range ops {
			t.True(ops[i][0].Equal(rops[i][0]))
			t.True(ops[i][1].Equal(rops[i][1]))
		}

		rpr, err := maker.Make(context.Background(), point, prev)
		t.NoError(err)
		t.NotEmpty(rpr.ProposalFact().Operations())
		t.True(pr.Fact().Hash().Equal(rpr.Fact().Hash()))
	})

	t.Run("old point", func() {
		maker := t.newMaker(
			nil,
			func() (base.BlockMap, bool, error) {
				return base.NewDummyBlockMap(base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())), true, nil
			},
		)

		_, err := maker.Make(context.Background(), point.PrevHeight().PrevHeight(), valuehash.RandomSHA256())
		t.Error(err)
		t.ErrorContains(err, "too old")
	})

	t.Run("unreachable point", func() {
		maker := t.newMaker(
			func(context.Context, base.Height) ([][2]util.Hash, error) {
				return [][2]util.Hash{
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
				}, nil
			},
			func() (base.BlockMap, bool, error) {
				return base.NewDummyBlockMap(base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())), true, nil
			},
		)

		pr, err := maker.Make(context.Background(), point.NextHeight().NextHeight(), valuehash.RandomSHA256())
		t.NoError(err)
		t.Empty(pr.ProposalFact().Operations())
	})

	t.Run("previous block unmarch", func() {
		maker := t.newMaker(
			func(context.Context, base.Height) ([][2]util.Hash, error) {
				return [][2]util.Hash{
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
				}, nil
			},
			func() (base.BlockMap, bool, error) {
				return base.NewDummyBlockMap(base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())), true, nil
			},
		)

		pr, err := maker.Make(context.Background(), point.NextHeight(), valuehash.RandomSHA256())
		t.NoError(err)
		t.Empty(pr.ProposalFact().Operations())
	})
}

func TestProposalMaker(t *testing.T) {
	suite.Run(t, new(testProposalMaker))
}
