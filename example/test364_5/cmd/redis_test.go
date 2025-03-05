package main

import (
	"context"
	"slices"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testOperationFact struct {
	dutil.BaseSuite
	enc  encoder.Encoder
	mini *miniredis.Miniredis
	rdb  *redis.Client
}

func (t *testOperationFact) SetupSuite() {
	t.enc = jsonenc.NewEncoder()
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.DummyPublickeyHint, Instance: base.DummyPublickey{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dop5.AccountOpFactHint, Instance: &dop5.AccountOpFact{}}))

	t.mini = miniredis.RunT(t.T())
	t.rdb = redis.NewClient(&redis.Options{
		Addr: t.mini.Addr(),
	})
}

func (t *testOperationFact) TearDownSuite() {
	t.rdb.Close()
	t.mini.Close()
}

func (t *testOperationFact) randomFact() *dop5.AccountOpFact {
	return dop5.NewAccountOpFact(
		util.UUID().Bytes(),
		33,
		base.NewDummyPublickey(util.UUID().String()),
		base.NewDummyPublickey(util.UUID().String()),
	)
}

func (t *testOperationFact) randomFacts(n int) []dop5.OperationFact {
	f := make([]dop5.OperationFact, n)

	for i := range n {
		f[i] = t.randomFact()
	}

	return f
}

func (t *testOperationFact) TestGet() {
	t.Run("not found", func() {
		var fact dop5.OperationFact

		found, err := getOperationFact(
			context.Background(),
			t.enc,
			t.rdb,
			valuehash.RandomSHA256(),
			&fact,
		)
		t.NoError(err)
		t.False(found)
	})
}

func (t *testOperationFact) TestSet() {
	t.Run("one", func() {
		facts := t.randomFacts(1)

		t.NoError(saveOperationFacts(
			context.Background(),
			t.enc,
			t.rdb,
			facts,
			2, 2,
		))

		var rfact dop5.OperationFact

		found, err := getOperationFact(
			context.Background(),
			t.enc,
			t.rdb,
			facts[0].Hash(),
			&rfact,
		)
		t.NoError(err)
		t.True(found)

		base.EqualFact(t.Assert(), facts[0], rfact)
	})

	t.Run("multiple", func() {
		facts := t.randomFacts(33)

		t.NoError(saveOperationFacts(
			context.Background(),
			t.enc,
			t.rdb,
			facts,
			2, 2,
		))

		for fact := range slices.Values(facts) {
			var rfact dop5.OperationFact

			found, err := getOperationFact(
				context.Background(),
				t.enc,
				t.rdb,
				fact.Hash(),
				&rfact,
			)
			t.NoError(err)
			t.True(found)

			base.EqualFact(t.Assert(), fact, rfact)
		}
	})
}

func TestOperationFact(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testOperationFact))
}
