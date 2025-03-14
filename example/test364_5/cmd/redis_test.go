package main

import (
	"context"
	"slices"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type baseTestRedis struct {
	dutil.BaseSuite
	enc  *jsonenc.Encoder
	mini *miniredis.Miniredis
	rdb  *redis.Client
}

func (t *baseTestRedis) SetupSuite() {
	t.enc = jsonenc.NewEncoder(1 << 9)
	t.NoError(jsonenc.AddDecode(t.enc, base.DummyPublickeyHint, base.DummyPublickey{}, dop5.DecodeJSONDummyPublickey))

	t.mini = miniredis.RunT(t.T())
	t.rdb = redis.NewClient(&redis.Options{
		Addr: t.mini.Addr(),
	})
}

func (t *baseTestRedis) TearDownSuite() {
	t.rdb.Close()
	t.mini.Close()
}

type testRedisOperationFact struct {
	baseTestRedis
}

func (t *testRedisOperationFact) SetupSuite() {
	t.baseTestRedis.SetupSuite()

	t.NoError(jsonenc.AddDecode(t.enc, dop5.AccountOpFactHint, &dop5.AccountOpFact{}, dop5.AccountOpFactDecodeJSON))
}

func (t *testRedisOperationFact) randomFacts(n int) []dop5.OperationFact {
	f := make([]dop5.OperationFact, n)

	for i := range n {
		f[i] = dop5.NewAccountOpFact(
			util.UUID().Bytes(),
			int64(i),
			base.NewDummyPublickey(util.UUID().String()),
			base.NewDummyPublickey(util.UUID().String()),
		)
	}

	return f
}

func (t *testRedisOperationFact) TestGet() {
	t.Run("not found", func() {
		var fact dop5.OperationFact

		found, err := redisGetOperationFact(
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

func (t *testRedisOperationFact) TestSave() {
	t.Run("one", func() {
		facts := t.randomFacts(1)

		t.NoError(redisSaveOperationFacts(
			context.Background(),
			t.enc,
			t.rdb,
			facts,
			2, 2,
		))

		var rfact dop5.OperationFact

		found, err := redisGetOperationFact(
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

		t.NoError(redisSaveOperationFacts(
			context.Background(),
			t.enc,
			t.rdb,
			facts,
			2, 2,
		))

		for fact := range slices.Values(facts) {
			var rfact dop5.OperationFact

			found, err := redisGetOperationFact(
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

func TestRedisOperationFact(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testRedisOperationFact))
}

type testRedisState struct {
	baseTestRedis
}

func (t *testRedisState) SetupSuite() {
	t.baseTestRedis.SetupSuite()

	t.NoError(jsonenc.AddDecode(t.enc, dop5.AccountSVHint, &dop5.AccountSV{}, dop5.AccountSVDecodeJSON))
	t.NoError(jsonenc.AddDecode(t.enc, dop5.BaseStateHint, &dop5.BaseState{}, dop5.BaseStateDecodeJSON))
}

func (t *testRedisState) randomStates(n int) []base.State {
	f := make([]base.State, n)

	for i := range n {
		sv := dop5.NewAccountSV(
			base.NewDummyPublickey(util.UUID().String()),
			33,
		)

		f[i] = dop5.NewBaseState(
			33,
			util.UUID().String(),
			sv,
			valuehash.RandomSHA256(),
			[]util.Hash{
				valuehash.RandomSHA256(),
				valuehash.RandomSHA256(),
				valuehash.RandomSHA256(),
			},
		)
	}

	return f
}

func (t *testRedisState) TestGet() {
	t.Run("not found", func() {
		var st base.State

		found, err := redisGetState(
			context.Background(),
			t.enc,
			t.rdb,
			util.UUID().String(),
			&st,
		)
		t.NoError(err)
		t.False(found)
	})
}

func (t *testRedisState) TestSave() {
	t.Run("one", func() {
		sts := t.randomStates(1)

		t.NoError(redisSaveStates(
			context.Background(),
			t.enc,
			t.rdb,
			slices.Values(sts),
		))

		var rst base.State

		found, err := redisGetState(
			context.Background(),
			t.enc,
			t.rdb,
			sts[0].Key(),
			&rst,
		)
		t.NoError(err)
		t.True(found)

		t.True(base.IsEqualState(sts[0], rst))
	})

	t.Run("multiple", func() {
		sts := t.randomStates(33)

		t.NoError(redisSaveStates(
			context.Background(),
			t.enc,
			t.rdb,
			slices.Values(sts),
		))

		for i := range len(sts) {
			var rst base.State

			found, err := redisGetState(
				context.Background(),
				t.enc,
				t.rdb,
				sts[i].Key(),
				&rst,
			)
			t.NoError(err)
			t.True(found)

			t.True(base.IsEqualState(sts[i], rst))
		}
	})
}

func TestRedisState(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testRedisState))
}
