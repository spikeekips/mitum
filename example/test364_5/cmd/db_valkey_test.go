//go:build valkey
// +build valkey

package main

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	dhash "github.com/spikeekips/mitum/example/test364_5/hash"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"github.com/valkey-io/valkey-go"
	"go.uber.org/goleak"
)

type baseTestValkey struct {
	baseTestDB
	client valkey.Client
	db     *valkeyDB
}

func (t *baseTestValkey) SetupSuite() {
	t.baseTestDB.SetupSuite()

	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6380"}})
	t.NoError(err)
	t.client = client

	t.db = &valkeyDB{enc: t.enc, client: t.client}
}

func (t *baseTestValkey) TearDownSuite() {
	t.baseTestDB.TearDownSuite()

	t.client.Close()
}

func (t *baseTestValkey) SetupTest() {
	t.db.flushAll(context.Background())
}

type testValkeyOperationFact struct {
	baseTestValkey
}

func (t *testValkeyOperationFact) TestGet() {
	t.Run("not found", func() {
		var fact dop5.OperationFact

		found, err := t.db.getOperationFact(
			context.Background(),
			dhash.RandomHex64(),
			&fact,
		)
		t.NoError(err)
		t.False(found)
	})
}

func (t *testValkeyOperationFact) TestSave() {
	t.Run("one", func() {
		facts := t.randomFacts(1)

		t.NoError(t.db.saveOperationFacts(
			context.Background(),
			2, 2,
			facts,
		))

		var rfact dop5.OperationFact

		found, err := t.db.getOperationFact(
			context.Background(),
			facts[0].Hash(),
			&rfact,
		)
		t.NoError(err)
		t.True(found)

		base.EqualFact(t.Assert(), facts[0], rfact)
	})

	t.Run("multiple", func() {
		facts := t.randomFacts(33)

		t.NoError(t.db.saveOperationFacts(
			context.Background(),
			2, 2,
			facts,
		))

		for fact := range slices.Values(facts) {
			var rfact dop5.OperationFact

			found, err := t.db.getOperationFact(
				context.Background(),
				fact.Hash(),
				&rfact,
			)
			t.NoError(err)
			t.True(found)

			base.EqualFact(t.Assert(), fact, rfact)
		}
	})
}

func TestValkeyOperationFact(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testValkeyOperationFact))
}

type testValkeyState struct {
	baseTestValkey
}

func (t *testValkeyState) SetupSuite() {
	t.baseTestValkey.SetupSuite()

	t.NoError(jsonenc.AddDecode(t.enc, dop5.AccountSVHint, &dop5.AccountSV{}, dop5.AccountSVDecodeJSON))
	t.NoError(jsonenc.AddDecode(t.enc, dop5.BaseStateHint, &dop5.BaseState{}, dop5.BaseStateDecodeJSON))
}

func (t *testValkeyState) randomStates(n int) []base.State {
	f := make([]base.State, n)

	for i := range n {
		sv := dop5.NewAccountSV(
			base.NewMPrivatekey().Publickey(),
			33,
		)

		f[i] = dop5.NewBaseState(
			33,
			util.UUID().String(),
			sv,
			dhash.RandomHex64(),
			[]util.Hash{
				dhash.RandomHex64(),
				dhash.RandomHex64(),
				dhash.RandomHex64(),
			},
		)
	}

	return f
}

func (t *testValkeyState) TestGet() {
	t.Run("not found", func() {
		var st base.State

		found, err := t.db.getState(
			context.Background(),
			util.UUID().String(),
			&st,
		)
		t.NoError(err)
		t.False(found)
	})
}

func (t *testValkeyState) TestSave() {
	t.Run("one", func() {
		sts := t.randomStates(1)

		t.NoError(t.db.saveStates(
			context.Background(),
			1<<9, 1<<9,
			sts,
		))

		var rst base.State

		found, err := t.db.getState(
			context.Background(),
			sts[0].Key(),
			&rst,
		)
		t.NoError(err)
		t.True(found)

		t.True(base.IsEqualState(sts[0], rst))
	})

	t.Run("multiple", func() {
		sts := t.randomStates(33)

		t.NoError(t.db.saveStates(
			context.Background(),
			1<<9, 1<<9,
			sts,
		))

		for i := range len(sts) {
			var rst base.State

			found, err := t.db.getState(
				context.Background(),
				sts[i].Key(),
				&rst,
			)
			t.NoError(err)
			t.True(found)

			t.True(base.IsEqualState(sts[i], rst))
		}
	})

	t.Run("duplicated", func() {
		sts := t.randomStates(3)

		t.NoError(t.db.saveStates(
			context.Background(),
			1<<9, 1<<9,
			sts,
		))

		asts := t.randomStates(3)
		nsts := make([]base.State, len(sts)+len(asts))
		copy(nsts, sts) // duplicated
		copy(nsts[len(sts):], asts)

		t.NoError(t.db.saveStates(
			context.Background(),
			1<<9, 1<<9,
			nsts,
		))

		for i := range len(nsts) {
			var rst base.State

			found, err := t.db.getState(
				context.Background(),
				nsts[i].Key(),
				&rst,
			)
			t.NoError(err)
			t.True(found)

			t.True(base.IsEqualState(nsts[i], rst))
		}
	})
}

func (t *testValkeyState) TestScan() {
	t.Run("get all", func() {
		sts := t.randomStates(1 << 10)

		t.NoError(t.db.saveStates(
			context.Background(),
			1<<9, 1<<9,
			sts,
		))

		seq := t.db.getStates(context.Background())

		rsts := make([]base.State, 0, len(sts))

		for st, err := range seq {
			t.NoError(err)
			rsts = append(rsts, st)
		}

		t.Equal(len(sts), len(rsts))

		slices.SortFunc(sts, func(a, b base.State) int {
			return strings.Compare(a.Key(), b.Key())
		})
		slices.SortFunc(rsts, func(a, b base.State) int {
			return strings.Compare(a.Key(), b.Key())
		})

		for i := range sts {
			t.True(base.IsEqualState(sts[i], rsts[i]))
		}
	})

	t.Run("reading error", func() {
		sts := t.randomStates(1 << 10)

		t.NoError(t.db.saveStates(
			context.Background(),
			1<<9, 1<<9,
			sts,
		))

		facts := t.randomFacts(3)
		for fact := range slices.Values(facts) {
			b, err := t.enc.Marshal(fact)
			t.NoError(err)

			t.NoError(t.client.Do(context.Background(), t.client.B().Set().Key(valkeyStateKey(fact.Hash().String())).Value(string(b)).Build()).Error())
		}

		seq := t.db.getStates(
			context.Background(),
		)

		var errcount int
		for _, err := range seq {
			if err != nil {
				t.ErrorContains(err, "expected")
				errcount++
			}
		}
		t.Equal(len(facts), errcount)
	})
}

func TestValkeyState(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testValkeyState))
}
