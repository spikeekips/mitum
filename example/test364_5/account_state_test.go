package main

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"gonum.org/v1/gonum/stat/combin"
)

type baseTestAccountStateMerger struct {
	baseTest
	networkID base.NetworkID
}

func (t *baseTestAccountStateMerger) SetupSuite() {
	t.networkID = util.UUID().Bytes()
}

func (t *baseTestAccountStateMerger) printStateMergeValue(v base.StateValue) string {
	switch i, ok := v.(*AccountStateValue); {
	case ok:
		return fmt.Sprintf("[address=%q balance=%d]",
			i.Address,
			i.Balance,
		)
	default:
		return t.Marshal(v)
	}
}

func (t *baseTestAccountStateMerger) Marshal(i interface{}) string {
	return util.MustMarshalJSONString(i)
}

func (t *baseTestAccountStateMerger) MarshalIndent(i interface{}) string {
	return util.MustMarshalJSONIndentString(i)
}

func (t *baseTestAccountStateMerger) randomMergeValue(balance int64) (base.Publickey, *AccountStateMergeValue) {
	pub := base.NewMPrivatekey().Publickey()

	return pub, t.newMergeValue(pub, balance)
}

func (t *baseTestAccountStateMerger) newMergeValue(address base.Publickey, balance int64) *AccountStateMergeValue {
	v, err := NewAccountStateMergeValue(address, balance)
	if err != nil {
		panic(err)
	}

	return v
}

func (t *baseTestAccountStateMerger) randomMergeValues(
	a ...interface{},
) (vs []StateMergeValue) {
	switch {
	case len(a) < 1:
		panic("empty balances")
	default:
		vs = make([]StateMergeValue, len(a))
	}

	for i := range vs {
		balance, ok := a[i].(int)
		if !ok {
			panic(errors.Errorf("expected int, not %T", a[i]))
		}

		_, v := t.randomMergeValue(int64(balance))
		vs[i] = v
	}

	return vs
}

func (t *baseTestAccountStateMerger) newMergeValues(
	a ...interface{},
) (vs []StateMergeValue) {
	switch {
	case len(a)%2 != 0:
		panic("invalid number of arguments")
	default:
		vs = make([]StateMergeValue, len(a)/2)
	}

	for i := range vs {
		address, ok := a[i*2].(base.Publickey)
		if !ok {
			panic("not base.Publickey")
		}
		balance, ok := a[(i*2)+1].(int)
		if !ok {
			panic("not int")
		}

		vs[i] = t.newMergeValue(address, int64(balance))
	}

	return vs
}

func (t *baseTestAccountStateMerger) randomMergeValuesVP(
	vp util.LockedMap[uint64, []StateMergeValue],
	opindex uint64,
	a ...interface{},
) []StateMergeValue {
	vs := t.randomMergeValues(a...)

	if !vp.SetValue(opindex, vs) {
		panic("already exists")
	}

	return vs
}

func (t *baseTestAccountStateMerger) newMergeValuesVP(
	vp util.LockedMap[uint64, []StateMergeValue],
	opindex uint64,
	a ...interface{},
) []StateMergeValue {
	vs := t.newMergeValues(a...)

	if !vp.SetValue(opindex, vs) {
		panic("already exists")
	}
	return vs
}

func (t *baseTestAccountStateMerger) newState(pub base.Publickey, balance int64) base.State {
	return newBaseState(
		33,
		AccountStateKey(pub),
		NewAccountStateValue(pub, balance),
		nil,
		[]util.Hash{valuehash.RandomSHA256()},
	)
}

func (t *baseTestAccountStateMerger) randomState() base.State {
	pub := base.NewDummyPublickey(util.UUID().String())

	return t.newState(pub, 1)
}

func (t *baseTestAccountStateMerger) newOp(issuer base.Privatekey, receiver base.Publickey, balance int64) *Op {
	op := NewOp(NewOpFact(
		util.UUID().Bytes(),
		balance,
		issuer.Publickey(),
		receiver,
	))

	if err := op.Sign(issuer, t.networkID); err != nil {
		panic(err)
	}

	return op
}

func (t *baseTestAccountStateMerger) randomOp(balance int64) *Op {
	return t.newOp(
		base.NewDummyPrivatekey(util.UUID().String()),
		base.NewDummyPublickey(util.UUID().String()),
		balance,
	)
}

func (t *baseTestAccountStateMerger) randomOps(n int, amount int64) []Operation {
	m := make([]Operation, n)

	for i := 0; i < n; i++ {
		m[i] = t.randomOp(amount)
	}

	return m
}

func (t *baseTestAccountStateMerger) processOps(ops []Operation) [][]StateMergeValue {
	values := make([][]StateMergeValue, len(ops))

	for i := range ops {
		subv, addvs, r, err := ops[i].StateMergeValues(context.Background())
		if err != nil {
			panic(err)
		}
		if r == "" {
			panic(`r == ""`)
		}

		values[i] = slices.Concat([]StateMergeValue{subv}, addvs)
	}

	return values
}

type testAccountStateMerger struct {
	suite.Suite
	baseTestAccountStateMerger
}

func (t *testAccountStateMerger) TestMergeError() {
	t.Run("0 balance", func() {
		priv := base.NewMPrivatekey()

		merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), &AccountStateValue{})

		value := NewAccountStateValue(priv.Publickey(), 0)

		commit, nextop, _, err := merger.Merge(0, value)
		t.Error(err)
		t.ErrorContains(err, "zero balance")
		t.Equal(uint64(0), nextop)
		t.Nil(commit)
	})

	t.Run("wrong address", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountStateValue(priv.Publickey(), 3)

		merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), prev)

		value := NewAccountStateValue(base.NewMPrivatekey().Publickey(), -3)

		commit, nextop, _, err := merger.Merge(0, value)
		t.Error(err)
		t.ErrorContains(err, "address not matched")
		t.Equal(uint64(0), nextop)
		t.Nil(commit)
	})

	t.Run("over balance", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountStateValue(priv.Publickey(), 3)

		merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), prev)

		value := NewAccountStateValue(priv.Publickey(), (prev.Balance+1)*-1)

		commit, nextop, reason, err := merger.Merge(0, value)
		t.NoError(err)
		t.Equal(uint64(0), nextop)
		t.Nil(commit)
		t.Equal(reason, "insufficient balance")
	})
}

func (t *testAccountStateMerger) TestMerge() {
	t.Run("add balance", func() {
		priv := base.NewMPrivatekey()

		merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), &AccountStateValue{})

		value := NewAccountStateValue(priv.Publickey(), 3)

		commit, nextop, reason, err := merger.Merge(0, value)
		t.NoError(err)
		t.Equal(uint64(0), nextop)
		t.NotNil(commit)
		t.Empty(reason)

		t.NoError(commit())

		t.Equal(int64(3), merger.sum)
	})

	t.Run("sub balance", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountStateValue(priv.Publickey(), 3)

		merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), prev)

		value := NewAccountStateValue(priv.Publickey(), -3)

		commit, nextop, reason, err := merger.Merge(0, value)
		t.NoError(err)
		t.Equal(uint64(0), nextop)
		t.NotNil(commit)
		t.Empty(reason)

		t.NoError(commit())

		t.Equal(int64(-3), merger.sum)
		t.Equal(uint64(0), *merger.lastop)
		t.Empty(merger.prepops)
	})

	t.Run("sub0, sub1", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountStateValue(priv.Publickey(), 3)

		merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), prev)

		sub0 := NewAccountStateValue(priv.Publickey(), -1)
		sub1 := NewAccountStateValue(priv.Publickey(), -1)

		commit, nextop, reason, err := merger.Merge(1, sub0)
		t.NoError(err)
		t.Equal(uint64(1), nextop)
		t.NotNil(commit)
		t.Empty(reason)

		t.NoError(commit())

		t.Equal(int64(-1), merger.sum)
		t.Equal(uint64(1), *merger.lastop)
		t.Empty(merger.prepops)

		commit, nextop, reason, err = merger.Merge(2, sub1)
		t.NoError(err)
		t.Equal(uint64(2), nextop)
		t.Nil(commit)
		t.Equal(reason, "previous issuer found")
	})

	t.Run("sub1, sub2, unmerge", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountStateValue(priv.Publickey(), 3)

		merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), prev)

		sub0 := NewAccountStateValue(priv.Publickey(), -1)
		sub1 := NewAccountStateValue(priv.Publickey(), -2)

		commit, nextop, reason, err := merger.Merge(2, sub1)
		t.NoError(err)
		t.Equal(uint64(2), nextop)
		t.NotNil(commit)
		t.Empty(reason)

		t.NoError(commit())

		t.Equal(int64(-2), merger.sum)
		t.Equal(uint64(2), *merger.lastop)
		t.Empty(merger.prepops)

		commit, nextop, reason, err = merger.Merge(1, sub0)
		t.NoError(err)
		t.Equal(uint64(2), nextop)
		t.NoError(commit())
		t.Equal(reason, "previous issuer found")

		t.Equal(int64(-3), merger.sum)
		t.Equal(uint64(1), *merger.lastop)
		t.Equal([]uint64{2}, merger.prepops)

		commit, nnextop, err := merger.Unmerge(nextop, sub1)
		t.NoError(err)
		t.Equal(nextop, nnextop)
		t.NoError(commit())

		t.Equal(int64(-1), merger.sum)
		t.Equal(uint64(1), *merger.lastop)
	})

	t.Run("adds", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountStateValue(priv.Publickey(), 3)

		values := make([]*AccountStateValue, 4)
		for i := range values {
			values[i] = NewAccountStateValue(priv.Publickey(), 1)
		}

		mergeValues := func(seq []int) {
			merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), prev)

			for i := range seq {
				op := uint64(seq[i])

				commit, nextop, reason, err := merger.Merge(op, values[op])
				t.NoError(err, seq)
				t.Equal(op, nextop, seq)
				t.Empty(reason)

				t.NoError(commit(), seq)
			}

			t.Equal(int64(4), merger.sum, seq)
		}

		gen := combin.NewPermutationGenerator(len(values), len(values))

		for gen.Next() {
			seq := gen.Permutation(nil)

			mergeValues(seq)
		}
	})

	t.Run("adds + sub", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountStateValue(priv.Publickey(), 4)

		values := make([]*AccountStateValue, 4)
		for i := range values {
			v := int64(1)
			if i == len(values)-1 {
				v *= -1
			}

			values[i] = NewAccountStateValue(priv.Publickey(), v)
		}

		mergeValues := func(seq []int) {
			merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), prev)

			for i := range seq {
				op := uint64(seq[i])

				commit, nextop, reason, err := merger.Merge(op, values[op])
				t.NoError(err, seq)
				t.Equal(op, nextop, seq)
				t.Empty(reason)

				t.NoError(commit(), seq)
			}

			t.Equal(int64(2), merger.sum)
			t.Equal(uint64(3), *merger.lastop)
		}

		gen := combin.NewPermutationGenerator(len(values), len(values))

		for gen.Next() {
			seq := gen.Permutation(nil)

			mergeValues(seq)
		}
	})
}

type stateMergerXMergeFunc func(uint64, base.StateValue) (func() error, uint64, error)

func noReasonMergeFunc(f func(uint64, base.StateValue) (func() error, uint64, string, error)) stateMergerXMergeFunc {
	return func(opindex uint64, value base.StateValue) (func() error, uint64, error) {
		commit, unmergeOp, _, err := f(opindex, value)

		return commit, unmergeOp, err
	}
}

func (t *testAccountStateMerger) TestMerges() {
	t.Run("adds + subs", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountStateValue(priv.Publickey(), 4)

		values := make([]*AccountStateValue, 5)
		for i := range values {
			v := int64(1)
			if i%2 == 0 {
				v *= -2
			}

			values[i] = NewAccountStateValue(priv.Publickey(), v)
		}

		var xmerge func(op uint64, value *AccountStateValue, mergef, unmergef stateMergerXMergeFunc) error

		xmerge = func(op uint64, value *AccountStateValue, mergef, unmergef stateMergerXMergeFunc) error {
			commit, nextop, err := mergef(op, value)
			if err != nil {
				return err
			}

			if commit != nil {
				if err := commit(); err != nil {
					return err
				}
			}

			if nextop != op {
				return xmerge(nextop, values[nextop], unmergef, mergef)
			}

			return nil
		}

		gen := combin.NewPermutationGenerator(len(values), len(values))

		for gen.Next() {
			seq := gen.Permutation(nil)

			merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), prev)

			for i := range seq {
				op := uint64(seq[i])
				value := values[op]

				t.NoError(xmerge(op, value, noReasonMergeFunc(merger.Merge), merger.Unmerge), seq)
			}

			t.Equal(int64(0), merger.sum)
		}
	})

	t.Run("2 account", func() {
		pub0 := base.NewMPrivatekey().Publickey()
		pub1 := base.NewMPrivatekey().Publickey()

		prev0 := NewAccountStateValue(pub0, 2)
		prev1 := NewAccountStateValue(pub1, 2)

		ops := map[uint64][]StateMergeValue{}

		var xmerge func(name string, op uint64, values []StateMergeValue, mergef, unmergef func(string) stateMergerXMergeFunc) error

		xmerge = func(name string, op uint64, values []StateMergeValue, mergef, unmergef func(string) stateMergerXMergeFunc) error {
			t.T().Logf("> op=%d %s", op, name)

			addCommits, doneCommits := util.CompactAppendSlice[func() error](len(values))
			addNextops, doneNextops := util.UniqCompactAppendSlice[uint64](len(values))

			for i := range values {
				value := values[i]

				t.T().Logf("> %s op=%d %d", name, op, i)
				commit, nextop, err := mergef(value.Key())(op, value.Value())
				if err != nil {
					return err
				}

				if commit == nil {
					t.T().Logf("> ignored: name=%s op=%d i=%d value=%v",
						name, op, i, value.Value().(*AccountStateValue).Balance)

					return nil
				}

				addCommits(commit)

				if nextop != op {
					addNextops(nextop)
				}
			}

			if err := traverseSlice(doneCommits(), func(_ int, f func() error) (bool, error) {
				return true, f()
			}); err != nil {
				return err
			}

			nextname := "unmerge"
			if name == "unmerge" {
				nextname = "merge"
			}

			if err := traverseSlice(doneNextops(), func(_ int, op uint64) (bool, error) {
				return true, xmerge(nextname, op, ops[op], unmergef, mergef)
			}); err != nil {
				return err
			}

			return nil
		}

		ops[0] = []StateMergeValue{
			t.newMergeValue(pub0, 1),
			t.newMergeValue(pub1, -2),
		}
		ops[1] = []StateMergeValue{ // ignored
			t.newMergeValue(pub0, -1),
			t.newMergeValue(pub1, -2),
		}
		ops[2] = []StateMergeValue{
			t.newMergeValue(pub0, -1),
			t.newMergeValue(pub1, 2),
		}
		ops[3] = []StateMergeValue{ // ignored
			t.newMergeValue(pub0, -1),
			t.newMergeValue(pub1, -2),
		}
		ops[4] = []StateMergeValue{
			t.newMergeValue(pub0, 1),
			t.newMergeValue(pub1, 2),
		}

		// expected: account 0: balance=3
		// expected: account 1: balance=4

		mergef := func(merger0, merger1 StateMerger) func(string) stateMergerXMergeFunc {
			return func(key string) stateMergerXMergeFunc {
				if key == AccountStateKey(pub0) {
					return noReasonMergeFunc(merger0.Merge)
				}

				return noReasonMergeFunc(merger1.Merge)
			}
		}

		unmergef := func(merger0, merger1 StateMerger) func(string) stateMergerXMergeFunc {
			return func(key string) stateMergerXMergeFunc {
				if key == AccountStateKey(pub0) {
					return merger0.Unmerge
				}

				return merger1.Unmerge
			}
		}

		gen := combin.NewPermutationGenerator(len(ops), len(ops))

		run := func(seq []int) {
			t.T().Log("seq:", seq)

			merger0 := NewAccountStateMerger(AccountStateKey(pub0), prev0)
			merger1 := NewAccountStateMerger(AccountStateKey(pub1), prev1)

			for i := range seq {
				op := uint64(seq[i])

				t.NoError(xmerge(
					"merge",
					op, ops[op],
					mergef(merger0, merger1), unmergef(merger0, merger1),
				))
			}

			{
				sv, err := merger0.Close()
				t.NoError(err)
				av := sv.(*AccountStateValue)
				t.T().Logf("account 0: balance=%d", av.Balance)

				t.Equal(int64(3), av.Balance)
			}

			{
				sv, err := merger1.Close()
				t.NoError(err)
				av := sv.(*AccountStateValue)
				t.T().Logf("account 1: balance=%d", av.Balance)

				t.Equal(int64(4), av.Balance)
			}
		}

		for gen.Next() {
			seq := gen.Permutation(nil)

			run(seq)
		}
	})
}

func (t *testAccountStateMerger) TestUnmergeError() {
	t.Run("0 balance", func() {
		priv := base.NewMPrivatekey()

		merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), &AccountStateValue{})

		commit, _, reason, err := merger.Merge(0, NewAccountStateValue(priv.Publickey(), 1))
		t.NoError(err)
		t.Empty(reason)

		t.NoError(commit())

		value := NewAccountStateValue(priv.Publickey(), 0)

		commit, nextop, err := merger.Unmerge(0, value)
		t.Error(err)
		t.ErrorContains(err, "zero balance")
		t.Equal(uint64(0), nextop)
		t.Nil(commit)
	})

	t.Run("wrong address", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountStateValue(priv.Publickey(), 3)

		merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), prev)

		commit, _, reason, err := merger.Merge(0, NewAccountStateValue(priv.Publickey(), 1))
		t.NoError(err)
		t.Empty(reason)

		t.NoError(commit())

		value := NewAccountStateValue(base.NewMPrivatekey().Publickey(), -3)

		commit, nextop, err := merger.Unmerge(0, value)
		t.Error(err)
		t.ErrorContains(err, "address not matched")
		t.Equal(uint64(0), nextop)
		t.Nil(commit)
	})
}

func TestAccountStateMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testAccountStateMerger))
}

func TestEncodeAccountStateValue(tt *testing.T) {
	t := new(encoder.BaseTestEncode)
	t.SetT(tt)

	enc := jsonenc.NewEncoder()

	t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.DummyPublickeyHint, Instance: base.DummyPublickey{}}))
	t.NoError(enc.Add(encoder.DecodeDetail{Hint: AccountStateValueHint, Instance: &AccountStateValue{}}))

	t.Encode = func() (interface{}, []byte) {
		v := NewAccountStateValue(base.NewDummyPublickey(util.UUID().String()), 1)

		b, err := util.MarshalJSON(v)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return v, b
	}
	t.Decode = func(b []byte) interface{} {
		var u *AccountStateValue
		t.NoError(encoder.Decode(enc, b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		av := a.(*AccountStateValue)
		bv := b.(*AccountStateValue)

		t.True(av.Hint().Equal(bv.Hint()))
		t.True(av.Address.Equal(bv.Address))
		t.Equal(av.Balance, bv.Balance)
	}

	t.TestDecode()
}
