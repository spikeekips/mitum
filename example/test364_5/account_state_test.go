package dop5

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	dhash "github.com/spikeekips/mitum/example/test364_5/hash"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	mencoder "github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"gonum.org/v1/gonum/stat/combin"
)

var dummyOpFactHint = hint.MustNewHint("dummy-fact-v0.0.1")

type dummyOpFact struct {
	base.BaseFact
	values []*AccountSMV
}

func newDummyOpFact(token base.Token, values []*AccountSMV) *dummyOpFact {
	fact := &dummyOpFact{
		BaseFact: base.NewBaseFact(dummyOpFactHint, token),
		values:   values,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact *dummyOpFact) SMVs(context.Context) (sub SMV, addvs []SMV, reason string, _ error) {
	for value := range slices.Values(fact.values) {
		switch {
		case value.Balance < 0:
			sub = value
		default:
			addvs = append(addvs, value)
		}
	}

	return sub, addvs, "", nil
}

func (fact *dummyOpFact) IsValid([]byte) error {
	if err := fact.BaseFact.IsValid(nil); err != nil {
		return err
	}

	var foundsub bool

	for value := range slices.Values(fact.values) {
		switch {
		case value.Key() == "":
			return util.ErrInvalid.Errorf("empty key")
		case foundsub && value.Balance < 0:
			return util.ErrInvalid.Errorf("multiple sub found")
		default:
			if err := value.Value().IsValid(nil); err != nil {
				return err
			}
		}
	}

	if !fact.Hash().Equal(fact.hash()) {
		return errors.Errorf("hash does not match")
	}

	return nil
}

func (fact *dummyOpFact) hash() util.Hash {
	buf := bytes.NewBuffer(nil)
	defer buf.Reset()

	_, _ = buf.Write(fact.Token())

	for value := range slices.Values(fact.values) {
		_, _ = buf.Write([]byte(value.Key()))
		_, _ = buf.Write(value.Value().HashBytes())
	}

	return dhash.NewSHA3(buf.Bytes())
}

type baseTestAccountSVM struct {
	dutil.BaseSuite
}

func (t *baseTestAccountSVM) printSMV(v base.StateValue) string {
	switch i, ok := v.(*AccountSV); {
	case ok:
		return fmt.Sprintf("[address=%q balance=%d]",
			i.Address,
			i.Balance,
		)
	default:
		return t.Marshal(v)
	}
}

func (t *baseTestAccountSVM) randomMergeValue(balance int64) (base.Publickey, *AccountSMV) {
	pub := base.NewMPrivatekey().Publickey()

	return pub, t.newMergeValue(pub, balance)
}

func (t *baseTestAccountSVM) newMergeValue(address base.Publickey, balance int64) *AccountSMV {
	v, err := NewAccountSMV(address, balance)
	t.IfErrPanic(err)

	return v
}

func (t *baseTestAccountSVM) randomMergeValues(
	a ...interface{},
) (vs []SMV) {
	t.IfTruePanic(len(a) < 1, "empty balances")
	vs = make([]SMV, len(a))

	for i := range vs {
		balance, ok := a[i].(int)
		t.IfTruePanic(!ok, "expected int, not %T", a[i])

		_, v := t.randomMergeValue(int64(balance))
		vs[i] = v
	}

	return vs
}

func (t *baseTestAccountSVM) newMergeValues(
	a ...interface{},
) (vs []SMV) {
	t.IfTruePanic(len(a)%2 != 0, "invalid number of arguments")

	vs = make([]SMV, len(a)/2)

	for i := range vs {
		address, ok := a[i*2].(base.Publickey)
		t.IfTruePanic(!ok, "not base.Publickey")
		balance, ok := a[(i*2)+1].(int)
		t.IfTruePanic(!ok, "not int")

		vs[i] = t.newMergeValue(address, int64(balance))
	}

	return vs
}

func (t *baseTestAccountSVM) randomMergeValuesVP(
	vp dutil.LockedMap[uint64, []SMV],
	opindex uint64,
	a ...interface{},
) []SMV {
	vs := t.randomMergeValues(a...)

	t.IfTruePanic(!vp.SetValue(opindex, vs), "already exists")

	return vs
}

func (t *baseTestAccountSVM) newMergeValuesVP(
	vp dutil.LockedMap[uint64, []SMV],
	opindex uint64,
	a ...interface{},
) []SMV {
	vs := t.newMergeValues(a...)

	t.IfTruePanic(!vp.SetValue(opindex, vs), "already exists")

	return vs
}

func (t *baseTestAccountSVM) newState(pub base.Publickey, balance int64) base.State {
	return NewBaseState(
		33,
		accountStateKey(pub),
		NewAccountSV(pub, balance),
		nil,
		[]util.Hash{dhash.RandomHex64()},
	)
}

func (t *baseTestAccountSVM) newOpFact(issuer base.Privatekey, receiver base.Publickey, balance int64) *AccountOpFact {
	var issuerPub base.Publickey

	if issuer != nil {
		issuerPub = issuer.Publickey()
	}

	return NewAccountOpFact(
		util.UUID().Bytes(),
		balance,
		issuerPub,
		receiver,
	)
}

func (t *baseTestAccountSVM) randomFact(balance int64) *AccountOpFact {
	return t.newOpFact(
		base.NewDummyPrivatekey(util.UUID().String()),
		base.NewDummyPublickey(util.UUID().String()),
		balance,
	)
}

func (t *baseTestAccountSVM) randomAddFact(balance int64) *AccountOpFact {
	return t.newOpFact(
		nil,
		base.NewDummyPublickey(util.UUID().String()),
		balance,
	)
}

func (t *baseTestAccountSVM) randomAddFacts(n int, amount int64) []OperationFact {
	m := make([]OperationFact, n)

	for i := 0; i < n; i++ {
		m[i] = t.randomAddFact(amount)
	}

	return m
}

func (t *baseTestAccountSVM) newDummyFact(token base.Token, a ...interface{}) *dummyOpFact {
	vs := t.newMergeValues(a...)

	values := make([]*AccountSMV, len(vs))
	for i, v := range slices.All(vs) {
		values[i] = v.(*AccountSMV)
	}

	return newDummyOpFact(token, values)
}

type testAccountSVM struct {
	suite.Suite
	baseTestAccountSVM
}

func (t *testAccountSVM) TestMergeError() {
	t.Run("0 balance", func() {
		priv := base.NewMPrivatekey()

		merger := newAccountSVM(accountStateKey(priv.Publickey()), &AccountSV{})

		value := NewAccountSV(priv.Publickey(), 0)

		commit, nextop, _, err := merger.Merge(0, value)
		t.Error(err)
		t.ErrorContains(err, "zero balance")
		t.Equal(uint64(0), nextop)
		t.Nil(commit)
	})

	t.Run("wrong address", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountSV(priv.Publickey(), 3)

		merger := newAccountSVM(accountStateKey(priv.Publickey()), prev)

		value := NewAccountSV(base.NewMPrivatekey().Publickey(), -3)

		commit, nextop, _, err := merger.Merge(0, value)
		t.Error(err)
		t.ErrorContains(err, "address not matched")
		t.Equal(uint64(0), nextop)
		t.Nil(commit)
	})

	t.Run("over balance", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountSV(priv.Publickey(), 3)

		merger := newAccountSVM(accountStateKey(priv.Publickey()), prev)

		value := NewAccountSV(priv.Publickey(), (prev.Balance+1)*-1)

		commit, nextop, reason, err := merger.Merge(0, value)
		t.NoError(err)
		t.Equal(uint64(0), nextop)
		t.Nil(commit)
		t.Equal(reason, "insufficient balance")
	})
}

func (t *testAccountSVM) TestMerge() {
	t.Run("add balance", func() {
		priv := base.NewMPrivatekey()

		merger := newAccountSVM(accountStateKey(priv.Publickey()), &AccountSV{})

		value := NewAccountSV(priv.Publickey(), 3)

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
		prev := NewAccountSV(priv.Publickey(), 3)

		merger := newAccountSVM(accountStateKey(priv.Publickey()), prev)

		value := NewAccountSV(priv.Publickey(), -3)

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
		prev := NewAccountSV(priv.Publickey(), 3)

		merger := newAccountSVM(accountStateKey(priv.Publickey()), prev)

		sub0 := NewAccountSV(priv.Publickey(), -1)
		sub1 := NewAccountSV(priv.Publickey(), -1)

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
		prev := NewAccountSV(priv.Publickey(), 3)

		merger := newAccountSVM(accountStateKey(priv.Publickey()), prev)

		sub0 := NewAccountSV(priv.Publickey(), -1)
		sub1 := NewAccountSV(priv.Publickey(), -2)

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
		prev := NewAccountSV(priv.Publickey(), 3)

		values := make([]*AccountSV, 4)
		for i := range values {
			values[i] = NewAccountSV(priv.Publickey(), 1)
		}

		mergeValues := func(seq []int) {
			merger := newAccountSVM(accountStateKey(priv.Publickey()), prev)

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
		prev := NewAccountSV(priv.Publickey(), 4)

		values := make([]*AccountSV, 4)
		for i := range values {
			v := int64(1)
			if i == len(values)-1 {
				v *= -1
			}

			values[i] = NewAccountSV(priv.Publickey(), v)
		}

		mergeValues := func(seq []int) {
			merger := newAccountSVM(accountStateKey(priv.Publickey()), prev)

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

type stateVlaueMergerXMergeFunc func(uint64, base.StateValue) (func() error, uint64, error)

func noReasonMergeFunc(f func(uint64, base.StateValue) (func() error, uint64, string, error)) stateVlaueMergerXMergeFunc {
	return func(opindex uint64, value base.StateValue) (func() error, uint64, error) {
		commit, unmergeOp, _, err := f(opindex, value)

		return commit, unmergeOp, err
	}
}

func (t *testAccountSVM) TestMerges() {
	t.Run("adds + subs", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountSV(priv.Publickey(), 4)

		values := make([]*AccountSV, 5)
		for i := range values {
			v := int64(1)
			if i%2 == 0 {
				v *= -2
			}

			values[i] = NewAccountSV(priv.Publickey(), v)
		}

		var xmerge func(op uint64, value *AccountSV, mergef, unmergef stateVlaueMergerXMergeFunc) error

		xmerge = func(op uint64, value *AccountSV, mergef, unmergef stateVlaueMergerXMergeFunc) error {
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

			merger := newAccountSVM(accountStateKey(priv.Publickey()), prev)

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

		prev0 := NewAccountSV(pub0, 2)
		prev1 := NewAccountSV(pub1, 2)

		ops := map[uint64][]SMV{}

		var xmerge func(name string, op uint64, values []SMV, mergef, unmergef func(string) stateVlaueMergerXMergeFunc) error

		xmerge = func(name string, op uint64, values []SMV, mergef, unmergef func(string) stateVlaueMergerXMergeFunc) error {
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
						name, op, i, value.Value().(*AccountSV).Balance)

					return nil
				}

				addCommits(commit)

				if nextop != op {
					addNextops(nextop)
				}
			}

			for f := range slices.Values(doneCommits()) {
				if err := f(); err != nil {
					return err
				}
			}

			nextname := "unmerge"
			if name == "unmerge" {
				nextname = "merge"
			}

			for op := range slices.Values(doneNextops()) {
				if err := xmerge(nextname, op, ops[op], unmergef, mergef); err != nil {
					return err
				}
			}

			return nil
		}

		ops[0] = []SMV{
			t.newMergeValue(pub0, 1),
			t.newMergeValue(pub1, -2),
		}
		ops[1] = []SMV{ // ignored
			t.newMergeValue(pub0, -1),
			t.newMergeValue(pub1, -2),
		}
		ops[2] = []SMV{
			t.newMergeValue(pub0, -1),
			t.newMergeValue(pub1, 2),
		}
		ops[3] = []SMV{ // ignored
			t.newMergeValue(pub0, -1),
			t.newMergeValue(pub1, -2),
		}
		ops[4] = []SMV{
			t.newMergeValue(pub0, 1),
			t.newMergeValue(pub1, 2),
		}

		// expected: account 0: balance=3
		// expected: account 1: balance=4

		mergef := func(merger0, merger1 SVM) func(string) stateVlaueMergerXMergeFunc {
			return func(key string) stateVlaueMergerXMergeFunc {
				if key == accountStateKey(pub0) {
					return noReasonMergeFunc(merger0.Merge)
				}

				return noReasonMergeFunc(merger1.Merge)
			}
		}

		unmergef := func(merger0, merger1 SVM) func(string) stateVlaueMergerXMergeFunc {
			return func(key string) stateVlaueMergerXMergeFunc {
				if key == accountStateKey(pub0) {
					return merger0.Unmerge
				}

				return merger1.Unmerge
			}
		}

		gen := combin.NewPermutationGenerator(len(ops), len(ops))

		run := func(seq []int) {
			t.T().Log("seq:", seq)

			merger0 := newAccountSVM(accountStateKey(pub0), prev0)
			merger1 := newAccountSVM(accountStateKey(pub1), prev1)

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
				av := sv.(*AccountSV)
				t.T().Logf("account 0: balance=%d", av.Balance)

				t.Equal(int64(3), av.Balance)
			}

			{
				sv, err := merger1.Close()
				t.NoError(err)
				av := sv.(*AccountSV)
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

func (t *testAccountSVM) TestUnmergeError() {
	t.Run("0 balance", func() {
		priv := base.NewMPrivatekey()

		merger := newAccountSVM(accountStateKey(priv.Publickey()), &AccountSV{})

		commit, _, reason, err := merger.Merge(0, NewAccountSV(priv.Publickey(), 1))
		t.NoError(err)
		t.Empty(reason)

		t.NoError(commit())

		value := NewAccountSV(priv.Publickey(), 0)

		commit, nextop, err := merger.Unmerge(0, value)
		t.Error(err)
		t.ErrorContains(err, "zero balance")
		t.Equal(uint64(0), nextop)
		t.Nil(commit)
	})

	t.Run("wrong address", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountSV(priv.Publickey(), 3)

		merger := newAccountSVM(accountStateKey(priv.Publickey()), prev)

		commit, _, reason, err := merger.Merge(0, NewAccountSV(priv.Publickey(), 1))
		t.NoError(err)
		t.Empty(reason)

		t.NoError(commit())

		value := NewAccountSV(base.NewMPrivatekey().Publickey(), -3)

		commit, nextop, err := merger.Unmerge(0, value)
		t.Error(err)
		t.ErrorContains(err, "address not matched")
		t.Equal(uint64(0), nextop)
		t.Nil(commit)
	})
}

func TestAccountSVM(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testAccountSVM))
}

func TestEncodeAccountSV(tt *testing.T) {
	t := new(mencoder.BaseTestEncode)
	t.SetT(tt)

	enc := jsonenc.NewEncoder(context.Background(), 1<<9)
	defer enc.Close()

	t.NoError(jsonenc.AddDecode(enc, base.MPublickeyHint, &base.MPublickey{}, DecodeJSONMPublickey))
	t.NoError(jsonenc.AddDecode(enc, AccountSVHint, &AccountSV{}, AccountSVDecodeJSON))

	t.Encode = func() (interface{}, []byte) {
		v := NewAccountSV(base.NewMPrivatekey().Publickey(), 1)

		b, err := util.MarshalJSON(v)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return v, b
	}
	t.Decode = func(b []byte) interface{} {
		var u *AccountSV
		t.NoError(encoder.Unmarshal(enc, b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		av := a.(*AccountSV)
		bv := b.(*AccountSV)

		t.True(av.Hint().Equal(bv.Hint()))
		t.True(av.Address.Equal(bv.Address))
		t.Equal(av.Balance, bv.Balance)
	}

	t.TestDecode()
}
