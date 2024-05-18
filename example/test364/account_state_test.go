package testopsworkers

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"gonum.org/v1/gonum/stat/combin"
)

type baseTestAccountStateMerger struct {
	logging.BaseTestSuite
}

func (t *baseTestAccountStateMerger) randomAccountStateMergeValue(balance int64) (base.Publickey, AccountStateMergeValue) {
	pub := base.NewMPrivatekey().Publickey()

	return pub, t.newAccountStateMergeValue(pub, balance)
}

func (t *baseTestAccountStateMerger) newAccountStateMergeValue(address base.Publickey, balance int64) AccountStateMergeValue {
	v, err := NewAccountStateMergeValue(address, balance)
	t.NoError(err)

	return v
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
		t.True(ok)
		balance, ok := a[(i*2)+1].(int)
		t.True(ok)

		mv, err := NewAccountStateMergeValue(address, int64(balance))
		t.NoError(err)

		vs[i] = mv
	}

	return vs
}

type testAccountStateMerger struct {
	baseTestAccountStateMerger
}

func (t *testAccountStateMerger) TestMergeError() {
	t.Run("0 balance", func() {
		priv := base.NewMPrivatekey()

		merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), AccountStateValue{})

		value := NewAccountStateValue(priv.Publickey(), 0)

		commit, nextop, err := merger.Merge(0, value)
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

		commit, nextop, err := merger.Merge(0, value)
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

		commit, nextop, err := merger.Merge(0, value)
		t.NoError(err)
		t.Equal(uint64(0), nextop)
		t.Nil(commit)
	})
}

func (t *testAccountStateMerger) TestMerge() {
	t.Run("add balance", func() {
		priv := base.NewMPrivatekey()

		merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), AccountStateValue{})

		value := NewAccountStateValue(priv.Publickey(), 3)

		commit, nextop, err := merger.Merge(0, value)
		t.NoError(err)
		t.Equal(uint64(0), nextop)
		t.NotNil(commit)

		t.NoError(commit())

		t.Equal(int64(3), merger.sum)
	})

	t.Run("sub balance", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountStateValue(priv.Publickey(), 3)

		merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), prev)

		value := NewAccountStateValue(priv.Publickey(), -3)

		commit, nextop, err := merger.Merge(0, value)
		t.NoError(err)
		t.Equal(uint64(0), nextop)
		t.NotNil(commit)

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

		commit, nextop, err := merger.Merge(1, sub0)
		t.NoError(err)
		t.Equal(uint64(1), nextop)
		t.NotNil(commit)

		t.NoError(commit())

		t.Equal(int64(-1), merger.sum)
		t.Equal(uint64(1), *merger.lastop)
		t.Empty(merger.prepops)

		commit, nextop, err = merger.Merge(2, sub1)
		t.NoError(err)
		t.Equal(uint64(2), nextop)
		t.Nil(commit)
	})

	t.Run("sub1, sub2, unmerge", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountStateValue(priv.Publickey(), 3)

		merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), prev)

		sub0 := NewAccountStateValue(priv.Publickey(), -1)
		sub1 := NewAccountStateValue(priv.Publickey(), -2)

		commit, nextop, err := merger.Merge(2, sub1)
		t.NoError(err)
		t.Equal(uint64(2), nextop)
		t.NotNil(commit)

		t.NoError(commit())

		t.Equal(int64(-2), merger.sum)
		t.Equal(uint64(2), *merger.lastop)
		t.Empty(merger.prepops)

		commit, nextop, err = merger.Merge(1, sub0)
		t.NoError(err)
		t.Equal(uint64(2), nextop)
		t.NoError(commit())

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

		values := make([]AccountStateValue, 4)
		for i := range values {
			values[i] = NewAccountStateValue(priv.Publickey(), 1)
		}

		mergeValues := func(seq []int) {
			merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), prev)

			for i := range seq {
				op := uint64(seq[i])

				commit, nextop, err := merger.Merge(op, values[op])
				t.NoError(err, seq)
				t.Equal(op, nextop, seq)
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

		values := make([]AccountStateValue, 4)
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

				commit, nextop, err := merger.Merge(op, values[op])
				t.NoError(err, seq)
				t.Equal(op, nextop, seq)
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

func (t *testAccountStateMerger) TestMerges() {
	t.Run("adds + subs", func() {
		priv := base.NewMPrivatekey()
		prev := NewAccountStateValue(priv.Publickey(), 4)

		values := make([]AccountStateValue, 5)
		for i := range values {
			v := int64(1)
			if i%2 == 0 {
				v *= -2
			}

			values[i] = NewAccountStateValue(priv.Publickey(), v)
		}

		var xmerge func(op uint64, value AccountStateValue, mergef, unmergef StateMergeXMergeFunc) error

		xmerge = func(op uint64, value AccountStateValue, mergef, unmergef StateMergeXMergeFunc) error {
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

				t.NoError(xmerge(op, value, merger.Merge, merger.Unmerge), seq)
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

		var xmerge func(string, uint64, []StateMergeValue, func(string) StateMergeXMergeFunc, func(string) StateMergeXMergeFunc) error

		xmerge = func(name string, op uint64, values []StateMergeValue, mergef, unmergef func(string) StateMergeXMergeFunc) error {
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
						name, op, i, value.Value().(AccountStateValue).Balance)

					return nil
				}

				addCommits(commit)

				if nextop != op {
					addNextops(nextop)
				}
			}

			if err := util.TraverseSlice(doneCommits(), func(_ int, f func() error) error {
				return f()
			}); err != nil {
				return err
			}

			nextname := "unmerge"
			if name == "unmerge" {
				nextname = "merge"
			}

			if err := util.TraverseSlice(doneNextops(), func(_ int, op uint64) error {
				return xmerge(nextname, op, ops[op], unmergef, mergef)
			}); err != nil {
				return err
			}

			return nil
		}

		ops[0] = []StateMergeValue{
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub1, -2),
		}
		ops[1] = []StateMergeValue{ // ignored
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -2),
		}
		ops[2] = []StateMergeValue{
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, 2),
		}
		ops[3] = []StateMergeValue{ // ignored
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -2),
		}
		ops[4] = []StateMergeValue{
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub1, 2),
		}

		// expected: account 0: balance=3
		// expected: account 1: balance=4

		mergef := func(merger0, merger1 StateMerger) func(key string) StateMergeXMergeFunc {
			return func(key string) StateMergeXMergeFunc {
				if key == AccountStateKey(pub0) {
					return merger0.Merge
				}

				return merger1.Merge
			}
		}

		unmergef := func(merger0, merger1 StateMerger) func(key string) StateMergeXMergeFunc {
			return func(key string) StateMergeXMergeFunc {
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
				av := sv.(AccountStateValue)
				t.T().Logf("account 0: balance=%d", av.Balance)

				t.Equal(int64(3), av.Balance)
			}

			{
				sv, err := merger1.Close()
				t.NoError(err)
				av := sv.(AccountStateValue)
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

		merger := NewAccountStateMerger(AccountStateKey(priv.Publickey()), AccountStateValue{})

		commit, _, err := merger.Merge(0, NewAccountStateValue(priv.Publickey(), 1))
		t.NoError(err)
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

		commit, _, err := merger.Merge(0, NewAccountStateValue(priv.Publickey(), 1))
		t.NoError(err)
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
