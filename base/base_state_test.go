package base

import (
	"context"
	"math"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

var dummySimpleStateValueHint = hint.MustNewHint("dummy-simple-state-value-v0.0.1")

type dummySimpleStateValue struct {
	hint.BaseHinter
	K string `json:"key"`
	I int64  `json:"value"`
}

func newDummySimpleStateValue(i int64) dummySimpleStateValue {
	return dummySimpleStateValue{BaseHinter: hint.NewBaseHinter(dummySimpleStateValueHint), I: i}
}

func (s dummySimpleStateValue) Key() string {
	return s.K
}

func (s dummySimpleStateValue) HashBytes() []byte {
	return util.Int64ToBytes(s.I)
}

func (s dummySimpleStateValue) IsValid([]byte) error {
	return nil
}

func (s dummySimpleStateValue) Merger(height Height, st State) StateValueMerger {
	if st == nil {
		st = newDummyState(NilHeight, s.K, nil, nil)
	}

	return newDummySimpleStateValueMerger(height, s.K, st)
}

type dummyState struct {
	BaseState
}

func newDummyState(height Height, k string, v StateValue, previous util.Hash) dummyState {
	return dummyState{
		BaseState: NewBaseState(height, k, v, previous, nil),
	}
}

type dummySimpleStateValueMerger struct {
	sync.RWMutex
	*BaseStateValueMerger
	S int64
}

func newDummySimpleStateValueMerger(height Height, key string, st State) *dummySimpleStateValueMerger {
	return &dummySimpleStateValueMerger{
		BaseStateValueMerger: NewBaseStateValueMerger(height, key, st),
	}
}

func (s *dummySimpleStateValueMerger) Merge(value StateValue, op util.Hash) error {
	s.Lock()
	defer s.Unlock()

	i, ok := value.(dummySimpleStateValue)
	if !ok {
		return errors.Errorf("not dummySimpleStateValue, but %T", value)
	}
	s.S += i.I

	s.addOperation(op)

	return nil
}

func (s *dummySimpleStateValueMerger) Close() error {
	s.Lock()
	defer s.Unlock()

	if len(s.ops) > 0 {
		s.SetValue(newDummySimpleStateValue(s.S))
	}

	return s.BaseStateValueMerger.Close()
}

type testStateValueMerger struct {
	suite.Suite
}

func (t *testStateValueMerger) TestNew() {
	sv := newDummySimpleStateValue(55)

	t.Run("newDummySimpleStateValue is StateValue", func() {
		_ = (interface{})(sv).(StateValue)
	})

	t.Run("NewBaseState is State", func() {
		st := NewBaseState(Height(33), util.UUID().String(), sv, valuehash.RandomSHA256(), nil)
		_ = (interface{})(st).(State)
	})

	t.Run("dummySimpleStateValueMerger is valid StateValueMerger", func() {
		st := newDummyState(Height(33), util.UUID().String(), sv, valuehash.RandomSHA256())
		_ = (interface{})(st).(State)

		v := newDummySimpleStateValue(55)
		merger := v.Merger(Height(44), st)
		_ = (merger).(State)
	})

	t.Run("not merged", func() {
		height := Height(33)

		st := newDummyState(height, util.UUID().String(), sv, valuehash.RandomSHA256())

		merger := sv.Merger(height+1, st)

		err := merger.Close()
		t.Error(err)
		t.True(errors.Is(err, ErrIgnoreStateValue))

		t.Nil(merger.Value())
	})
}

func (t *testStateValueMerger) TestAsyncMerge() {
	worker, _ := util.NewErrgroupWorker(context.Background(), math.MaxInt16)
	defer worker.Close()

	sv := newDummySimpleStateValue(55)
	st := newDummyState(Height(33), util.UUID().String(), sv, valuehash.RandomSHA256())
	merger := sv.Merger(Height(44), st)

	ops := make([]util.Hash, 301)

	go func() {
		for i := range ops {
			ops[i] = valuehash.RandomSHA256()
			v := newDummySimpleStateValue(int64(i))

			i := i
			t.NoError(worker.NewJob(func(context.Context, uint64) error {
				return merger.Merge(v, ops[i])
			}))
		}
		worker.Done()
	}()

	t.NoError(worker.Wait())

	t.NoError(merger.Close())

	dm := merger.(*dummySimpleStateValueMerger)

	t.Run("added value", func() {
		t.Equal(int64(301*150), dm.S)
	})

	t.Run("State inside merger still same", func() {
		t.True(IsEqualState(st, dm.State))
		t.False(IsEqualState(st, dm))
	})
}

func (t *testStateValueMerger) TestMergedSameHash() {
	worker, _ := util.NewErrgroupWorker(context.Background(), math.MaxInt16)
	defer worker.Close()

	sv := newDummySimpleStateValue(55)
	st := newDummyState(Height(33), util.UUID().String(), sv, valuehash.RandomSHA256())
	merger0 := sv.Merger(Height(44), st)
	merger1 := sv.Merger(Height(44), st)

	ops := make([]util.Hash, 301)

	go func() {
		for i := range ops {
			if ops[i] == nil {
				ops[i] = valuehash.RandomSHA256()
			}
			if ops[len(ops)-i-1] == nil {
				ops[len(ops)-i-1] = valuehash.RandomSHA256()
			}
			v := newDummySimpleStateValue(int64(i))

			i := i
			t.NoError(worker.NewJob(func(context.Context, uint64) error {
				_ = merger0.Merge(v, ops[i])

				return merger1.Merge(v, ops[len(ops)-i-1])
			}))
		}

		worker.Done()
	}()

	t.NoError(worker.Wait())

	t.NoError(merger0.Close())
	t.NoError(merger1.Close())

	dm0 := merger0.(*dummySimpleStateValueMerger)
	dm1 := merger1.(*dummySimpleStateValueMerger)

	t.Run("2 mergers has same value", func() {
		t.Equal(int64(301*150), dm0.S)
		t.Equal(dm0.S, dm1.S)
	})

	t.Run("2 mergers has same hash", func() {
		t.NotNil(dm0.Hash())
		t.NotNil(dm1.Hash())

		t.True(dm0.Hash().Equal(dm1.Hash()))
	})
}

func TestStateValueMerger(t *testing.T) {
	suite.Run(t, new(testStateValueMerger))
}

func TestBaseStateEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		sv := newDummySimpleStateValue(66)
		st := NewBaseState(Height(33), util.UUID().String(), sv, valuehash.RandomSHA256(), nil)

		b, err := enc.Marshal(st)
		t.NoError(err)

		return st, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: dummySimpleStateValueHint, Instance: dummySimpleStateValue{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: BaseStateHint, Instance: BaseState{}}))

		hinter, err := enc.Decode(b)
		t.NoError(err)

		return hinter.(BaseState)
	}
	t.Compare = func(a interface{}, b interface{}) {
		as := a.(State)
		bs := b.(State)

		t.NoError(as.IsValid(nil))
		t.NoError(bs.IsValid(nil))

		t.True(IsEqualState(as, bs))
	}

	suite.Run(tt, t)
}

func TestStateValueMergerEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	sv := newDummySimpleStateValue(55)
	st := newDummyState(Height(33), util.UUID().String(), sv, valuehash.RandomSHA256())
	merger := sv.Merger(Height(44), st)

	t.Encode = func() (interface{}, []byte) {
		t.NoError(merger.Merge(newDummySimpleStateValue(77), valuehash.RandomSHA256()))
		t.NoError(merger.Close())

		b, err := enc.Marshal(merger)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return merger.(*dummySimpleStateValueMerger), b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: dummySimpleStateValueHint, Instance: dummySimpleStateValue{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: BaseStateHint, Instance: BaseState{}}))

		hinter, err := enc.Decode(b)
		t.NoError(err)

		return hinter.(BaseState)
	}
	t.Compare = func(a interface{}, b interface{}) {
		as := a.(State)
		bs := b.(State)

		t.NoError(as.IsValid(nil))
		t.NoError(bs.IsValid(nil))

		t.True(IsEqualState(as, bs))
	}

	suite.Run(tt, t)
}
