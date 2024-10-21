package base

import (
	"encoding/json"
	"sort"
	"sync"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"golang.org/x/exp/slices"
)

var stateValueMergerPool = sync.Pool{
	New: func() any {
		return &BaseStateValueMerger{}
	},
}

var BaseStateHint = hint.MustNewHint("base-state-v0.0.1")

var ErrIgnoreStateValue = util.NewIDError("ignore state value")

type BaseState struct {
	h        util.Hash
	previous util.Hash
	v        StateValue
	k        string
	ops      []util.Hash
	hint.BaseHinter
	height Height
}

func NewBaseState(
	height Height,
	k string,
	v StateValue,
	previous util.Hash,
	ops []util.Hash,
) BaseState {
	s := BaseState{
		BaseHinter: hint.NewBaseHinter(BaseStateHint),
		height:     height,
		k:          k,
		v:          v,
		previous:   previous,
		ops:        ops,
	}

	s.h = s.generateHash()

	return s
}

func (s BaseState) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid base state")

	vs := make([]util.IsValider, len(s.ops)+5)
	vs[0] = s.BaseHinter
	vs[1] = s.h
	vs[2] = s.height
	vs[3] = util.DummyIsValider(func([]byte) error {
		if len(s.k) < 1 {
			return e.Errorf("empty state key")
		}

		return nil
	})
	vs[4] = s.v

	for i := range s.ops {
		vs[i+5] = s.ops[i]
	}

	if err := util.CheckIsValiders(nil, false, vs...); err != nil {
		return e.Wrap(err)
	}

	if s.previous != nil {
		if err := s.previous.IsValid(nil); err != nil {
			return e.WithMessage(err, "invalid previous state hash")
		}
	}

	if !s.h.Equal(s.generateHash()) {
		return e.Errorf("wrong hash")
	}

	return nil
}

func (s BaseState) Hash() util.Hash {
	return s.h
}

func (s BaseState) Previous() util.Hash {
	return s.previous
}

func (s BaseState) Key() string {
	return s.k
}

func (s BaseState) Value() StateValue {
	return s.v
}

func (s BaseState) Height() Height {
	return s.height
}

func (s BaseState) Operations() []util.Hash {
	return s.ops
}

func (s BaseState) generateHash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.DummyByter(func() []byte {
			if s.previous == nil {
				return nil
			}

			return s.previous.Bytes()
		}),
		util.BytesToByter([]byte(s.k)),
		util.DummyByter(func() []byte {
			if s.v == nil {
				return nil
			}

			return s.v.HashBytes()
		}),
		util.DummyByter(func() []byte {
			if len(s.ops) < 1 {
				return nil
			}

			bs := make([][]byte, len(s.ops))

			for i := range s.ops {
				if s.ops[i] == nil {
					continue
				}

				bs[i] = s.ops[i].Bytes()
			}

			return util.ConcatBytesSlice(bs...)
		}),
	))
}

type baseStateJSONMarshaler struct {
	Hash       util.Hash   `json:"hash"`
	Previous   util.Hash   `json:"previous"`
	Value      StateValue  `json:"value"`
	Key        string      `json:"key"`
	Operations []util.Hash `json:"operations"`
	hint.BaseHinter
	Height Height `json:"height"`
}

func (s BaseState) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseStateJSONMarshaler{
		BaseHinter: s.BaseHinter,
		Hash:       s.h,
		Previous:   s.previous,
		Height:     s.height,
		Key:        s.k,
		Value:      s.v,
		Operations: s.ops,
	})
}

type baseStateJSONUnmarshaler struct {
	Hash       valuehash.HashDecoder   `json:"hash"`
	Previous   valuehash.HashDecoder   `json:"previous"`
	Key        string                  `json:"key"`
	Value      json.RawMessage         `json:"value"`
	Operations []valuehash.HashDecoder `json:"operations"`
	Height     HeightDecoder           `json:"height"`
}

func (s *BaseState) DecodeJSON(b []byte, enc encoder.Encoder) error {
	e := util.StringError("decode BaseState")

	var u baseStateJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	s.h = u.Hash.Hash()
	s.previous = u.Previous.Hash()
	s.height = u.Height.Height()
	s.k = u.Key

	s.ops = make([]util.Hash, len(u.Operations))

	for i := range u.Operations {
		s.ops[i] = u.Operations[i].Hash()
	}

	switch i, err := DecodeStateValue(u.Value, enc); {
	case err != nil:
		return e.Wrap(err)
	default:
		s.v = i
	}

	return nil
}

type BaseStateValueMerger struct {
	st     State
	value  StateValue
	key    string
	ops    []util.Hash
	height Height
	l      sync.Mutex
}

func NewBaseStateValueMerger(height Height, key string, st State) *BaseStateValueMerger {
	s := stateValueMergerPool.Get().(*BaseStateValueMerger) //nolint:forcetypeassert //...

	s.Reset(height, key, st)

	return s
}

func (s *BaseStateValueMerger) Reset(height Height, key string, st State) {
	nkey := key

	if st != nil {
		nkey = st.Key()
	}

	s.st = st
	s.height = height
	s.key = nkey
	s.value = nil
	s.ops = nil
}

func (s *BaseStateValueMerger) Close() error {
	s.l.Lock()
	defer s.l.Unlock()

	s.height = 0
	s.st = nil
	s.key = ""
	s.value = nil
	s.ops = nil

	stateValueMergerPool.Put(s)

	return nil
}

func (s *BaseStateValueMerger) Key() string {
	return s.key
}

func (s *BaseStateValueMerger) Merge(value StateValue, op util.Hash) error {
	s.l.Lock()
	defer s.l.Unlock()

	s.value = value

	s.addOperation(op)

	return nil
}

func (s *BaseStateValueMerger) CloseValue() (State, error) {
	s.l.Lock()
	defer s.l.Unlock()

	if s.value == nil || len(s.ops) < 1 {
		return nil, ErrIgnoreStateValue.Errorf("empty state value")
	}

	sort.Slice(s.ops, func(i, j int) bool {
		return s.ops[i].String() < s.ops[j].String()
	})

	var previous util.Hash
	if s.st != nil {
		previous = s.st.Hash()
	}

	return NewBaseState(s.height, s.key, s.value, previous, s.ops), nil
}

func (s *BaseStateValueMerger) AddOperation(op util.Hash) {
	s.l.Lock()
	defer s.l.Unlock()

	s.addOperation(op)
}

func (s *BaseStateValueMerger) Height() Height {
	return s.height
}

func (s *BaseStateValueMerger) State() State {
	return s.st
}

func (s *BaseStateValueMerger) SetValue(v StateValue) {
	s.l.Lock()
	defer s.l.Unlock()

	s.value = v
}

func (s *BaseStateValueMerger) addOperation(op util.Hash) {
	if slices.IndexFunc(s.ops, func(i util.Hash) bool {
		return i.Equal(op)
	}) >= 0 {
		return
	}

	nops := make([]util.Hash, len(s.ops)+1)
	copy(nops[:len(s.ops)], s.ops)
	nops[len(s.ops)] = op

	s.ops = nops
}

type BaseStateMergeValue struct {
	StateValue
	merger func(Height, State) StateValueMerger
	key    string
}

func NewBaseStateMergeValue(
	key string,
	value StateValue,
	merger func(Height, State) StateValueMerger,
) BaseStateMergeValue {
	v := BaseStateMergeValue{StateValue: value, key: key, merger: merger}

	if merger == nil {
		v.merger = v.defaultMerger
	}

	return v
}

func (v BaseStateMergeValue) Key() string {
	return v.key
}

func (v BaseStateMergeValue) Value() StateValue {
	return v.StateValue
}

func (v BaseStateMergeValue) Merger(height Height, st State) StateValueMerger {
	return v.merger(height, st)
}

func (v BaseStateMergeValue) defaultMerger(height Height, st State) StateValueMerger {
	nst := st
	if st == nil {
		nst = NewBaseState(NilHeight, v.key, nil, nil, nil)
	}

	return NewBaseStateValueMerger(height, nst.Key(), nst)
}

func DecodeStateValue(b []byte, enc encoder.Encoder) (StateValue, error) {
	e := util.StringError("decode StateValue")

	var s StateValue
	if err := encoder.Decode(enc, b, &s); err != nil {
		return nil, e.Wrap(err)
	}

	return s, nil
}
