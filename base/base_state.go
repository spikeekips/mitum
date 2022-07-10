package base

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var BaseStateHint = hint.MustNewHint("base-state-v0.0.1")

type BaseState struct {
	h        util.Hash
	previous util.Hash
	v        StateValue
	k        string
	ops      []util.Hash
	util.DefaultJSONMarshaled
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
	e := util.StringErrorFunc("invalid base state")

	vs := make([]util.IsValider, len(s.ops)+5)
	vs[0] = s.BaseHinter
	vs[1] = s.h
	vs[2] = s.height
	vs[3] = util.DummyIsValider(func([]byte) error {
		if len(s.k) < 1 {
			return util.ErrInvalid.Errorf("empty state key")
		}

		return nil
	})
	vs[4] = s.v

	for i := range s.ops {
		vs[i+5] = s.ops[i]
	}

	if err := util.CheckIsValid(nil, false, vs...); err != nil {
		return e(err, "")
	}

	if s.previous != nil {
		if err := s.previous.IsValid(nil); err != nil {
			return e(err, "invalid previous state hash")
		}
	}

	if !s.h.Equal(s.generateHash()) {
		return util.ErrInvalid.Errorf("wrong hash")
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

func (s *BaseState) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to unmarshal BaseState")

	var u baseStateJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
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
		return e(err, "")
	default:
		s.v = i
	}

	return nil
}

type BaseStateValueMerger struct {
	State
	value  StateValue
	nst    State
	ops    []util.Hash
	height Height
	key    string
	sync.RWMutex
}

func NewBaseStateValueMerger(height Height, key string, st State) *BaseStateValueMerger {
	var value StateValue
	if st != nil {
		key = st.Key()
		value = st.Value()
	}

	return &BaseStateValueMerger{
		State:  st,
		height: height,
		value:  value,
		key:    key,
	}
}

func (s *BaseStateValueMerger) Hash() util.Hash {
	s.RLock()
	defer s.RUnlock()

	if s.nst == nil {
		return nil
	}

	return s.nst.Hash()
}

func (s *BaseStateValueMerger) Height() Height {
	return s.height
}

func (s *BaseStateValueMerger) Previous() util.Hash {
	s.RLock()
	defer s.RUnlock()

	if s.nst == nil {
		return nil
	}

	return s.nst.Previous()
}

func (s *BaseStateValueMerger) Value() StateValue {
	s.RLock()
	defer s.RUnlock()

	if s.nst == nil {
		return nil
	}

	return s.nst.Value()
}

func (s *BaseStateValueMerger) SetValue(v StateValue) {
	s.Lock()
	defer s.Unlock()

	s.value = v
}

func (s *BaseStateValueMerger) Operations() []util.Hash {
	s.RLock()
	defer s.RUnlock()

	return s.nst.Operations()
}

func (s *BaseStateValueMerger) Merge(value StateValue, ops []util.Hash) error {
	s.Lock()
	defer s.Unlock()

	s.value = value

	s.AddOperations(ops)

	return nil
}

func (s *BaseStateValueMerger) Close() error {
	s.Lock()
	defer s.Unlock()

	e := util.StringErrorFunc("failed to close BaseStateValueMerger")

	if s.value == nil {
		return e(nil, "empty value")
	}

	sort.Slice(s.ops, func(i, j int) bool {
		return strings.Compare(s.ops[i].String(), s.ops[j].String()) < 0
	})

	var previous util.Hash
	if s.State != nil {
		previous = s.State.Hash()
	}

	s.nst = NewBaseState(s.height, s.key, s.value, previous, s.ops)

	return nil
}

func (s *BaseStateValueMerger) AddOperations(ops []util.Hash) {
	nops := make([]util.Hash, len(s.ops)+len(ops))
	copy(nops[:len(s.ops)], s.ops)
	copy(nops[len(s.ops):], ops)

	s.ops = nops
}

func (s *BaseStateValueMerger) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(s.nst)
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
	e := util.StringErrorFunc("failed to decode StateValue")

	var s StateValue
	if err := encoder.Decode(enc, b, &s); err != nil {
		return nil, e(err, "")
	}

	return s, nil
}
