package main

import (
	"encoding/json"
	"slices"

	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var BaseStateHint = hint.MustNewHint("test-base-state-v0.0.1")

type BaseState struct {
	h        util.Hash
	previous util.Hash
	v        base.StateValue
	k        string
	ops      []util.Hash
	hint.BaseHinter
	height base.Height
}

func newBaseState(
	height base.Height,
	k string,
	v base.StateValue,
	previous util.Hash,
	ops []util.Hash,
) *BaseState {
	s := &BaseState{
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

func (s *BaseState) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid base state")

	if err := util.CheckIsValiders(nil, false,
		s.BaseHinter,
		s.h,
		s.height,
		util.DummyIsValider(func([]byte) error {
			if len(s.k) < 1 {
				return e.Errorf("empty state key")
			}

			return nil
		}),
		s.v,
	); err != nil {
		return e.Wrap(err)
	}

	for h := range slices.Values(s.ops) {
		if err := h.IsValid(nil); err != nil {
			return e.Wrap(err)
		}
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

func (s *BaseState) Hash() util.Hash {
	return s.h
}

func (s *BaseState) Previous() util.Hash {
	return s.previous
}

func (s *BaseState) Key() string {
	return s.k
}

func (s *BaseState) Value() base.StateValue {
	return s.v
}

func (s *BaseState) Height() base.Height {
	return s.height
}

func (s *BaseState) Operations() []util.Hash {
	return s.ops
}

func (s *BaseState) generateHash() util.Hash {
	var bs [][]byte
	var bsSize int

	switch {
	case len(s.ops) < 1:
		bsSize = 2
	default:
		bsSize = len(s.ops) + 2
	}

	bs = make([][]byte, bsSize)
	defer dutil.ClearSlice(bs)

	if s.previous != nil {
		bs[0] = s.previous.Bytes()
	}

	if s.v != nil {
		bs[1] = s.v.HashBytes()
	}

	for i := range s.ops {
		if s.ops[i] == nil {
			continue
		}

		bs[i+2] = s.ops[i].Bytes()
	}

	return valuehash.NewSHA256(slices.Concat(bs...))
}

type baseStateJSONMarshaler struct {
	Hash       util.Hash       `json:"hash"`
	Previous   util.Hash       `json:"previous"`
	Value      base.StateValue `json:"value"`
	Key        string          `json:"key"`
	Operations []util.Hash     `json:"operations"`
	hint.BaseHinter
	Height base.Height `json:"height"`
}

func (s *BaseState) MarshalJSON() ([]byte, error) {
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
	Height     base.HeightDecoder      `json:"height"`
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

	switch i, err := base.DecodeStateValue(u.Value, enc); {
	case err != nil:
		return e.Wrap(err)
	default:
		s.v = i
	}

	return nil
}

type StateMergeValue interface {
	Key() string
	Value() base.StateValue
	Merger(prev base.StateValue) (StateMerger, error)
}

type StateMerger interface {
	// Key is state key.
	Key() string
	// Close sets StateMerger not to get no more Merge or Unmerge.
	Close() (mergedValue base.StateValue, _ error)
	// Merge merges new (partial) state value to state. If failed to merge,
	// *reason* describes the reason why merge(unmerge) opeartion is not merged.
	// *unmergeOp* is to be unmerge after merging the new state value.
	//
	// - If *commit* is nil and *reason* is not empty, *reason* indicates
	// the *opindex* operation is not merged with *reason*.
	// - If *unmergeOp* is not same with the *opindex* opeation, *reason*
	// indicates the *unmergeOp* is not merged with *reason*.
	Merge(opindex uint64, value base.StateValue) (
		commit func() error,
		unmergeOp uint64,
		reason string,
		_ error,
	)
	// Unmerge cancels merged state value. mergeOp is the operation index
	// in proposal to be merge after canceling the state value. *reason*
	// describes the reason why operation is unmerged. It can be empty without
	// error.
	//
	// - If *mergeOp* is same with the operation index, it means there is
	// nothing to cancel to merge.
	Unmerge(opindex uint64, value base.StateValue) (
		commit func() error,
		mergeOp uint64,
		_ error,
	)
}

func IsEqualStateMergeValue(a, b StateMergeValue) bool {
	switch {
	case a == nil && b == nil,
		a == nil, b == nil:
		return false
	case a.Key() != b.Key(),
		!base.IsEqualStateValue(a.Value(), b.Value()):
		return false
	default:
		return true
	}
}
