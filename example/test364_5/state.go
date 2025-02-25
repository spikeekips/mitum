package dop5

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

type baseState struct {
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
) *baseState {
	s := &baseState{
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

func (s *baseState) IsValid([]byte) error {
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

func (s *baseState) Hash() util.Hash {
	return s.h
}

func (s *baseState) Previous() util.Hash {
	return s.previous
}

func (s *baseState) Key() string {
	return s.k
}

func (s *baseState) Value() base.StateValue {
	return s.v
}

func (s *baseState) Height() base.Height {
	return s.height
}

func (s *baseState) Operations() []util.Hash {
	return s.ops
}

func (s *baseState) generateHash() util.Hash {
	w, sum := dutil.NewSHA256HashWriter()

	if s.previous != nil {
		w(s.previous.Bytes())
	}

	if s.v != nil {
		w(s.v.HashBytes())
	}

	for i := range s.ops {
		if s.ops[i] == nil {
			continue
		}

		w(s.ops[i].Bytes())
	}

	return sum()
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

func (s *baseState) MarshalJSON() ([]byte, error) {
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

func (s *baseState) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u baseStateJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
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
		return err
	default:
		s.v = i
	}

	return nil
}

type SMV interface { // State Merge Value
	Key() string
	Value() base.StateValue
	Merger(prev base.StateValue) (SVM, error)
}

type SVM interface { // state Value Merger
	// Key is state key.
	Key() string
	// Close sets SVM not to get no more Merge or Unmerge.
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

func isEqualSMV(a, b SMV) bool {
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
