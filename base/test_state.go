//go:build test
// +build test

package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

func (s *BaseState) SetOperations(ops []util.Hash) BaseState {
	s.ops = ops

	return *s
}

var DummyStateValueHint = hint.MustNewHint("dummy-state-value-v0.0.1")

type DummyStateValue struct {
	hint.BaseHinter
	S string `json:"s"`
}

func NewDummyStateValue(s string) DummyStateValue {
	return DummyStateValue{BaseHinter: hint.NewBaseHinter(DummyStateValueHint), S: s}
}

func (s DummyStateValue) HashBytes() []byte {
	return []byte(s.S)
}

func (s DummyStateValue) IsValid([]byte) error {
	if len(s.S) < 1 {
		return util.ErrInvalid.Errorf("empty string in DummyStateValue")
	}

	return nil
}

func NilGetState(string) (State, bool, error) {
	return nil, false, nil
}
