//go:build test
// +build test

package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

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
		return util.InvalidError.Errorf("empty string in DummyStateValue")
	}

	return nil
}

func (s DummyStateValue) Equal(b StateValue) bool {
	switch {
	case b == nil:
		return false
	case s.Hint().Type() != b.Hint().Type():
		return false
	}

	switch j, ok := b.(DummyStateValue); {
	case !ok:
		return false
	case s.S != j.S:
		return false
	default:
		return true
	}
}
