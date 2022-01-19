//go:build test
// +build test

package base

import "github.com/spikeekips/mitum/util"

type DummySuffrageInfo struct{}

func (sf DummySuffrageInfo) HashBytes() []byte {
	return nil
}

func (sf DummySuffrageInfo) Hash() util.Hash {
	return nil
}

func (sf DummySuffrageInfo) IsValid([]byte) error {
	return nil
}

func (sf DummySuffrageInfo) Threshold() Threshold {
	return Threshold{}
}

func (sf DummySuffrageInfo) Nodes() []Address {
	return nil
}
