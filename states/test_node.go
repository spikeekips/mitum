//go:build test
// +build test

package states

import "github.com/spikeekips/mitum/base"

func RandomLocalNode() *LocalNode {
	return NewLocalNode(base.NewMPrivatekey(), base.RandomAddress("local-"))
}
