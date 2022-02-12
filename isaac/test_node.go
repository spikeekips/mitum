//go:build test
// +build test

package isaac

import "github.com/spikeekips/mitum/base"

func RandomLocalNode() *LocalNode {
	return NewLocalNode(base.NewMPrivatekey(), base.RandomAddress("local-"))
}
