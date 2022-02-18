//go:build test
// +build test

package isaac

import "github.com/spikeekips/mitum/base"

func (suf suffrage) Locals() []*LocalNode {
	locals := make([]*LocalNode, suf.Len())
	for i := range suf.ns {
		locals[i] = suf.ns[i].(*LocalNode)
	}

	return locals
}

func newTestSuffrage(n int) (suffrage, []*LocalNode) {
	locals := make([]*LocalNode, n)
	nodes := make([]base.Node, n)
	for i := range nodes {
		n := RandomLocalNode()
		nodes[i] = n
		locals[i] = n
	}

	suf, _ := newSuffrage(nodes)

	return suf, locals
}
