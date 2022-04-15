//go:build test
// +build test

package isaac

import (
	"fmt"
	"sort"

	"github.com/spikeekips/mitum/base"
)

func (suf Suffrage) Locals() []LocalNode {
	locals := make([]LocalNode, suf.Len())
	for i := range suf.ns {
		locals[i] = suf.ns[i].(LocalNode)
	}

	return locals
}

func NewTestSuffrage(n int, extras ...LocalNode) (Suffrage, []LocalNode) {
	locals := make([]LocalNode, n+len(extras))
	nodes := make([]base.Node, n+len(extras))
	for i := range make([]int, n) {
		l := RandomLocalNode()
		l.addr = base.NewStringAddress(fmt.Sprintf("no%02d", i))

		nodes[i] = l
		locals[i] = l
	}

	for i := range extras {
		l := extras[i]
		locals[i+n] = l
		nodes[i+n] = l
	}

	suf, _ := NewSuffrage(nodes)

	sort.Slice(locals, func(i, j int) bool {
		return locals[i].Address().String() < locals[j].Address().String()
	})

	return suf, locals
}
