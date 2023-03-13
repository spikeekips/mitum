//go:build test
// +build test

package isaac

import (
	"fmt"
	"sort"

	"github.com/spikeekips/mitum/base"
)

func (suf Suffrage) Locals() []base.LocalNode {
	locals := make([]base.LocalNode, suf.Len())
	for i := range suf.ns {
		locals[i] = suf.ns[i].(base.LocalNode)
	}

	return locals
}

func NewTestSuffrage(n int, extras ...base.LocalNode) (Suffrage, []base.LocalNode) {
	locals := make([]base.LocalNode, n+len(extras))
	nodes := make([]base.Node, n+len(extras))
	for i := range make([]int, n) {
		l := NewLocalNode(base.NewMPrivatekey(), base.NewStringAddress(fmt.Sprintf("no%02d", i)))

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

func (s *SuffrageStateBuilder) SetBatchLimit(i uint64) {
	s.batchlimit = i
}
