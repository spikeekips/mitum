package states

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type suffrage struct {
	m  map[string]struct{}
	ns []base.Address
}

func newSuffrage(nodes []base.Address) (suffrage, error) {
	e := util.StringErrorFunc("failed to newsuffrage")

	m := map[string]struct{}{}
	for i := range nodes {
		if nodes[i] == nil {
			return suffrage{}, e(nil, "nil node address")
		}
		m[nodes[i].String()] = struct{}{}
	}

	if util.CheckSliceDuplicated(nodes, func(i interface{}) string {
		return i.(base.Address).String()
	}) {
		return suffrage{}, e(nil, "duplicated node address found")
	}

	return suffrage{m: m, ns: nodes}, nil
}

func (suf suffrage) Exists(node base.Address) bool {
	_, found := suf.m[node.String()]

	return found
}

func (suf suffrage) Nodes() []base.Address {
	return suf.ns
}

func (suf suffrage) Len() int {
	return len(suf.ns)
}
