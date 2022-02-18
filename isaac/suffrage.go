package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type suffrage struct {
	m  map[string]base.Node
	ns []base.Node
}

func newSuffrage(nodes []base.Node) (suffrage, error) {
	e := util.StringErrorFunc("failed to newsuffrage")

	m := map[string]base.Node{}
	for i := range nodes {
		n := nodes[i]
		if n == nil {
			return suffrage{}, e(nil, "nil node address")
		}

		m[n.Address().String()] = n
	}

	if util.CheckSliceDuplicated(nodes, func(i interface{}) string {
		return i.(base.Node).Address().String()
	}) {
		return suffrage{}, e(nil, "duplicated node address found")
	}

	return suffrage{m: m, ns: nodes}, nil
}

func (suf suffrage) Exists(node base.Address) bool {
	_, found := suf.m[node.String()]

	return found
}

func (suf suffrage) ExistsPublickey(node base.Address, pub base.Publickey) bool {
	switch n, found := suf.m[node.String()]; {
	case !found:
		return false
	case !n.Publickey().Equal(pub):
		return false
	default:
		return true
	}
}

func (suf suffrage) Nodes() []base.Node {
	return suf.ns
}

func (suf suffrage) Len() int {
	return len(suf.ns)
}
