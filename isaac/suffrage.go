package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type Suffrage struct {
	m  map[string]base.Node
	ns []base.Node
}

func NewSuffrage(nodes []base.Node) (Suffrage, error) {
	e := util.StringErrorFunc("failed to newsuffrage")

	if len(nodes) < 1 {
		return Suffrage{}, e(nil, "empty suffrage nodes")
	}

	m := map[string]base.Node{}

	for i := range nodes {
		n := nodes[i]
		if n == nil {
			return Suffrage{}, e(nil, "nil node address")
		}

		m[n.Address().String()] = n
	}

	if _, found := util.CheckSliceDuplicated(nodes, func(_ interface{}, i int) string {
		return nodes[i].Address().String()
	}); found {
		return Suffrage{}, e(nil, "duplicated node address found")
	}

	return Suffrage{m: m, ns: nodes}, nil
}

func (suf Suffrage) Exists(node base.Address) bool {
	_, found := suf.m[node.String()]

	return found
}

func (suf Suffrage) ExistsPublickey(node base.Address, pub base.Publickey) bool {
	switch n, found := suf.m[node.String()]; {
	case !found:
		return false
	case !n.Publickey().Equal(pub):
		return false
	default:
		return true
	}
}

func (suf Suffrage) Nodes() []base.Node {
	return suf.ns
}

func (suf Suffrage) Len() int {
	return len(suf.ns)
}
