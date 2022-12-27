package isaac

import (
	"github.com/pkg/errors"
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

	if _, found := util.IsDuplicatedSlice(nodes, func(i base.Node) (bool, string) {
		return true, i.Address().String()
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

func NewSuffrageWithWithdraws(
	suf base.Suffrage,
	threshold base.Threshold,
	withdraws []base.SuffrageWithdrawOperation,
) (base.Suffrage, error) {
	if len(withdraws) < 1 {
		return suf, nil
	}

	th := threshold.Threshold(uint(suf.Len()))
	if n := uint(len(withdraws)); n > uint(suf.Len())-th {
		th = uint(suf.Len()) - n
	}

	for i := range withdraws {
		if n := uint(len(withdraws[i].NodeSigns())); n < th {
			return nil, errors.Errorf("insufficient withdraw node signs; node signs=%d threshold=%d", n, th)
		}
	}

	nodes := suf.Nodes()

	filtered := util.Filter2Slices(nodes, withdraws, func(x base.Node, y base.SuffrageWithdrawOperation) bool {
		return x.Address().Equal(y.WithdrawFact().Node())
	})

	return NewSuffrage(filtered)
}
