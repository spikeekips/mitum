package isaac

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	SuffrageStateValueHint = hint.MustNewHint("suffrage-state-value-v0.0.1")
	SuffrageCandidateHint  = hint.MustNewHint("suffrage-candidate-v0.0.1")
)

var (
	SuffrageStateKey          = "suffrage"
	SuffrageCandidateStateKey = "suffrage_candidate"
	NetworkPolicyStateKey     = "network_policy"
)

type GetSuffrageByBlockHeight func(nextheight base.Height) (base.Suffrage, bool, error)

type Suffrage struct {
	m  map[string]base.Node
	ns []base.Node
}

func NewSuffrageFromState(st base.State) (suf Suffrage, _ error) {
	switch v, err := base.LoadSuffrageState(st); {
	case err != nil:
		return suf, errors.Wrap(err, "")
	default:
		i, err := NewSuffrage(v.Nodes())
		if err != nil {
			return suf, errors.Wrap(err, "")
		}

		return i, nil
	}
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

	if util.CheckSliceDuplicated(nodes, func(i interface{}) string {
		return i.(base.Node).Address().String()
	}) {
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

type SuffrageStateValue struct {
	nodes []base.Node
	hint.BaseHinter
	height base.Height
}

func NewSuffrageStateValue(suffrageheight base.Height, nodes []base.Node) SuffrageStateValue {
	return SuffrageStateValue{
		BaseHinter: hint.NewBaseHinter(SuffrageStateValueHint),
		height:     suffrageheight,
		nodes:      nodes,
	}
}

func (s SuffrageStateValue) HashBytes() []byte {
	return util.ConcatByters(
		s.height,
		util.DummyByter(func() []byte {
			n := make([][]byte, len(s.nodes))
			for i := range s.nodes {
				n[i] = s.nodes[i].HashBytes()
			}

			return util.ConcatBytesSlice(n...)
		}),
	)
}

func (s SuffrageStateValue) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid SuffrageStateValue")
	if err := s.BaseHinter.IsValid(SuffrageStateValueHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	vs := make([]util.IsValider, len(s.nodes)+2)
	vs[0] = s.height
	for i := range s.nodes {
		vs[i+1] = s.nodes[i]
	}

	if err := util.CheckIsValid(nil, false, vs...); err != nil {
		return e(err, "")
	}

	return nil
}

func (s SuffrageStateValue) Height() base.Height {
	return s.height
}

func (s SuffrageStateValue) Nodes() []base.Node {
	return s.nodes
}

func (s SuffrageStateValue) Suffrage() (base.Suffrage, error) {
	return NewSuffrage(s.nodes)
}

type SuffrageCandidate struct {
	base.Node
	hint.BaseHinter
	start    base.Height
	deadline base.Height
}

func NewSuffrageCandidate(node base.Node, start, deadline base.Height) SuffrageCandidate {
	return SuffrageCandidate{
		BaseHinter: hint.NewBaseHinter(SuffrageCandidateHint),
		Node:       node,
		start:      start,
		deadline:   deadline,
	}
}

func (suf SuffrageCandidate) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid SuffrageCandidate")

	if err := suf.BaseHinter.IsValid(SuffrageCandidateHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := suf.Node.IsValid(nil); err != nil {
		return e(err, "")
	}

	if suf.start >= suf.deadline {
		return e(util.InvalidError.Errorf("start >= deadline"), "")
	}

	return nil
}

func (suf SuffrageCandidate) Start() base.Height {
	return suf.start
}

func (suf SuffrageCandidate) Deadline() base.Height {
	return suf.deadline
}
