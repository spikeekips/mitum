package isaac

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	SuffrageStateKey          = "suffrage"
	SuffrageCandidateStateKey = "suffrage_candidate"
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

var (
	SuffrageStateValueHint              = hint.MustNewHint("suffrage-state-value-v0.0.1")
	SuffrageCandidateStateValueHint     = hint.MustNewHint("suffrage-candidate-state-value-v0.0.1")
	SuffrageCandidateStateNodeValueHint = hint.MustNewHint("suffrage-candidate-state-node-value-v0.0.1")
)

type SuffrageStateValue struct {
	hint.BaseHinter
	height   base.Height // NOTE suffrage height
	previous util.Hash
	nodes    []base.Node
}

func NewSuffrageStateValue(suffrageheight base.Height, previous util.Hash, nodes []base.Node) SuffrageStateValue {
	return SuffrageStateValue{
		BaseHinter: hint.NewBaseHinter(SuffrageStateValueHint),
		height:     suffrageheight,
		previous:   previous,
		nodes:      nodes,
	}
}

func (s SuffrageStateValue) HashBytes() []byte {
	return util.ConcatByters(
		s.height,
		s.previous,
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
	vs := make([]util.IsValider, len(s.nodes)+2)
	vs[0] = s.height
	vs[1] = s.previous
	for i := range s.nodes {
		vs[i+2] = s.nodes[i]
	}

	if err := util.CheckIsValid(nil, false, vs...); err != nil {
		return errors.Wrap(err, "invalid SuffrageStateValue")
	}

	return nil
}

func (s SuffrageStateValue) Height() base.Height {
	return s.height
}

func (s SuffrageStateValue) Previous() util.Hash {
	return s.previous
}

func (s SuffrageStateValue) Nodes() []base.Node {
	return s.nodes
}

func (s SuffrageStateValue) Equal(b base.StateValue) bool {
	switch {
	case b == nil:
		return false
	case s.Hint().Type() != b.Hint().Type():
		return false
	}

	switch j, ok := b.(SuffrageStateValue); {
	case !ok:
		return false
	case s.height != j.height:
		return false
	case !s.previous.Equal(j.previous):
		return false
	case !base.IsEqualNodes(s.nodes, j.nodes):
		return false
	default:
		return true
	}
}

func (s SuffrageStateValue) Suffrage() (base.Suffrage, error) {
	return NewSuffrage(s.nodes)
}

type SuffrageCandidateStateNodeValue struct {
	base.BaseNode
	startHeight base.Height
	endHeight   base.Height
}

func NewSuffrageCandidateStateNodeValue(
	pub base.Publickey,
	addr base.Address,
	startHeight,
	endHeight base.Height,
) SuffrageCandidateStateNodeValue {
	return SuffrageCandidateStateNodeValue{
		BaseNode:    base.NewBaseNode(SuffrageCandidateStateNodeValueHint, pub, addr),
		startHeight: startHeight,
		endHeight:   endHeight,
	}
}

func (suf SuffrageCandidateStateNodeValue) StartHeight() base.Height {
	return suf.startHeight
}

func (suf SuffrageCandidateStateNodeValue) EndHeight() base.Height {
	return suf.endHeight
}
