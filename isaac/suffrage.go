package isaac

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var SuffrageStateKey = "suffrage"

type suffrage struct {
	m  map[string]base.Node
	ns []base.Node
}

func newSuffrage(nodes []base.Node) (suffrage, error) {
	e := util.StringErrorFunc("failed to newsuffrage")

	if len(nodes) < 1 {
		return suffrage{}, e(nil, "empty suffrage nodes")
	}

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

var SuffrageStateValueHint = hint.MustNewHint("suffrage-state-value-v0.0.1")

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
	return newSuffrage(s.nodes)
}

type suffrageStateValueJSONMarshaler struct {
	hint.BaseHinter
	Height   base.Height `json:"height"`
	Previous util.Hash   `json:"previous"`
	Nodes    []base.Node `json:"nodes"`
}

func (s SuffrageStateValue) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(suffrageStateValueJSONMarshaler{
		BaseHinter: s.BaseHinter,
		Height:     s.height,
		Previous:   s.previous,
		Nodes:      s.nodes,
	})
}

type suffrageStateValueJSONUnmarshaler struct {
	Height   base.Height           `json:"height"`
	Previous valuehash.HashDecoder `json:"previous"`
	Nodes    []json.RawMessage     `json:"nodes"`
}

func (s *SuffrageStateValue) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageStateValue")

	var u suffrageStateValueJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	nodes := make([]base.Node, len(u.Nodes))
	for i := range u.Nodes {
		j, err := base.DecodeNode(u.Nodes[i], enc)
		if err != nil {
			return e(err, "")
		}

		nodes[i] = j
	}

	s.height = u.Height
	s.previous = u.Previous.Hash()
	s.nodes = nodes

	return nil
}
