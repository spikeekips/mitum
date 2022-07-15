package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	SuffrageStateValueHint          = hint.MustNewHint("suffrage-state-value-v0.0.1")
	SuffrageCandidateStateValueHint = hint.MustNewHint("suffrage-candidate-state-value-v0.0.1")
	SuffrageCandidateHint           = hint.MustNewHint("suffrage-candidate-v0.0.1")
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
		return suf, err
	default:
		return NewSuffrageFromStateValue(v)
	}
}

func NewSuffrageFromStateValue(v base.SuffrageStateValue) (suf Suffrage, _ error) {
	i, err := NewSuffrage(v.Nodes())
	if err != nil {
		return suf, err
	}

	return i, nil
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

	vs := make([]util.IsValider, len(s.nodes)+1)
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

	if suf.start >= suf.deadline {
		return e(util.ErrInvalid.Errorf("start >= deadline"), "")
	}

	if err := suf.Node.IsValid(nil); err != nil {
		return e(err, "")
	}

	return nil
}

func (suf SuffrageCandidate) Start() base.Height {
	return suf.start
}

func (suf SuffrageCandidate) Deadline() base.Height {
	return suf.deadline
}

func (suf SuffrageCandidate) HashBytes() []byte {
	return util.ConcatByters(
		util.DummyByter(suf.Node.HashBytes),
		suf.start,
		suf.deadline,
	)
}

type SuffrageCandidateStateValue struct {
	nodes []base.SuffrageCandidate
	hint.BaseHinter
}

func NewSuffrageCandidateStateValue(nodes []base.SuffrageCandidate) SuffrageCandidateStateValue {
	return SuffrageCandidateStateValue{
		BaseHinter: hint.NewBaseHinter(SuffrageCandidateStateValueHint),
		nodes:      nodes,
	}
}

func (s SuffrageCandidateStateValue) HashBytes() []byte {
	n := make([]util.Byter, len(s.nodes))

	for i := range s.nodes {
		n[i] = util.DummyByter(s.nodes[i].HashBytes)
	}

	return util.ConcatByters(n...)
}

func (s SuffrageCandidateStateValue) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid SuffrageCandidateStateValue")

	if err := s.BaseHinter.IsValid(SuffrageCandidateStateValueHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	vs := make([]util.IsValider, len(s.nodes))

	for i := range s.nodes {
		vs[i] = s.nodes[i]
	}

	if err := util.CheckIsValid(nil, false, vs...); err != nil {
		return e(err, "")
	}

	return nil
}

func (s SuffrageCandidateStateValue) Nodes() []base.SuffrageCandidate {
	return s.nodes
}

type SuffrageRemoveCandidateStateValue struct {
	nodes []base.Address
}

func NewSuffrageRemoveCandidateStateValue(nodes []base.Address) SuffrageRemoveCandidateStateValue {
	return SuffrageRemoveCandidateStateValue{
		nodes: nodes,
	}
}

func (SuffrageRemoveCandidateStateValue) HashBytes() []byte {
	return nil
}

func (s SuffrageRemoveCandidateStateValue) IsValid([]byte) error {
	vs := make([]util.IsValider, len(s.nodes))
	for i := range s.nodes {
		vs[i] = s.nodes[i]
	}

	if err := util.CheckIsValid(nil, false, vs...); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid SuffrageRemoveCandidateStateValue")
	}

	return nil
}

func (s SuffrageRemoveCandidateStateValue) Nodes() []base.Address {
	return s.nodes
}

func GetSuffrageFromDatabase(
	db Database,
	blockheight base.Height,
) (base.Suffrage, bool, error) {
	height := blockheight.Prev()

	if height < base.GenesisHeight { // NOTE signer node of genesis suffrage proof will be used
		proof, found, err := db.SuffrageProofByBlockHeight(base.GenesisHeight)

		switch {
		case err != nil:
			return nil, false, err
		case !found:
			return nil, false, nil
		default:
			m := proof.Map()
			suf, err := NewSuffrage([]base.Node{NewNode(m.Signer(), m.Node())})

			return suf, true, err
		}
	}

	proof, found, err := db.SuffrageProofByBlockHeight(height)

	switch {
	case err != nil:
		return nil, false, err
	case !found:
		return nil, false, nil
	default:
		suf, err := proof.Suffrage()

		return suf, true, err
	}
}

func LastCandidatesFromState(height base.Height, getStateFunc base.GetStateFunc) ([]base.SuffrageCandidate, error) {
	st, found, err := getStateFunc(SuffrageCandidateStateKey)

	switch {
	case err != nil:
		return nil, err
	case !found || st == nil:
		return nil, nil
	}

	nodes := st.Value().(base.SuffrageCandidateStateValue).Nodes() //nolint:forcetypeassert //...

	filtered := util.FilterSlices(nodes, func(_ interface{}, i int) bool {
		return height <= nodes[i].Deadline()
	})

	if len(filtered) < 1 {
		return nil, nil
	}

	candidates := make([]base.SuffrageCandidate, len(filtered))
	for i := range filtered {
		candidates[i] = filtered[i].(base.SuffrageCandidate) //nolint:forcetypeassert //...
	}

	return candidates, nil
}

type NodeInConsensusNodesFunc func(base.Node, base.Height) (base.Suffrage, bool, error)

func NewNodeInConsensusNodesFunc(getSuffrage GetSuffrageByBlockHeight) NodeInConsensusNodesFunc {
	return func(node base.Node, height base.Height) (base.Suffrage, bool, error) {
		switch suf, found, err := getSuffrage(height); {
		case err != nil:
			return nil, false, err
		case !found:
			return nil, false, nil
		default:
			return suf, suf.ExistsPublickey(node.Address(), node.Publickey()), nil
		}
	}
}
