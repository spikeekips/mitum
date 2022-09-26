package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	SuffrageNodeStateValueHint       = hint.MustNewHint("suffrage-node-state-value-v0.0.1")
	SuffrageNodesStateValueHint      = hint.MustNewHint("suffrage-nodes-state-value-v0.0.1")
	SuffrageCandidateStateValueHint  = hint.MustNewHint("suffrage-candidate-state-value-v0.0.1")
	SuffrageCandidatesStateValueHint = hint.MustNewHint("suffrage-candidates-state-value-v0.0.1")
)

var (
	SuffrageStateKey          = "suffrage"
	SuffrageCandidateStateKey = "suffrage_candidate"
	NetworkPolicyStateKey     = "network_policy"
)

type GetSuffrageByBlockHeight func(nextheight base.Height) (base.Suffrage, bool, error)

func NewSuffrageFromState(st base.State) (suf base.Suffrage, _ error) {
	switch v, err := base.LoadSuffrageNodesStateValue(st); {
	case err != nil:
		return suf, err
	default:
		return v.Suffrage()
	}
}

type SuffrageNodeStateValue struct {
	base.Node
	hint.BaseHinter
	start base.Height
}

func NewSuffrageNodeStateValue(node base.Node, start base.Height) SuffrageNodeStateValue {
	return SuffrageNodeStateValue{
		BaseHinter: hint.NewBaseHinter(SuffrageNodeStateValueHint),
		Node:       node,
		start:      start,
	}
}

func (s SuffrageNodeStateValue) Start() base.Height {
	return s.start
}

func (s SuffrageNodeStateValue) Hint() hint.Hint {
	return s.BaseHinter.Hint()
}

func (s SuffrageNodeStateValue) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalie SuffrageNodeStateValue")

	if err := s.BaseHinter.IsValid(SuffrageNodeStateValueHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValid(nil, false, s.Node, s.start); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (s SuffrageNodeStateValue) HashBytes() []byte {
	return util.ConcatBytesSlice(s.Node.HashBytes(), s.start.Bytes())
}

type SuffrageNodesStateValue struct {
	nodes []base.SuffrageNodeStateValue
	hint.BaseHinter
	height base.Height
}

func NewSuffrageNodesStateValue(
	suffrageheight base.Height, nodes []base.SuffrageNodeStateValue,
) SuffrageNodesStateValue {
	return SuffrageNodesStateValue{
		BaseHinter: hint.NewBaseHinter(SuffrageNodesStateValueHint),
		height:     suffrageheight,
		nodes:      nodes,
	}
}

func (s SuffrageNodesStateValue) HashBytes() []byte {
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

func (s SuffrageNodesStateValue) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid SuffrageNodesStateValue")
	if err := s.BaseHinter.IsValid(SuffrageNodesStateValueHint.Type().Bytes()); err != nil {
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

func (s SuffrageNodesStateValue) Height() base.Height {
	return s.height
}

func (s SuffrageNodesStateValue) Nodes() []base.SuffrageNodeStateValue {
	return s.nodes
}

func (s SuffrageNodesStateValue) Suffrage() (base.Suffrage, error) {
	nodes := make([]base.Node, len(s.nodes))
	for i := range s.nodes {
		nodes[i] = s.nodes[i]
	}

	return NewSuffrage(nodes)
}

type SuffrageCandidateStateValue struct {
	base.Node
	hint.BaseHinter
	start    base.Height
	deadline base.Height
}

func NewSuffrageCandidateStateValue(node base.Node, start, deadline base.Height) SuffrageCandidateStateValue {
	return SuffrageCandidateStateValue{
		BaseHinter: hint.NewBaseHinter(SuffrageCandidateStateValueHint),
		Node:       node,
		start:      start,
		deadline:   deadline,
	}
}

func (suf SuffrageCandidateStateValue) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid SuffrageCandidateStateValue")

	if err := suf.BaseHinter.IsValid(SuffrageCandidateStateValueHint.Type().Bytes()); err != nil {
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

func (suf SuffrageCandidateStateValue) Start() base.Height {
	return suf.start
}

func (suf SuffrageCandidateStateValue) Deadline() base.Height {
	return suf.deadline
}

func (suf SuffrageCandidateStateValue) HashBytes() []byte {
	return util.ConcatByters(
		util.DummyByter(suf.Node.HashBytes),
		suf.start,
		suf.deadline,
	)
}

type SuffrageCandidatesStateValue struct {
	nodes []base.SuffrageCandidateStateValue
	hint.BaseHinter
}

func NewSuffrageCandidatesStateValue(nodes []base.SuffrageCandidateStateValue) SuffrageCandidatesStateValue {
	return SuffrageCandidatesStateValue{
		BaseHinter: hint.NewBaseHinter(SuffrageCandidatesStateValueHint),
		nodes:      nodes,
	}
}

func (s SuffrageCandidatesStateValue) HashBytes() []byte {
	n := make([]util.Byter, len(s.nodes))

	for i := range s.nodes {
		n[i] = util.DummyByter(s.nodes[i].HashBytes)
	}

	return util.ConcatByters(n...)
}

func (s SuffrageCandidatesStateValue) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid SuffrageCandidatesStateValue")

	if err := s.BaseHinter.IsValid(SuffrageCandidatesStateValueHint.Type().Bytes()); err != nil {
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

func (s SuffrageCandidatesStateValue) Nodes() []base.SuffrageCandidateStateValue {
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
			return nil, false, storage.ErrNotFound.Errorf("suffrage not found")
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

func FilterCandidates(
	height base.Height, candidates []base.SuffrageCandidateStateValue,
) []base.SuffrageCandidateStateValue {
	n := util.FilterSlices(candidates, func(_ interface{}, i int) bool {
		return candidates[i].Deadline() >= height
	})

	if n == nil {
		return nil
	}

	c := make([]base.SuffrageCandidateStateValue, len(n))
	for i := range n {
		c[i] = n[i].(base.SuffrageCandidateStateValue) //nolint:forcetypeassert //...
	}

	return c
}

func LastCandidatesFromState(
	height base.Height, getStateFunc base.GetStateFunc,
) (base.Height, []base.SuffrageCandidateStateValue, error) {
	st, found, err := getStateFunc(SuffrageCandidateStateKey)

	switch {
	case err != nil:
		return base.NilHeight, nil, err
	case !found || st == nil:
		return base.NilHeight, nil, nil
	}

	nodes := st.Value().(base.SuffrageCandidatesStateValue).Nodes() //nolint:forcetypeassert //...

	filtered := FilterCandidates(height, nodes)

	if len(filtered) < 1 {
		return st.Height(), nil, nil
	}

	return st.Height(), filtered, nil
}

func InCandidates(node base.Node, candidates []base.SuffrageCandidateStateValue) bool {
	for i := range candidates {
		c := candidates[i]

		switch {
		case !node.Address().Equal(c.Address()):
		case !node.Publickey().Equal(c.Publickey()):
		default:
			return true
		}
	}

	return false
}

type NodeInConsensusNodesFunc func(base.Node, base.Height) (base.Suffrage, bool, error)
