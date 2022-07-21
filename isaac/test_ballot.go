//go:build test
// +build test

package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type BaseTestBallots struct {
	suite.Suite
	Local      LocalNode
	NodePolicy NodePolicy
	PRPool     *proposalPool
}

func (t *BaseTestBallots) SetupTest() {
	local := RandomLocalNode()
	nodePolicy := DefaultNodePolicy(base.RandomNetworkID())
	nodePolicy.SetThreshold(base.Threshold(100))

	t.Local = local
	t.NodePolicy = nodePolicy

	t.PRPool = newProposalPool(func(point base.Point) base.ProposalSignedFact {
		fs := NewProposalSignedFact(NewProposalFact(point, local.Address(), []util.Hash{valuehash.RandomSHA256()}))
		_ = fs.Sign(local.Privatekey(), nodePolicy.NetworkID())

		return fs
	})
}

func (t *BaseTestBallots) NewINITBallotFact(point base.Point, prev, pr util.Hash) INITBallotFact {
	if prev == nil {
		prev = valuehash.RandomSHA256()
	}
	if pr == nil {
		pr = valuehash.RandomSHA256()
	}
	return NewINITBallotFact(point, prev, pr)
}

func (t *BaseTestBallots) NewACCEPTBallotFact(point base.Point, pr, block util.Hash) ACCEPTBallotFact {
	if pr == nil {
		pr = valuehash.RandomSHA256()
	}
	if block == nil {
		block = valuehash.RandomSHA256()
	}
	return NewACCEPTBallotFact(point, pr, block)
}

func (t *BaseTestBallots) NewProposalFact(point base.Point, local LocalNode, ops []util.Hash) ProposalFact {
	return NewProposalFact(point, local.Address(), ops)
}

func (t *BaseTestBallots) NewProposal(local LocalNode, fact ProposalFact) ProposalSignedFact {
	fs := NewProposalSignedFact(fact)
	t.NoError(fs.Sign(local.Privatekey(), t.NodePolicy.NetworkID()))

	return fs
}

func (t *BaseTestBallots) NewINITVoteproof(
	fact INITBallotFact,
	local LocalNode,
	nodes []LocalNode,
) (INITVoteproof, error) {
	suffrage := []LocalNode{local}
	for i := range nodes {
		n := nodes[i]
		if !n.Address().Equal(local.Address()) {
			suffrage = append(suffrage, n)
		}
	}

	sfs := make([]base.BallotSignedFact, len(suffrage))
	for i := range suffrage {
		n := suffrage[i]
		fs := NewINITBallotSignedFact(n.Address(), fact)
		if err := fs.Sign(n.Privatekey(), t.NodePolicy.NetworkID()); err != nil {
			return INITVoteproof{}, err
		}

		sfs[i] = fs
	}

	vp := NewINITVoteproof(fact.Point().Point)
	vp.SetResult(base.VoteResultMajority).
		SetMajority(fact).
		SetSignedFacts(sfs).
		SetThreshold(t.NodePolicy.Threshold()).
		Finish()

	return vp, nil
}

func (t *BaseTestBallots) NewACCEPTVoteproof(
	fact ACCEPTBallotFact,
	local LocalNode,
	nodes []LocalNode,
) (ACCEPTVoteproof, error) {
	suffrage := []LocalNode{local}
	for i := range nodes {
		n := nodes[i]
		if !n.Address().Equal(local.Address()) {
			suffrage = append(suffrage, n)
		}
	}

	sfs := make([]base.BallotSignedFact, len(suffrage))
	for i := range suffrage {
		n := suffrage[i]
		fs := NewACCEPTBallotSignedFact(n.Address(), fact)
		if err := fs.Sign(n.Privatekey(), t.NodePolicy.NetworkID()); err != nil {
			return ACCEPTVoteproof{}, err
		}

		sfs[i] = fs
	}

	vp := NewACCEPTVoteproof(fact.Point().Point)
	vp.SetResult(base.VoteResultMajority).
		SetMajority(fact).
		SetSignedFacts(sfs).
		SetThreshold(t.NodePolicy.Threshold()).
		Finish()

	return vp, nil
}

func (t *BaseTestBallots) Locals(n int) ([]LocalNode, []base.Node) {
	suf := make([]LocalNode, n)
	nodes := make([]base.Node, n)
	for i := range suf {
		j := RandomLocalNode()
		suf[i] = j
		nodes[i] = j
	}

	return suf, nodes
}

func (t *BaseTestBallots) VoteproofsPair(prevpoint, point base.Point, prev, pr, nextpr util.Hash, nodes []LocalNode) (ACCEPTVoteproof, INITVoteproof) {
	if prev == nil {
		prev = valuehash.RandomSHA256()
	}
	if pr == nil {
		pr = valuehash.RandomSHA256()
	}
	if nextpr == nil {
		nextpr = valuehash.RandomSHA256()
	}

	afact := t.NewACCEPTBallotFact(prevpoint, pr, prev)
	avp, err := t.NewACCEPTVoteproof(afact, t.Local, nodes)
	t.NoError(err)

	ifact := t.NewINITBallotFact(point, prev, nextpr)
	ivp, err := t.NewINITVoteproof(ifact, t.Local, nodes)
	t.NoError(err)

	return avp, ivp
}

func (t *BaseTestBallots) SuffrageState(height, sufheight base.Height, nodes []base.Node) (base.State, base.SuffrageStateValue) {
	sv := NewSuffrageStateValue(sufheight, nodes)

	_ = (interface{})(sv).(base.SuffrageStateValue)

	st := base.NewBaseState(
		height,
		SuffrageStateKey,
		sv,
		valuehash.RandomSHA256(),
		[]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256(), valuehash.RandomSHA256()},
	)

	return st, sv
}

func (t *BaseTestBallots) NetworkPolicyState(height base.Height, policy NetworkPolicy) (base.State, base.NetworkPolicyStateValue) {
	sv := NewNetworkPolicyStateValue(policy)

	_ = (interface{})(sv).(base.NetworkPolicyStateValue)

	st := base.NewBaseState(
		height,
		NetworkPolicyStateKey,
		sv,
		valuehash.RandomSHA256(),
		nil,
	)
	st.SetOperations([]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256(), valuehash.RandomSHA256()})

	return st, sv
}

type proposalPool struct {
	*util.LockedMap
	newproposal func(base.Point) base.ProposalSignedFact
}

func newProposalPool(
	newproposal func(base.Point) base.ProposalSignedFact,
) *proposalPool {
	return &proposalPool{
		LockedMap:   util.NewLockedMap(),
		newproposal: newproposal,
	}
}

func (p *proposalPool) Hash(point base.Point) util.Hash {
	return p.Get(point).Fact().Hash()
}

func (p *proposalPool) Get(point base.Point) base.ProposalSignedFact {
	i, _, _ := p.LockedMap.Get(point.String(), func() (interface{}, error) {
		return p.newproposal(point), nil
	})

	return i.(base.ProposalSignedFact)
}

func (p *proposalPool) GetFact(point base.Point) base.ProposalFact {
	return p.Get(point).ProposalFact()
}

func (p *proposalPool) ByPoint(point base.Point) base.ProposalSignedFact {
	switch i, found := p.Value(point.String()); {
	case !found:
		return nil
	default:
		return i.(base.ProposalSignedFact)
	}
}

func (p *proposalPool) ByHash(h util.Hash) (base.ProposalSignedFact, error) {
	var pr base.ProposalSignedFact
	p.Traverse(func(_, v interface{}) bool {
		if i := v.(base.ProposalSignedFact); i.Fact().Hash().Equal(h) {
			pr = i

			return false
		}

		return true
	})

	if pr == nil {
		return nil, util.ErrNotFound.Call()
	}

	return pr, nil
}

func (p *proposalPool) FactByHash(h util.Hash) (base.ProposalFact, error) {
	pr, err := p.ByHash(h)
	if err != nil {
		return nil, util.ErrNotFound.Call()
	}

	return pr.ProposalFact(), nil
}
