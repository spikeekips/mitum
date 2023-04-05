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
	Local       base.LocalNode
	LocalParams *LocalParams
	PRPool      *proposalPool
}

func (t *BaseTestBallots) SetupTest() {
	local := base.RandomLocalNode()
	params := DefaultLocalParams(base.RandomNetworkID())
	params.SetThreshold(base.Threshold(100))

	t.Local = local
	t.LocalParams = params

	t.PRPool = newProposalPool(func(point base.Point) base.ProposalSignFact {
		fs := NewProposalSignFact(NewProposalFact(point, local.Address(), valuehash.RandomSHA256(), []util.Hash{valuehash.RandomSHA256()}))
		_ = fs.Sign(local.Privatekey(), params.NetworkID())

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
	return NewINITBallotFact(point, prev, pr, nil)
}

func (t *BaseTestBallots) NewACCEPTBallotFact(point base.Point, pr, block util.Hash) ACCEPTBallotFact {
	if pr == nil {
		pr = valuehash.RandomSHA256()
	}
	if block == nil {
		block = valuehash.RandomSHA256()
	}
	return NewACCEPTBallotFact(point, pr, block, nil)
}

func (t *BaseTestBallots) NewProposalFact(point base.Point, local base.LocalNode, ops []util.Hash) ProposalFact {
	return NewProposalFact(point, local.Address(), valuehash.RandomSHA256(), ops)
}

func (t *BaseTestBallots) NewProposal(local base.LocalNode, fact ProposalFact) ProposalSignFact {
	fs := NewProposalSignFact(fact)
	t.NoError(fs.Sign(local.Privatekey(), t.LocalParams.NetworkID()))

	return fs
}

func (t *BaseTestBallots) prepareINITVoteproof(
	fact base.INITBallotFact,
	local base.LocalNode,
	nodes []base.LocalNode,
) ([]base.BallotSignFact, error) {
	suffrage := []base.LocalNode{local}
	for i := range nodes {
		n := nodes[i]
		if !n.Address().Equal(local.Address()) {
			suffrage = append(suffrage, n)
		}
	}

	sfs := make([]base.BallotSignFact, len(suffrage))
	for i := range suffrage {
		n := suffrage[i]
		fs := NewINITBallotSignFact(fact)
		if err := fs.NodeSign(n.Privatekey(), t.LocalParams.NetworkID(), n.Address()); err != nil {
			return nil, err
		}

		sfs[i] = fs
	}

	return sfs, nil
}

func (t *BaseTestBallots) NewINITVoteproof(
	fact base.INITBallotFact,
	local base.LocalNode,
	nodes []base.LocalNode,
) (INITVoteproof, error) {
	sfs, err := t.prepareINITVoteproof(fact, local, nodes)
	if err != nil {
		return INITVoteproof{}, err
	}

	vp := NewINITVoteproof(fact.Point().Point)
	vp.
		SetMajority(fact).
		SetSignFacts(sfs).
		SetThreshold(t.LocalParams.Threshold()).
		Finish()

	return vp, nil
}

func (t *BaseTestBallots) NewINITExpelVoteproof(
	fact base.INITBallotFact,
	local base.LocalNode,
	nodes []base.LocalNode,
	expels []base.SuffrageExpelOperation,
) (INITExpelVoteproof, error) {
	sfs, err := t.prepareINITVoteproof(fact, local, nodes)
	if err != nil {
		return INITExpelVoteproof{}, err
	}

	vp := NewINITExpelVoteproof(fact.Point().Point)
	vp.
		SetMajority(fact).
		SetSignFacts(sfs).
		SetThreshold(t.LocalParams.Threshold())
	vp.SetExpels(expels)
	vp.Finish()

	return vp, nil
}

func (t *BaseTestBallots) NewACCEPTVoteproof(
	fact ACCEPTBallotFact,
	local base.LocalNode,
	nodes []base.LocalNode,
) (ACCEPTVoteproof, error) {
	suffrage := []base.LocalNode{local}
	for i := range nodes {
		n := nodes[i]
		if !n.Address().Equal(local.Address()) {
			suffrage = append(suffrage, n)
		}
	}

	sfs := make([]base.BallotSignFact, len(suffrage))
	for i := range suffrage {
		n := suffrage[i]
		fs := NewACCEPTBallotSignFact(fact)
		if err := fs.NodeSign(n.Privatekey(), t.LocalParams.NetworkID(), n.Address()); err != nil {
			return ACCEPTVoteproof{}, err
		}

		sfs[i] = fs
	}

	vp := NewACCEPTVoteproof(fact.Point().Point)
	vp.
		SetMajority(fact).
		SetSignFacts(sfs).
		SetThreshold(t.LocalParams.Threshold()).
		Finish()

	return vp, nil
}

func (t *BaseTestBallots) Locals(n int) ([]base.LocalNode, []base.Node) {
	suf := make([]base.LocalNode, n)
	nodes := make([]base.Node, n)
	for i := range suf {
		j := base.RandomLocalNode()
		suf[i] = j
		nodes[i] = j
	}

	return suf, nodes
}

func (t *BaseTestBallots) VoteproofsPair(prevpoint, point base.Point, prev, pr, nextpr util.Hash, nodes []base.LocalNode) (ACCEPTVoteproof, INITVoteproof) {
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

func (t *BaseTestBallots) SuffrageState(height, sufheight base.Height, nodes []base.Node) (base.State, base.SuffrageNodesStateValue) {
	sufnodes := make([]base.SuffrageNodeStateValue, len(nodes))
	for i := range nodes {
		sufnodes[i] = NewSuffrageNodeStateValue(nodes[i], height)
	}

	sv := NewSuffrageNodesStateValue(sufheight, sufnodes)

	_ = (interface{})(sv).(base.SuffrageNodesStateValue)

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

func (t *BaseTestBallots) Expels(height base.Height, expelnodes []base.Address, signs []base.LocalNode) []base.SuffrageExpelOperation {
	ops := make([]base.SuffrageExpelOperation, len(expelnodes))

	for i := range expelnodes {
		expelnode := expelnodes[i]

		fact := NewSuffrageExpelFact(expelnode, height, height+1, util.UUID().String())
		op := NewSuffrageExpelOperation(fact)

		for j := range signs {
			node := signs[j]

			if node.Address().Equal(expelnode) {
				continue
			}

			t.NoError(op.NodeSign(node.Privatekey(), t.LocalParams.NetworkID(), node.Address()))
		}

		ops[i] = op
	}

	return ops
}

type proposalPool struct {
	*util.SingleLockedMap[string, base.ProposalSignFact]
	newproposal func(base.Point) base.ProposalSignFact
}

func newProposalPool(
	newproposal func(base.Point) base.ProposalSignFact,
) *proposalPool {
	return &proposalPool{
		SingleLockedMap: util.NewSingleLockedMap("", (base.ProposalSignFact)(nil)),
		newproposal:     newproposal,
	}
}

func (p *proposalPool) Hash(point base.Point) util.Hash {
	return p.Get(point).Fact().Hash()
}

func (p *proposalPool) Get(point base.Point) base.ProposalSignFact {
	i, _, _ := p.SingleLockedMap.GetOrCreate(point.String(), func() (base.ProposalSignFact, error) {
		return p.newproposal(point), nil
	})

	return i
}

func (p *proposalPool) GetFact(point base.Point) base.ProposalFact {
	return p.Get(point).ProposalFact()
}

func (p *proposalPool) ByPoint(point base.Point) base.ProposalSignFact {
	switch i, found := p.Value(point.String()); {
	case !found:
		return nil
	default:
		return i
	}
}

func (p *proposalPool) ByHash(h util.Hash) (base.ProposalSignFact, error) {
	var pr base.ProposalSignFact
	p.Traverse(func(_ string, v base.ProposalSignFact) bool {
		if v.Fact().Hash().Equal(h) {
			pr = v

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
