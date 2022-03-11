package isaac

import (
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type baseTestHandler struct {
	suite.Suite
	local  LocalNode
	policy Policy
}

func (t *baseTestHandler) SetupTest() {
	local := RandomLocalNode()
	policy := NewPolicy()
	policy.SetNetworkID(base.RandomNetworkID())
	policy.SetThreshold(base.Threshold(100))
	policy.SetWaitProcessingProposal(time.Nanosecond)

	t.local = local
	t.policy = policy
}

func (t *baseTestHandler) newINITBallotFact(point base.Point, prev, pr util.Hash) INITBallotFact {
	if prev == nil {
		prev = valuehash.RandomSHA256()
	}
	if pr == nil {
		pr = valuehash.RandomSHA256()
	}
	return NewINITBallotFact(point, prev, pr)
}

func (t *baseTestHandler) newACCEPTBallotFact(point base.Point, pr, block util.Hash) ACCEPTBallotFact {
	if pr == nil {
		pr = valuehash.RandomSHA256()
	}
	if block == nil {
		block = valuehash.RandomSHA256()
	}
	return NewACCEPTBallotFact(point, pr, block)
}

func (t *baseTestHandler) newProposalFact(point base.Point, local LocalNode, ops []util.Hash) ProposalFact {
	return NewProposalFact(point, local.Address(), ops)
}

func (t *baseTestHandler) newProposal(local LocalNode, fact ProposalFact) ProposalSignedFact {
	fs := NewProposalSignedFact(fact)
	t.NoError(fs.Sign(local.Privatekey(), t.policy.NetworkID()))

	return fs
}

func (t *baseTestHandler) newINITVoteproof(
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
		if err := fs.Sign(n.Privatekey(), t.policy.NetworkID()); err != nil {
			return INITVoteproof{}, err
		}

		sfs[i] = fs
	}

	vp := NewINITVoteproof(fact.Point().Point)
	vp.SetResult(base.VoteResultMajority).
		SetMajority(fact).
		SetSignedFacts(sfs).
		SetThreshold(t.policy.Threshold()).
		finish()

	return vp, nil
}

func (t *baseTestHandler) newACCEPTVoteproof(
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
		if err := fs.Sign(n.Privatekey(), t.policy.NetworkID()); err != nil {
			return ACCEPTVoteproof{}, err
		}

		sfs[i] = fs
	}

	vp := NewACCEPTVoteproof(fact.Point().Point)
	vp.SetResult(base.VoteResultMajority).
		SetMajority(fact).
		SetSignedFacts(sfs).
		SetThreshold(t.policy.Threshold()).
		finish()

	return vp, nil
}

func (t *baseTestHandler) locals(n int) ([]LocalNode, []base.Node) {
	suf := make([]LocalNode, n)
	nodes := make([]base.Node, n)
	for i := range suf {
		j := RandomLocalNode()
		suf[i] = j
		nodes[i] = j
	}

	return suf, nodes
}

func (t *baseTestHandler) voteproofsPair(prevpoint, point base.Point, prev, pr, nextpr util.Hash, nodes []LocalNode) (ACCEPTVoteproof, INITVoteproof) {
	if prev == nil {
		prev = valuehash.RandomSHA256()
	}
	if pr == nil {
		pr = valuehash.RandomSHA256()
	}
	if nextpr == nil {
		nextpr = valuehash.RandomSHA256()
	}

	afact := t.newACCEPTBallotFact(prevpoint, pr, prev)
	avp, err := t.newACCEPTVoteproof(afact, t.local, nodes)
	t.NoError(err)

	ifact := t.newINITBallotFact(point, prev, nextpr)
	ivp, err := t.newINITVoteproof(ifact, t.local, nodes)
	t.NoError(err)

	return avp, ivp
}

func (t *baseTestHandler) suffrageState(height, sufheight base.Height, nodes []base.Node) (base.State, base.SuffrageStateValue) {
	sv := NewSuffrageStateValue(
		sufheight,
		valuehash.RandomSHA256(),
		nodes,
	)

	_ = (interface{})(sv).(base.SuffrageStateValue)

	sufstt := base.NewBaseState(
		height,
		base.SuffrageStateKey,
		sv,
	)
	sufstt.SetOperations([]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256(), valuehash.RandomSHA256()})

	return sufstt, sv
}

type baseStateTestHandler struct {
	baseTestHandler
	prpool *proposalPool
}

func (t *baseStateTestHandler) SetupTest() {
	t.baseTestHandler.SetupTest()

	local := t.local
	policy := t.policy

	t.prpool = newProposalPool(func(point base.Point) base.ProposalSignedFact {
		fs := NewProposalSignedFact(NewProposalFact(point, local.Address(), []util.Hash{valuehash.RandomSHA256()}))
		_ = fs.Sign(local.Privatekey(), policy.NetworkID())

		return fs
	})
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

func (p *proposalPool) hash(point base.Point) util.Hash {
	return p.get(point).Fact().Hash()
}

func (p *proposalPool) get(point base.Point) base.ProposalSignedFact {
	i, _, _ := p.Get(point.String(), func() (interface{}, error) {
		return p.newproposal(point), nil
	})

	return i.(base.ProposalSignedFact)
}

func (p *proposalPool) getfact(point base.Point) base.ProposalFact {
	return p.get(point).ProposalFact()
}

func (p *proposalPool) byPoint(point base.Point) base.ProposalSignedFact {
	i, found := p.Value(point.String())
	if !found {
		return nil
	}

	return i.(base.ProposalSignedFact)
}

func (p *proposalPool) byHash(h util.Hash) base.ProposalSignedFact {
	var pr base.ProposalSignedFact
	p.Traverse(func(_, v interface{}) bool {
		if i := v.(base.ProposalSignedFact); i.Fact().Hash().Equal(h) {
			pr = i

			return false
		}

		return true
	})

	return pr
}

func (p *proposalPool) factByHash(h util.Hash) (base.ProposalFact, error) {
	pr := p.byHash(h)
	if pr == nil {
		return nil, util.NotFoundError.Call()
	}

	return pr.ProposalFact(), nil
}
