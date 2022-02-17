package isaac

import (
	"sync"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type baseTestStateHandler struct {
	suite.Suite
	local  *LocalNode
	policy Policy
	prpool *proposalPool
}

func (t *baseTestStateHandler) SetupTest() {
	local := RandomLocalNode()
	policy := NewPolicy()
	policy.SetNetworkID(base.RandomNetworkID())
	policy.SetThreshold(base.Threshold(100))

	networkID := t.policy.NetworkID()

	t.local = local
	t.policy = policy

	t.prpool = newProposalPool(func(point base.Point) base.Proposal {
		fs := NewProposalSignedFact(local.Address(), NewProposalFact(point, []util.Hash{valuehash.RandomSHA256()}))
		_ = fs.Sign(local.Privatekey(), networkID)

		return NewProposal(fs)
	})
}

func (t *baseTestStateHandler) newINITBallotFact(point base.Point, prev, pr util.Hash) INITBallotFact {
	return NewINITBallotFact(point, prev, pr)
}

func (t *baseTestStateHandler) newACCEPTBallotFact(point base.Point, pr, block util.Hash) ACCEPTBallotFact {
	return NewACCEPTBallotFact(point, pr, block)
}

func (t *baseTestStateHandler) newProposalFact(point base.Point, ops []util.Hash) ProposalFact {
	return NewProposalFact(point, ops)
}

func (t *baseTestStateHandler) newProposal(local *LocalNode, fact ProposalFact) Proposal {
	fs := NewProposalSignedFact(local.Address(), fact)
	t.NoError(fs.Sign(local.Privatekey(), t.policy.NetworkID()))

	return NewProposal(fs)
}

func (t *baseTestStateHandler) newINITVoteproof(
	fact INITBallotFact,
	local *LocalNode,
	nodes []*LocalNode,
) (INITVoteproof, error) {
	suffrage := []*LocalNode{local}
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

func (t *baseTestStateHandler) newACCEPTVoteproof(
	fact ACCEPTBallotFact,
	local *LocalNode,
	nodes []*LocalNode,
) (ACCEPTVoteproof, error) {
	suffrage := []*LocalNode{local}
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

func (t *baseTestStateHandler) nodes(n int) []*LocalNode {
	suf := make([]*LocalNode, n)
	for i := range suf {
		suf[i] = RandomLocalNode()
	}

	return suf
}

func (t *baseTestStateHandler) voteproofsPair(prevpoint, point base.Point, prev, pr, nextpr util.Hash, nodes []*LocalNode) (ACCEPTVoteproof, INITVoteproof) {
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

type proposalPool struct {
	sync.RWMutex
	p           map[base.Point]base.Proposal
	newproposal func(base.Point) base.Proposal
}

func newProposalPool(
	newproposal func(base.Point) base.Proposal,
) *proposalPool {
	return &proposalPool{
		p:           map[base.Point]base.Proposal{},
		newproposal: newproposal,
	}
}

func (p *proposalPool) hash(point base.Point) util.Hash {
	pr := p.get(point)

	return pr.SignedFact().Fact().Hash()
}

func (p *proposalPool) get(point base.Point) base.Proposal {
	p.Lock()
	defer p.Unlock()

	if pr, found := p.p[point]; found {
		return pr
	}

	pr := p.newproposal(point)

	p.p[point] = pr

	return pr
}

func (p *proposalPool) getfact(point base.Point) base.ProposalFact {
	pr := p.get(point)

	return pr.BallotSignedFact().BallotFact()
}

func (p *proposalPool) byPoint(point base.Point) base.Proposal {
	p.RLock()
	defer p.RUnlock()

	if pr, found := p.p[point]; found {
		return pr
	}

	return nil
}

func (p *proposalPool) byHash(h util.Hash) base.Proposal {
	p.RLock()
	defer p.RUnlock()

	for i := range p.p {
		pr := p.p[i]
		if pr.SignedFact().Fact().Hash().Equal(h) {
			return pr
		}

	}

	return nil
}

func (p *proposalPool) factByHash(h util.Hash) (base.ProposalFact, error) {
	pr := p.byHash(h)
	if pr == nil {
		return nil, util.NotFoundError.Call()
	}

	return pr.BallotSignedFact().BallotFact(), nil
}
