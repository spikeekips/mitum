package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type baseTestStateHandler struct {
	suite.Suite
	local  *LocalNode
	policy base.BasePolicy
	prpool *proposalPool
}

func (t *baseTestStateHandler) SetupTest() {
	local := RandomLocalNode()
	policy := base.NewBasePolicy()
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
	vp.SetResult(base.VoteResultMajority)
	vp.SetMajority(fact)
	vp.SetSignedFacts(sfs)
	vp.SetThreshold(t.policy.Threshold())
	vp.finish()

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
	vp.SetResult(base.VoteResultMajority)
	vp.SetMajority(fact)
	vp.SetSignedFacts(sfs)
	vp.SetThreshold(t.policy.Threshold())
	vp.finish()

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
