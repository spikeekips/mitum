package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type baseTestStateHandler struct {
	suite.Suite
	local  *LocalNode
	policy base.BasePolicy
}

func (t *baseTestStateHandler) SetupTest() {
	t.local = RandomLocalNode()
	t.policy = base.NewBasePolicy()
	t.policy.SetNetworkID(base.RandomNetworkID())
	t.policy.SetThreshold(base.Threshold(100))
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
