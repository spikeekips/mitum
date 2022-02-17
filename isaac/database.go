package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type BlockDatabase struct{}

func (db *BlockDatabase) SaveProposal(pr base.Proposal) error {
	// BLOCK the combination of point and proposer must be unique
	return nil
}

func (db *BlockDatabase) ProposalByPoint(point base.Point, proposer base.Address) (base.Proposal, error) {
	return nil, nil
}

func (db *BlockDatabase) Proposal(facthash util.Hash) (base.Proposal, error) {
	return nil, nil
}
