package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type BlockDatabase interface {
	SaveProposal(pr base.ProposalSignedFact) error // BLOCK the combination of point and proposer must be unique
	ProposalByPoint(point base.Point, proposer base.Address) (base.ProposalSignedFact, error)
	Proposal(facthash util.Hash) (base.ProposalSignedFact, error)
}
