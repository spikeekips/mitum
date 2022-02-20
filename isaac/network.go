package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type NodeNetwork interface {
	SelectProposal(base.Point, base.Address /* proposer */) (base.ProposalSignedFact, error)
	Proposal(util.Hash /* fact hash */) (base.ProposalSignedFact, error)
}
