package isaac

import (
	"context"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type ProposalMaker struct {
	local  *LocalNode
	policy base.Policy
}

func NewProposalMaker(local *LocalNode, policy base.Policy) *ProposalMaker {
	return &ProposalMaker{
		local:  local,
		policy: policy,
	}
}

func (p *ProposalMaker) New(point base.Point) (Proposal, error) {
	e := util.StringErrorFunc("failed to make proposal, %q", point)

	fact := NewProposalFact(point, nil) // BLOCK operations will be retrieved from database

	signedFact := NewProposalSignedFact(p.local.Address(), fact)
	if err := signedFact.Sign(p.local.Privatekey(), p.policy.NetworkID()); err != nil {
		return Proposal{}, e(err, "")
	}

	return NewProposal(signedFact), nil
}

// ProposalSelector fetchs proposal from selected proposer
type ProposalSelector interface {
	Select(base.Point) (base.Proposal, error)
}

func runLoopP(ctx context.Context, f func(int) (bool, error), d time.Duration) error {
	var i int

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			switch keep, err := f(i); {
			case err != nil:
				return err
			case !keep:
				return nil
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d):
				i++
			}
		}
	}
}
