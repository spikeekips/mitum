//go:build test
// +build test

package isaac

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
)

type DummyProposalProcessor struct {
	proposal   base.ProposalSignedFact
	processerr func(context.Context, base.ProposalFact) (base.Manifest, error)
	saveerr    func(context.Context, base.ACCEPTVoteproof) error
	cancelerr  func() error
}

func NewDummyProposalProcessor() *DummyProposalProcessor {
	return &DummyProposalProcessor{}
}

func (p *DummyProposalProcessor) make(proposal base.ProposalSignedFact) proposalProcessor {
	return DummyProposalProcessor{
		proposal:   proposal,
		processerr: p.processerr,
		saveerr:    p.saveerr,
		cancelerr:  p.cancelerr,
	}
}

func (p DummyProposalProcessor) Process(ctx context.Context) (base.Manifest, error) {
	if p.processerr != nil {
		return p.processerr(ctx, p.proposal.ProposalFact())
	}

	return nil, errors.Errorf("wrong processing")
}

func (p DummyProposalProcessor) Save(ctx context.Context, avp base.ACCEPTVoteproof) error {
	if p.saveerr != nil {
		return p.saveerr(ctx, avp)
	}

	return nil
}

func (p DummyProposalProcessor) Cancel() error {
	if p.cancelerr != nil {
		return p.cancelerr()
	}

	return nil
}

func (p DummyProposalProcessor) Proposal() base.ProposalSignedFact {
	return p.proposal
}

type DummyProposalSelector func(context.Context, base.Point) (base.ProposalSignedFact, error)

func (ps DummyProposalSelector) Select(ctx context.Context, point base.Point) (base.ProposalSignedFact, error) {
	return ps(ctx, point)
}
