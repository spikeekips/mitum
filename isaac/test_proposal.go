//go:build test
// +build test

package isaac

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

func (pps *ProposalProcessors) SetMakeNew(f func(proposal base.ProposalSignedFact, previous base.Manifest) ProposalProcessor) {
	pps.makenew = f
}

func (pps *ProposalProcessors) SetGetProposal(f func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error)) {
	pps.getproposal = f
}

type DummyProposalProcessor struct {
	proposal   base.ProposalSignedFact
	previous   base.Manifest
	Processerr func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error)
	Saveerr    func(context.Context, base.ACCEPTVoteproof) error
	Cancelerr  func() error
}

func NewDummyProposalProcessor() *DummyProposalProcessor {
	return &DummyProposalProcessor{}
}

func (p *DummyProposalProcessor) Make(proposal base.ProposalSignedFact, previous base.Manifest) ProposalProcessor {
	return DummyProposalProcessor{
		proposal:   proposal,
		previous:   previous,
		Processerr: p.Processerr,
		Saveerr:    p.Saveerr,
		Cancelerr:  p.Cancelerr,
	}
}

func (p DummyProposalProcessor) Process(ctx context.Context, ivp base.INITVoteproof) (base.Manifest, error) {
	if p.Processerr != nil {
		return p.Processerr(ctx, p.proposal.ProposalFact(), ivp)
	}

	return nil, errors.Errorf("wrong processing")
}

func (p DummyProposalProcessor) Save(ctx context.Context, avp base.ACCEPTVoteproof) error {
	if p.Saveerr != nil {
		return p.Saveerr(ctx, avp)
	}

	return nil
}

func (p DummyProposalProcessor) Cancel() error {
	if p.Cancelerr != nil {
		return p.Cancelerr()
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
