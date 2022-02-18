//go:build test
// +build test

package isaac

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
)

type DummyProposalProcessor struct {
	fact       base.ProposalFact
	processerr func(context.Context) (base.Manifest, error)
	saveerr    func(context.Context, base.ACCEPTVoteproof) error
	cancelerr  func() error
}

func NewDummyProposalProcessor() *DummyProposalProcessor {
	return &DummyProposalProcessor{}
}

func (p *DummyProposalProcessor) make(fact base.ProposalFact) proposalProcessor {
	return DummyProposalProcessor{
		fact:       fact,
		processerr: p.processerr,
		saveerr:    p.saveerr,
		cancelerr:  p.cancelerr,
	}
}

func (p DummyProposalProcessor) process(ctx context.Context) (base.Manifest, error) {
	if p.processerr != nil {
		return p.processerr(ctx)
	}

	return nil, errors.Errorf("wrong processing")
}

func (p DummyProposalProcessor) save(ctx context.Context, avp base.ACCEPTVoteproof) error {
	if p.saveerr != nil {
		return p.saveerr(ctx, avp)
	}

	return nil
}

func (p DummyProposalProcessor) cancel() error {
	if p.cancelerr != nil {
		return p.cancelerr()
	}

	return nil
}

func (p DummyProposalProcessor) proposal() base.ProposalFact {
	return p.fact
}

type DummyProposalSelector func(context.Context, base.Point) (base.ProposalSignedFact, error)

func (ps DummyProposalSelector) Select(ctx context.Context, point base.Point) (base.ProposalSignedFact, error) {
	return ps(ctx, point)
}
