package states

import (
	"context"

	"github.com/spikeekips/mitum/base"
)

type DummyProposalProcessor struct {
	fact     base.ProposalFact
	manifest base.Manifest
	errf     func() error
}

func NewDummyProposalProcessor(manifest base.Manifest, errf func() error) *DummyProposalProcessor {
	return &DummyProposalProcessor{
		manifest: manifest,
		errf:     errf,
	}
}

func (p *DummyProposalProcessor) make(fact base.ProposalFact) proposalProcessor {
	p.fact = fact
	return p
}

func (p *DummyProposalProcessor) process(ctx context.Context) proposalProcessResult {
	var err error
	if p.errf != nil {
		err = p.errf()
	}

	return proposalProcessResult{
		fact:     p.fact,
		manifest: p.manifest,
		err:      err,
	}
}

func (p *DummyProposalProcessor) save(ctx context.Context) error {
	return nil
}

func (p *DummyProposalProcessor) cancel() error {
	return nil
}

func (p *DummyProposalProcessor) proposal() base.ProposalFact {
	return p.fact
}
