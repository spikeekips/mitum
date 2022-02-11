package states

import (
	"context"

	"github.com/spikeekips/mitum/base"
)

type DummyProposalProcessor struct {
	fact       base.ProposalFact
	manifest   base.Manifest
	processerr func() error
	saveerr    func(base.ACCEPTVoteproof) error
}

func NewDummyProposalProcessor(manifest base.Manifest) *DummyProposalProcessor {
	return &DummyProposalProcessor{
		manifest: manifest,
	}
}

func (p *DummyProposalProcessor) make(fact base.ProposalFact) proposalProcessor {
	p.fact = fact

	return p
}

func (p *DummyProposalProcessor) process(ctx context.Context) proposalProcessResult {
	var err error
	if p.processerr != nil {
		err = p.processerr()
	}

	return proposalProcessResult{
		fact:     p.fact,
		manifest: p.manifest,
		err:      err,
	}
}

func (p *DummyProposalProcessor) save(_ context.Context, avp base.ACCEPTVoteproof) error {
	if p.saveerr != nil {
		return p.saveerr(avp)
	}

	return nil
}

func (p *DummyProposalProcessor) cancel() error {
	return nil
}

func (p *DummyProposalProcessor) proposal() base.ProposalFact {
	return p.fact
}
