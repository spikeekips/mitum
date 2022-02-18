//go:build test
// +build test

package isaac

import (
	"context"

	"github.com/spikeekips/mitum/base"
)

type DummyProposalProcessor struct {
	fact       base.ProposalFact
	manifest   base.Manifest
	processerr func() error
	saveerr    func(base.ACCEPTVoteproof) error
	cancelerr  func() error
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

func (p *DummyProposalProcessor) process(ctx context.Context) (base.Manifest, error) {
	var err error
	if p.processerr != nil {
		err = p.processerr()
	}

	return p.manifest, err
}

func (p *DummyProposalProcessor) save(_ context.Context, avp base.ACCEPTVoteproof) error {
	if p.saveerr != nil {
		return p.saveerr(avp)
	}

	return nil
}

func (p *DummyProposalProcessor) cancel() error {
	if p.cancelerr != nil {
		return p.cancelerr()
	}

	return nil
}

func (p *DummyProposalProcessor) proposal() base.ProposalFact {
	return p.fact
}

type DummyProposalSelector func(base.Point) (base.ProposalSignedFact, error)

func (ps DummyProposalSelector) Select(point base.Point) (base.ProposalSignedFact, error) {
	return ps(point)
}
