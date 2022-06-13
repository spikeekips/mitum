package main

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
)

type newProposalProcessorFunc func(proposal base.ProposalSignedFact, previous base.Manifest) (
	isaac.ProposalProcessor, error)
