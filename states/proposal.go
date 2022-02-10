package states

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

// IgnoreErrorProposalProcessorError ignores error from proposalProcessor, it means
// none IgnoreErrorProposalProcessorError from proposalProcessor will break
// consensus.
var (
	IgnoreErrorProposalProcessorError = util.NewError("proposal processor somthing wrong; ignore")
	RetryProposalProcessorError       = util.NewError("proposal processor somthing wrong; but retry")
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

	fact := NewProposalFact(point, nil) // BLOCK operations will be retrived from database

	signedFact := NewProposalSignedFact(p.local.Address(), fact)
	if err := signedFact.Sign(p.local.Privatekey(), p.policy.NetworkID()); err != nil {
		return Proposal{}, e(err, "")
	}

	return NewProposal(signedFact), nil
}

type proposalProcessor interface {
	process(context.Context) proposalProcessResult
	save(context.Context) error
	cancel() error
	proposal() base.ProposalFact
}

type proposalProcessors struct {
	sync.RWMutex
	*logging.Logging
	makenew func(base.ProposalFact) proposalProcessor
	getfact func(util.Hash) (base.ProposalFact, error)
	p       proposalProcessor
}

func newProposalProcessors(
	makenew func(base.ProposalFact) proposalProcessor,
	getfact func(util.Hash) (base.ProposalFact, error),
) *proposalProcessors {
	return &proposalProcessors{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "proposal-processors")
		}),
		makenew: makenew,
		getfact: getfact,
	}
}

func (pps *proposalProcessors) process(
	ctx context.Context,
	facthash util.Hash,
	ch chan<- proposalProcessResult,
) error {
	pps.Lock()
	defer pps.Unlock()

	l := pps.Log().With().Stringer("fact", facthash).Logger()

	e := util.StringErrorFunc("failed to process proposal, %q", facthash)

	if pps.p != nil {
		if pps.p.proposal().Hash().Equal(facthash) {
			l.Debug().Msg("propsal already processed")

			return nil
		}
	}

	// NOTE fetchs proposal fact
	fact, err := pps.getfact(facthash)
	if err != nil {
		return e(err, "failed to get proposal fact")
	}

	if pps.p != nil {
		if err := pps.p.cancel(); err != nil {
			l.Debug().
				Stringer("previous_processor", pps.p.proposal().Hash()).
				Msg("failed to cancel previous running processor")

			return nil
		}
	}

	p := pps.makenew(fact)

	go pps.runProcessor(ctx, p, ch)

	pps.p = p

	return nil
}

func (pps *proposalProcessors) runProcessor(
	ctx context.Context,
	p proposalProcessor,
	ch chan<- proposalProcessResult,
) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	var r proposalProcessResult

end:
	for ; true; <-t.C {
		r = p.process(ctx)

		switch {
		case r.err == nil:
			break end
		case errors.Is(r.err, RetryProposalProcessorError):
			continue end
		case errors.Is(r.err, IgnoreErrorProposalProcessorError):
			r = proposalProcessResult{}

			break end
		default:
			break end
		}
	}

	ch <- r
}

func (pps *proposalProcessors) cancel(facthash util.Hash) error {
	pps.Lock()
	defer pps.Unlock()

	e := util.StringErrorFunc("failed to cancel proposal processor, %q", facthash)
	if pps.p == nil {
		return e(nil, "no active processor")
	}

	if !pps.p.proposal().Hash().Equal(facthash) {
		return e(nil, "processor not found")
	}

	if err := pps.p.cancel(); err != nil {
		return e(err, "")
	}

	pps.p = nil

	return nil
}

type proposalProcessResult struct {
	fact     base.ProposalFact
	manifest base.Manifest
	err      error
}
