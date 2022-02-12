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
	IgnoreErrorProposalProcessorError  = util.NewError("proposal processor somthing wrong; ignore")
	RetryProposalProcessorError        = util.NewError("proposal processor somthing wrong; but retry")
	NotProposalProcessorProcessedError = util.NewError("proposal processor not processed")
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

type proposalProcessor interface {
	process(context.Context) proposalProcessResult
	save(context.Context, base.ACCEPTVoteproof) error
	cancel() error
	proposal() base.ProposalFact
}

type proposalProcessors struct {
	sync.RWMutex
	*logging.Logging
	makenew func(base.ProposalFact) proposalProcessor
	getfact func(context.Context, util.Hash) (base.ProposalFact, error)
	p       proposalProcessor
}

func newProposalProcessors(
	makenew func(base.ProposalFact) proposalProcessor,
	getfact func(context.Context, util.Hash) (base.ProposalFact, error),
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
		p := pps.p
		if p.proposal().Hash().Equal(facthash) {
			l.Debug().Msg("propsal already processed")

			return nil
		}

		if err := p.cancel(); err != nil {
			l.Debug().
				Stringer("previous_processor", p.proposal().Hash()).
				Msg("failed to cancel previous running processor")

			return nil
		}
	}

	// NOTE fetchs proposal fact
	fact, err := pps.fetchFact(ctx, facthash)
	switch {
	case err != nil:
		return e(err, "failed to get proposal fact")
	case fact == nil:
		return nil
	}

	p := pps.makenew(fact)

	go pps.runProcessor(ctx, p, ch)

	pps.p = p

	return nil
}

func (pps *proposalProcessors) save(ctx context.Context, facthash util.Hash, avp base.ACCEPTVoteproof) error {
	pps.Lock()
	defer pps.Unlock()

	l := pps.Log().With().Stringer("fact", facthash).Logger()

	e := util.StringErrorFunc("failed to save proposal, %q", facthash)

	switch {
	case pps.p == nil:
		l.Debug().Msg("proposal processor not found")

		return NotProposalProcessorProcessedError.Call()
	case !pps.p.proposal().Hash().Equal(facthash):
		l.Debug().Msg("proposal processor not found")

		return NotProposalProcessorProcessedError.Call()
	}

	limit := 15
	err := runLoopP(
		ctx,
		func(i int) (bool, error) {
			if i >= limit {
				return false, e(nil, "too many retry; stop")
			}

			err := pps.p.save(ctx, avp)
			switch {
			case err == nil:
				return false, nil
			case errors.Is(err, context.Canceled):
				return false, NotProposalProcessorProcessedError.Call()
			case errors.Is(err, RetryProposalProcessorError):
				pps.Log().Debug().Msg("failed to save proposal; will retry")

				return true, nil
			default:
				return false, e(err, "")
			}
		},
		time.Millisecond*600,
	)

	return err
}

func (pps *proposalProcessors) fetchFact(ctx context.Context, facthash util.Hash) (base.ProposalFact, error) {
	e := util.StringErrorFunc("failed to fetch fact")

	var fact base.ProposalFact

	limit := 15 // NOTE endure failure for almost 9 seconds, it is almost 3 consensus cycle.
	err := runLoopP(
		ctx,
		func(i int) (bool, error) {
			if i >= limit {
				return false, e(nil, "too many retry; stop")
			}

			j, err := pps.getfact(ctx, facthash)
			switch {
			case err == nil:
				fact = j

				return false, nil
			case errors.Is(err, context.Canceled):
				return false, nil
			case errors.Is(err, RetryProposalProcessorError):
				pps.Log().Debug().Msg("failed to fetch fact; will retry")

				return true, nil
			default:
				return false, e(err, "failed to get proposal fact")
			}
		},
		time.Millisecond*600,
	)

	return fact, err
}

func (pps *proposalProcessors) runProcessor(
	ctx context.Context,
	p proposalProcessor,
	ch chan<- proposalProcessResult,
) {
	var r proposalProcessResult

	limit := 15 // NOTE endure failure for almost 9 seconds, it is almost 3 consensus cycle.
	_ = runLoopP(
		ctx,
		func(i int) (bool, error) {
			if i >= limit {
				return false, errors.Errorf("too many retry; stop")
			}

			r = p.process(ctx)

			switch {
			case r.err == nil:
				return false, nil
			case errors.Is(r.err, RetryProposalProcessorError):
				return true, nil
			case errors.Is(r.err, IgnoreErrorProposalProcessorError):
				r = proposalProcessResult{}

				return false, nil
			case errors.Is(r.err, context.Canceled):
				r = proposalProcessResult{}

				return false, nil
			default:
				return false, nil
			}
		},
		time.Millisecond*600,
	)

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

func (pps *proposalProcessors) close() error {
	pps.Lock()
	defer pps.Unlock()

	if pps.p == nil {
		return nil
	}

	e := util.StringErrorFunc("failed to close proposal processors")
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
