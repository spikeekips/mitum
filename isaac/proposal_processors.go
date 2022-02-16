package isaac

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

type proposalProcessor interface {
	process(context.Context) (base.Manifest, error)
	save(context.Context, base.ACCEPTVoteproof) error
	cancel() error
	proposal() base.ProposalFact
}

type proposalProcessors struct {
	sync.RWMutex
	*logging.Logging
	makenew       func(base.ProposalFact) proposalProcessor
	getfact       func(context.Context, util.Hash) (base.ProposalFact, error)
	p             proposalProcessor
	limit         int
	retryinterval time.Duration
}

func newProposalProcessors(
	makenew func(base.ProposalFact) proposalProcessor,
	getfact func(context.Context, util.Hash) (base.ProposalFact, error),
) *proposalProcessors {
	return &proposalProcessors{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "proposal-processors")
		}),
		makenew:       makenew,
		getfact:       getfact,
		limit:         15, // NOTE endure failure for almost 9 seconds, it is almost 3 consensus cycle.
		retryinterval: time.Millisecond * 600,
	}
}

func (pps *proposalProcessors) processor() proposalProcessor {
	pps.RLock()
	defer pps.RUnlock()

	return pps.p
}

func (pps *proposalProcessors) process(ctx context.Context, facthash util.Hash) (base.Manifest, error) {
	l := pps.Log().With().Stringer("fact", facthash).Logger()

	e := util.StringErrorFunc("failed to process proposal, %q", facthash)

	switch p, err := pps.newProcessor(ctx, facthash); {
	case err != nil:
		l.Error().Err(err).Msg("fialed to process proposal")

		return nil, e(err, "")
	case p == nil:
		return nil, nil
	default:
		return pps.runProcessor(ctx, p)
	}
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

	err := runLoopP(
		ctx,
		func(i int) (bool, error) {
			if i >= pps.limit {
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
		pps.retryinterval,
	)

	return err
}

func (pps *proposalProcessors) fetchFact(ctx context.Context, facthash util.Hash) (base.ProposalFact, error) {
	e := util.StringErrorFunc("failed to fetch fact")

	var fact base.ProposalFact

	err := runLoopP(
		ctx,
		func(i int) (bool, error) {
			if i >= pps.limit {
				return false, e(nil, "too many retry; stop")
			}

			j, err := pps.getfact(ctx, facthash)
			switch {
			case err == nil:
				fact = j

				return false, nil
			case errors.Is(err, RetryProposalProcessorError):
				pps.Log().Debug().Msg("failed to fetch fact; will retry")

				return true, nil
			default:
				return false, e(err, "failed to get proposal fact")
			}
		},
		pps.retryinterval,
	)

	return fact, err
}

func (pps *proposalProcessors) newProcessor(ctx context.Context, facthash util.Hash) (proposalProcessor, error) {
	pps.Lock()
	defer pps.Unlock()

	e := util.StringErrorFunc("failed new processor, %q", facthash)

	l := pps.Log().With().Stringer("fact", facthash).Logger()
	if pps.p != nil {
		p := pps.p
		if p.proposal().Hash().Equal(facthash) {
			l.Debug().Msg("propsal already processed")

			return nil, nil
		}

		if err := p.cancel(); err != nil {
			l.Debug().
				Err(err).
				Stringer("previous_processor", p.proposal().Hash()).
				Msg("failed to cancel previous running processor")

			return nil, e(err, "")
		}
	}

	// NOTE fetch proposal fact
	fact, err := pps.fetchFact(ctx, facthash)
	if err != nil {
		return nil, e(err, "failed to get proposal fact")
	}

	pps.p = pps.makenew(fact)

	return pps.p, nil
}

func (pps *proposalProcessors) runProcessor(ctx context.Context, p proposalProcessor) (base.Manifest, error) {
	var manifest base.Manifest
	err := runLoopP(
		ctx,
		func(i int) (bool, error) {
			if i >= pps.limit {
				return false, errors.Errorf("too many retry; stop")
			}

			switch m, err := p.process(ctx); {
			case err == nil:
				manifest = m

				return false, nil
			case errors.Is(err, RetryProposalProcessorError):
				return true, nil
			case errors.Is(err, IgnoreErrorProposalProcessorError):
				return false, nil
			default:
				return false, err
			}
		},
		pps.retryinterval,
	)

	return manifest, err
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
