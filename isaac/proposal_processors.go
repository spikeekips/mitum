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
// not IgnoreErrorProposalProcessorError from proposalProcessor will break
// consensus.
var (
	IgnoreErrorProposalProcessorError  = util.NewError("proposal processor somthing wrong; ignore")
	NotProposalProcessorProcessedError = util.NewError("proposal processor not processed")
)

type proposalProcessors struct {
	sync.RWMutex
	*logging.Logging
	makenew       func(base.ProposalFact) proposalProcessor
	getfact       func(context.Context, util.Hash) (base.ProposalFact, error) // BLOCK use NewProposalPool
	p             proposalProcessor
	retrylimit    int
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
		retrylimit:    15, // NOTE endure failure for almost 9 seconds, it is almost 3 consensus cycle.
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
	case !pps.p.Proposal().Hash().Equal(facthash):
		l.Debug().Msg("proposal processor not found")

		return NotProposalProcessorProcessedError.Call()
	}

	switch err := pps.p.Save(ctx, avp); {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return NotProposalProcessorProcessedError.Call()
	default:
		return e(err, "")
	}
}

func (pps *proposalProcessors) fetchFact(ctx context.Context, facthash util.Hash) (base.ProposalFact, error) {
	e := util.StringErrorFunc("failed to fetch fact")

	var fact base.ProposalFact

	err := util.Retry(
		ctx,
		func() (bool, error) {
			j, err := pps.getfact(ctx, facthash)
			switch {
			case err == nil:
				fact = j

				return false, nil
			default:
				return true, e(err, "failed to get proposal fact")
			}
		},
		pps.retrylimit,
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
		if p.Proposal().Hash().Equal(facthash) {
			l.Debug().Msg("proposal already processed")

			return nil, nil
		}

		if err := p.Cancel(); err != nil {
			l.Debug().
				Err(err).
				Stringer("previous_processor", p.Proposal().Hash()).
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
	if l, ok := pps.p.(logging.SetLogging); ok {
		_ = l.SetLogging(pps.Logging)
	}

	return pps.p, nil
}

func (pps *proposalProcessors) runProcessor(ctx context.Context, p proposalProcessor) (base.Manifest, error) {
	manifest, err := p.Process(ctx)
	switch {
	case err == nil:
		return manifest, nil
	case errors.Is(err, IgnoreErrorProposalProcessorError):
		return nil, nil
	default:
		if e := p.Cancel(); e != nil {
			return nil, errors.Wrap(e, "failed to run processor")
		}

		return nil, err
	}
}

func (pps *proposalProcessors) close() error {
	pps.Lock()
	defer pps.Unlock()

	if pps.p == nil {
		return nil
	}

	e := util.StringErrorFunc("failed to close proposal processors")
	if err := pps.p.Cancel(); err != nil {
		return e(err, "")
	}

	pps.p = nil

	return nil
}
