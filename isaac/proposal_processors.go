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

type ProposalProcessors struct {
	p ProposalProcessor
	*logging.Logging
	makenew       func(proposal base.ProposalSignedFact, previous base.Manifest) (ProposalProcessor, error)
	getproposal   func(_ context.Context, operationhash util.Hash) (base.ProposalSignedFact, error)
	retryinterval time.Duration
	retrylimit    int
	sync.RWMutex
}

func NewProposalProcessors(
	makenew func(base.ProposalSignedFact, base.Manifest) (ProposalProcessor, error),
	getproposal func(context.Context, util.Hash) (base.ProposalSignedFact, error),
) *ProposalProcessors {
	return &ProposalProcessors{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "proposal-processors")
		}),
		makenew:     makenew,
		getproposal: getproposal,
		// NOTE endure failure for almost 9 seconds, it is almost 3 consensus
		// cycle.
		retrylimit:    15,                     //nolint:gomnd //...
		retryinterval: time.Millisecond * 600, //nolint:gomnd //...
	}
}

func (pps *ProposalProcessors) Processor() ProposalProcessor {
	pps.RLock()
	defer pps.RUnlock()

	return pps.p
}

func (pps *ProposalProcessors) Process(
	ctx context.Context,
	facthash util.Hash,
	previous base.Manifest,
	ivp base.INITVoteproof,
) (base.Manifest, error) {
	l := pps.Log().With().Stringer("fact", facthash).Logger()

	e := util.StringErrorFunc("failed to process proposal, %q", facthash)

	switch p, err := pps.newProcessor(ctx, facthash, previous); {
	case err != nil:
		l.Error().Err(err).Msg("failed to process proposal")

		return nil, e(err, "")
	case p == nil:
		return nil, nil
	default:
		return pps.runProcessor(ctx, p, ivp)
	}
}

func (pps *ProposalProcessors) Save(ctx context.Context, facthash util.Hash, avp base.ACCEPTVoteproof) error {
	pps.Lock()
	defer pps.Unlock()

	l := pps.Log().With().Stringer("fact", facthash).Logger()

	defer func() {
		if err := pps.close(); err != nil {
			l.Error().Err(err).Msg("failed to close proposal processor")
		}
	}()

	e := util.StringErrorFunc("failed to save proposal, %q", facthash)

	switch {
	case pps.p == nil:
		l.Debug().Msg("proposal processor not found")

		return e(NotProposalProcessorProcessedError.Call(), "")
	case !pps.p.Proposal().Fact().Hash().Equal(facthash):
		l.Debug().Msg("proposal processor not found")

		return e(NotProposalProcessorProcessedError.Call(), "")
	}

	switch err := pps.p.Save(ctx, avp); {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return e(NotProposalProcessorProcessedError.Call(), "")
	default:
		return e(err, "")
	}
}

func (pps *ProposalProcessors) Cancel() error {
	pps.Lock()
	defer pps.Unlock()

	if pps.p != nil {
		if err := pps.p.Cancel(); err != nil {
			return errors.Wrap(err, "failed to cancel")
		}
	}

	return pps.close()
}

func (pps *ProposalProcessors) close() error {
	if pps.p == nil {
		return nil
	}

	pps.p = nil

	return nil
}

func (pps *ProposalProcessors) SetRetryLimit(l int) *ProposalProcessors {
	pps.retrylimit = l

	return pps
}

func (pps *ProposalProcessors) SetRetryInterval(i time.Duration) *ProposalProcessors {
	pps.retryinterval = i

	return pps
}

func (pps *ProposalProcessors) fetchFact(ctx context.Context, facthash util.Hash) (base.ProposalSignedFact, error) {
	e := util.StringErrorFunc("failed to fetch fact")

	var pr base.ProposalSignedFact

	err := util.Retry(
		ctx,
		func() (bool, error) {
			j, err := pps.getproposal(ctx, facthash)

			switch {
			case err == nil:
				pr = j

				return false, nil
			default:
				return true, e(err, "failed to get proposal fact")
			}
		},
		pps.retrylimit,
		pps.retryinterval,
	)

	return pr, err
}

func (pps *ProposalProcessors) newProcessor(
	ctx context.Context, facthash util.Hash, previous base.Manifest,
) (ProposalProcessor, error) {
	pps.Lock()
	defer pps.Unlock()

	e := util.StringErrorFunc("failed new processor, %q", facthash)

	l := pps.Log().With().Stringer("fact", facthash).Logger()

	if pps.p != nil {
		p := pps.p
		if p.Proposal().Fact().Hash().Equal(facthash) {
			l.Debug().Msg("proposal already processed")

			return nil, nil
		}

		if err := p.Cancel(); err != nil {
			l.Debug().
				Err(err).
				Stringer("previous_processor", p.Proposal().Fact().Hash()).
				Msg("failed to cancel previous running processor")

			return nil, e(err, "")
		}
	}

	// NOTE fetch proposal fact
	fact, err := pps.fetchFact(ctx, facthash)

	switch {
	case err != nil:
		return nil, e(err, "failed to get proposal fact")
	case fact == nil:
		return nil, e(util.ErrNotFound.Call(), "failed to get proposal fact; empty fact")
	}

	if err := util.Retry(ctx, func() (bool, error) {
		switch i, err := pps.makenew(fact, previous); {
		case err != nil:
			return true, err
		default:
			pps.p = i

			return false, nil
		}
	}, pps.retrylimit, pps.retryinterval); err != nil {
		return nil, e(err, "")
	}

	if l, ok := pps.p.(logging.SetLogging); ok {
		_ = l.SetLogging(pps.Logging)
	}

	return pps.p, nil
}

func (*ProposalProcessors) runProcessor(
	ctx context.Context, p ProposalProcessor, ivp base.INITVoteproof,
) (base.Manifest, error) {
	manifest, err := p.Process(ctx, ivp)

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
