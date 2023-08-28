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

// ErrIgnoreErrorProposalProcessor ignores error from proposalProcessor, it means
// not ErrIgnoreErrorProposalProcessor from proposalProcessor will break
// consensus.
var (
	ErrIgnoreErrorProposalProcessor  = util.NewIDError("proposal processor somthing wrong; ignore")
	ErrNotProposalProcessorProcessed = util.NewIDError("proposal processor not processed")
)

type ProcessorProcessFunc func(context.Context) (base.Manifest, error)

type ProposalProcessors struct {
	p ProposalProcessor
	*logging.Logging
	makenew       func(proposal base.ProposalSignFact, previous base.Manifest) (ProposalProcessor, error)
	getproposal   func(_ context.Context, _ base.Point, proposalFactHash util.Hash) (base.ProposalSignFact, error)
	retryinterval time.Duration
	retrylimit    int
	sync.RWMutex
	previousSaved base.Height
}

func NewProposalProcessors(
	makenew func(base.ProposalSignFact, base.Manifest) (ProposalProcessor, error),
	getproposal func(context.Context, base.Point, util.Hash) (base.ProposalSignFact, error),
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
		previousSaved: base.NilHeight,
	}
}

func (pps *ProposalProcessors) Processor() ProposalProcessor {
	pps.RLock()
	defer pps.RUnlock()

	return pps.p
}

func (pps *ProposalProcessors) Process(
	ctx context.Context,
	point base.Point,
	facthash util.Hash,
	previous base.Manifest,
	ivp base.INITVoteproof,
) (ProcessorProcessFunc, error) {
	pps.Lock()

	l := pps.Log().With().Stringer("point", point).Stringer("fact", facthash).Logger()

	e := util.StringError("process proposal, %q", facthash)

	p, err := pps.newProcessor(ctx, point, facthash, previous)

	switch {
	case err != nil:
		pps.Unlock()

		l.Error().Err(err).Msg("failed to process proposal")

		return nil, e.Wrap(err)
	case p == nil:
		pps.Unlock()

		return nil, nil
	}

	ch := make(chan [2]interface{}, 1)

	go func() {
		defer pps.Unlock()

		m, err := pps.runProcessor(ctx, p, ivp)

		ch <- [2]interface{}{m, err}
	}()

	return func(ctx context.Context) (base.Manifest, error) {
		select {
		case <-ctx.Done():
			return nil, errors.WithStack(ctx.Err())
		case i := <-ch:
			j, k := i[0], i[1]

			var err error

			if k != nil {
				err = k.(error)                       //nolint:forcetypeassert //...
				if errors.Is(err, context.Canceled) { // NOTE ignore context.Canceled
					return nil, ErrNotProposalProcessorProcessed.Wrap(err)
				}
			}

			var m base.Manifest

			if j != nil {
				m = j.(base.Manifest) //nolint:forcetypeassert //...
			}

			return m, err
		}
	}, nil
}

func (pps *ProposalProcessors) Save(
	ctx context.Context, facthash util.Hash, avp base.ACCEPTVoteproof,
) (base.BlockMap, error) {
	pps.Lock()
	defer pps.Unlock()

	switch m, err := pps.save(ctx, facthash, avp); {
	case err != nil:
		if pps.p != nil {
			_ = pps.p.Cancel()

			pps.p = nil
		}

		return nil, errors.WithMessage(err, "save proposal")
	default:
		if pps.p != nil {
			pps.p = nil
		}

		return m, nil
	}
}

func (pps *ProposalProcessors) save(
	ctx context.Context, facthash util.Hash, avp base.ACCEPTVoteproof,
) (base.BlockMap, error) {
	l := pps.Log().With().Interface("height", avp.Point().Height()).Stringer("fact", facthash).Logger()

	if avp.Point().Height() <= pps.previousSaved {
		l.Debug().Interface("previous", pps.previousSaved).Msg("already saved")

		return nil, ErrProcessorAlreadySaved.WithStack()
	}

	switch {
	case pps.p == nil:
		l.Debug().Msg("proposal processor not found")

		return nil, ErrNotProposalProcessorProcessed
	case !pps.p.Proposal().Fact().Hash().Equal(facthash):
		l.Debug().Msg("proposal processor not found")

		return nil, ErrNotProposalProcessorProcessed
	}

	switch bm, err := pps.p.Save(ctx, avp); {
	case err == nil:
		l.Debug().Msg("proposal processed and saved")

		pps.previousSaved = avp.Point().Height()

		return bm, nil
	case errors.Is(err, context.Canceled):
		l.Error().Err(err).Msg("proposal processed canceled")

		return nil, ErrNotProposalProcessorProcessed
	default:
		l.Error().Err(err).Msg("failed to save proposal processed")

		return nil, err
	}
}

func (pps *ProposalProcessors) Cancel() error {
	pps.Lock()
	defer pps.Unlock()

	if pps.p != nil {
		if err := pps.p.Cancel(); err != nil {
			return errors.Wrap(err, "cancel")
		}
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

func (pps *ProposalProcessors) fetchFact(
	ctx context.Context, point base.Point, facthash util.Hash,
) (base.ProposalSignFact, error) {
	e := util.StringError("fetch fact")

	var pr base.ProposalSignFact

	err := util.Retry(
		ctx,
		func() (bool, error) {
			j, err := pps.getproposal(ctx, point, facthash)

			switch {
			case err == nil:
				pr = j

				return false, nil
			default:
				return true, e.WithMessage(err, "get proposal fact")
			}
		},
		pps.retrylimit,
		pps.retryinterval,
	)

	return pr, err
}

func (pps *ProposalProcessors) newProcessor(
	ctx context.Context, point base.Point, facthash util.Hash, previous base.Manifest,
) (ProposalProcessor, error) {
	e := util.StringError(" processor, %q", facthash)

	l := pps.Log().With().Stringer("point", point).Stringer("fact", facthash).Logger()

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

			return nil, e.Wrap(err)
		}
	}

	// NOTE fetch proposal fact
	fact, err := pps.fetchFact(ctx, point, facthash)

	// NOTE if failed to get fact, returns NotProposalProcessorProcessedError
	switch {
	case err != nil:
		return nil, e.WithMessage(ErrNotProposalProcessorProcessed.Wrap(err), "get proposal fact")
	case fact == nil:
		return nil, e.WithMessage(ErrNotProposalProcessorProcessed, "get proposal fact; empty fact")
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
		return nil, e.Wrap(err)
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
	case errors.Is(err, ErrIgnoreErrorProposalProcessor):
		return nil, nil
	default:
		if e := p.Cancel(); e != nil {
			return nil, errors.Wrap(e, "run processor")
		}

		return nil, err
	}
}
