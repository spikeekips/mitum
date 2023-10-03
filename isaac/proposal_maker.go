package isaac

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type ProposalMaker struct {
	*logging.Logging
	local         base.LocalNode
	pool          ProposalPool
	getOperations func(context.Context, base.Height) ([][2]util.Hash, error)
	networkID     base.NetworkID
	sync.Mutex
}

func NewProposalMaker(
	local base.LocalNode,
	networkID base.NetworkID,
	getOperations func(context.Context, base.Height) ([][2]util.Hash, error),
	pool ProposalPool,
) *ProposalMaker {
	return &ProposalMaker{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "proposal-maker")
		}),
		local:         local,
		networkID:     networkID,
		getOperations: getOperations,
		pool:          pool,
	}
}

func (p *ProposalMaker) Empty(
	_ context.Context, point base.Point, previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	p.Lock()
	defer p.Unlock()

	e := util.StringError("make empty proposal")

	switch pr, found, err := p.pool.ProposalByPoint(point, p.local.Address(), previousBlock); {
	case err != nil:
		return nil, e.Wrap(err)
	case found:
		return pr, nil
	}

	pr, err := p.makeProposal(point, previousBlock, nil)
	if err != nil {
		return nil, e.WithMessage(err, "make empty proposal, %q", point)
	}

	return pr, nil
}

func (p *ProposalMaker) New(
	ctx context.Context, point base.Point, previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	p.Lock()
	defer p.Unlock()

	e := util.StringError("make proposal, %q", point)

	switch pr, found, err := p.pool.ProposalByPoint(point, p.local.Address(), previousBlock); {
	case err != nil:
		return nil, e.Wrap(err)
	case found:
		return pr, nil
	}

	ops, err := p.getOperations(ctx, point.Height())
	if err != nil {
		return nil, e.WithMessage(err, "get operations")
	}

	p.Log().Trace().Func(func(e *zerolog.Event) {
		for i := range ops {
			e.Interface("operation", ops[i])
		}
	}).Msg("new operation for proposal maker")

	pr, err := p.makeProposal(point, previousBlock, ops)
	if err != nil {
		return nil, e.Wrap(err)
	}

	return pr, nil
}

func (p *ProposalMaker) makeProposal(
	point base.Point, previousBlock util.Hash, ops [][2]util.Hash,
) (sf ProposalSignFact, _ error) {
	fact := NewProposalFact(point, p.local.Address(), previousBlock, ops)

	signfact := NewProposalSignFact(fact)
	if err := signfact.Sign(p.local.Privatekey(), p.networkID); err != nil {
		return sf, err
	}

	if _, err := p.pool.SetProposal(signfact); err != nil {
		return sf, err
	}

	return signfact, nil
}
