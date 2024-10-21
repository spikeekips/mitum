package isaac

import (
	"context"
	"sync"

	"github.com/pkg/errors"
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
	lastBlockMap  func() (base.BlockMap, bool, error)
	networkID     base.NetworkID
	l             sync.Mutex
}

func NewProposalMaker(
	local base.LocalNode,
	networkID base.NetworkID,
	getOperations func(context.Context, base.Height) ([][2]util.Hash, error),
	pool ProposalPool,
	lastBlockMap func() (base.BlockMap, bool, error),
) *ProposalMaker {
	if getOperations == nil {
		getOperations = func( //revive:disable-line:modifies-parameter
			context.Context, base.Height,
		) ([][2]util.Hash, error) {
			return nil, nil
		}
	}

	if lastBlockMap == nil {
		lastBlockMap = func() (base.BlockMap, bool, error) { //revive:disable-line:modifies-parameter
			return nil, false, nil
		}
	}

	return &ProposalMaker{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "proposal-maker")
		}),
		local:         local,
		networkID:     networkID,
		getOperations: getOperations,
		pool:          pool,
		lastBlockMap:  lastBlockMap,
	}
}

func (p *ProposalMaker) PreferEmpty(
	ctx context.Context, point base.Point, previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	p.l.Lock()
	defer p.l.Unlock()

	e := util.StringError("make empty proposal")

	switch m, found, err := p.lastBlockMap(); {
	case err != nil:
		return nil, e.Wrap(err)
	case !found:
	case point.Height() < m.Manifest().Height()-1:
		return nil, e.Errorf("too old; ignored")
	}

	pr, err := p.preferEmpty(ctx, point, previousBlock)

	return pr, e.Wrap(err)
}

func (p *ProposalMaker) preferEmpty(
	_ context.Context, point base.Point, previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	switch pr, found, err := p.pool.ProposalByPoint(point, p.local.Address(), previousBlock); {
	case err != nil:
		return nil, err
	case found:
		return pr, nil
	}

	pr, err := p.makeProposal(point, previousBlock, nil)
	if err != nil {
		return nil, errors.WithMessagef(err, "make empty proposal, %q", point)
	}

	return pr, nil
}

func (p *ProposalMaker) Make(
	ctx context.Context, point base.Point, previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	p.l.Lock()
	defer p.l.Unlock()

	e := util.StringError("make proposal, %q", point)

	switch m, found, err := p.lastBlockMap(); {
	case err != nil:
		return nil, e.Wrap(err)
	case !found:
	case point.Height() < m.Manifest().Height()-1:
		return nil, e.Errorf("too old; ignored")
	case point.Height() > m.Manifest().Height()+1: // NOTE empty proposal for unreachable point
		pr, err := p.preferEmpty(context.Background(), point, previousBlock)

		return pr, e.Wrap(err)
	case point.Height() == m.Manifest().Height()+1 && !previousBlock.Equal(m.Manifest().Hash()):
		pr, err := p.preferEmpty(context.Background(), point, previousBlock)

		return pr, e.Wrap(err)
	}

	pr, err := p.makeNew(ctx, point, previousBlock)

	return pr, e.Wrap(err)
}

func (p *ProposalMaker) makeNew(
	ctx context.Context, point base.Point, previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	switch pr, found, err := p.pool.ProposalByPoint(point, p.local.Address(), previousBlock); {
	case err != nil:
		return nil, errors.WithStack(err)
	case found:
		return pr, nil
	}

	ops, err := p.getOperations(ctx, point.Height())
	if err != nil {
		return nil, errors.WithMessage(err, "get operations")
	}

	p.Log().Trace().Func(func(e *zerolog.Event) {
		for i := range ops {
			e.Interface("operation", ops[i])
		}
	}).Msg("new operation for proposal maker")

	pr, err := p.makeProposal(point, previousBlock, ops)
	if err != nil {
		return nil, errors.WithStack(err)
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
