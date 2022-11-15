package isaac

import (
	"context"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var (
	errFailedToRequestProposalToNode = util.NewError("failed to request proposal to node")
	ErrEmptyAvailableNodes           = util.NewError("empty available nodes for new proposal")
)

// ProposerSelector selects proposer between suffrage nodes
type ProposerSelector interface {
	Select(context.Context, base.Point, []base.Node) (base.Node, error)
}

// ProposalSelector fetchs proposal from selected proposer
type ProposalSelector interface {
	Select(context.Context, base.Point) (base.ProposalSignFact, error)
}

type BaseProposalSelector struct {
	local             base.LocalNode
	pool              ProposalPool
	proposerSelector  ProposerSelector
	maker             *ProposalMaker
	getAvailableNodes func(base.Height) ([]base.Node, bool, error)
	request           func(context.Context, base.Point, base.Address) (base.ProposalSignFact, error)
	params            *LocalParams
	sync.Mutex
}

func NewBaseProposalSelector(
	local base.LocalNode,
	params *LocalParams,
	proposerSelector ProposerSelector,
	maker *ProposalMaker,
	getAvailableNodes func(base.Height) ([]base.Node, bool, error),
	request func(context.Context, base.Point, base.Address) (base.ProposalSignFact, error),
	pool ProposalPool,
) *BaseProposalSelector {
	return &BaseProposalSelector{
		local:             local,
		params:            params,
		proposerSelector:  proposerSelector,
		maker:             maker,
		getAvailableNodes: getAvailableNodes,
		request:           request,
		pool:              pool,
	}
}

func (p *BaseProposalSelector) Select(ctx context.Context, point base.Point) (base.ProposalSignFact, error) {
	p.Lock()
	defer p.Unlock()

	e := util.StringErrorFunc("failed to select proposal")

	var nodes []base.Node

	switch i, found, err := p.getAvailableNodes(point.Height()); {
	case err != nil:
		if !errors.Is(err, ErrEmptyAvailableNodes) {
			return nil, e(err, "failed to get suffrage for height, %d", point.Height())
		}

		nodes = []base.Node{p.local}
	case !found:
		return nil, e(nil, "suffrage not found for height, %d", point.Height())
	default:
		nodes = i
	}

	switch n := len(nodes); {
	case n < 1:
		return nil, errors.Errorf("empty suffrage nodes")
	case n < 2: //nolint:gomnd //...
		pr, err := p.findProposal(ctx, point, nodes[0])
		if err != nil {
			return nil, e(err, "")
		}

		return pr, nil
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Address().String() < nodes[j].Address().String()
	})

	for {
		proposer, err := p.proposerSelector.Select(ctx, point, nodes)
		if err != nil {
			return nil, e(err, "failed to select proposer")
		}

		switch pr, err := p.findProposal(ctx, point, proposer); {
		case err == nil:
			return pr, nil
		case errors.Is(err, errFailedToRequestProposalToNode):
			// NOTE if failed to request to remote node, remove the node from
			// candidates.
			nodes = p.filterDeadNodes(nodes, []base.Address{proposer.Address()})
			if len(nodes) < 1 {
				return nil, e(err, "no valid nodes left")
			}
		default:
			return nil, e(err, "failed to find proposal")
		}
	}
}

func (p *BaseProposalSelector) findProposal(
	ctx context.Context,
	point base.Point,
	proposer base.Node,
) (base.ProposalSignFact, error) {
	e := util.StringErrorFunc("failed to find proposal")

	switch pr, found, err := p.pool.ProposalByPoint(point, proposer.Address()); {
	case err != nil:
		return nil, e(err, "")
	case found:
		return pr, nil
	}

	pr, err := p.findProposalFromProposer(ctx, point, proposer.Address())
	if err != nil {
		return nil, e(err, "")
	}

	if !proposer.Address().Equal(p.local.Address()) {
		if !pr.Signs()[0].Signer().Equal(proposer.Publickey()) {
			return nil, e(nil, "proposal not signed by proposer")
		}
	}

	return pr, nil
}

func (p *BaseProposalSelector) findProposalFromProposer(
	ctx context.Context,
	point base.Point,
	proposer base.Address,
) (base.ProposalSignFact, error) {
	if proposer.Equal(p.local.Address()) {
		return p.maker.New(ctx, point)
	}

	// NOTE if not found in local, request to proposer node
	var pr base.ProposalSignFact
	var err error

	done := make(chan struct{}, 1)
	rctx, cancel := context.WithTimeout(ctx, p.params.TimeoutRequestProposal())

	go func() {
		defer cancel()

		pr, err = p.request(rctx, point, proposer)
		done <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		cancel()

		return nil, ctx.Err()
	case <-rctx.Done():
		<-done

		if err != nil || errors.Is(rctx.Err(), context.DeadlineExceeded) {
			return nil, errFailedToRequestProposalToNode.Errorf("remote node, %q", proposer)
		}

		if _, err := p.pool.SetProposal(pr); err != nil {
			return nil, err
		}

		return pr, nil
	}
}

func (*BaseProposalSelector) filterDeadNodes(n []base.Node, b []base.Address) []base.Node {
	return util.Filter2Slices( // NOTE filter long dead nodes
		n, b,
		func(x base.Node, y base.Address) bool {
			return x.Address().Equal(y)
		},
	)
}

type FuncProposerSelector struct {
	selectfunc func(base.Point, []base.Node) (base.Node, error)
}

func NewFixedProposerSelector(
	selectfunc func(base.Point, []base.Node) (base.Node, error),
) FuncProposerSelector {
	return FuncProposerSelector{selectfunc: selectfunc}
}

func (p FuncProposerSelector) Select(_ context.Context, point base.Point, nodes []base.Node) (base.Node, error) {
	return p.selectfunc(point, nodes)
}

type BlockBasedProposerSelector struct {
	getManifestHash func(base.Height) (util.Hash, error)
}

func NewBlockBasedProposerSelector(
	getManifestHash func(base.Height) (util.Hash, error),
) BlockBasedProposerSelector {
	return BlockBasedProposerSelector{
		getManifestHash: getManifestHash,
	}
}

func (p BlockBasedProposerSelector) Select(_ context.Context, point base.Point, nodes []base.Node) (base.Node, error) {
	var manifest util.Hash

	switch h, err := p.getManifestHash(point.Height() - 1); {
	case err != nil:
		return nil, err
	case h == nil:
		return nil, util.ErrNotFound.Errorf("manifest hash not found in height, %d", point.Height()-1)
	default:
		manifest = h
	}

	switch n := len(nodes); {
	case n < 1:
		return nil, errors.Errorf("empty suffrage nodes")
	case n < 2: //nolint:gomnd //...
		return nodes[0], nil
	}

	var sum uint64

	for _, b := range manifest.Bytes() {
		sum += uint64(b)
	}

	sum += uint64(point.Height().Int64()) + point.Round().Uint64()

	return nodes[int(sum%uint64(len(nodes)))], nil
}

type ProposalMaker struct {
	*logging.Logging
	local         base.LocalNode
	params        base.LocalParams
	pool          ProposalPool
	getOperations func(context.Context, base.Height) ([]util.Hash, error)
	sync.Mutex
}

func NewProposalMaker(
	local base.LocalNode,
	params base.LocalParams,
	getOperations func(context.Context, base.Height) ([]util.Hash, error),
	pool ProposalPool,
) *ProposalMaker {
	return &ProposalMaker{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "proposal-maker")
		}),
		local:         local,
		params:        params,
		getOperations: getOperations,
		pool:          pool,
	}
}

func (p *ProposalMaker) Empty(_ context.Context, point base.Point) (base.ProposalSignFact, error) {
	p.Lock()
	defer p.Unlock()

	e := util.StringErrorFunc("failed to make empty proposal")

	switch pr, found, err := p.pool.ProposalByPoint(point, p.local.Address()); {
	case err != nil:
		return nil, e(err, "")
	case found:
		return pr, nil
	}

	pr, err := p.makeProposal(point, nil)
	if err != nil {
		return nil, e(err, "failed to make empty proposal, %q", point)
	}

	return pr, nil
}

func (p *ProposalMaker) New(ctx context.Context, point base.Point) (base.ProposalSignFact, error) {
	p.Lock()
	defer p.Unlock()

	e := util.StringErrorFunc("failed to make proposal, %q", point)

	switch pr, found, err := p.pool.ProposalByPoint(point, p.local.Address()); {
	case err != nil:
		return nil, e(err, "")
	case found:
		return pr, nil
	}

	ops, err := p.getOperations(ctx, point.Height())
	if err != nil {
		return nil, e(err, "failed to get operations")
	}

	p.Log().Trace().Func(func(e *zerolog.Event) {
		for i := range ops {
			e.Stringer("operation", ops[i])
		}
	}).Msg("new operation for proposal maker")

	pr, err := p.makeProposal(point, ops)
	if err != nil {
		return nil, e(err, "")
	}

	return pr, nil
}

func (p *ProposalMaker) makeProposal(point base.Point, ops []util.Hash) (sf ProposalSignFact, _ error) {
	fact := NewProposalFact(point, p.local.Address(), ops)

	signfact := NewProposalSignFact(fact)
	if err := signfact.Sign(p.local.Privatekey(), p.params.NetworkID()); err != nil {
		return sf, err
	}

	if _, err := p.pool.SetProposal(signfact); err != nil {
		return sf, err
	}

	return signfact, nil
}
