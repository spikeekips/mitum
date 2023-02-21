package isaac

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var (
	errFailedToRequestProposalToNode = util.NewMError("failed to request proposal to node")
	ErrEmptyNodes                    = util.NewMError("empty nodes for selecting proposal")
)

// ProposerSelector selects proposer between suffrage nodes
type ProposerSelector interface {
	Select(context.Context, base.Point, []base.Node) (base.Node, error)
}

type BaseProposalSelectorArgs struct {
	Pool                    ProposalPool
	ProposerSelector        ProposerSelector
	Maker                   *ProposalMaker
	GetNodesFunc            func(base.Height) ([]base.Node, bool, error)
	RequestFunc             func(context.Context, base.Point, base.Node) (base.ProposalSignFact, bool, error)
	RequestProposalInterval time.Duration
	MinProposerWait         time.Duration
}

func NewBaseProposalSelectorArgs() *BaseProposalSelectorArgs {
	return &BaseProposalSelectorArgs{
		GetNodesFunc: func(base.Height) ([]base.Node, bool, error) {
			return nil, false, context.Canceled
		},
		RequestFunc: func(context.Context, base.Point, base.Node) (base.ProposalSignFact, bool, error) {
			return nil, false, util.ErrNotImplemented.Errorf("request")
		},
		RequestProposalInterval: time.Millisecond * 10,                       //nolint:gomnd //...
		MinProposerWait:         defaultTimeoutRequestProposal + time.Second, //nolint:gomnd //...
	}
}

type BaseProposalSelector struct {
	local  base.LocalNode
	params *LocalParams
	args   *BaseProposalSelectorArgs
	sync.Mutex
}

func NewBaseProposalSelector(
	local base.LocalNode,
	params *LocalParams,
	args *BaseProposalSelectorArgs,
) *BaseProposalSelector {
	return &BaseProposalSelector{
		local:  local,
		params: params,
		args:   args,
	}
}

func (p *BaseProposalSelector) Select(
	ctx context.Context,
	point base.Point,
	wait time.Duration,
) (base.ProposalSignFact, error) {
	p.Lock()
	defer p.Unlock()

	var nodes []base.Node

	switch i, found, err := p.getNodes(point.Height(), p.args.GetNodesFunc); {
	case err != nil, !found:
		if err == nil {
			err = errors.Errorf("nodes not found for height, %v", point)
		}

		return nil, errors.WithMessagef(err, "failed to get suffrage for height, %d", point.Height())
	case len(i) < 2: //nolint:gomnd //...
		return p.findProposal(ctx, point, i[0])
	default:
		nodes = i
	}

	var failed base.Address

	if wait > p.args.MinProposerWait {
		switch pr, proposer, err := p.selectFromProposer(ctx, point, wait, nodes); {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			failed = proposer
		case err != nil:
			return nil, err
		case pr != nil:
			return pr, nil
		default:
			failed = proposer
		}
	}

	if failed != nil {
		nodes = p.filterDeadNodes(nodes, []base.Address{failed})
	}

	return p.proposalFromOthers(ctx, point, nodes)
}

func (p *BaseProposalSelector) selectFromProposer(
	ctx context.Context,
	point base.Point,
	wait time.Duration,
	nodes []base.Node,
) (base.ProposalSignFact, base.Address, error) {
	e := util.StringErrorFunc("failed to select proposal from proposer")

	pctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	proposer, err := p.args.ProposerSelector.Select(pctx, point, nodes)
	if err != nil {
		return nil, nil, e(err, "failed to select proposer")
	}

	pr, err := p.proposalFromNode(pctx, point, proposer)
	if err != nil {
		return nil, proposer.Address(), e(err, "")
	}

	return pr, proposer.Address(), err
}

func (p *BaseProposalSelector) proposalFromNode(
	ctx context.Context,
	point base.Point,
	proposer base.Node,
) (base.ProposalSignFact, error) {
	ticker := time.NewTicker(1)
	defer ticker.Stop()

	var reset sync.Once

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			reset.Do(func() {
				ticker.Reset(p.args.RequestProposalInterval)
			})

			switch pr, err := p.findProposal(ctx, point, proposer); {
			case err == nil:
				return pr, nil
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				// NOTE ignore context error fro findProposal; if context error
				// is from main context, it will be catched from the main select
				// ctx.Done().
			case errors.Is(err, errFailedToRequestProposalToNode):
			default:
				return nil, errors.WithMessage(err, "failed to find proposal")
			}
		}
	}
}

func (p *BaseProposalSelector) proposalFromOthers(
	ctx context.Context,
	point base.Point,
	nodes []base.Node,
) (base.ProposalSignFact, error) {
	if len(nodes) < 1 {
		return nil, errors.Errorf("empty nodes")
	}

	ticker := time.NewTicker(1)
	defer ticker.Stop()

	var reset sync.Once

	filtered := nodes

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			reset.Do(func() {
				ticker.Reset(p.args.RequestProposalInterval)
			})

			proposer, err := p.args.ProposerSelector.Select(ctx, point, filtered)
			if err != nil {
				return nil, errors.WithMessage(err, "failed to select proposer")
			}

			switch pr, err := p.findProposal(ctx, point, proposer); {
			case err == nil:
				return pr, nil
			case errors.Is(err, errFailedToRequestProposalToNode):
				// NOTE if failed to request to remote node, remove the node from
				// candidates.
				filtered = p.filterDeadNodes(filtered, []base.Address{proposer.Address()})
				if len(filtered) < 1 {
					return nil, errors.WithMessage(err, "no valid nodes left")
				}
			default:
				return nil, errors.WithMessage(err, "failed to find proposal")
			}
		}
	}
}

func (p *BaseProposalSelector) findProposal(
	ctx context.Context,
	point base.Point,
	proposer base.Node,
) (base.ProposalSignFact, error) {
	e := util.StringErrorFunc("failed to find proposal")

	switch pr, found, err := p.args.Pool.ProposalByPoint(point, proposer.Address()); {
	case err != nil:
		return nil, e(err, "")
	case found:
		return pr, nil
	}

	pr, err := p.findProposalFromProposer(ctx, point, proposer)
	if err != nil {
		return nil, e(err, "")
	}

	return pr, nil
}

func (p *BaseProposalSelector) findProposalFromProposer(
	ctx context.Context,
	point base.Point,
	proposer base.Node,
) (base.ProposalSignFact, error) {
	if proposer.Address().Equal(p.local.Address()) {
		return p.args.Maker.New(ctx, point)
	}

	// NOTE if not found in local, request to proposer node
	rctx, cancel := context.WithTimeout(ctx, p.params.TimeoutRequestProposal())
	defer cancel()

	donech := make(chan interface{})

	go func() {
		switch pr, found, err := p.args.RequestFunc(rctx, point, proposer); {
		case err != nil, !found:
			if !found {
				err = errors.Errorf("empty proposal")
			}

			donech <- err
		default:
			donech <- pr
		}
	}()

	select {
	case <-rctx.Done():
		return nil, rctx.Err()
	case i := <-donech:
		switch t := i.(type) {
		case error:
			return nil, errFailedToRequestProposalToNode.Wrapf(t, "remote node, %q", proposer.Address())
		case base.ProposalSignFact:
			if _, err := p.args.Pool.SetProposal(t); err != nil {
				return nil, err
			}

			return t, nil
		}
	}

	return nil, errors.Errorf("empty propsal")
}

func (*BaseProposalSelector) filterDeadNodes(n []base.Node, b []base.Address) []base.Node {
	return util.Filter2Slices( // NOTE filter long dead nodes
		n, b,
		func(x base.Node, y base.Address) bool {
			return x.Address().Equal(y)
		},
	)
}

func (*BaseProposalSelector) getNodes(
	height base.Height,
	f func(base.Height) ([]base.Node, bool, error),
) ([]base.Node, bool, error) {
	switch nodes, found, err := f(height); {
	case err != nil, !found:
		return nil, found, err
	case len(nodes) < 1:
		return nil, false, errors.Errorf("empty suffrage nodes")
	case len(nodes) < 2: //nolint:gomnd //...
		return nodes, true, nil
	default:
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].Address().String() < nodes[j].Address().String()
		})

		return nodes, true, nil
	}
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

var errConcurrentRequestProposalFound = util.NewMError("proposal found")

func ConcurrentRequestProposal(
	ctx context.Context,
	point base.Point,
	proposer base.Node,
	client NetworkClient,
	cis []quicstream.UDPConnInfo,
	networkID base.NetworkID,
) (base.ProposalSignFact, bool, error) {
	worker := util.NewErrgroupWorker(ctx, int64(len(cis)))
	defer worker.Close()

	prlocked := util.EmptyLocked((base.ProposalSignFact)(nil))

	go func() {
		defer worker.Done()

		for i := range cis {
			i := i
			ci := cis[i]

			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				switch pr, found, err := client.RequestProposal(ctx, ci, point, proposer.Address()); {
				case err != nil:
					return nil
				case !found:
					return nil
				case !isExpectedValidProposal(point, proposer, pr, networkID):
					return nil
				default:
					_ = prlocked.SetValue(pr)

					return errConcurrentRequestProposalFound
				}
			}); err != nil {
				return
			}
		}
	}()

	switch err := worker.Wait(); {
	case err == nil:
	case errors.Is(err, errConcurrentRequestProposalFound):
	default:
		return nil, false, err
	}

	switch pr, isempty := prlocked.Value(); {
	case isempty, pr == nil:
		return nil, false, nil
	default:
		return pr, true, nil
	}
}

func isExpectedValidProposal(
	point base.Point,
	proposer base.Node,
	pr base.ProposalSignFact,
	networkID base.NetworkID,
) bool {
	if err := pr.IsValid(networkID); err != nil {
		return false
	}

	switch {
	case !pr.Point().Equal(point):
		return false
	case !proposer.Address().Equal(pr.ProposalFact().Proposer()):
		return false
	case !proposer.Publickey().Equal(pr.Signs()[0].Signer()):
		return false
	default:
		return true
	}
}
