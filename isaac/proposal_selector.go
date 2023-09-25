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

var errFailedToRequestProposalToNode = util.NewIDError("request proposal to node")

// ProposerSelectFunc selects proposer between suffrage nodes. If failed to
// request proposal from remotes, local will be proposer.
type ProposerSelectFunc func(context.Context, base.Point, []base.Node, util.Hash) (base.Node, error)

type BaseProposalSelectorArgs struct {
	Pool                    ProposalPool
	ProposerSelectFunc      ProposerSelectFunc
	Maker                   *ProposalMaker
	GetNodesFunc            func(base.Height) ([]base.Node, bool, error)
	RequestFunc             func(context.Context, base.Point, base.Node, util.Hash) (base.ProposalSignFact, bool, error)
	TimeoutRequest          func() time.Duration
	RequestProposalInterval time.Duration
	MinProposerWait         time.Duration
}

func NewBaseProposalSelectorArgs() *BaseProposalSelectorArgs {
	return &BaseProposalSelectorArgs{
		GetNodesFunc: func(base.Height) ([]base.Node, bool, error) {
			return nil, false, errors.Wrapf(context.Canceled, "get nodes")
		},
		RequestFunc: func(context.Context, base.Point, base.Node, util.Hash) (base.ProposalSignFact, bool, error) {
			return nil, false, util.ErrNotImplemented.Errorf("request")
		},
		RequestProposalInterval: time.Millisecond * 666,              //nolint:gomnd //...
		MinProposerWait:         DefaultTimeoutRequest + time.Second, //nolint:gomnd //...
		TimeoutRequest: func() time.Duration {
			return DefaultTimeoutRequest
		},
	}
}

type BaseProposalSelector struct {
	local base.LocalNode
	args  *BaseProposalSelectorArgs
	sync.Mutex
}

func NewBaseProposalSelector(
	local base.LocalNode,
	args *BaseProposalSelectorArgs,
) *BaseProposalSelector {
	return &BaseProposalSelector{
		local: local,
		args:  args,
	}
}

func (p *BaseProposalSelector) Select(
	ctx context.Context,
	point base.Point,
	previousBlock util.Hash,
	wait time.Duration,
) (base.ProposalSignFact, error) {
	switch pr, err := p.selectInternal(ctx, point, previousBlock, wait); {
	case errors.Is(err, errFailedToRequestProposalToNode),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		pr, err = p.args.Maker.New(ctx, point, previousBlock)
		if err != nil {
			return nil, err
		}

		if _, eerr := p.args.Pool.SetProposal(pr); eerr != nil {
			return nil, eerr
		}

		return pr, nil
	case err != nil:
		return nil, err
	default:
		return pr, nil
	}
}

func (p *BaseProposalSelector) selectInternal(
	ctx context.Context,
	point base.Point,
	previousBlock util.Hash,
	wait time.Duration,
) (base.ProposalSignFact, error) {
	p.Lock()
	defer p.Unlock()

	pwait := wait
	if pwait < p.args.MinProposerWait {
		pwait = p.args.MinProposerWait
	}

	wctx, cancel := context.WithTimeout(ctx, pwait)
	defer cancel()

	var nodes []base.Node

	switch i, found, err := p.getNodes(point.Height(), p.args.GetNodesFunc); {
	case err != nil, !found:
		if err == nil {
			err = errors.Errorf("nodes not found for height, %v", point)
		}

		return nil, errors.WithMessagef(err, "get suffrage for height, %d", point.Height())
	case len(i) < 2:
		return p.proposalFromNode(wctx, point, i[0], previousBlock)
	default:
		nodes = i
	}

	var failed base.Address

	switch pr, proposer, err := p.selectFromProposer(wctx, point, nodes, previousBlock); {
	case errors.Is(err, errFailedToRequestProposalToNode),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		failed = proposer
	case err != nil:
		return nil, err
	case pr != nil:
		return pr, nil
	default:
		failed = proposer
	}

	if failed != nil {
		nodes = p.filterDeadNodes(nodes, []base.Address{failed})
	}

	if len(nodes) < 1 {
		return nil, errFailedToRequestProposalToNode.Errorf("empty nodes")
	}

	// NOTE if failed from original proposer, request to the other nodes. The
	// previous context may be already expired, so proposalFromOthers uses new
	// context.
	wctx, cancel = context.WithTimeout(ctx, pwait)
	defer cancel()

	return p.proposalFromOthers(wctx, point, nodes, previousBlock)
}

func (p *BaseProposalSelector) selectFromProposer(
	ctx context.Context,
	point base.Point,
	nodes []base.Node,
	previousBlock util.Hash,
) (base.ProposalSignFact, base.Address, error) {
	e := util.StringError("select proposal from proposer")

	proposer, err := p.args.ProposerSelectFunc(ctx, point, nodes, previousBlock)
	if err != nil {
		return nil, nil, e.WithMessage(err, "select proposer")
	}

	pr, err := p.proposalFromNode(ctx, point, proposer, previousBlock)
	if err != nil {
		return nil, proposer.Address(), e.Wrap(err)
	}

	return pr, proposer.Address(), err
}

func (p *BaseProposalSelector) proposalFromNode(
	ctx context.Context,
	point base.Point,
	proposer base.Node,
	previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	ticker := time.NewTicker(time.Millisecond * 33)
	defer ticker.Stop()

	var reset sync.Once

	for {
		select {
		case <-ctx.Done():
			return nil, errors.WithStack(ctx.Err())
		case <-ticker.C:
			reset.Do(func() {
				ticker.Reset(p.args.RequestProposalInterval)
			})

			switch pr, err := p.findProposal(ctx, point, proposer, previousBlock); {
			case err == nil:
				return pr, nil
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				// NOTE ignore context error from findProposal; if context error
				// is from main context, it will be catched from the main select
				// ctx.Done().
			case errors.Is(err, errFailedToRequestProposalToNode):
			default:
				return nil, errors.WithMessage(err, "find proposal")
			}
		}
	}
}

func (p *BaseProposalSelector) proposalFromOthers(
	ctx context.Context,
	point base.Point,
	nodes []base.Node,
	previousBlock util.Hash,
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
			return nil, errors.WithStack(ctx.Err())
		case <-ticker.C:
			reset.Do(func() {
				ticker.Reset(p.args.RequestProposalInterval)
			})

			proposer, err := p.args.ProposerSelectFunc(ctx, point, filtered, previousBlock)
			if err != nil {
				return nil, errors.WithMessage(err, "select proposer")
			}

			switch pr, err := p.findProposal(ctx, point, proposer, previousBlock); {
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
				return nil, errors.WithMessage(err, "find proposal")
			}
		}
	}
}

func (p *BaseProposalSelector) findProposal(
	ctx context.Context,
	point base.Point,
	proposer base.Node,
	previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	e := util.StringError("find proposal")

	switch pr, found, err := p.args.Pool.ProposalByPoint(point, proposer.Address(), previousBlock); {
	case err != nil:
		return nil, e.Wrap(err)
	case found:
		return pr, nil
	}

	pr, err := p.findProposalFromProposer(ctx, point, proposer, previousBlock)
	if err != nil {
		return nil, e.Wrap(err)
	}

	return pr, nil
}

func (p *BaseProposalSelector) findProposalFromProposer(
	ctx context.Context,
	point base.Point,
	proposer base.Node,
	previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	if proposer.Address().Equal(p.local.Address()) {
		return p.args.Maker.New(ctx, point, previousBlock)
	}

	// NOTE if not found in local, request to proposer node
	rctx, cancel := context.WithTimeout(ctx, p.args.TimeoutRequest())
	defer cancel()

	donech := make(chan interface{})

	go func() {
		switch pr, found, err := p.args.RequestFunc(rctx, point, proposer, previousBlock); {
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
		return nil, errFailedToRequestProposalToNode.WithMessage(
			rctx.Err(), "context error; remote node, %q", proposer.Address())
	case i := <-donech:
		switch t := i.(type) {
		case error:
			return nil, errFailedToRequestProposalToNode.WithMessage(
				t, "request failed; remote node, %q", proposer.Address())
		case base.ProposalSignFact:
			if _, err := p.args.Pool.SetProposal(t); err != nil {
				return nil, err
			}

			return t, nil
		}
	}

	return nil, errors.Errorf("empty proposal")
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
	switch nodes, found, err := f(height.SafePrev()); {
	case err != nil, !found:
		return nil, found, err
	case len(nodes) < 1:
		return nil, false, errors.Errorf("empty suffrage nodes")
	case len(nodes) < 2:
		return nodes, true, nil
	default:
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].Address().String() < nodes[j].Address().String()
		})

		return nodes, true, nil
	}
}

type FuncProposerSelector struct {
	selectfunc func(base.Point, []base.Node, util.Hash) (base.Node, error)
}

func NewFixedProposerSelector(
	selectfunc func(base.Point, []base.Node, util.Hash) (base.Node, error),
) FuncProposerSelector {
	return FuncProposerSelector{selectfunc: selectfunc}
}

func (p FuncProposerSelector) Select(
	_ context.Context, point base.Point, nodes []base.Node, previousBlock util.Hash,
) (base.Node, error) {
	return p.selectfunc(point, nodes, previousBlock)
}

type BlockBasedProposerSelector struct{}

func NewBlockBasedProposerSelector() BlockBasedProposerSelector {
	return BlockBasedProposerSelector{}
}

func (BlockBasedProposerSelector) Select(
	_ context.Context, point base.Point, nodes []base.Node, previousBlock util.Hash,
) (base.Node, error) {
	switch n := len(nodes); {
	case n < 1:
		return nil, errors.Errorf("empty suffrage nodes")
	case n < 2:
		return nodes[0], nil
	}

	var sum uint64

	for _, b := range previousBlock.Bytes() {
		sum += uint64(b)
	}

	sum += uint64(point.Height().Int64()) + point.Round().Uint64()

	return nodes[int(sum%uint64(len(nodes)))], nil
}

type ProposalMaker struct {
	*logging.Logging
	local         base.LocalNode
	pool          ProposalPool
	getOperations func(context.Context, base.Height) ([]util.Hash, error)
	networkID     base.NetworkID
	sync.Mutex
}

func NewProposalMaker(
	local base.LocalNode,
	networkID base.NetworkID,
	getOperations func(context.Context, base.Height) ([]util.Hash, error),
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
			e.Stringer("operation", ops[i])
		}
	}).Msg("new operation for proposal maker")

	pr, err := p.makeProposal(point, previousBlock, ops)
	if err != nil {
		return nil, e.Wrap(err)
	}

	return pr, nil
}

func (p *ProposalMaker) makeProposal(
	point base.Point, previousBlock util.Hash, ops []util.Hash,
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

var errConcurrentRequestProposalFound = util.NewIDError("proposal found")

func ConcurrentRequestProposal(
	ctx context.Context,
	point base.Point,
	proposer base.Node,
	previousBlock util.Hash,
	client NetworkClient,
	cis []quicstream.ConnInfo,
	networkID base.NetworkID,
) (base.ProposalSignFact, bool, error) {
	worker, err := util.NewErrgroupWorker(ctx, int64(len(cis)))
	if err != nil {
		return nil, false, err
	}

	defer worker.Close()

	prlocked := util.EmptyLocked[base.ProposalSignFact]()

	go func() {
		defer worker.Done()

		for i := range cis {
			i := i
			ci := cis[i]

			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				switch pr, found, err := client.RequestProposal(ctx, ci, point, proposer.Address(), previousBlock); {
				case err != nil:
					return nil
				case !found:
					return nil
				case !isExpectedValidProposal(point, proposer, pr, networkID):
					return nil
				default:
					_ = prlocked.SetValue(pr)

					return errConcurrentRequestProposalFound.WithStack()
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
