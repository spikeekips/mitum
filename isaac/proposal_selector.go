package isaac

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/bluele/gcache"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

var failedToRequestProposalToNodeError = util.NewError("failed to request proposal to node")

// ProposerSelector selects proposer between suffrage nodes
type ProposerSelector interface {
	Select(context.Context, base.Point, []base.Node) (base.Node, error)
}

// ProposalSelector fetchs proposal from selected proposer
type ProposalSelector interface {
	Select(context.Context, base.Point) (base.ProposalSignedFact, error)
}

type BaseProposalSelector struct {
	sync.Mutex
	local            *LocalNode
	policy           Policy
	proposerSelector ProposerSelector
	maker            *ProposalMaker
	getSuffrage      func(base.Height) base.Suffrage
	getLongDeadNodes func() []base.Address
	request          func(context.Context, base.Point, base.Address) (base.ProposalSignedFact, error)
	cache            *ProposalPool
}

func NewBaseProposalSelector(
	local *LocalNode,
	policy Policy,
	proposerSelector ProposerSelector,
	maker *ProposalMaker,
	getSuffrage func(base.Height) base.Suffrage,
	getLongDeadNodes func() []base.Address,
	request func(context.Context, base.Point, base.Address) (base.ProposalSignedFact, error),
	cache *ProposalPool,
) *BaseProposalSelector {
	if cache == nil {
		cache = NewProposalPool(333) // NOTE big enough :)
	}

	return &BaseProposalSelector{
		local:            local,
		policy:           policy,
		proposerSelector: proposerSelector,
		maker:            maker,
		getSuffrage:      getSuffrage,
		getLongDeadNodes: getLongDeadNodes,
		request:          request,
		cache:            cache,
	}
}

func (p *BaseProposalSelector) Select(ctx context.Context, point base.Point) (base.ProposalSignedFact, error) {
	p.Lock()
	defer p.Unlock()

	e := util.StringErrorFunc("failed to select proposal")

	suf := p.getSuffrage(point.Height())
	if suf == nil {
		return nil, e(nil, "failed to get suffrage for height, %d", point.Height())
	}

	switch n := suf.Len(); {
	case n < 1:
		return nil, errors.Errorf("empty suffrage nodes")
	case n < 2:
		pr, err := p.findProposal(ctx, point, suf.Nodes()[0])
		if err != nil {
			return nil, e(err, "")
		}

		return pr, nil
	}

	sufnodes := suf.Nodes()
	nodes := make([]base.Node, len(sufnodes))
	for i := range nodes {
		nodes[i] = sufnodes[i]
	}

	nodes = filterDeadNodes(nodes, p.getLongDeadNodes())
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
		case errors.Is(err, failedToRequestProposalToNodeError):
			// NOTE if failed to request to remote node, remove the node from
			// candidates.
			nodes = filterDeadNodes(nodes, []base.Address{proposer.Address()})
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
) (base.ProposalSignedFact, error) {
	if pr := p.cache.ByPoint(point, proposer.Address()); pr != nil {
		return pr, nil
	}

	pr, err := p.findProposalFromProposer(ctx, point, proposer.Address())
	if err != nil {
		return nil, err
	}

	if !pr.Signed()[0].Signer().Equal(proposer.Publickey()) {
		return nil, errors.Errorf("proposal not signed by proposer")
	}

	_ = p.cache.Set(pr)

	return pr, nil
}

func (p *BaseProposalSelector) findProposalFromProposer(
	ctx context.Context,
	point base.Point,
	proposer base.Address,
) (base.ProposalSignedFact, error) {
	if proposer.Equal(p.local.Address()) {
		return p.maker.New(ctx, point)
	}

	// NOTE if not found in local, request to proposer node
	var pr base.ProposalSignedFact
	var err error

	done := make(chan struct{}, 1)
	rctx, cancel := context.WithTimeout(ctx, p.policy.TimeoutRequestProposal())
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
			return nil, failedToRequestProposalToNodeError.Errorf("remote node, %q", proposer)
		}

		return pr, nil
	}
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

func (p BlockBasedProposerSelector) Select(
	_ context.Context,
	point base.Point,
	nodes []base.Node,
) (base.Node, error) {
	var manifest util.Hash
	switch h, err := p.getManifestHash(point.Height() - 1); {
	case err != nil:
		return nil, err
	case h == nil:
		return nil, util.NotFoundError.Errorf("manifest hash not found in height, %d", point.Height()-1)
	default:
		manifest = h
	}

	switch n := len(nodes); {
	case n < 1:
		return nil, errors.Errorf("empty suffrage nodes")
	case n < 2:
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
	local         *LocalNode
	policy        base.Policy
	getOperations func(context.Context) ([]util.Hash, error)
}

func NewProposalMaker(
	local *LocalNode,
	policy base.Policy,
	getOperations func(context.Context) ([]util.Hash, error),
) *ProposalMaker {
	return &ProposalMaker{
		local:         local,
		policy:        policy,
		getOperations: getOperations,
	}
}

func (p *ProposalMaker) New(ctx context.Context, point base.Point) (ProposalSignedFact, error) {
	e := util.StringErrorFunc("failed to make proposal, %q", point)

	ops, err := p.getOperations(ctx)
	if err != nil {
		return ProposalSignedFact{}, e(err, "failed to get operations")
	}

	fact := NewProposalFact(point, p.local.Address(), ops)

	signedFact := NewProposalSignedFact(fact)
	if err := signedFact.Sign(p.local.Privatekey(), p.policy.NetworkID()); err != nil {
		return ProposalSignedFact{}, e(err, "")
	}

	return signedFact, nil
}

func filterDeadNodes(n []base.Node, b []base.Address) []base.Node {
	l := util.FilterSlice( // NOTE filter long dead nodes
		n, b,
		func(a, b interface{}) bool {
			return a.(base.Node).Address().Equal(b.(base.Address))
		},
	)

	m := make([]base.Node, len(l))
	for i := range l {
		m[i] = l[i].(base.Node)
	}

	return m
}

type ProposalPool struct {
	sync.RWMutex
	facthashs gcache.Cache
	points    gcache.Cache
}

func NewProposalPool(size int) *ProposalPool {
	return &ProposalPool{
		facthashs: gcache.New(size).LRU().Build(),
		points:    gcache.New(size).LRU().Build(),
	}
}

func (p *ProposalPool) ByFactHash(facthash util.Hash) base.ProposalSignedFact {
	p.RLock()
	defer p.RUnlock()

	return p.get(facthash.String())
}

func (p *ProposalPool) ByPoint(point base.Point, proposer base.Address) base.ProposalSignedFact {
	p.RLock()
	defer p.RUnlock()

	switch i, err := p.points.Get(p.pointkey(point, proposer)); {
	case errors.Is(err, gcache.KeyNotFoundError):
		return nil
	case err != nil:
		return nil
	case i == nil:
		return nil
	default:
		return p.get(i.(string))
	}
}

func (p *ProposalPool) Set(pr base.ProposalSignedFact) bool {
	p.Lock()
	defer p.Unlock()

	facthash := pr.Fact().Hash().String()
	if p.facthashs.Has(facthash) {
		return false
	}

	_ = p.facthashs.Set(facthash, pr)
	_ = p.points.Set(p.pointkey(pr.Point(), pr.ProposalFact().Proposer()), facthash)

	return true
}

func (p *ProposalPool) get(facthash string) base.ProposalSignedFact {
	switch i, err := p.facthashs.Get(facthash); {
	case errors.Is(err, gcache.KeyNotFoundError):
		return nil
	case err != nil:
		return nil
	case i == nil:
		return nil
	default:
		return i.(base.ProposalSignedFact)
	}
}

func (p *ProposalPool) pointkey(point base.Point, proposer base.Address) string {
	return fmt.Sprintf("%d-%d-%s",
		point.Height(),
		point.Round(),
		proposer.String(),
	)
}
