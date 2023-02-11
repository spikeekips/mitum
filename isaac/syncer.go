package isaac

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/bluele/gcache"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
)

var (
	ErrEmptySyncSources = util.NewError("empty sync sources; will retry")
	ErrRetrySyncSources = util.NewError("sync sources problem; will retry")
)

type Syncer interface {
	util.Daemon
	Add(base.Height) bool
	Finished() <-chan base.Height
	Done() <-chan struct{} // revive:disable-line:nested-structs
	Err() error
	IsFinished() (base.Height, bool)
	Cancel() error
}

type SyncSourcePool struct {
	problems gcache.Cache
	nonfixed map[string]NodeConnInfo
	fixed    []NodeConnInfo
	fixedids []string
	sync.RWMutex
	renewTimeout time.Duration
}

func NewSyncSourcePool(fixed []NodeConnInfo) *SyncSourcePool {
	p := &SyncSourcePool{
		nonfixed:     map[string]NodeConnInfo{},
		problems:     gcache.New(1 << 14).LRU().Build(), //nolint:gomnd // big enough for suffrage size
		renewTimeout: time.Second * 3,                   //nolint:gomnd //...
	}

	_ = p.UpdateFixed(fixed)

	return p
}

func (p *SyncSourcePool) Pick() (NodeConnInfo, func(error), error) {
	p.Lock()
	defer p.Unlock()

	_, nci, report, err := p.pick("")

	return nci, report, err
}

func (p *SyncSourcePool) PickMultiple(n int) ([]NodeConnInfo, []func(error), error) {
	p.Lock()
	defer p.Unlock()

	if n < 1 {
		return nil, nil, errors.Errorf("zero")
	}

	ncis := make([]NodeConnInfo, n)
	reports := make([]func(error), n)

	var last string

	switch i, nci, report, err := p.pick(""); {
	case err != nil:
		return nil, nil, err
	default:
		last = i
		ncis[0] = nci
		reports[0] = report
	}

	if n == 1 {
		return ncis, reports, nil
	}

	i := 1

	for {
		id, nci, report, err := p.pick(last)
		if err != nil || nci == nil {
			break
		}

		ncis[i] = nci
		reports[i] = report

		i++

		if i == n {
			break
		}

		last = id
	}

	if len(ncis[:i]) < 1 {
		return nil, nil, ErrEmptySyncSources.Call()
	}

	return ncis[:i], reports[:i], nil
}

func (p *SyncSourcePool) IsInFixed(node base.Address) bool {
	p.RLock()
	defer p.RUnlock()

	return p.nodeIsInFixed(node) >= 0
}

func (p *SyncSourcePool) IsInNonFixed(node base.Address) bool {
	p.RLock()
	defer p.RUnlock()

	if p.nodeIsInFixed(node) >= 0 {
		return false
	}

	for id := range p.nonfixed {
		if p.nonfixed[id].Address().Equal(node) {
			return true
		}
	}

	return false
}

func (p *SyncSourcePool) NodeExists(node base.Address) bool {
	p.RLock()
	defer p.RUnlock()

	for i := range p.fixed {
		if p.fixed[i].Address().Equal(node) {
			return true
		}
	}

	for i := range p.nonfixed {
		if p.nonfixed[i].Address().Equal(node) {
			return true
		}
	}

	return false
}

func (p *SyncSourcePool) UpdateFixed(fixed []NodeConnInfo) bool {
	p.Lock()
	defer p.Unlock()

	if len(p.fixed) == len(fixed) {
		var notsame bool

		for i := range fixed {
			if p.fixedids[i] != p.makeid(fixed[i]) {
				notsame = true

				break
			}
		}

		if !notsame {
			return false
		}
	}

	p.fixed = fixed
	p.fixedids = make([]string, len(fixed))

	for i := range fixed {
		p.fixedids[i] = p.makeid(fixed[i])
	}

	for id := range p.nonfixed {
		if util.InSlice(p.fixedids, id) >= 0 {
			delete(p.nonfixed, id)
		}
	}

	return true
}

func (p *SyncSourcePool) AddNonFixed(ncis ...NodeConnInfo) bool {
	p.Lock()
	defer p.Unlock()

	var isnew bool

	for i := range ncis {
		nci := ncis[i]
		id := p.makeid(nci)

		if util.InSlice(p.fixedids, id) >= 0 {
			continue
		}

		if _, found := p.nonfixed[id]; !found {
			isnew = true
		}

		p.nonfixed[id] = nci
	}

	return isnew
}

func (p *SyncSourcePool) RemoveNonFixed(nci NodeConnInfo) bool {
	p.Lock()
	defer p.Unlock()

	id := p.makeid(nci)

	if _, found := p.nonfixed[id]; found {
		delete(p.nonfixed, id)

		return true
	}

	return false
}

func (p *SyncSourcePool) RemoveNonFixedNode(nodes ...base.Address) bool {
	p.Lock()
	defer p.Unlock()

	var found bool

	for i := range nodes {
		for id := range p.nonfixed {
			if p.nonfixed[id].Address().Equal(nodes[i]) {
				found = true

				delete(p.nonfixed, id)
			}
		}
	}

	return found
}

func (p *SyncSourcePool) Retry(
	ctx context.Context,
	f func(NodeConnInfo) (bool, error),
	limit int,
	interval time.Duration,
) error {
	return util.Retry(
		ctx,
		func() (bool, error) {
			nci, report, err := p.Pick()
			if errors.Is(err, ErrEmptySyncSources) {
				return true, nil
			}

			if _, err = nci.UDPConnInfo(); err != nil {
				report(err)

				return true, err
			}

			keep, err := f(nci)

			if isSyncSourceProblem(err) {
				report(err)

				return true, nil
			}

			return keep, err
		},
		limit,
		interval,
	)
}

func (p *SyncSourcePool) Len() int {
	p.RLock()
	defer p.RUnlock()

	return len(p.fixed) + len(p.nonfixed)
}

func (p *SyncSourcePool) Traverse(f func(NodeConnInfo) bool) {
	p.RLock()
	defer p.RUnlock()

	for i := range p.fixedids {
		if !f(p.fixed[i]) {
			return
		}
	}

	for id := range p.nonfixed {
		if !f(p.nonfixed[id]) {
			return
		}
	}
}

func (p *SyncSourcePool) Actives(f func(NodeConnInfo) bool) {
	p.RLock()
	defer p.RUnlock()

	for i := range p.fixedids {
		if p.problems.Has(p.fixedids[i]) {
			continue
		}

		if !f(p.fixed[i]) {
			return
		}
	}

	for id := range p.nonfixed {
		if p.problems.Has(id) {
			continue
		}

		if !f(p.nonfixed[id]) {
			return
		}
	}
}

func (*SyncSourcePool) makeid(nci NodeConnInfo) string {
	return nci.Address().String() + "-" + nci.String()
}

func (p *SyncSourcePool) pick(skipid string) (_ string, _ NodeConnInfo, report func(error), _ error) {
	foundid := len(skipid) < 1

	for i := range p.fixedids {
		id := p.fixedids[i]

		switch {
		case skipid == id && (!foundid && len(skipid) > 0):
			foundid = true

			continue
		case !foundid:
			continue
		case p.problems.Has(id):
			continue
		default:
			return id, p.fixed[i], func(err error) { p.reportProblem(id, err) }, nil
		}
	}

	foundid = false

	for id := range p.nonfixed {
		switch {
		case skipid == id && (!foundid && len(skipid) > 0):
			foundid = true

			continue
		case !foundid:
			continue
		case p.problems.Has(id):
			continue
		default:
			return id, p.nonfixed[id], func(err error) { p.reportProblem(id, err) }, nil
		}
	}

	return "", nil, nil, ErrEmptySyncSources.Call()
}

func (p *SyncSourcePool) reportProblem(id string, err error) {
	if !isSyncSourceProblem(err) {
		return
	}

	p.Lock()
	defer p.Unlock()

	if util.InSlice(p.fixedids, id) < 0 {
		if _, found := p.nonfixed[id]; !found {
			return
		}
	}

	_ = p.problems.SetWithExpire(id, struct{}{}, p.renewTimeout)
}

func (p *SyncSourcePool) NodeConnInfo(node base.Address) []NodeConnInfo {
	var founds []NodeConnInfo

	for i := range p.fixed {
		nci := p.fixed[i]

		if nci.Address().Equal(node) {
			founds = append(founds, nci)
		}
	}

	for i := range p.nonfixed {
		nci := p.nonfixed[i]

		if nci.Address().Equal(node) {
			founds = append(founds, nci)
		}
	}

	return founds
}

func (p *SyncSourcePool) nodeIsInFixed(node base.Address) int {
	return util.InSliceFunc(p.fixed, func(i NodeConnInfo) bool {
		return i.Address().Equal(node)
	})
}

func isSyncSourceProblem(err error) bool {
	var dnserr *net.DNSError

	switch {
	case err == nil:
		return false
	case errors.Is(err, ErrRetrySyncSources),
		quicstream.IsNetworkError(err),
		errors.As(err, &dnserr):
		return true
	default:
		return false
	}
}

func DistributeWorkerWithSyncSourcePool(
	ctx context.Context,
	pool *SyncSourcePool,
	picksize int,
	semsize uint64,
	errch chan error,
	f func(ctx context.Context, i, jobid uint64, nci NodeConnInfo) error,
) error {
	ncis, reports, err := pool.PickMultiple(picksize)

	switch {
	case errors.Is(err, ErrEmptySyncSources):
		return nil
	case err != nil:
		return err
	case len(ncis) < 1:
		return nil
	}

	if n := uint64(len(ncis)); n < semsize {
		semsize = n //revive:disable-line:modifies-parameter
	}

	return util.RunDistributeWorker(ctx, semsize, errch, func(ctx context.Context, i, jobid uint64) error {
		index := i % uint64(len(ncis))
		nci := ncis[index]

		if _, err := nci.UDPConnInfo(); err != nil {
			reports[index](err)

			return err
		}

		err := f(ctx, i, jobid, nci)
		if err != nil {
			reports[index](err)
		}

		return err
	})
}

func ErrGroupWorkerWithSyncSourcePool(
	ctx context.Context,
	pool *SyncSourcePool,
	picksize int,
	semsize uint64,
	f func(ctx context.Context, i, jobid uint64, nci NodeConnInfo) error,
) error {
	ncis, reports, err := pool.PickMultiple(picksize)

	switch {
	case errors.Is(err, ErrEmptySyncSources):
		return nil
	case err != nil:
		return err
	case len(ncis) < 1:
		return nil
	}

	if n := uint64(len(ncis)); n < semsize {
		semsize = n //revive:disable-line:modifies-parameter
	}

	return util.RunErrgroupWorker(ctx, semsize, func(ctx context.Context, i, jobid uint64) error {
		index := i % uint64(len(ncis))
		nci := ncis[index]

		if _, err := nci.UDPConnInfo(); err != nil {
			reports[index](err)

			return nil
		}

		err := f(ctx, i, jobid, nci)
		if err != nil {
			reports[index](err)

			if isSyncSourceProblem(err) {
				err = nil
			}
		}

		return err
	})
}
