package isaac

import (
	"context"
	"maps"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/exp/slices"
)

var (
	ErrEmptySyncSources = util.NewIDError("empty sync sources; will retry")
	ErrRetrySyncSources = util.NewIDError("sync sources problem; will retry")
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
	problems     util.GCache[string, any]
	nonfixed     map[string]NodeConnInfo
	fixed        []NodeConnInfo
	fixedids     []string
	l            sync.RWMutex
	renewTimeout time.Duration
}

func NewSyncSourcePool(fixed []NodeConnInfo) *SyncSourcePool {
	p := &SyncSourcePool{
		nonfixed:     map[string]NodeConnInfo{},
		problems:     util.NewLRUGCache[string, any](1 << 14), //nolint:mnd // big enough for suffrage size
		renewTimeout: time.Second * 3,                         //nolint:mnd //...
	}

	_ = p.UpdateFixed(fixed)

	return p
}

func (p *SyncSourcePool) Pick() (NodeConnInfo, func(error), error) {
	p.l.Lock()
	defer p.l.Unlock()

	_, nci, report, err := p.pick("")

	return nci, report, err
}

func (p *SyncSourcePool) PickMultiple(n int) ([]NodeConnInfo, []func(error), error) {
	p.l.Lock()
	defer p.l.Unlock()

	if n < 1 {
		return nil, nil, errors.Errorf("zero")
	}

	addNcis, doneNcis := util.CompactAppendSlice[NodeConnInfo](n)
	addReports, doneReports := util.CompactAppendSlice[func(error)](n)

	var last string

	switch i, nci, report, err := p.pick(""); {
	case err != nil:
		return nil, nil, err
	case n == 1:
		return []NodeConnInfo{nci}, []func(error){report}, nil
	default:
		last = i

		_ = addNcis(nci)
		_ = addReports(report)
	}

	for {
		id, nci, report, err := p.pick(last)
		if err != nil || nci == nil {
			break
		}

		_ = addNcis(nci)

		if addReports(report) {
			break
		}

		last = id
	}

	ncis := doneNcis()

	if len(ncis) < 1 {
		return nil, nil, ErrEmptySyncSources.WithStack()
	}

	return ncis, doneReports(), nil
}

func (p *SyncSourcePool) IsInFixed(node base.Address) bool {
	p.l.RLock()
	defer p.l.RUnlock()

	return p.nodeIsInFixed(node) >= 0
}

func (p *SyncSourcePool) IsInNonFixed(node base.Address) bool {
	p.l.RLock()
	defer p.l.RUnlock()

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
	p.l.RLock()
	defer p.l.RUnlock()

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
	p.l.Lock()
	defer p.l.Unlock()

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

	maps.DeleteFunc(p.nonfixed, func(id string, _ NodeConnInfo) bool {
		return slices.Index(p.fixedids, id) >= 0
	})

	return true
}

func (p *SyncSourcePool) AddNonFixed(ncis ...NodeConnInfo) bool {
	p.l.Lock()
	defer p.l.Unlock()

	var isnew bool

	for i := range ncis {
		nci := ncis[i]
		id := p.makeid(nci)

		if slices.Index(p.fixedids, id) >= 0 {
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
	p.l.Lock()
	defer p.l.Unlock()

	id := p.makeid(nci)

	if _, found := p.nonfixed[id]; found {
		delete(p.nonfixed, id)

		return true
	}

	return false
}

func (p *SyncSourcePool) RemoveNonFixedNode(nodes ...base.Address) bool {
	p.l.Lock()
	defer p.l.Unlock()

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

			switch keep, err := f(nci); {
			case isSyncSourceProblem(err):
				report(err)

				return true, nil
			default:
				return keep, err
			}
		},
		limit,
		interval,
	)
}

func (p *SyncSourcePool) Len() int {
	p.l.RLock()
	defer p.l.RUnlock()

	return len(p.fixed) + len(p.nonfixed)
}

func (p *SyncSourcePool) Traverse(f func(NodeConnInfo) bool) {
	p.l.RLock()
	defer p.l.RUnlock()

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
	p.l.RLock()
	defer p.l.RUnlock()

	for i := range p.fixedids {
		if p.problems.Exists(p.fixedids[i]) {
			continue
		}

		if !f(p.fixed[i]) {
			return
		}
	}

	for id := range p.nonfixed {
		if p.problems.Exists(id) {
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
		case !foundid && skipid == id:
			foundid = true

			continue
		case !foundid:
			continue
		case p.problems.Exists(id):
			continue
		default:
			return id, p.fixed[i], func(err error) { p.reportProblem(id, err) }, nil
		}
	}

	for id := range p.nonfixed {
		switch {
		case skipid == id, p.problems.Exists(id):
			continue
		default:
			return id, p.nonfixed[id], func(err error) { p.reportProblem(id, err) }, nil
		}
	}

	return "", nil, nil, ErrEmptySyncSources.WithStack()
}

func (p *SyncSourcePool) reportProblem(id string, err error) {
	if !isSyncSourceProblem(err) {
		return
	}

	p.l.Lock()
	defer p.l.Unlock()

	if slices.Index(p.fixedids, id) < 0 {
		if _, found := p.nonfixed[id]; !found {
			return
		}
	}

	p.problems.Set(id, nil, p.renewTimeout)
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
	return slices.IndexFunc(p.fixed, func(i NodeConnInfo) bool {
		return i.Address().Equal(node)
	})
}

func isSyncSourceProblem(err error) bool {
	var dnserr *net.DNSError

	switch {
	case err == nil:
		return false
	case errors.Is(err, ErrRetrySyncSources),
		quicstream.IsSeriousError(err),
		errors.As(err, &dnserr):
		return true
	default:
		return false
	}
}

func ErrCallbackWorkerWithSyncSourcePool(
	ctx context.Context,
	pool *SyncSourcePool,
	picksize int,
	semsize int64,
	errf func(error),
	f func(ctx context.Context, i, jobid uint64, nci NodeConnInfo) error,
) error {
	return workerWithSyncSourcePool(ctx, pool, picksize, semsize, f,
		func(ctx context.Context, nsemsize, n int64, f func(context.Context, uint64, uint64) error) error {
			return util.RunErrCallbackJobWorker(ctx, nsemsize, n, errf, f)
		},
	)
}

func JobWorkerWithSyncSourcePool(
	ctx context.Context,
	pool *SyncSourcePool,
	picksize int,
	semsize int64,
	f func(ctx context.Context, i, jobid uint64, nci NodeConnInfo) error,
) error {
	return workerWithSyncSourcePool(ctx, pool, picksize, semsize, f, util.RunJobWorker)
}

func workerWithSyncSourcePool(
	ctx context.Context,
	pool *SyncSourcePool,
	picksize int,
	semsize int64,
	f func(ctx context.Context, i, jobid uint64, nci NodeConnInfo) error,
	workerf func(context.Context, int64, int64, func(context.Context, uint64, uint64) error) error,
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

	n := int64(len(ncis))
	nsemsize := semsize

	if n < nsemsize {
		nsemsize = n
	}

	return workerf(ctx, nsemsize, n, func(ctx context.Context, i, jobid uint64) error {
		index := i % uint64(len(ncis))
		nci := ncis[index]

		if err := f(ctx, i, jobid, nci); err != nil {
			reports[index](err)

			return nil
		}

		return errors.Errorf("stop")
	})
}
