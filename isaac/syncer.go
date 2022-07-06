package isaac

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"golang.org/x/crypto/sha3"
)

var (
	ErrEmptySyncSources = util.NewError("empty sync sources; will retry")
	ErrRetrySyncSources = util.NewError("sync sources problem; will retry")
)

type Syncer interface {
	Add(base.Height) bool
	Finished() <-chan base.Height
	Done() <-chan struct{} // revive:disable-line:nested-structs
	Err() error
	IsFinished() (base.Height, bool)
	Cancel() error
}

type SyncSourcePool struct {
	sources   []NodeConnInfo
	sourceids []string
	problems  []*time.Time
	sync.RWMutex
	fixedlen     int
	renewTimeout time.Duration
}

func NewSyncSourcePool(fixed []NodeConnInfo) *SyncSourcePool {
	p := &SyncSourcePool{
		sources:      fixed,
		renewTimeout: time.Second * 3, //nolint:gomnd //...
	}

	p.sourceids = p.makeid(fixed)
	p.problems = make([]*time.Time, len(fixed))
	p.fixedlen = len(fixed)

	return p
}

func (p *SyncSourcePool) Pick() (NodeConnInfo, func(error), error) {
	p.Lock()
	defer p.Unlock()

	_, nci, report, err := p.pick(0)

	return nci, report, err
}

func (p *SyncSourcePool) PickMultiple(n int) ([]NodeConnInfo, []func(error), error) {
	switch {
	case n < 1:
		return nil, nil, errors.Errorf("zero")
	case n == 1:
		nci, report, err := p.Pick()
		if err != nil {
			return nil, nil, err
		}

		return []NodeConnInfo{nci}, []func(error){report}, nil
	}

	p.Lock()
	defer p.Unlock()

	sources := make([]NodeConnInfo, n)
	reports := make([]func(error), n)

	var index int

	var i int

	for {
		j, source, report, err := p.pick(index)
		if err != nil {
			break
		}

		sources[i] = source
		reports[i] = report

		i++

		if i == n {
			break
		}

		index = j + 1
	}

	if len(sources[:i]) < 1 {
		return nil, nil, ErrEmptySyncSources.Call()
	}

	return sources[:i], reports[:i], nil
}

func (p *SyncSourcePool) UpdateFixed(fixed []NodeConnInfo) bool {
	p.Lock()
	defer p.Unlock()

	existingids := p.sourceids[:p.fixedlen]
	fixedids := p.makeid(fixed)

	if len(existingids) == len(fixedids) {
		var found bool

		for i := range fixedids {
			if fixedids[i] != existingids[i] {
				found = true

				break
			}
		}

		if !found {
			return false
		}
	}

	extras := p.sources[p.fixedlen:]
	extraids := p.sourceids[p.fixedlen:]

	sources := make([]NodeConnInfo, len(fixed)+len(extras))
	copy(sources, fixed)
	copy(sources[len(fixed):], extras)

	ids := make([]string, len(fixedids)+len(extraids))
	copy(ids, fixedids)
	copy(ids[len(fixedids):], extraids)

	problems := make([]*time.Time, len(fixed)+len(extras))
	copy(problems[len(fixed):], p.problems[p.fixedlen:])

	p.sources = sources
	p.sourceids = ids
	p.problems = problems
	p.fixedlen = len(fixed)

	return true
}

func (p *SyncSourcePool) Add(added ...NodeConnInfo) bool {
	sources := make([]NodeConnInfo, len(p.sources))
	copy(sources, p.sources)

	sourceids := make([]string, len(p.sourceids))
	copy(sourceids, p.sourceids)

	problems := make([]*time.Time, len(p.problems))
	copy(problems, p.problems)

	var updatedcount int

	for i := range added {
		var updated bool

		sources, sourceids, problems, updated = p.add(sources, sourceids, problems, added[i])

		if updated {
			updatedcount++
		}
	}

	if updatedcount > 0 {
		p.sources = sources
		p.sourceids = sourceids
		p.problems = problems
	}

	return updatedcount > 0
}

func (p *SyncSourcePool) Remove(node base.Address, publish string) bool {
	index := util.InSlice(p.sources, func(_ interface{}, i int) bool {
		switch {
		case p.sources[i].Address().Equal(node) &&
			p.sources[i].String() == publish:
			return true
		default:
			return false
		}
	})

	if index < 0 {
		return false
	}

	sources := make([]NodeConnInfo, len(p.sources)-1)
	copy(sources, p.sources[:index])
	copy(sources[index:], p.sources[index+1:])

	sourceids := make([]string, len(p.sourceids)-1)
	copy(sourceids, p.sourceids[:index])
	copy(sourceids[index:], p.sourceids[index+1:])

	problems := make([]*time.Time, len(p.problems)-1)
	copy(problems, p.problems[:index])
	copy(problems[index:], p.problems[index+1:])

	p.sources = sources
	p.sourceids = sourceids
	p.problems = problems

	if index < p.fixedlen {
		p.fixedlen--
	}

	return true
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

			if isProblem(err) {
				report(nil)

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

	return len(p.sources)
}

func (p *SyncSourcePool) Actives(f func(NodeConnInfo) bool) {
	p.RLock()
	defer p.RUnlock()

	if len(p.sources) < 1 {
		return
	}

	for i := range p.problems {
		d := p.problems[i]

		if d == nil || time.Since(*d) > p.renewTimeout {
			if !f(p.sources[i]) {
				return
			}
		}
	}
}

func (p *SyncSourcePool) makeid(sources []NodeConnInfo) []string {
	if len(sources) < 1 {
		return nil
	}

	ids := make([]string, len(sources))

	for i := range sources {
		id := p.makesourceid(sources[i])
		ids[i] = id
	}

	return ids
}

func (*SyncSourcePool) makesourceid(source NodeConnInfo) string {
	h := sha3.New256()
	_, _ = h.Write(source.Address().Bytes())
	_, _ = h.Write([]byte(source.String()))

	var v valuehash.L32

	h.Sum(v[:0])

	return v.String()
}

func (p *SyncSourcePool) reportProblem(id string, err error) {
	if err != nil && !isProblem(err) {
		return
	}

	p.Lock()
	defer p.Unlock()

	for i := range p.sourceids {
		if id == p.sourceids[i] {
			now := time.Now()

			p.problems[i] = &now

			return
		}
	}
}

func (p *SyncSourcePool) add(
	sources []NodeConnInfo,
	sourceids []string,
	problems []*time.Time,
	source NodeConnInfo,
) ([]NodeConnInfo, []string, []*time.Time, bool) {
	var update bool

	index := util.InSlice(sources, func(_ interface{}, i int) bool {
		a := sources[i]

		if !a.Address().Equal(source.Address()) {
			return false
		}

		if a.String() != source.String() {
			update = true
		}

		return true
	})

	switch {
	case index < 0:
		newsources := make([]NodeConnInfo, len(sources)+1)
		copy(newsources, sources)
		newsources[len(sources)] = source

		newsourceids := make([]string, len(sourceids)+1)
		copy(newsourceids, sourceids)
		newsourceids[len(sources)] = p.makesourceid(source)

		newproblems := make([]*time.Time, len(problems)+1)
		copy(newproblems, problems)

		return newsources, newsourceids, newproblems, true
	case update:
		sources[index] = source
		sourceids[index] = p.makesourceid(source)
		problems[index] = nil

		return sources, sourceids, problems, true
	default:
		return sources, sourceids, problems, false
	}
}

func (p *SyncSourcePool) pick(from int) (found int, _ NodeConnInfo, report func(error), _ error) {
	switch {
	case len(p.sources) < 1:
		return -1, nil, nil, ErrEmptySyncSources.Call()
	case from >= len(p.sources):
		return -1, nil, nil, ErrEmptySyncSources.Call()
	}

	for i := range p.problems[from:] {
		index := from + i
		d := p.problems[index]

		switch {
		case d == nil:
			return index, p.sources[index], func(err error) { p.reportProblem(p.sourceids[index], err) }, nil
		case time.Since(*d) > p.renewTimeout:
			p.problems[index] = nil

			return index, p.sources[index], func(err error) { p.reportProblem(p.sourceids[index], err) }, nil
		}
	}

	return -1, nil, nil, ErrEmptySyncSources.Call()
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
	if err != nil {
		return err
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
	if err != nil {
		return err
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

			if isProblem(err) {
				err = nil
			}
		}

		return err
	})
}

func isProblem(err error) bool {
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
