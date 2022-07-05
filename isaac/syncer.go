package isaac

import (
	"context"
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
	current   NodeConnInfo
	currentid string
	sources   []NodeConnInfo
	sourceids []string
	index     int
	sync.RWMutex
	fixedlen int
}

func NewSyncSourcePool(fixed []NodeConnInfo) *SyncSourcePool {
	p := &SyncSourcePool{
		sources: fixed,
	}

	p.sourceids = p.makeid(fixed)
	p.fixedlen = len(fixed)

	return p
}

func (p *SyncSourcePool) Retry(
	ctx context.Context,
	f func(NodeConnInfo) (bool, error),
	limit int,
	interval time.Duration,
) error {
	var lastid string

	return util.Retry(
		ctx,
		func() (bool, error) {
			nci, id, err := p.Next(lastid)
			if errors.Is(err, ErrEmptySyncSources) {
				return true, nil
			}
			lastid = id

			if _, err = nci.UDPConnInfo(); err != nil {
				return true, err
			}

			keep, err := f(nci)

			switch {
			case err == nil:
			case errors.Is(err, ErrRetrySyncSources),
				quicstream.IsNetworkError(err):
				return true, nil
			}

			return keep, err
		},
		limit,
		interval,
	)
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

	p.sources = sources
	p.sourceids = ids
	p.fixedlen = len(fixed)

	p.index = 0
	p.current = nil
	p.currentid = ""

	return true
}

func (p *SyncSourcePool) Add(added ...NodeConnInfo) bool {
	sources := make([]NodeConnInfo, len(p.sources))
	copy(sources, p.sources)

	sourceids := make([]string, len(p.sourceids))
	copy(sourceids, p.sourceids)

	var updatedcount int

	for i := range added {
		var updated bool
		var index int

		sources, sourceids, index, updated = p.add(sources, sourceids, added[i])

		if updated {
			updatedcount++

			if index == p.index-1 {
				p.index -= 2
				if p.index < 0 {
					p.index = 0
				}

				p.currentid = ""
				p.current = nil
			}
		}
	}

	if updatedcount > 0 {
		p.sources = sources
		p.sourceids = sourceids
	}

	return updatedcount > 0
}

func (p *SyncSourcePool) add(
	sources []NodeConnInfo,
	sourceids []string,
	source NodeConnInfo,
) ([]NodeConnInfo, []string, int, bool) {
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

		return newsources, newsourceids, len(sources), true
	case update:
		sources[index] = source
		sourceids[index] = p.makesourceid(source)

		return sources, sourceids, index, true
	default:
		return sources, sourceids, -1, false
	}
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

	sources := make([]NodeConnInfo, len(p.sources)-1)
	copy(sources, p.sources[:index])
	copy(sources[index:], p.sources[index+1:])

	sourceids := make([]string, len(p.sourceids)-1)
	copy(sourceids, p.sourceids[:index])
	copy(sourceids[index:], p.sourceids[index+1:])

	p.sources = sources
	p.sourceids = sourceids

	if index < p.fixedlen {
		p.fixedlen--
	}

	if index == p.index-1 {
		p.index -= 2
		if p.index < 0 {
			p.index = 0
		}

		p.currentid = ""
		p.current = nil
	}

	return true
}

func (p *SyncSourcePool) Next(previd string) (NodeConnInfo, string, error) {
	p.Lock()
	defer p.Unlock()

	if len(p.sources) < 1 {
		return nil, "", ErrEmptySyncSources.Call()
	}

	// revive:disable-next-line:optimize-operands-order
	if p.current != nil && (len(previd) < 1 || previd != p.currentid) {
		return p.current, p.currentid, nil
	}

	index := p.nextIndex()

	p.current = p.sources[index]
	p.currentid = p.sourceids[index]

	return p.current, p.currentid, nil
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

func (p *SyncSourcePool) nextIndex() int {
	switch {
	case p.index == len(p.sources):
		p.index = 1
	default:
		p.index++
	}

	return p.index - 1
}
