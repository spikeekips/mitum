package isaacstates

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	SyncerBlockMapFunc     func(context.Context, base.Height) (base.BlockMap, bool, error)
	SyncerBlockMapItemFunc func(context.Context, base.Height, base.BlockMapItemType) (util.ChecksumReader, bool, error)
)

type Syncer struct {
	tempsyncpool isaac.TempSyncPool
	*logging.Logging
	lastvalue     *util.Locked
	blockMapf     SyncerBlockMapFunc
	blockMapItemf SyncerBlockMapItemFunc
	*util.ContextDaemon
	startsyncch chan base.Height
	finishedch  chan base.Height
	donech      chan struct{} // revive:disable-line:nested-structs
	doneerr     *util.Locked
	topvalue    *util.Locked
	isdonevalue *atomic.Value
	batchlimit  int64
	cancelonece sync.Once
}

func NewSyncer(
	last base.BlockMap,
	blockMapf SyncerBlockMapFunc,
	blockMapItemf SyncerBlockMapItemFunc,
	tempsyncpool isaac.TempSyncPool,
) *Syncer {
	lastheight := base.NilHeight
	if last != nil {
		lastheight = last.Manifest().Height()
	}

	s := &Syncer{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "syncer")
		}),
		lastvalue:     util.NewLocked(last),
		blockMapf:     blockMapf,
		blockMapItemf: blockMapItemf,
		tempsyncpool:  tempsyncpool,
		batchlimit:    333, //nolint:gomnd // big enough size
		finishedch:    make(chan base.Height),
		donech:        make(chan struct{}),
		doneerr:       util.EmptyLocked(),
		topvalue:      util.NewLocked(lastheight),
		isdonevalue:   &atomic.Value{},
		startsyncch:   make(chan base.Height),
	}

	s.ContextDaemon = util.NewContextDaemon("syncer", s.start)

	return s
}

func (s *Syncer) Add(height base.Height) bool {
	if s.isdonevalue.Load() != nil {
		return false
	}

	var startsync bool

	if _, err := s.topvalue.Set(func(i interface{}) (interface{}, error) {
		top := i.(base.Height) //nolint:forcetypeassert //...
		synced := s.lastheight()

		switch {
		case height <= top:
			return nil, errors.Errorf("old height")
		case top == synced:
			startsync = true
		}

		return height, nil
	}); err != nil {
		return false
	}

	if startsync {
		go func() {
			s.startsyncch <- height
		}()
	}

	return true
}

func (s *Syncer) Finished() <-chan base.Height {
	return s.finishedch
}

func (s *Syncer) Done() <-chan struct{} {
	return s.donech
}

func (s *Syncer) Err() error {
	i, _ := s.doneerr.Value()
	if i == nil {
		return nil
	}

	return i.(error) //nolint:forcetypeassert //...
}

func (s *Syncer) IsFinished() (base.Height, bool) {
	top := s.top()

	return top, top == s.lastheight()
}

func (s *Syncer) Cancel() error {
	var err error
	s.cancelonece.Do(func() {
		defer func() {
			_ = s.tempsyncpool.Cancel()
		}()

		_, _ = s.doneerr.Set(func(interface{}) (interface{}, error) {
			s.isdonevalue.Store(true)

			close(s.donech)

			err = s.ContextDaemon.Stop()

			return nil, nil
		})
	})

	return err
}

func (s *Syncer) start(ctx context.Context) error {
	defer func() {
		_ = s.tempsyncpool.Cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case height := <-s.startsyncch:
			last := s.last()
			if last != nil && height <= last.Manifest().Height() {
				continue
			}

			go s.sync(ctx, last, height)
		}
	}
}

func (s *Syncer) top() base.Height {
	i, _ := s.topvalue.Value()

	return i.(base.Height) //nolint:forcetypeassert //...
}

func (s *Syncer) last() base.BlockMap {
	i, _ := s.lastvalue.Value()
	if i == nil {
		return nil
	}

	return i.(base.BlockMap) //nolint:forcetypeassert //...
}

func (s *Syncer) lastheight() base.Height {
	m := s.last()
	if m == nil {
		return base.NilHeight
	}

	return m.Manifest().Height()
}

func (s *Syncer) sync(ctx context.Context, last base.BlockMap, to base.Height) { // revive:disable-line:import-shadowing
	var err error
	_, _ = s.doneerr.Set(func(i interface{}) (interface{}, error) {
		var newlast base.BlockMap

		newlast, err = s.doSync(ctx, last, to)
		if err != nil {
			return err, nil
		}

		_ = s.lastvalue.SetValue(newlast)

		switch top := s.top(); {
		case newlast.Manifest().Height() == top:
			go func() {
				s.finishedch <- newlast.Manifest().Height()
			}()
		case newlast.Manifest().Height() < top:
			go func() {
				s.startsyncch <- top
			}()
		}

		return nil, nil
	})

	if err != nil {
		s.donech <- struct{}{}
	}
}

func (s *Syncer) doSync(ctx context.Context, last base.BlockMap, to base.Height) (base.BlockMap, error) {
	e := util.StringErrorFunc("failed to sync")

	previous := last

	for {
		switch newlast, err := s.fetchMaps(ctx, previous, to); { // BLOCK use retry
		case err != nil:
			return nil, e(err, "")
		case newlast.Manifest().Height() < to:
			previous = newlast
		default:
			return newlast, nil
		}
	}
}

func (s *Syncer) fetchMaps(ctx context.Context, last base.BlockMap, to base.Height) (base.BlockMap, error) {
	lastheight := base.NilHeight
	if last != nil {
		lastheight = last.Manifest().Height()
	}

	if to <= lastheight {
		return nil, nil
	}

	size := (to - lastheight).Int64()
	if size > s.batchlimit {
		size = s.batchlimit
	}

	worker := util.NewErrgroupWorker(ctx, math.MaxInt32)
	defer worker.Close()

	e := util.StringErrorFunc("failed to fetch BlockMaps")

	var validateLock sync.Mutex
	var lastMap base.BlockMap
	maps := make([]base.BlockMap, size)

	for i := int64(0); i < size; i++ {
		i := i
		height := base.Height(i+1) + lastheight

		if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
			m, err := s.fetchMap(ctx, height)
			if err != nil {
				return err
			}

			if err = func() error {
				validateLock.Lock()
				defer validateLock.Unlock()

				if err = s.validateMaps(m, maps, last); err != nil {
					return err
				}

				if (m.Manifest().Height() - lastheight).Int64() == size {
					lastMap = m
				}

				return nil
			}(); err != nil {
				return err
			}

			if err := s.tempsyncpool.SetMap(m); err != nil {
				return errors.Wrap(err, "")
			}

			return nil
		}); err != nil {
			return nil, e(err, "")
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return nil, e(err, "")
	}

	return lastMap, nil
}

func (s *Syncer) fetchMap(ctx context.Context, height base.Height) (base.BlockMap, error) {
	e := util.StringErrorFunc("failed to fetch BlockMap")

	switch m, found, err := s.blockMapf(ctx, height); {
	case err != nil:
		return nil, e(err, "")
	case !found:
		return nil, e(nil, "not found")
	default:
		// BLOCK blockMap should be passed IsValid() in s.blockMap.
		return m, nil
	}
}

func (*Syncer) validateMaps(m base.BlockMap, maps []base.BlockMap, previous base.BlockMap) error {
	last := base.NilHeight
	if previous != nil {
		last = previous.Manifest().Height()
	}

	index := (m.Manifest().Height() - last - 1).Int64()

	e := util.StringErrorFunc("failed to validate BlockMaps")

	if index < 0 || index >= int64(len(maps)) {
		return e(nil, "invalid BlockMaps found; wrong index")
	}

	maps[index] = m

	switch {
	case index == 0 && m.Manifest().Height() == base.GenesisHeight:
	case index == 0 && m.Manifest().Height() != base.GenesisHeight:
		if err := base.ValidateManifests(m.Manifest(), previous.Manifest().Hash()); err != nil {
			return e(err, "")
		}
	case maps[index-1] != nil:
		if err := base.ValidateManifests(m.Manifest(), maps[index-1].Manifest().Hash()); err != nil {
			return e(err, "")
		}
	}

	// revive:disable-next-line:optimize-operands-order
	if index+1 < int64(len(maps)) && maps[index+1] != nil {
		if err := base.ValidateManifests(maps[index+1].Manifest(), m.Manifest().Hash()); err != nil {
			return e(err, "")
		}
	}

	return nil
}
