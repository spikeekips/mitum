package isaacstates

import (
	"context"
	"math"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	SyncerBlockMapFunc     func(context.Context, base.Height) (base.BlockMap, bool, error)
	SyncerBlockMapItemFunc func(context.Context, base.Height, base.BlockMapItemType) (util.ChecksumReader, bool, error)
)

type Syncer struct {
	*logging.Logging
	*util.ContextDaemon
	last               base.BlockMap
	blockMapf          SyncerBlockMapFunc
	blockMapItemf      SyncerBlockMapItemFunc
	topLocked          *util.Locked
	addch              chan base.Height
	syncedch           chan base.Height
	finishedch         chan base.Height
	syncedheightLocked *util.Locked
	donech             chan struct{} // revive:disable-line:nested-structs
	err                *util.Locked
	batchlimit         int64
}

func NewSyncer(
	last base.BlockMap,
	blockMapf SyncerBlockMapFunc,
	blockMapItemf SyncerBlockMapItemFunc,
) *Syncer {
	topLocked := util.EmptyLocked()
	if last != nil {
		topLocked = util.NewLocked(last.Manifest().Height())
	}

	s := &Syncer{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "syncer")
		}),
		last:               last,
		blockMapf:          blockMapf,
		blockMapItemf:      blockMapItemf,
		topLocked:          topLocked,
		addch:              make(chan base.Height, 1),
		syncedch:           make(chan base.Height),
		finishedch:         make(chan base.Height, 33),
		syncedheightLocked: util.EmptyLocked(),
		donech:             make(chan struct{}),
		err:                util.EmptyLocked(),
		batchlimit:         333, //nolint:gomnd // big enough size
	}

	s.ContextDaemon = util.NewContextDaemon("syncer", s.start)

	return s
}

func (s *Syncer) SetLogging(l *logging.Logging) *logging.Logging {
	_ = s.ContextDaemon.SetLogging(l)

	return s.Logging.SetLogging(l)
}

func (s *Syncer) Top() base.Height {
	switch i, isnil := s.topLocked.Value(); {
	case i == nil, isnil:
		return base.NilHeight
	default:
		return i.(base.Height) //nolint:forcetypeassert //...
	}
}

func (s *Syncer) Add(height base.Height) bool {
	if s.Err() != nil {
		return false
	}

	var isnew bool
	_, err := s.topLocked.Set(func(i interface{}) (interface{}, error) {
		var top base.Height
		switch {
		case i == nil:
			isnew = true

			top = base.NilHeight
		default:
			top = i.(base.Height) //nolint:forcetypeassert //...

			if s.last != nil && top == s.last.Manifest().Height() {
				isnew = true
			}
		}

		switch {
		case height <= top:
			return nil, errors.Errorf("old height")
		case top == s.syncedheight():
			isnew = true
		}

		return height, nil
	})

	if isnew {
		go func() {
			s.addch <- height
		}()
	}

	return err == nil
}

func (s *Syncer) Finished() <-chan base.Height {
	return s.finishedch
}

func (s *Syncer) IsFinished() bool {
	switch t := s.Top(); {
	case s.Err() != nil:
		return true
	case t < base.GenesisHeight:
		return false
	default:
		return t == s.syncedheight()
	}
}

func (s *Syncer) Cancel() error {
	switch err := s.Stop(); {
	case err == nil:
	case !errors.Is(err, util.ErrDaemonAlreadyStopped):
		return err
	}

	return nil
}

func (s *Syncer) Done() <-chan struct{} {
	return s.donech
}

func (s *Syncer) Err() error {
	switch i, _ := s.err.Value(); {
	case i == nil:
		return nil
	default:
		return i.(error) //nolint:forcetypeassert //...
	}
}

func (s *Syncer) start(ctx context.Context) error {
	e := util.StringErrorFunc("failed to start")

	for {
		select {
		case <-ctx.Done():
			return e(ctx.Err(), "")
		case <-s.addch:
			go s.doSync(ctx)
		case height := <-s.syncedch:
			_, _ = s.syncedheightLocked.Set(func(i interface{}) (interface{}, error) {
				h := base.NilHeight
				if i != nil {
					h = i.(base.Height) //nolint:forcetypeassert //...
				}

				if height <= h {
					return nil, errors.Errorf("old height")
				}

				if height == s.Top() {
					s.finishedch <- height
				}

				return height, nil
			})
		}
	}
}

func (s *Syncer) syncedheight() base.Height {
	switch i, _ := s.syncedheightLocked.Value(); {
	case i == nil:
		return base.NilHeight
	default:
		return i.(base.Height) //nolint:forcetypeassert //...
	}
}

func (s *Syncer) doSync(ctx context.Context) {
	var err error
	_, _ = s.err.Set(func(i interface{}) (interface{}, error) {
		if i != nil {
			return nil, errors.Errorf("already done")
		}

		err = s.syncWithError(ctx)

		return err, nil
	})

	if err != nil {
		s.donech <- struct{}{}

		_ = s.Stop()
	}
}

func (s *Syncer) syncWithError(ctx context.Context) error {
	e := util.StringErrorFunc("failed to sync")

	// NOTE fetch all BlockMaps and validates them.
	var last base.BlockMap
	previous := s.last
end:
	for {
		switch i, err := s.fetchMaps(ctx, previous); { // BLOCK use retry
		case err != nil:
			return e(err, "")
		case i.Manifest().Height() == s.Top():
			last = i

			break end
		default:
			previous = i
		}
	}

	// BLOCK fetch and stores other block map items

	// BLOCK merge

	s.syncedch <- last.Manifest().Height()

	return nil
}

func (s *Syncer) fetchMaps(ctx context.Context, previous base.BlockMap) (base.BlockMap, error) {
	top := s.Top()

	last := base.NilHeight
	if previous != nil {
		last = previous.Manifest().Height()
	}

	if top <= last {
		return nil, nil
	}

	size := (top - last).Int64()
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
		height := base.Height(i+1) + last

		if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
			m, err := s.fetchMap(ctx, height)
			if err != nil {
				return err
			}

			if err = func() error {
				validateLock.Lock()
				defer validateLock.Unlock()

				if err = s.validateMaps(m, maps, previous); err != nil {
					return err
				}

				if (m.Manifest().Height() - last).Int64() == size {
					lastMap = m
				}

				return nil
			}(); err != nil {
				return err
			}

			// BLOCK save maps

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
