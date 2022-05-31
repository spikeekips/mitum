package isaacstates

import (
	"context"
	"io"
	"os"
	"path/filepath"
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
	SyncerBlockMapItemFunc func(context.Context, base.Height, base.BlockMapItemType) (io.ReadCloser, bool, error)
	NewBlockImporterFunc   func(root string, blockmap base.BlockMap) (isaac.BlockImporter, error)
)

type Syncer struct {
	tempsyncpool     isaac.TempSyncPool
	startsyncch      chan base.Height
	newBlockImporter NewBlockImporterFunc
	*logging.Logging
	prevvalue     *util.Locked
	blockMapf     SyncerBlockMapFunc
	blockMapItemf SyncerBlockMapItemFunc
	*util.ContextDaemon
	isdonevalue           *atomic.Value
	finishedch            chan base.Height
	donech                chan struct{} // revive:disable-line:nested-structs
	doneerr               *util.Locked
	topvalue              *util.Locked
	setLastVoteproofsFunc func(isaac.BlockReader) error
	root                  string
	batchlimit            int64
	cancelonece           sync.Once
}

func NewSyncer(
	root string,
	newBlockImporter NewBlockImporterFunc,
	prev base.BlockMap,
	blockMapf SyncerBlockMapFunc,
	blockMapItemf SyncerBlockMapItemFunc,
	tempsyncpool isaac.TempSyncPool,
	setLastVoteproofsFunc func(isaac.BlockReader) error,
) (*Syncer, error) {
	e := util.StringErrorFunc("failed NewSyncer")

	abs, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return nil, e(err, "")
	}

	switch fi, err := os.Stat(abs); {
	case err == nil:
	case os.IsNotExist(err):
		return nil, e(err, "root directory does not exist")
	case !fi.IsDir():
		return nil, e(nil, "root is not directory")
	default:
		return nil, e(err, "wrong root directory")
	}

	prevheight := base.NilHeight
	if prev != nil {
		prevheight = prev.Manifest().Height()
	}

	s := &Syncer{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "syncer")
		}),
		root:                  abs,
		newBlockImporter:      newBlockImporter,
		prevvalue:             util.NewLocked(prev),
		blockMapf:             blockMapf,
		blockMapItemf:         blockMapItemf,
		tempsyncpool:          tempsyncpool,
		batchlimit:            333, //nolint:gomnd // big enough size
		finishedch:            make(chan base.Height),
		donech:                make(chan struct{}),
		doneerr:               util.EmptyLocked(),
		topvalue:              util.NewLocked(prevheight),
		isdonevalue:           &atomic.Value{},
		startsyncch:           make(chan base.Height),
		setLastVoteproofsFunc: setLastVoteproofsFunc,
	}

	s.ContextDaemon = util.NewContextDaemon("syncer", s.start)

	return s, nil
}

func (s *Syncer) Add(height base.Height) bool {
	if s.isdonevalue.Load() != nil {
		return false
	}

	var startsync bool

	if _, err := s.topvalue.Set(func(i interface{}) (interface{}, error) {
		top := i.(base.Height) //nolint:forcetypeassert //...
		synced := s.prevheight()

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

	return top, top == s.prevheight()
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
			prev := s.prev()
			if prev != nil && height <= prev.Manifest().Height() {
				continue
			}

			go s.sync(ctx, prev, height)
		}
	}
}

func (s *Syncer) top() base.Height {
	i, _ := s.topvalue.Value()

	return i.(base.Height) //nolint:forcetypeassert //...
}

func (s *Syncer) prev() base.BlockMap {
	i, _ := s.prevvalue.Value()
	if i == nil {
		return nil
	}

	return i.(base.BlockMap) //nolint:forcetypeassert //...
}

func (s *Syncer) prevheight() base.Height {
	m := s.prev()
	if m == nil {
		return base.NilHeight
	}

	return m.Manifest().Height()
}

func (s *Syncer) sync(ctx context.Context, prev base.BlockMap, to base.Height) { // revive:disable-line:import-shadowing
	var err error
	_, _ = s.doneerr.Set(func(i interface{}) (interface{}, error) {
		var newprev base.BlockMap

		newprev, err = s.doSync(ctx, prev, to)
		if err != nil {
			return err, nil
		}

		_ = s.prevvalue.SetValue(newprev)

		switch top := s.top(); {
		case newprev.Manifest().Height() == top:
			go func() {
				s.finishedch <- newprev.Manifest().Height()
			}()
		case newprev.Manifest().Height() < top:
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

func (s *Syncer) doSync(ctx context.Context, prev base.BlockMap, to base.Height) (base.BlockMap, error) {
	e := util.StringErrorFunc("failed to sync")

	// NOTE fetch and store all BlockMaps
	newprev, err := s.prepareMaps(ctx, prev, to)
	if err != nil {
		return nil, e(err, "")
	}

	if err := s.syncBlocks(ctx, prev, to); err != nil {
		return nil, e(err, "")
	}

	return newprev, nil
}

func (s *Syncer) prepareMaps(ctx context.Context, prev base.BlockMap, to base.Height) (base.BlockMap, error) {
	var last base.BlockMap

	if err := base.BatchValidateMaps(
		ctx,
		prev,
		to,
		uint64(s.batchlimit),
		s.fetchMap,
		func(m base.BlockMap) error {
			if err := s.tempsyncpool.SetMap(m); err != nil {
				return errors.Wrap(err, "")
			}

			if m.Manifest().Height() == to {
				last = m
			}

			return nil
		},
	); err != nil {
		return nil, errors.Wrap(err, "")
	}

	return last, nil
}

func (s *Syncer) fetchMap(ctx context.Context, height base.Height) (base.BlockMap, error) {
	e := util.StringErrorFunc("failed to fetch BlockMap")

	switch m, found, err := s.blockMapf(ctx, height); {
	case err != nil:
		return nil, e(err, "")
	case !found:
		return nil, e(nil, "not found")
	default:
		// FIXME blockMap should be passed IsValid() in s.blockMap.
		return m, nil
	}
}

func (s *Syncer) syncBlocks(ctx context.Context, prev base.BlockMap, to base.Height) error {
	e := util.StringErrorFunc("failed to sync blocks")

	from := base.GenesisHeight
	if prev != nil {
		from = prev.Manifest().Height() + 1
	}

	if err := ImportBlocks(
		ctx,
		from, to,
		s.batchlimit,
		s.tempsyncpool.Map,
		s.blockMapItemf,
		func(m base.BlockMap) (isaac.BlockImporter, error) {
			return s.newBlockImporter(s.root, m)
		},
		s.setLastVoteproofsFunc,
	); err != nil {
		return e(err, "")
	}

	return nil
}

func ValidateMaps(m base.BlockMap, maps []base.BlockMap, previous base.BlockMap) error {
	prev := base.NilHeight
	if previous != nil {
		prev = previous.Manifest().Height()
	}

	index := (m.Manifest().Height() - prev - 1).Int64()

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
