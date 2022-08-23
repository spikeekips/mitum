package isaacstates

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	SyncerBlockMapFunc     func(context.Context, base.Height) (base.BlockMap, bool, error)
	SyncerBlockMapItemFunc func(
		context.Context, base.Height, base.BlockMapItemType) (io.ReadCloser, func() error, bool, error)
	newBlockWriteDatabaseFunc func(base.Height) (_ isaac.BlockWriteDatabase, merge func(context.Context) error, _ error)
	NewBlockImporterFunc      func(
		root string, _ base.BlockMap, _ isaac.BlockWriteDatabase) (isaac.BlockImporter, error)
	SyncerLastBlockMapFunc func(_ context.Context, manifest util.Hash) (
		_ base.BlockMap, updated bool, _ error) // NOTE BlockMap.IsValid() should be called
)

type Syncer struct {
	tempsyncpool           isaac.TempSyncPool
	finishedch             chan base.Height
	newBlockWriteDatabasef newBlockWriteDatabaseFunc
	newBlockImporter       NewBlockImporterFunc
	*logging.Logging
	prevvalue     *util.Locked
	blockMapf     SyncerBlockMapFunc
	blockMapItemf SyncerBlockMapItemFunc
	*util.ContextDaemon
	isdonevalue           *atomic.Value
	startsyncch           chan base.Height
	donech                chan struct{} // revive:disable-line:nested-structs
	doneerr               *util.Locked
	topvalue              *util.Locked
	setLastVoteproofsFunc func(isaac.BlockReader) error
	whenStoppedf          func() error
	lastBlockMapf         SyncerLastBlockMapFunc
	root                  string
	batchlimit            int64
	lastBlockMapInterval  time.Duration
	lastBlockMapTimeout   time.Duration
	cancelonece           sync.Once
}

func NewSyncer(
	root string,
	newBlockWriteDatabasef newBlockWriteDatabaseFunc,
	newBlockImporter NewBlockImporterFunc,
	prev base.BlockMap,
	lastBlockMapf SyncerLastBlockMapFunc,
	blockMapf SyncerBlockMapFunc,
	blockMapItemf SyncerBlockMapItemFunc,
	tempsyncpool isaac.TempSyncPool,
	setLastVoteproofsf func(isaac.BlockReader) error,
	whenStoppedf func() error,
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

	if whenStoppedf == nil {
		whenStoppedf = func() error { return nil } //revive:disable-line:modifies-parameter
	}

	s := &Syncer{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "syncer")
		}),
		root:                   abs,
		newBlockWriteDatabasef: newBlockWriteDatabasef,
		newBlockImporter:       newBlockImporter,
		prevvalue:              util.NewLocked(prev),
		lastBlockMapf:          lastBlockMapf,
		blockMapf:              blockMapf,
		blockMapItemf:          blockMapItemf,
		tempsyncpool:           tempsyncpool,
		batchlimit:             33, //nolint:gomnd // big enough size
		finishedch:             make(chan base.Height),
		donech:                 make(chan struct{}, 2),
		doneerr:                util.EmptyLocked(),
		topvalue:               util.NewLocked(prevheight),
		isdonevalue:            &atomic.Value{},
		startsyncch:            make(chan base.Height),
		setLastVoteproofsFunc:  setLastVoteproofsf,
		whenStoppedf:           whenStoppedf,
		lastBlockMapInterval:   time.Second * 2, //nolint:gomnd //...
		lastBlockMapTimeout:    time.Second * 2, //nolint:gomnd //...
	}

	s.ContextDaemon = util.NewContextDaemon(s.start)

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
		err = s.ContextDaemon.Stop()

		defer func() {
			_ = s.tempsyncpool.Cancel()
		}()

		_, _ = s.doneerr.Set(func(i interface{}) (interface{}, error) {
			s.isdonevalue.Store(true)

			// close(s.donech)

			return i, nil
		})
	})

	return err
}

func (s *Syncer) start(ctx context.Context) error {
	defer func() {
		_ = s.tempsyncpool.Cancel()
	}()

	go s.updateLastBlockMap(ctx)

	var err error

end:
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()

			break end
		case height := <-s.startsyncch:
			prev := s.prev()
			if prev != nil && height <= prev.Manifest().Height() {
				continue
			}

			go s.sync(ctx, prev, height)
		}
	}

	switch serr := s.whenStoppedf(); {
	case err == nil,
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		err = serr
	}

	return err
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
	err, _ := s.doneerr.Set(func(i interface{}) (interface{}, error) {
		newprev, err := s.doSync(ctx, prev, to)
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
			if h := m.Manifest().Height(); h%100 == 0 || h == to {
				s.Log().Debug().Interface("height", h).Msg("blockmap prepared")
			}

			if err := s.tempsyncpool.SetBlockMap(m); err != nil {
				return err
			}

			if m.Manifest().Height() == to {
				last = m
			}

			return nil
		},
	); err != nil {
		return nil, err
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
		s.tempsyncpool.BlockMap,
		s.blockMapItemf,
		s.newBlockWriteDatabasef,
		func(m base.BlockMap, bwdb isaac.BlockWriteDatabase) (isaac.BlockImporter, error) {
			return s.newBlockImporter(s.root, m, bwdb)
		},
		s.setLastVoteproofsFunc,
	); err != nil {
		return e(err, "")
	}

	return nil
}

func (s *Syncer) updateLastBlockMap(ctx context.Context) {
	var last util.Hash

	if s.prev() != nil {
		last = s.prev().Manifest().Hash()
	}

	ticker := time.NewTicker(s.lastBlockMapInterval)
	defer ticker.Stop()

end:
	for {
		select {
		case <-ctx.Done():
			break end
		case <-ticker.C:
			_, err := s.doneerr.Set(func(i interface{}) (interface{}, error) {
				if i != nil {
					return nil, errors.Errorf("already done by error")
				}

				top := s.top()

				nctx, cancel := context.WithTimeout(ctx, s.lastBlockMapTimeout)
				defer cancel()

				switch m, updated, err := s.lastBlockMapf(nctx, last); {
				case err != nil:
					s.Log().Error().Err(err).Msg("failed to update last BlockMap")

					return nil, nil
				case !updated:
					go func() {
						s.finishedch <- top
					}()

					return nil, nil
				default:
					_ = s.Add(m.Manifest().Height())

					return nil, nil
				}
			})
			if err != nil {
				break end
			}
		}
	}
}
