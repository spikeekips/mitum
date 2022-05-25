package isaacstates

import (
	"context"
	"io"
	"math"
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
	SyncerBlockMapItemFunc func(context.Context, base.Height, base.BlockMapItemType) (io.Reader, bool, error)
	NewBlockImporterFunc   func(root string, blockmap base.BlockMap) (isaac.BlockImporter, error)
)

type Syncer struct {
	tempsyncpool     isaac.TempSyncPool
	isdonevalue      *atomic.Value
	newBlockImporter NewBlockImporterFunc
	*logging.Logging
	prevvalue     *util.Locked
	blockMapf     SyncerBlockMapFunc
	blockMapItemf SyncerBlockMapItemFunc
	*util.ContextDaemon
	startsyncch           chan base.Height
	finishedch            chan base.Height
	donech                chan struct{} // revive:disable-line:nested-structs
	doneerr               *util.Locked
	topvalue              *util.Locked
	root                  string
	batchlimit            int64
	cancelonece           sync.Once
	setLastVoteproofsFunc func(isaac.BlockImporter) error
}

func NewSyncer(
	root string,
	newBlockImporter NewBlockImporterFunc,
	prev base.BlockMap,
	blockMapf SyncerBlockMapFunc,
	blockMapItemf SyncerBlockMapItemFunc,
	tempsyncpool isaac.TempSyncPool,
	setLastVoteproofsFunc func(isaac.BlockImporter) error,
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
	prevheight := base.NilHeight
	if prev != nil {
		prevheight = prev.Manifest().Height()
	}

	var validateLock sync.Mutex
	var maps []base.BlockMap

	var lastprev base.BlockMap
	newprev := prev

	if err := util.BatchWork(
		ctx,
		uint64((to - prevheight).Int64()),
		uint64(s.batchlimit),
		func(ctx context.Context, last uint64) error {
			lastprev = newprev

			switch r := (last + 1) % uint64(s.batchlimit); {
			case r == 0:
				maps = make([]base.BlockMap, s.batchlimit)
			default:
				maps = make([]base.BlockMap, r)
			}

			return nil
		},
		func(ctx context.Context, i, last uint64) error {
			height := prevheight + base.Height(int64(i)) + 1
			lastheight := prevheight + base.Height(int64(last)) + 1

			m, err := s.fetchMap(ctx, height)
			if err != nil {
				return err
			}

			if err = func() error {
				validateLock.Lock()
				defer validateLock.Unlock()

				if err = ValidateMaps(m, maps, lastprev); err != nil {
					return err
				}

				if m.Manifest().Height() == lastheight {
					newprev = m
				}

				return nil
			}(); err != nil {
				return err
			}

			if err := s.tempsyncpool.SetMap(m); err != nil {
				return errors.Wrap(err, "")
			}

			return nil
		},
	); err != nil {
		return nil, errors.Wrap(err, "failed to prepare BlockMaps")
	}

	return newprev, nil
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

func (s *Syncer) syncBlocks(ctx context.Context, prev base.BlockMap, to base.Height) error {
	e := util.StringErrorFunc("failed to sync blocks")

	from := base.GenesisHeight
	if prev != nil {
		from = prev.Manifest().Height() + 1
	}

	var lastim isaac.BlockImporter
	var ims []isaac.BlockImporter

	if err := util.BatchWork(
		ctx,
		uint64((to - from + 1).Int64()),
		uint64(s.batchlimit),
		func(ctx context.Context, last uint64) error {
			if ims != nil {
				if err := s.saveImporters(ctx, ims); err != nil {
					return errors.Wrap(err, "")
				}
			}

			switch r := (last + 1) % uint64(s.batchlimit); {
			case r == 0:
				ims = make([]isaac.BlockImporter, s.batchlimit)
			default:
				ims = make([]isaac.BlockImporter, r)
			}

			return nil
		},
		func(ctx context.Context, i, _ uint64) error {
			height := from + base.Height(int64(i))

			im, err := s.fetchBlock(ctx, height)
			if err != nil {
				return errors.Wrap(err, "")
			}

			ims[(height-from).Int64()%s.batchlimit] = im

			if height == to {
				lastim = im
			}

			return nil
		},
	); err != nil {
		return e(err, "")
	}

	if err := s.saveImporters(ctx, ims); err != nil {
		return errors.Wrap(err, "")
	}

	if err := s.setLastVoteproofsFunc(lastim); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (s *Syncer) fetchBlock(ctx context.Context, height base.Height) (isaac.BlockImporter, error) {
	e := util.StringErrorFunc("failed to fetch block, %d", height)

	m, found, err := s.tempsyncpool.Map(height)

	switch {
	case err != nil:
		return nil, e(err, "")
	case !found:
		return nil, e(nil, "BlockMap not found") // BLOCK use util.ErrNotFound
	}

	im, err := s.newBlockImporter(s.root, m)
	if err != nil {
		return nil, e(err, "")
	}

	worker := util.NewErrgroupWorker(ctx, math.MaxInt32)
	defer worker.Close()

	m.Items(func(item base.BlockMapItem) bool {
		if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
			return s.fetchBlockItem(ctx, height, item.Type(), im)
		}); err != nil {
			return false
		}

		return true
	})

	worker.Done()

	if err := worker.Wait(); err != nil {
		_ = im.CancelImport(ctx)

		return nil, e(err, "")
	}

	return im, nil
}

func (s *Syncer) fetchBlockItem(
	ctx context.Context, height base.Height, item base.BlockMapItemType, im isaac.BlockImporter,
) error {
	e := util.StringErrorFunc("failed to fetch block item, %q", item)

	switch r, found, err := s.blockMapItemf(ctx, height, item); {
	case err != nil:
		return e(err, "")
	case !found:
		return e(nil, "blockMapItem not found")
	default:
		if err := im.WriteItem(item, r); err != nil {
			return e(err, "")
		}
	}

	return nil
}

func (s *Syncer) saveImporters(ctx context.Context, ims []isaac.BlockImporter) error {
	e := util.StringErrorFunc("failed to cancel importers")

	switch {
	case len(ims) < 1:
		return errors.Errorf("empty BlockImporters")
	case len(ims) < 2: //nolint:gomnd //...
		if err := ims[0].Save(ctx); err != nil {
			return e(err, "")
		}

		return nil
	}

	if err := util.RunErrgroupWorker(ctx, uint64(len(ims)), func(ctx context.Context, i, _ uint64) error {
		return errors.Wrap(ims[i].Save(ctx), "")
	}); err != nil {
		_ = s.cancelImporters(ctx, ims)

		return e(err, "")
	}

	if err := util.RunErrgroupWorker(ctx, uint64(len(ims)), func(ctx context.Context, i, _ uint64) error {
		return errors.Wrap(ims[i].Merge(ctx), "")
	}); err != nil {
		return e(err, "")
	}

	return nil
}

func (*Syncer) cancelImporters(ctx context.Context, ims []isaac.BlockImporter) error {
	e := util.StringErrorFunc("failed to cancel importers")

	switch {
	case len(ims) < 1:
		return nil
	case len(ims) < 2: //nolint:gomnd //...
		if err := ims[0].CancelImport(ctx); err != nil {
			return e(err, "")
		}

		return nil
	}

	if err := util.RunErrgroupWorker(ctx, uint64(len(ims)), func(ctx context.Context, i, _ uint64) error {
		return errors.Wrap(ims[i].CancelImport(ctx), "")
	}); err != nil {
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
