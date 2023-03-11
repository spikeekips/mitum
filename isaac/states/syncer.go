package isaacstates

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	NewBlockImporterFunc   func(base.BlockMap) (isaac.BlockImporter, error)
	SyncerLastBlockMapFunc func(_ context.Context, manifest util.Hash) (
		_ base.BlockMap, updated bool, _ error) // NOTE BlockMap.IsValid() should be called
	NewImportBlocksFunc func(
		_ context.Context,
		from, to base.Height,
		batchlimit int64,
		blockMapFunc func(context.Context, base.Height) (base.BlockMap, bool, error),
	) error
)

type SyncerArgs struct {
	LastBlockMapFunc     SyncerLastBlockMapFunc
	BlockMapFunc         isaacblock.ImportBlocksBlockMapFunc
	TempSyncPool         isaac.TempSyncPool
	WhenStoppedFunc      func() error
	RemovePrevBlockFunc  func(base.Height) (bool, error)
	NewImportBlocksFunc  NewImportBlocksFunc
	BatchLimit           int64
	LastBlockMapInterval time.Duration
	LastBlockMapTimeout  time.Duration
}

func NewSyncerArgs() SyncerArgs {
	return SyncerArgs{
		LastBlockMapFunc:    func(context.Context, util.Hash) (base.BlockMap, bool, error) { return nil, false, nil },
		BlockMapFunc:        func(context.Context, base.Height) (base.BlockMap, bool, error) { return nil, false, nil },
		WhenStoppedFunc:     func() error { return nil },
		RemovePrevBlockFunc: func(base.Height) (bool, error) { return false, nil },
		NewImportBlocksFunc: func(context.Context, base.Height, base.Height, int64,
			func(context.Context, base.Height) (base.BlockMap, bool, error),
		) error {
			return errors.Errorf("nothing happened")
		},
		BatchLimit:           33,              //nolint:gomnd // big enough size
		LastBlockMapInterval: time.Second * 2, //nolint:gomnd //...
		LastBlockMapTimeout:  time.Second * 2, //nolint:gomnd //...
	}
}

type Syncer struct {
	finishedch   chan base.Height
	prevvalue    *util.Locked[base.BlockMap]
	isdonevalue  *atomic.Value
	checkedprevs *util.GCache[base.Height, string]
	startsyncch  chan base.Height
	donech       chan struct{}
	doneerr      *util.Locked[error]
	topvalue     *util.Locked[base.Height]
	*logging.Logging
	*util.ContextDaemon
	args        SyncerArgs
	cancelonece sync.Once
}

func NewSyncer(prev base.BlockMap, args SyncerArgs) *Syncer {
	prevheight := base.NilHeight
	if prev != nil {
		prevheight = prev.Manifest().Height()
	}

	s := &Syncer{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "syncer")
		}),
		prevvalue:    util.NewLocked(prev),
		args:         args,
		finishedch:   make(chan base.Height),
		donech:       make(chan struct{}, 2),
		doneerr:      util.EmptyLocked((error)(nil)),
		topvalue:     util.NewLocked(prevheight),
		isdonevalue:  &atomic.Value{},
		startsyncch:  make(chan base.Height),
		checkedprevs: util.NewLRUGCache(base.NilHeight, "", 1<<3), //nolint:gomnd //...
	}

	s.ContextDaemon = util.NewContextDaemon(s.start)

	return s
}

func (s *Syncer) Add(height base.Height) bool {
	if s.isdonevalue.Load() != nil {
		return false
	}

	var startsync bool

	if _, err := s.topvalue.Set(func(top base.Height, _ bool) (base.Height, error) {
		synced := s.prevheight()

		switch {
		case height <= top:
			return base.NilHeight, errors.Errorf("old height")
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

	s.Log().Debug().Interface("height", height).Msg("added")

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

	return i
}

func (s *Syncer) IsFinished() (base.Height, bool) {
	top := s.top()

	return top, top == s.prevheight()
}

func (s *Syncer) Cancel() error {
	var err error
	s.cancelonece.Do(func() {
		err = s.ContextDaemon.Stop()
		if errors.Is(err, util.ErrDaemonAlreadyStopped) {
			err = nil
		}

		defer func() {
			_ = s.args.TempSyncPool.Cancel()
		}()

		_, _ = s.doneerr.Set(func(i error, _ bool) (error, error) {
			s.isdonevalue.Store(true)

			// close(s.donech)

			return i, nil
		})
	})

	return err
}

func (s *Syncer) start(ctx context.Context) error {
	defer func() {
		_ = s.args.TempSyncPool.Cancel()
	}()

	uctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go s.updateLastBlockMap(uctx)

	var gerr error

end:
	for {
		select {
		case <-uctx.Done():
			gerr = uctx.Err()

			break end
		case height := <-s.startsyncch:
			if err := s.donewitherror(func() error {
				return s.sync(uctx, height)
			}); err != nil {
				gerr = err

				break end
			}
		}
	}

	switch serr := s.args.WhenStoppedFunc(); {
	case gerr == nil,
		errors.Is(gerr, context.Canceled),
		errors.Is(gerr, context.DeadlineExceeded):
		gerr = serr
	}

	return gerr
}

func (s *Syncer) top() base.Height {
	i, _ := s.topvalue.Value()

	return i
}

func (s *Syncer) prev() base.BlockMap {
	i, _ := s.prevvalue.Value()

	return i
}

func (s *Syncer) prevheight() base.Height {
	m := s.prev()
	if m == nil {
		return base.NilHeight
	}

	return m.Manifest().Height()
}

func (s *Syncer) sync( // revive:disable-line:import-shadowing
	ctx context.Context,
	to base.Height,
) error {
	prev, err := s.prevvalue.Set(func(i base.BlockMap, _ bool) (base.BlockMap, error) {
		switch {
		case i == nil:
			return nil, util.ErrLockedSetIgnore.Call()
		case s.checkedprevs.Exists(i.Manifest().Height()):
			return nil, util.ErrLockedSetIgnore.Call()
		}

		newprev, err := s.checkPrevMap(ctx, i)
		if err != nil {
			return nil, err
		}

		s.checkedprevs.Set(i.Manifest().Height(), "", 0)

		switch {
		case newprev != nil:
			return newprev, nil
		default:
			return nil, util.ErrLockedSetIgnore.Call()
		}
	})
	if err != nil {
		return err
	}

	if prev != nil && to <= prev.Manifest().Height() {
		return nil
	}

	newprev, err := s.doSync(ctx, prev, to)
	if err != nil {
		return err
	}

	_ = s.prevvalue.SetValue(newprev)

	switch top := s.top(); {
	case newprev.Manifest().Height() == top:
		go func() {
			s.finishedch <- top
		}()
	case newprev.Manifest().Height() < top:
		go func() {
			s.startsyncch <- top
		}()
	}

	return nil
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
		uint64(s.args.BatchLimit),
		s.fetchMap,
		func(m base.BlockMap) error {
			if h := m.Manifest().Height(); h%100 == 0 || h == to {
				s.Log().Debug().Interface("height", h).Msg("blockmap prepared")
			}

			if err := s.args.TempSyncPool.SetBlockMap(m); err != nil {
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

func (s *Syncer) checkPrevMap(ctx context.Context, prev base.BlockMap) (base.BlockMap, error) {
	var mp base.BlockMap

	switch m, err := s.fetchMap(ctx, prev.Manifest().Height()); {
	case err != nil:
		return nil, err
	case prev.Manifest().Hash().Equal(m.Manifest().Hash()):
		return prev, nil
	default:
		s.Log().Debug().
			Interface("previous_block", prev).
			Interface("different_previous_block", m).
			Msg("different previous block found; will be removed from local")

		mp = m
	}

	// NOTE remove last block from database; if failed, return error
	switch removed, err := s.args.RemovePrevBlockFunc(prev.Manifest().Height()); {
	case err != nil:
		return nil, err
	case !removed:
		return nil, errors.Errorf("previous manifest does not match with remotes")
	default:
		return mp, nil
	}
}

func (s *Syncer) fetchMap(ctx context.Context, height base.Height) (base.BlockMap, error) {
	e := util.StringErrorFunc("failed to fetch BlockMap")

	switch m, found, err := s.args.BlockMapFunc(ctx, height); {
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

	if err := util.Retry(ctx, func() (bool, error) {
		if err := s.args.NewImportBlocksFunc(
			ctx,
			from, to,
			s.args.BatchLimit,
			func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
				return s.args.TempSyncPool.BlockMap(height)
			},
		); err != nil {
			return true, err
		}

		return false, nil
	},
		-1,
		time.Second,
	); err != nil {
		return e(err, "")
	}

	return nil
}

func (s *Syncer) updateLastBlockMap(ctx context.Context) {
	var last util.Hash

	if prev := s.prev(); prev != nil {
		last = prev.Manifest().Hash()
	}

	ticker := time.NewTicker(s.args.LastBlockMapInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			switch m, updated, err := s.lastBlockMap(ctx, last); {
			case err != nil:
				s.Log().Error().Err(err).Msg("failed to update last BlockMap")
			case updated:
				_ = s.Add(m.Manifest().Height())

				last = m.Manifest().Hash()
			}
		}
	}
}

func (s *Syncer) donewitherror(f func() error) error {
	err, _ := s.doneerr.Set(func(i error, _ bool) (error, error) {
		if i != nil {
			return nil, errors.Errorf("already done by error")
		}

		return f(), nil
	})

	if err != nil {
		s.donech <- struct{}{}
	}

	return err
}

func (s *Syncer) lastBlockMap(ctx context.Context, manifest util.Hash) (_ base.BlockMap, updated bool, _ error) {
	nctx, cancel := context.WithTimeout(ctx, s.args.LastBlockMapTimeout)
	defer cancel()

	return s.args.LastBlockMapFunc(nctx, manifest)
}
