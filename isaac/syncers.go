package isaac

import (
	"sync"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/base/block"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"golang.org/x/xerrors"
)

type Syncers struct {
	sync.RWMutex
	*util.FunctionDaemon
	*logging.Logging
	local          *network.LocalNode
	storage        storage.Storage
	blockFS        *storage.BlockFS
	policy         *LocalPolicy
	syncers        []Syncer
	baseManifest   block.Manifest
	stateChan      chan SyncerStateChangedContext
	lp             int
	finished       int
	whenFinished   func(base.Height)
	whenBlockSaved func([]block.Block)
}

func NewSyncers(
	local *network.LocalNode,
	st storage.Storage,
	blockFS *storage.BlockFS,
	policy *LocalPolicy,
	baseManifest block.Manifest,
) *Syncers {
	sy := &Syncers{
		Logging: logging.NewLogging(func(c logging.Context) logging.Emitter {
			return c.Str("module", "syncers")
		}),
		local:          local,
		storage:        st,
		blockFS:        blockFS,
		policy:         policy,
		baseManifest:   baseManifest,
		lp:             -1,
		stateChan:      make(chan SyncerStateChangedContext),
		whenFinished:   func(base.Height) {},
		whenBlockSaved: func([]block.Block) {},
	}

	sy.FunctionDaemon = util.NewFunctionDaemon(sy.start, true)

	return sy
}

func (sy *Syncers) start(stopChan chan struct{}) error {
	go func() {
	end:
		for {
			select {
			case <-stopChan:
				break end
			case cxt := <-sy.stateChan:
				if err := sy.stateChanged(cxt); err != nil {
					sy.Log().Error().Err(err).Msg("failed to handle state changed")
				}
			}
		}
	}()

	return nil
}

func (sy *Syncers) Stop() error {
	sy.Lock()
	defer sy.Unlock()

	if err := sy.FunctionDaemon.Stop(); err != nil {
		if xerrors.Is(err, util.DaemonAlreadyStoppedError) {
			return nil
		}

		return err
	}

	for _, syncer := range sy.syncers {
		if err := syncer.Close(); err != nil {
			return xerrors.Errorf("failed to close syncer: %w", err)
		}
	}

	sy.syncers = nil
	sy.finished = 0

	return nil
}

func (sy *Syncers) SetLogger(l logging.Logger) logging.Logger {
	_ = sy.Logging.SetLogger(l)
	_ = sy.FunctionDaemon.SetLogger(l)

	return sy.Log()
}

func (sy *Syncers) IsFinished() bool {
	sy.RLock()
	defer sy.RUnlock()

	return len(sy.syncers)-sy.finished < 1
}

func (sy *Syncers) LastSyncer() Syncer {
	sy.RLock()
	defer sy.RUnlock()

	if len(sy.syncers) < 1 {
		return nil
	}

	return sy.syncers[len(sy.syncers)-1]
}

func (sy *Syncers) getFrom(to base.Height) (Syncer, base.Height, error) {
	var lastSyncer Syncer
	if len(sy.syncers) > 0 {
		lastSyncer = sy.syncers[len(sy.syncers)-1]
	}

	var from base.Height
	if lastSyncer == nil {
		if sy.baseManifest == nil {
			from = base.PreGenesisHeight
		} else {
			from = sy.baseManifest.Height() + 1
		}
	} else {
		from = lastSyncer.HeightTo() + 1
	}

	if to < from {
		return nil, base.NilHeight, xerrors.Errorf("target height, %v is lower than from height, %v", to, from)
	}

	return lastSyncer, from, nil
}

func (sy *Syncers) nextUnpreparedSyncer() Syncer {
	sy.RLock()
	defer sy.RUnlock()

	if sy.lp < 0 || sy.lp >= len(sy.syncers)-1 {
		return nil
	}

	return sy.syncers[sy.lp+1]
}

func (sy *Syncers) stateChanged(ctx SyncerStateChangedContext) error {
	l := sy.Log().WithLogger(func(lctx logging.Context) logging.Emitter {
		return lctx.Str("syncer", ctx.Syncer().ID()).Str("state", ctx.State().String())
	})
	l.Debug().Msg("syncer changed it's state")

	switch ctx.State() {
	case SyncerPrepared:
		sy.Lock()
		sy.lp++
		sy.Unlock()

		next := sy.nextUnpreparedSyncer()
		if next == nil {
			sy.Log().Debug().Msg("every syncers was prepared")

			return nil
		}

		l.Debug().Str("next_syncer", next.ID()).Msg("trying prepare next syncer")
		if err := next.Prepare(ctx.Syncer().TailManifest()); err != nil {
			return err
		}
	case SyncerSaved:
		sy.Lock()
		sy.finished++
		sy.Unlock()

		sy.whenBlockSaved(ctx.Blocks())

		if sy.IsFinished() {
			l.Debug().Msg("every syncers was finished")

			if st, ok := sy.storage.(storage.LastBlockSaver); ok {
				if err := st.SaveLastBlock(sy.LastSyncer().HeightTo()); err != nil {
					return err
				}
			}

			sy.whenFinished(sy.LastSyncer().HeightTo())
		}
	}

	return nil
}

func (sy *Syncers) Add(to base.Height, sourceNodes []network.Node) error {
	sy.Lock()
	defer sy.Unlock()

	l := sy.Log().WithLogger(func(ctx logging.Context) logging.Emitter {
		return ctx.Hinted("to", to)
	})

	if err := sy.add(to, sourceNodes); err != nil {
		l.Debug().Err(err).Msg("failed to add new syncer")

		return nil
	}

	l.Debug().Msg("new syncer added")

	return nil
}

func (sy *Syncers) add(to base.Height, sourceNodes []network.Node) error {
	if len(sourceNodes) < 1 {
		return xerrors.Errorf("empty source nodes")
	}

	var lastSyncer Syncer
	var from base.Height
	if s, f, err := sy.getFrom(to); err != nil {
		return err
	} else {
		lastSyncer = s
		from = f
	}

	l := sy.Log().WithLogger(func(ctx logging.Context) logging.Emitter {
		return ctx.Hinted("from", from).Hinted("to", to)
	})

	var syncer Syncer
	if s, err := NewGeneralSyncer(sy.local, sy.storage, sy.blockFS, sy.policy, sourceNodes, from, to); err != nil {
		return err
	} else {
		syncer = s.SetStateChan(sy.stateChan)

		if l, ok := syncer.(logging.SetLogger); ok {
			_ = l.SetLogger(sy.Log())
		}
	}

	sy.syncers = append(sy.syncers, syncer)

	l.Debug().Msg("added to syncers")

	if lastSyncer == nil {
		l.Debug().Msg("no last syncer; start to prepare from base manifest")
		if err := syncer.Prepare(sy.baseManifest); err != nil {
			return err
		}
	} else {
		if lastSyncer.State() < SyncerPrepared {
			l.Debug().Str("state", lastSyncer.State().String()).Msg("last syncer not yet prepared")
		} else {
			l.Debug().Hinted("base", lastSyncer.HeightTo()).Msg("start to prepare from last prepared syncer")
			if err := syncer.Prepare(lastSyncer.TailManifest()); err != nil {
				return err
			}
		}
	}

	return nil
}

func (sy *Syncers) WhenFinished(callback func(base.Height)) {
	sy.Lock()
	defer sy.Unlock()

	sy.whenFinished = callback
}

func (sy *Syncers) WhenBlockSaved(callback func([]block.Block)) {
	sy.Lock()
	defer sy.Unlock()

	sy.whenBlockSaved = callback
}
