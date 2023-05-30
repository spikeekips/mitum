package isaacstates

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type SyncingHandlerArgs struct {
	NodeInConsensusNodesFunc isaac.NodeInConsensusNodesFunc
	NewSyncerFunc            func(base.Height) (isaac.Syncer, error)
	WhenReachedTopFunc       func(base.Height)
	JoinMemberlistFunc       func(context.Context, base.Suffrage) error
	LeaveMemberlistFunc      func(time.Duration) error
	WhenNewBlockSavedFunc    func(base.Height)
	WaitStuckInterval        time.Duration
}

func NewSyncingHandlerArgs(params *isaac.LocalParams) *SyncingHandlerArgs {
	return &SyncingHandlerArgs{
		NodeInConsensusNodesFunc: func(base.Node, base.Height) (base.Suffrage, bool, error) {
			return nil, false, util.ErrNotImplemented.Errorf("NodeInConsensusNodesFunc")
		},
		NewSyncerFunc: func(base.Height) (isaac.Syncer, error) {
			return nil, util.ErrNotImplemented.Errorf("NewSyncerFunc")
		},
		WhenReachedTopFunc: func(base.Height) {},
		JoinMemberlistFunc: func(context.Context, base.Suffrage) error {
			return util.ErrNotImplemented.Errorf("JoinMemberlistFunc")
		},
		LeaveMemberlistFunc: func(time.Duration) error {
			return util.ErrNotImplemented.Errorf("LeaveMemberlistFunc")
		},
		WhenNewBlockSavedFunc: func(base.Height) {},
		WaitStuckInterval:     params.IntervalBroadcastBallot()*2 + params.WaitPreparingINITBallot(),
	}
}

type SyncingHandler struct {
	syncer isaac.Syncer
	*baseHandler
	args              *SyncingHandlerArgs
	stuckwaitcancel   func()
	askHandoverFunc   func() (bool, error)
	waitStuckInterval *util.Locked[time.Duration]
	finishedLock      sync.RWMutex
	stuckwaitlock     sync.RWMutex
}

type NewSyncingHandlerType struct {
	*SyncingHandler
}

func NewNewSyncingHandlerType(
	local base.LocalNode,
	params *isaac.LocalParams,
	args *SyncingHandlerArgs,
) *NewSyncingHandlerType {
	return &NewSyncingHandlerType{
		SyncingHandler: &SyncingHandler{
			baseHandler: newBaseHandlerType(StateSyncing, local, params),
			args:        args,
		},
	}
}

func (h *NewSyncingHandlerType) new() (handler, error) {
	return &SyncingHandler{
		baseHandler:       h.baseHandler.new(),
		args:              h.args,
		waitStuckInterval: util.NewLocked(h.args.WaitStuckInterval),
	}, nil
}

func (st *SyncingHandler) enter(from StateType, i switchContext) (func(), error) {
	e := util.StringError("enter syncing state")

	deferred, err := st.baseHandler.enter(from, i)
	if err != nil {
		return nil, e.Wrap(err)
	}

	sctx, ok := i.(SyncingSwitchContext)
	if !ok {
		return nil, e.Errorf("invalid stateSwitchContext, not for syncing state; %T", i)
	}

	switch sc, err := st.args.NewSyncerFunc(sctx.height); {
	case err != nil:
		return nil, e.Wrap(err)
	case sc == nil:
		return nil, e.Errorf("empty syncer") // NOTE only for testing
	default:
		if l, ok := sc.(logging.SetLogging); ok {
			_ = l.SetLogging(st.Logging)
		}

		st.syncer = sc
	}

	return func() {
		deferred()

		l := st.Log().With().Dict("state", switchContextLog(sctx)).Logger()

		allowedConsensus := st.allowedConsensus()

		// NOTE if syncing is switched from consensus state, the other nodes can
		// not get the last INIT ballot.
		switch from {
		case StateConsensus:
			go func() {
				if allowedConsensus {
					wait := st.params.WaitPreparingINITBallot() * 2
					l.Debug().Dur("wait", wait).Msg("timers will be stopped")

					<-time.After(wait)
				}

				if st.sts != nil && st.sts.Current() == StateSyncing {
					l.Debug().Msg("timers stopped")

					if err := st.timers.StopAllTimers(); err != nil {
						l.Error().Err(err).Msg("failed to stop all timers")
					}
				}
			}()
		default:
			if err := st.timers.StopAllTimers(); err != nil {
				l.Error().Err(err).Msg("failed to stop all timers")
			}
		}

		_ = st.syncer.Start(st.ctx)

		go st.finishing(st.syncer)

		if lvp := st.lastVoteproofs().Cap(); lvp != nil {
			st.newStuckWait(lvp)
		}

		if !allowedConsensus {
			st.whenNotAllowedConsensus()
		}
	}, nil
}

func (st *SyncingHandler) exit(sctx switchContext) (func(), error) {
	e := util.StringError("exit from syncing state")

	if st.syncer != nil {
		if _, isfinished := st.syncer.IsFinished(); !isfinished {
			return nil, ErrIgnoreSwitchingState.Errorf("syncer not yet finished")
		}

		if err := st.syncer.Cancel(); err != nil {
			return nil, e.WithMessage(err, "stop syncer")
		}
	}

	deferred, err := st.baseHandler.exit(sctx)
	if err != nil {
		return nil, e.Wrap(err)
	}

	st.cancelstuck()

	if err := st.timers.StopAllTimers(); err != nil {
		st.Log().Error().Err(err).Msg("failed to stop all timers")
	}

	return func() {
		deferred()
	}, nil
}

func (st *SyncingHandler) newVoteproof(vp base.Voteproof) error {
	e := util.StringError("handle new voteproof")

	if _, v, isnew := st.baseHandler.setNewVoteproof(vp); v == nil || !isnew {
		return nil
	}

	if _, err := st.checkFinished(vp); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (st *SyncingHandler) checkFinished(vp base.Voteproof) (notstuck bool, _ error) {
	if vp == nil {
		return false, nil
	}

	st.finishedLock.Lock()
	defer st.finishedLock.Unlock()

	height := syncerHeightWithStagePoint(vp.Point(), vp.Result() == base.VoteResultMajority)

	l := st.Log().With().Str("voteproof", vp.ID()).Interface("height", height).Logger()

	_ = st.add(height)

	top, isfinished := st.syncer.IsFinished()

	l.Debug().
		Func(base.VoteproofLogFunc("voteproof", vp)).
		Bool("allow_consensus", st.allowedConsensus()).
		Bool("is_finished", isfinished).
		Interface("top", top).
		Msg("checking finished")

	if isfinished {
		st.args.WhenReachedTopFunc(top)

		go st.args.WhenNewBlockSavedFunc(top)
	}

	checkMoveState := st.allowedConsensus()

	if !st.allowedConsensus() {
		st.whenNotAllowedConsensus()

		switch alreadyAsked, err := st.askHandover(); {
		case err != nil:
			st.Log().Error().Err(err).Msg("failed to ask")
		case alreadyAsked, st.allowedConsensus():
			checkMoveState = true
		}
	}

	if checkMoveState {
		return st.checkFinishedMoveNext(vp)
	}

	return true, nil
}

func (st *SyncingHandler) checkFinishedMoveNext(vp base.Voteproof) (notstuck bool, _ error) {
	l := st.Log().With().Str("voteproof", vp.ID()).Logger()

	top, isfinished := st.syncer.IsFinished()

	if isfinished {
		joined, err := st.checkAndJoinMemberlist(top)
		if err != nil || !joined {
			return false, err
		}
	}

	switch {
	case vp.Point().Height() == top+1:
		if !isfinished {
			l.Debug().Msg("expected init voteproof found; but not yet finished")

			return true, nil
		}

		switch {
		case vp.Point().Stage() == base.StageINIT:
		case vp.Point().Stage() == base.StageACCEPT && vp.Result() == base.VoteResultDraw:
		default:
			return true, nil
		}

		// NOTE expected init voteproof found, moves to consensus state
		l.Debug().Msg("expected init voteproof found; moves to consensus state")

		sctx, err := newConsensusSwitchContext(StateSyncing, vp)
		if err != nil {
			return false, err
		}

		return false, sctx
	case isfinished && vp.Point().Stage() == base.StageACCEPT && vp.Point().Height() == top:
		l.Debug().Msg("start new stuck cancel")

		st.newStuckWait(vp)

		return false, nil
	default:
		return true, nil
	}
}

func (st *SyncingHandler) add(h base.Height) bool {
	if st.syncer == nil {
		return false
	}

	st.cancelstuck()

	return st.syncer.Add(h)
}

func (st *SyncingHandler) finishing(sc isaac.Syncer) {
	var err error

end:
	for {
		select {
		case <-st.ctx.Done():
			return
		case <-sc.Done():
			if err = sc.Err(); err != nil {
				_ = st.syncer.Cancel()

				st.Log().Error().Err(err).Msg("syncer canceled by error")
			}
		case top := <-sc.Finished():
			st.Log().Debug().Interface("height", top).Msg("syncer finished")

			var notstuck bool

			if notstuck, err = st.checkFinished(st.lastVoteproofs().Cap()); err != nil || notstuck {
				st.cancelstuck()
			}
		}

		if err != nil {
			var sctx switchContext

			if !errors.As(err, &sctx) {
				sctx = newBrokenSwitchContext(StateSyncing, err)
			}

			go st.switchState(sctx)

			break end
		}
	}
}

func (st *SyncingHandler) cancelstuck() {
	st.stuckwaitlock.RLock()
	defer st.stuckwaitlock.RUnlock()

	if st.stuckwaitcancel != nil {
		st.stuckwaitcancel()
	}
}

func (st *SyncingHandler) newStuckWait(vp base.Voteproof) {
	st.stuckwaitlock.Lock()
	defer st.stuckwaitlock.Unlock()

	if st.stuckwaitcancel != nil {
		st.stuckwaitcancel()
	}

	waitStuckInterval, _ := st.waitStuckInterval.Value()

	l := st.Log().With().Dur("wait", waitStuckInterval).Stringer("id", util.ULID()).Logger()

	l.Debug().Msg("will wait for stucked")

	ctx, cancel := context.WithTimeout(st.ctx, waitStuckInterval)
	st.stuckwaitcancel = cancel

	go func() {
		defer func() {
			// NOTE loop newStuckWait
			go func() {
				i, _ := st.waitStuckInterval.Value()

				select {
				case <-st.ctx.Done():
					return
				case <-time.After(i * 2):
					if lvp := st.lastVoteproofs().Cap(); lvp != nil {
						st.newStuckWait(lvp)
					}
				}
			}()
		}()

		<-ctx.Done()

		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return
		}

		st.finishedLock.Lock()
		defer st.finishedLock.Unlock()

		top, isfinished := st.syncer.IsFinished()

		switch {
		case isfinished:
		case top <= vp.Point().Height():
		default:
			return
		}

		if st.allowedConsensus() {
			st.Log().Debug().Dur("wait", waitStuckInterval).
				Msg("stuck accept voteproof found; moves to joining state")

			// NOTE no more valid voteproof received, moves to joining state
			go st.switchState(newJoiningSwitchContext(StateSyncing, vp))
		}
	}()
}

func (st *SyncingHandler) checkAndJoinMemberlist(height base.Height) (joined bool, _ error) {
	l := st.Log().With().Interface("height", height).Logger()

	switch suf, found, err := st.args.NodeInConsensusNodesFunc(st.local, height); {
	case errors.Is(err, storage.ErrNotFound):
		l.Debug().Interface("height", height).Msg("suffrage not found after syncer finished")

		return false, nil
	case err != nil:
		l.Error().Err(err).Msg("failed to get consensus nodes after syncer finished")

		return false, err
	case suf == nil:
		return false, newBrokenSwitchContext(StateSyncing, errors.Errorf("empty suffrage"))
	case !found:
		if err := st.args.LeaveMemberlistFunc(time.Second); err != nil {
			l.Error().Err(err).Msg("failed to leave memberilst; ignored")
		}

		l.Debug().Msg("local is not in consensus nodes after syncer finished; keep syncing")

		return false, nil
	case suf.Exists(st.local.Address()) && suf.Len() < 2: // NOTE local is alone in suffrage node
	default:
		ctx, cancel := context.WithTimeout(st.ctx, time.Second*10) //nolint:gomnd //...
		defer cancel()

		if err := st.args.JoinMemberlistFunc(ctx, suf); err != nil {
			l.Debug().Err(err).
				Msg("local is in consensus nodes after syncer finished; but failed to join memberlist")

			return false, nil
		}

		l.Debug().Msg("local is in consensus nodes after syncer finished; joined memberlist")
	}

	return true, nil
}

func (st *SyncingHandler) whenNotAllowedConsensus() {
	if broker := st.handoverYBroker(); broker != nil {
		return
	}

	switch err := st.args.LeaveMemberlistFunc(time.Second); {
	case err != nil:
		st.Log().Error().Err(err).Msg("not allowed consensus; failed to leave memberilst; ignored")
	default:
		st.Log().Debug().Msg("left memberlist; not allowed consensus")
	}
}

func (st *SyncingHandler) askHandover() (alreadyAsked bool, _ error) {
	if st.askHandoverFunc != nil {
		return st.askHandoverFunc()
	}

	broker := st.handoverYBroker()

	switch {
	case broker == nil:
		return false, nil
	case broker.IsAsked():
		return true, nil
	}

	// NOTE ask handover to x
	switch canMoveConsensus, isAsked, err := broker.Ask(); {
	case err != nil:
		return false, err
	case !isAsked:
		return false, nil
	default:
		st.Log().Debug().Bool("can_move_consensus", canMoveConsensus).Msg("handover asked")

		if canMoveConsensus {
			_ = st.setAllowConsensus(true)
		}

		return false, nil
	}
}

type SyncingSwitchContext struct { //nolint:errname //...
	baseSwitchContext
	height base.Height
}

func emptySyncingSwitchContext(from StateType) SyncingSwitchContext {
	return SyncingSwitchContext{
		baseSwitchContext: newBaseSwitchContext(from, StateSyncing),
		height:            base.GenesisHeight,
	}
}

func newSyncingSwitchContext(from StateType, height base.Height) SyncingSwitchContext {
	return SyncingSwitchContext{
		baseSwitchContext: newBaseSwitchContext(from, StateSyncing),
		height:            height,
	}
}

func (s SyncingSwitchContext) MarshalZerologObject(e *zerolog.Event) {
	s.baseSwitchContext.MarshalZerologObject(e)

	e.Interface("height", s.height)
}

func syncerHeightWithStagePoint(point base.StagePoint, isMajority bool) base.Height {
	height := point.Height()

	switch s := point.Stage(); {
	case s == base.StageINIT,
		s == base.StageACCEPT && !isMajority:
		height = height.SafePrev()
	}

	return height
}

func newSyncingSwitchContextWithVoteproof(from StateType, vp base.Voteproof) SyncingSwitchContext {
	height := base.GenesisHeight
	if vp != nil {
		height = syncerHeightWithStagePoint(vp.Point(), vp.Result() == base.VoteResultMajority)
	}

	return newSyncingSwitchContext(from, height)
}
