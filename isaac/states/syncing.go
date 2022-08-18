package isaacstates

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var SyncerCanNotCancelError = util.NewError("can not cancel syncer")

type SyncingHandler struct {
	syncer isaac.Syncer
	*baseHandler
	newSyncer            func(base.Height) (isaac.Syncer, error)
	stuckcancel          func()
	whenFinishedf        func(base.Height)
	joinMemberlistf      func(context.Context, base.Suffrage) error
	leaveMemberlistf     func(time.Duration) error
	nodeInConsensusNodes isaac.NodeInConsensusNodesFunc
	waitStuckInterval    *util.Locked
	finishedLock         sync.RWMutex
	stuckcancellock      sync.RWMutex
}

type NewSyncingHandlerType struct {
	*SyncingHandler
}

func NewNewSyncingHandlerType(
	local base.LocalNode,
	policy *isaac.NodePolicy,
	proposalSelector isaac.ProposalSelector,
	newSyncer func(base.Height) (isaac.Syncer, error),
	nodeInConsensusNodes isaac.NodeInConsensusNodesFunc,
	joinMemberlistf func(context.Context, base.Suffrage) error,
	leaveMemberlistf func(time.Duration) error,
) *NewSyncingHandlerType {
	if nodeInConsensusNodes == nil {
		//revive:disable-next-line:modifies-parameter
		nodeInConsensusNodes = func(base.Node, base.Height) (base.Suffrage, bool, error) {
			return nil, false, errors.Errorf("empty consensus node")
		}
	}

	return &NewSyncingHandlerType{
		SyncingHandler: &SyncingHandler{
			baseHandler:          newBaseHandler(StateSyncing, local, policy, proposalSelector),
			newSyncer:            newSyncer,
			waitStuckInterval:    util.NewLocked(policy.IntervalBroadcastBallot()*2 + policy.WaitPreparingINITBallot()),
			whenFinishedf:        func(base.Height) {},
			nodeInConsensusNodes: nodeInConsensusNodes,
			joinMemberlistf:      joinMemberlistf,
			leaveMemberlistf:     leaveMemberlistf,
		},
	}
}

func (h *NewSyncingHandlerType) new() (handler, error) {
	return &SyncingHandler{
		baseHandler:          h.baseHandler.new(),
		newSyncer:            h.newSyncer,
		waitStuckInterval:    h.waitStuckInterval,
		whenFinishedf:        h.whenFinishedf,
		nodeInConsensusNodes: h.nodeInConsensusNodes,
		joinMemberlistf:      h.joinMemberlistf,
		leaveMemberlistf:     h.leaveMemberlistf,
	}, nil
}

func (st *SyncingHandler) enter(i switchContext) (func(), error) {
	e := util.StringErrorFunc("failed to enter syncing state")

	deferred, err := st.baseHandler.enter(i)
	if err != nil {
		return nil, e(err, "")
	}

	sctx, ok := i.(syncingSwitchContext)
	if !ok {
		return nil, e(nil, "invalid stateSwitchContext, not for syncing state; %T", i)
	}

	sc, err := st.newSyncer(sctx.height)
	if err != nil {
		return nil, e(err, "")
	}

	if sc == nil {
		return nil, e(nil, "empty syncer") // NOTE only for testing
	}

	if l, ok := sc.(logging.SetLogging); ok {
		_ = l.SetLogging(st.Logging)
	}

	st.syncer = sc

	if err := st.timers.StopTimersAll(); err != nil {
		return nil, e(err, "")
	}

	go st.finished(sc)

	return func() {
		deferred()
	}, nil
}

func (st *SyncingHandler) exit(sctx switchContext) (func(), error) {
	e := util.StringErrorFunc("failed to exit from syncing state")

	deferred, err := st.baseHandler.exit(sctx)
	if err != nil {
		return nil, e(err, "")
	}

	st.cancelstuck()

	if st.syncer != nil {
		if _, isfinished := st.syncer.IsFinished(); !isfinished {
			return nil, ignoreSwithingStateError.Errorf("syncer not yet finished")
		}

		switch err := st.syncer.Cancel(); {
		case err == nil:
		case errors.Is(err, SyncerCanNotCancelError):
			return nil, ignoreSwithingStateError.Wrap(err)
		default:
			return nil, e(err, "failed to stop syncer")
		}
	}

	return func() {
		deferred()
	}, nil
}

func (st *SyncingHandler) newVoteproof(vp base.Voteproof) error {
	e := util.StringErrorFunc("failed to handle new voteproof")

	if _, v := st.baseHandler.setNewVoteproof(vp); v == nil {
		return nil
	}

	if _, err := st.checkFinished(vp); err != nil {
		return e(err, "")
	}

	return nil
}

func (st *SyncingHandler) checkFinished(vp base.Voteproof) (done bool, _ error) {
	if vp == nil {
		return false, nil
	}

	st.finishedLock.Lock()
	defer st.finishedLock.Unlock()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	height := vp.Point().Height()

	if vp.Point().Stage() == base.StageINIT {
		height--
	}

	_ = st.add(height)

	top, isfinished := st.syncer.IsFinished()

	if isfinished {
		joined, err := st.checkAndJoinMemberlist(top + 1)
		if err != nil || !joined {
			return false, err
		}
	}

	switch {
	case vp.Point().Height() < top:
		return true, nil
	case vp.Point().Stage() == base.StageINIT && vp.Point().Height() == top+1:
		if !isfinished {
			l.Debug().Msg("expected init voteproof found; but not yet finished")

			return true, nil
		}

		// NOTE expected init voteproof found, moves to consensus state
		l.Debug().Msg("expected init voteproof found; moves to consensus state")

		return true, newConsensusSwitchContext(
			StateSyncing, vp.(base.INITVoteproof)) //nolint:forcetypeassert //...
	case isfinished && vp.Point().Stage() == base.StageACCEPT && vp.Point().Height() == top:
		st.newStuckCancel(vp)

		return true, nil
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

func (st *SyncingHandler) finished(sc isaac.Syncer) {
end:
	for {
		var err error
		select {
		case <-st.ctx.Done():
			return
		case <-sc.Done():
			_ = st.syncer.Cancel()

			if err = sc.Err(); err != nil {
				st.Log().Error().Err(err).Msg("syncer canceled by error")
			}
		case top := <-sc.Finished():
			st.Log().Debug().Interface("height", top).Msg("syncer finished")

			st.whenFinishedf(top)

			var done bool
			switch done, err = st.checkFinished(st.lastVoteproofs().Cap()); {
			case err != nil:
			case !done:
				st.cancelstuck()

				continue end
			default:
				continue end
			}
		}

		var sctx switchContext

		if err != nil && !errors.As(err, &sctx) {
			sctx = newBrokenSwitchContext(StateSyncing, err)
		}

		if sctx != nil {
			go st.switchState(sctx)
		}

		st.cancel()

		break
	}
}

func (st *SyncingHandler) cancelstuck() {
	st.stuckcancellock.RLock()
	defer st.stuckcancellock.RUnlock()

	if st.stuckcancel != nil {
		st.stuckcancel()
	}
}

func (st *SyncingHandler) newStuckCancel(vp base.Voteproof) {
	st.stuckcancellock.Lock()
	defer st.stuckcancellock.Unlock()

	if st.stuckcancel != nil {
		st.stuckcancel()
	}

	st.Log().Debug().Dur("wait", st.waitStuck()).Msg("will wait for stucked")

	ctx, cancel := context.WithTimeout(st.ctx, st.waitStuck())
	st.stuckcancel = cancel

	go func() {
		<-ctx.Done()

		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return
		}

		st.finishedLock.Lock()
		defer st.finishedLock.Unlock()

		lvp := st.lastVoteproofs().Cap()
		if lvp.Point().Compare(vp.Point()) != 0 {
			return
		}

		st.Log().Debug().Dur("wait", st.waitStuck()).Msg("stuck accept voteproof found; moves to joining state")

		// NOTE no more valid voteproof received, moves to joining state
		go st.switchState(newJoiningSwitchContext(StateSyncing, vp))
	}()
}

func (st *SyncingHandler) SetWhenFinished(f func(base.Height)) {
	st.whenFinishedf = f
}

func (st *SyncingHandler) waitStuck() time.Duration {
	i, _ := st.waitStuckInterval.Value()

	return i.(time.Duration) //nolint:forcetypeassert //...
}

func (st *SyncingHandler) checkAndJoinMemberlist(height base.Height) (joined bool, _ error) {
	switch suf, found, err := st.nodeInConsensusNodes(st.local, height); {
	case err != nil:
		st.Log().Error().Err(err).Msg("failed to get consensus nodes after syncer finished")

		return false, err
	case suf == nil:
		return false, newBrokenSwitchContext(StateSyncing, errors.Errorf("empty suffrage"))
	case !found:
		if err := st.leaveMemberlistf(time.Second); err != nil {
			st.Log().Error().Err(err).Msg("failed to leave memberilst; ignored")
		}

		st.Log().Debug().Msg("local is not in consensus nodes after syncer finished; keep syncing")

		return false, nil
	case suf.Exists(st.local.Address()) && suf.Len() < 2: //nolint:gomnd // local is alone in suffrage node
	default:
		ctx, cancel := context.WithTimeout(st.ctx, time.Second*10) //nolint:gomnd //...
		defer cancel()

		if err := st.joinMemberlistf(ctx, suf); err != nil {
			st.Log().Debug().Msg("local is in consensus nodes after syncer finished; but failed to join memberlist")

			return false, nil
		}

		st.Log().Debug().Msg("local is in consensus nodes after syncer finished; joined memberlist")
	}

	return true, nil
}

type syncingSwitchContext struct { //nolint:errname //...
	baseSwitchContext
	height base.Height
}

func newSyncingSwitchContext(from StateType, height base.Height) syncingSwitchContext {
	return syncingSwitchContext{
		baseSwitchContext: newBaseSwitchContext(from, StateSyncing),
		height:            height,
	}
}

func (syncingSwitchContext) Error() string {
	return ""
}

func (s syncingSwitchContext) MarshalZerologObject(e *zerolog.Event) {
	s.baseSwitchContext.MarshalZerologObject(e)

	e.Interface("height", s.height)
}
