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
)

var SyncerCanNotCancelError = util.NewError("can not cancel syncer")

type SyncingHandler struct {
	syncer isaac.Syncer
	*baseHandler
	newSyncer       func(base.Height) (isaac.Syncer, error)
	stuckcancel     func()
	waitStuck       time.Duration
	finishedLock    sync.RWMutex
	stuckcancellock sync.RWMutex
}

func NewSyncingHandler(
	local base.LocalNode,
	policy isaac.NodePolicy,
	proposalSelector isaac.ProposalSelector,
	newSyncer func(base.Height) (isaac.Syncer, error),
) *SyncingHandler {
	return &SyncingHandler{
		baseHandler: newBaseHandler(StateSyncing, local, policy, proposalSelector),
		newSyncer:   newSyncer,
		waitStuck:   policy.IntervalBroadcastBallot()*2 + policy.WaitProcessingProposal(),
	}
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
		return nil, e(nil, "empty syncer") // BlOCK remove; only for testing
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

		st.syncer = nil
	}

	deferred, err := st.baseHandler.exit(sctx)
	if err != nil {
		return nil, e(err, "")
	}

	return func() {
		deferred()
	}, nil
}

func (st *SyncingHandler) newVoteproof(vp base.Voteproof) error {
	e := util.StringErrorFunc("failed to handle new voteproof")

	if _, _, err := st.checkFinished(vp); err != nil {
		return e(err, "")
	}

	return nil
}

func (st *SyncingHandler) checkFinished(vp base.Voteproof) (added bool, isfinished bool, _ error) {
	st.finishedLock.Lock()
	defer st.finishedLock.Unlock()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	top, isfinished := st.syncer.IsFinished()

	switch {
	case vp.Point().Height() <= top:
		return false, isfinished, nil
	case vp.Point().Stage() == base.StageINIT && vp.Point().Height() == top+1:
		if !isfinished {
			l.Debug().Msg("expected init voteproof found; but not yet finished")

			return false, isfinished, nil
		}

		// NOTE expected init voteproof found, moves to consensus state
		l.Debug().Msg("expected init voteproof found; moves to syncing state")

		return false, isfinished,
			newConsensusSwitchContext(StateSyncing, vp.(base.INITVoteproof)) //nolint:forcetypeassert //...
	default:
		height := vp.Point().Height()
		if vp.Point().Stage() == base.StageINIT {
			height--
		}

		return st.add(height), isfinished, nil
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
			err = sc.Err()
		case top := <-sc.Finished():
			st.Log().Debug().Interface("height", top).Msg("syncer finished")

			st.cancelstuck()

			lvp := st.lastVoteproofs().Cap()
			if lvp == nil {
				continue
			}

			var added, isfinished bool
			if added, isfinished, err = st.checkFinished(lvp); err == nil {
				if isfinished && !added && lvp.Point().Height() == top && lvp.Point().Stage() == base.StageACCEPT {
					st.newStuckCancel(lvp)
				}

				continue end
			}
		}

		var sctx switchContext

		if !errors.As(err, &sctx) {
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

	ctx, cancel := context.WithTimeout(st.ctx, st.waitStuck)
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

		st.Log().Debug().Dur("wait", st.waitStuck).Msg("stuck accept voteproof found; moves to joining state")

		// NOTE no more valid voteproof received, moves to joining state
		go st.switchState(newJoiningSwitchContext(StateSyncing, vp))
	}()
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
