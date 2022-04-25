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

type SyncingHandler struct {
	*baseHandler
	newSyncer       func() Syncer
	syncer          Syncer
	finishedLock    sync.RWMutex
	stuckcancel     func()
	stuckcancellock sync.RWMutex
	waitStuck       time.Duration
}

func NewSyncingHandler(
	local base.LocalNode,
	policy isaac.NodePolicy,
	proposalSelector isaac.ProposalSelector,
	newSyncer func() Syncer,
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

	sc := st.newSyncer()

	st.syncer = sc
	_ = st.add(sctx.height)

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
		if !st.syncer.IsFinished() {
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

	if _, err := st.checkFinished(vp); err != nil {
		if _, ok := err.(switchContext); ok { // nolint:errorlint
			return err
		}

		return e(err, "")
	}

	return nil
}

func (st *SyncingHandler) checkFinished(vp base.Voteproof) (bool, error) {
	st.finishedLock.Lock()
	defer st.finishedLock.Unlock()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	top := st.syncer.Top()

	switch {
	case vp.Point().Height() <= top:
		return false, nil
	case vp.Point().Stage() == base.StageINIT && vp.Point().Height() == top+1:
		if !st.syncer.IsFinished() {
			l.Debug().Msg("expected init voteproof found; but not yet finished")

			return false, nil
		}

		// NOTE expected init voteproof found, moves to consensus state
		l.Debug().Msg("expected init voteproof found; moves to syncing state")

		return false, newConsensusSwitchContext(StateSyncing, vp.(base.INITVoteproof))
	default:
		height := vp.Point().Height()
		if vp.Point().Stage() == base.StageINIT {
			height--
		}

		_ = st.add(height)

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

func (st *SyncingHandler) finished(sc Syncer) {
end:
	for {
		select {
		case <-st.ctx.Done():
			return
		case h := <-sc.Finished():
			st.Log().Debug().Object("height", h).Msg("syncer finished")

			st.cancelstuck()

			lvp := st.lastVoteproof().cap()
			if lvp == nil {
				continue
			}

			var sctx switchContext
			switch added, err := st.checkFinished(lvp); {
			case err == nil:
				if !added && lvp.Point().Height() == sc.Top() && lvp.Point().Stage() == base.StageACCEPT {
					st.newStuckCancel(lvp)
				}

				continue
			case !errors.As(err, &sctx):
				sctx = newBrokenSwitchContext(StateSyncing, err)
			}

			go st.switchState(sctx)

			st.cancel()

			break end
		}
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

		lvp := st.lastVoteproof().cap()
		if lvp.Point().Compare(vp.Point()) != 0 {
			return
		}

		st.Log().Debug().Dur("wait", st.waitStuck).Msg("stuck accept voteproof found; moves to joining state")

		// NOTE no more valid voteproof received, moves to joining state
		go st.switchState(newJoiningSwitchContext(StateSyncing, vp))
	}()
}

type syncingSwitchContext struct {
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

	e.Object("height", s.height)
}
