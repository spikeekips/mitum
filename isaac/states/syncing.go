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
	newSyncer       func(base.Height) (isaac.Syncer, error)
	stuckcancel     func()
	whenFinishedf   func(base.Height)
	getSuffrage     isaac.GetSuffrageByBlockHeight
	waitStuck       time.Duration
	finishedLock    sync.RWMutex
	stuckcancellock sync.RWMutex
}

type NewSyncingHandlerType struct {
	*SyncingHandler
}

func NewNewSyncingHandlerType(
	local base.LocalNode,
	policy isaac.NodePolicy,
	proposalSelector isaac.ProposalSelector,
	newSyncer func(base.Height) (isaac.Syncer, error),
	getSuffrage isaac.GetSuffrageByBlockHeight,
) *NewSyncingHandlerType {
	if getSuffrage == nil {
		//revive:disable-next-line:modifies-parameter
		getSuffrage = func(nextheight base.Height) (base.Suffrage, bool, error) {
			return nil, false, errors.Errorf("empty getSuffrage")
		}
	}

	return &NewSyncingHandlerType{
		SyncingHandler: &SyncingHandler{
			baseHandler:   newBaseHandler(StateSyncing, local, policy, proposalSelector),
			newSyncer:     newSyncer,
			waitStuck:     policy.IntervalBroadcastBallot()*2 + policy.WaitProcessingProposal(),
			whenFinishedf: func(base.Height) {},
			getSuffrage:   getSuffrage,
		},
	}
}

func (h *NewSyncingHandlerType) new() (handler, error) {
	return &SyncingHandler{
		baseHandler:   h.baseHandler.new(),
		newSyncer:     h.newSyncer,
		waitStuck:     h.waitStuck,
		whenFinishedf: h.whenFinishedf,
		getSuffrage:   h.getSuffrage,
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
		return nil, e(nil, "empty syncer") // BlOCK remove; only for testing
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

	if err := st.checkFinished(vp); err != nil {
		return e(err, "")
	}

	return nil
}

func (st *SyncingHandler) checkFinished(vp base.Voteproof) error {
	if vp == nil {
		return nil
	}

	st.finishedLock.Lock()
	defer st.finishedLock.Unlock()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	top, isfinished := st.syncer.IsFinished()

	switch {
	case vp.Point().Height() < top:
		return nil
	case vp.Point().Stage() == base.StageINIT && vp.Point().Height() == top+1:
		if !isfinished {
			l.Debug().Msg("expected init voteproof found; but not yet finished")

			return nil
		}

		// NOTE expected init voteproof found, moves to consensus state
		l.Debug().Msg("expected init voteproof found; moves to syncing state")

		return newConsensusSwitchContext(
			StateSyncing, vp.(base.INITVoteproof)) //nolint:forcetypeassert //...
	case isfinished && vp.Point().Stage() == base.StageACCEPT && vp.Point().Height() == top:
		st.newStuckCancel(vp)

		return nil
	default:
		height := vp.Point().Height()
		if vp.Point().Stage() == base.StageINIT {
			height--
		}

		_ = st.add(height)

		return nil
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

			switch suf, found, eerr := st.getSuffrage(top + 1); {
			case eerr != nil:
				st.Log().Error().Err(eerr).Msg("failed to get suffrage after syncer finished")

				continue end
			case !found:
				st.Log().Error().Msg("suffrage not found after syncer finished")

				continue end
			case !suf.ExistsPublickey(st.local.Address(), st.local.Publickey()):
				// NOTE if local is not in suffrage, keep syncing
				st.Log().Debug().Msg("local is not in suffrage after syncer finished; keep syncing")

				continue end
			}

			st.whenFinishedf(top)

			st.cancelstuck()

			if err = st.checkFinished(st.lastVoteproofs().Cap()); err == nil {
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

	st.Log().Debug().Dur("wait", st.waitStuck).Msg("will wait for stucked")

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

func (st *SyncingHandler) SetWhenFinished(f func(base.Height)) {
	st.whenFinishedf = f
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
