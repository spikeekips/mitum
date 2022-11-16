package isaacstates

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
)

type JoiningHandler struct {
	*baseBallotHandler
	lastManifest         func() (base.Manifest, bool, error)
	joinMemberlistf      func(context.Context, base.Suffrage) error
	leaveMemberlistf     func(time.Duration) error
	nodeInConsensusNodes isaac.NodeInConsensusNodesFunc
	waitFirstVoteproof   time.Duration
	newvoteproofLock     sync.Mutex
}

type NewJoiningHandlerType struct {
	*JoiningHandler
}

func NewNewJoiningHandlerType(
	local base.LocalNode,
	params *isaac.LocalParams,
	proposalSelector isaac.ProposalSelector,
	lastManifest func() (base.Manifest, bool, error),
	nodeInConsensusNodes isaac.NodeInConsensusNodesFunc,
	voteFunc func(base.Ballot) (bool, error),
	joinMemberlistf func(context.Context, base.Suffrage) error,
	leaveMemberlistf func(time.Duration) error,
	svf SuffrageVotingFindFunc,
) *NewJoiningHandlerType {
	baseBallotHandler := newBaseBallotHandler(StateJoining, local, params, proposalSelector, svf)

	if voteFunc != nil {
		baseBallotHandler.voteFunc = preventVotingWithEmptySuffrage(voteFunc, local, nodeInConsensusNodes)
	}

	return &NewJoiningHandlerType{
		JoiningHandler: &JoiningHandler{
			baseBallotHandler:    baseBallotHandler,
			lastManifest:         lastManifest,
			waitFirstVoteproof:   params.IntervalBroadcastBallot()*2 + params.WaitPreparingINITBallot(),
			nodeInConsensusNodes: nodeInConsensusNodes,
			joinMemberlistf:      joinMemberlistf,
			leaveMemberlistf:     leaveMemberlistf,
		},
	}
}

func (h *NewJoiningHandlerType) new() (handler, error) {
	return &JoiningHandler{
		baseBallotHandler:    h.baseBallotHandler.new(),
		lastManifest:         h.lastManifest,
		waitFirstVoteproof:   h.waitFirstVoteproof,
		nodeInConsensusNodes: h.nodeInConsensusNodes,
		joinMemberlistf:      h.joinMemberlistf,
		leaveMemberlistf:     h.leaveMemberlistf,
	}, nil
}

func (st *JoiningHandler) enter(from StateType, i switchContext) (func(), error) {
	e := util.StringErrorFunc("failed to enter joining state")

	deferred, err := st.baseBallotHandler.enter(from, i)
	if err != nil {
		return nil, e(err, "")
	}

	jctx, ok := i.(joiningSwitchContext)
	if !ok {
		return nil, e(nil, "invalid stateSwitchContext, not for joining state; %T", i)
	}

	vp := jctx.vp
	lvp := st.lastVoteproofs().Cap()

	switch {
	case lvp == nil:
	case vp == nil:
		vp = lvp
	default:
		if lvp.Point().Point.Compare(vp.Point().Point) > 0 {
			vp = lvp
		}
	}

	var manifest base.Manifest

	switch m, found, err := st.lastManifest(); {
	case err != nil:
		return nil, e(err, "")
	case !found:
		return nil, e(nil, "last manifest not found")
	default:
		manifest = m
	}

	if err := st.checkSuffrage(manifest.Height()); err != nil {
		return nil, e(err, "")
	}

	return func() {
		deferred()

		if err := st.timers.StopTimersAll(); err != nil {
			st.Log().Error().Err(err).Msg("failed to stop all timers")
		}

		go st.firstVoteproof(vp, manifest)
	}, nil
}

func (st *JoiningHandler) exit(sctx switchContext) (func(), error) {
	e := util.StringErrorFunc("failed to exit from joining state")

	deferred, err := st.baseBallotHandler.exit(sctx)
	if err != nil {
		return nil, e(err, "")
	}

	return func() {
		deferred()

		var timers []util.TimerID

		if sctx != nil {
			switch sctx.next() { //nolint:exhaustive //...
			case StateConsensus, StateHandover:
				timers = []util.TimerID{timerIDBroadcastINITBallot}
			}
		}

		if len(timers) < 1 {
			if err := st.timers.StopTimersAll(); err != nil {
				st.Log().Error().Err(err).Msg("failed to stop timers; ignore")
			}
		} else if err := st.timers.StartTimers([]util.TimerID{
			timerIDBroadcastINITBallot, // NOTE keep broadcasting init ballot and stops others
		}, true); err != nil {
			st.Log().Error().Err(err).Msg("failed to start timers; ignore")
		}
	}, nil
}

func (st *JoiningHandler) newVoteproof(vp base.Voteproof) error {
	if _, _, isnew := st.baseBallotHandler.setNewVoteproof(vp); !isnew {
		return nil
	}

	return st.handleNewVoteproof(vp)
}

func (st *JoiningHandler) handleNewVoteproof(vp base.Voteproof) error {
	st.newvoteproofLock.Lock()
	defer st.newvoteproofLock.Unlock()

	e := util.StringErrorFunc("failed to handle new voteproof")

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	var manifest base.Manifest

	switch i, found, err := st.lastManifest(); {
	case err != nil:
		err = e(err, "failed to get last manifest")

		l.Error().Err(err).Msg("moves to syncing state")

		return newBrokenSwitchContext(StateJoining, err)
	case !found:
		l.Debug().Msg("empty last manifest; moves to syncing state")

		height := vp.Point().Height()
		if vp.Point().Stage() == base.StageINIT {
			height = vp.Point().Height() - 1
		}

		return newSyncingSwitchContext(StateJoining, height)
	default:
		manifest = i

		st.Log().Debug().Interface("last_manifest", manifest).Msg("new valid voteproof")
	}

	switch vp.Point().Stage() {
	case base.StageINIT:
		return st.newINITVoteproof(vp.(base.INITVoteproof), manifest) //nolint:forcetypeassert //...
	case base.StageACCEPT:
		return st.newACCEPTVoteproof(vp.(base.ACCEPTVoteproof), manifest) //nolint:forcetypeassert //...
	default:
		return e(nil, "invalid voteproof received, %T", vp)
	}
}

func (st *JoiningHandler) newINITVoteproof(ivp base.INITVoteproof, manifest base.Manifest) error {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(ivp)).Logger()

	switch expectedheight := manifest.Height() + 1; {
	case ivp.Point().Height() < expectedheight: // NOTE lower height; ignore
		return nil
	case ivp.Point().Height() > expectedheight:
		l.Debug().Msg("new init voteproof found; moves to syncing")

		return newSyncingSwitchContext(StateJoining, ivp.Point().Height()-1)
	case ivp.Result() != base.VoteResultMajority:
		l.Debug().Msg("init voteproof not majroity; moves to next round")

		go st.nextRound(ivp, manifest.Hash())

		return nil
	case !ivp.Majority().(isaac.INITBallotFact).PreviousBlock().Equal(manifest.Hash()): //nolint:forcetypeassert //...
		l.Debug().Msg("previous block of init voteproof does tno match with last manifest; moves to syncing state")

		return newSyncingSwitchContext(StateJoining, ivp.Point().Height()-1)
	default:
		l.Debug().Msg("found valid init voteproof; moves to consensus state")

		return newConsensusSwitchContext(StateJoining, ivp)
	}
}

func (st *JoiningHandler) newACCEPTVoteproof(avp base.ACCEPTVoteproof, manifest base.Manifest) error {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Logger()

	switch lastheight := manifest.Height(); {
	case avp.Point().Height() < lastheight+1: // NOTE lower height; ignore
		return nil
	case avp.Point().Height() > lastheight+1,
		avp.Point().Height() == lastheight+1 && avp.Result() == base.VoteResultMajority:
		l.Debug().Msg("new accept voteproof found; moves to syncing")

		height := avp.Point().Height()
		if avp.Result() != base.VoteResultMajority {
			height = avp.Point().Height() - 1
		}

		return newSyncingSwitchContext(StateJoining, height)
	default:
		l.Debug().Msg("init voteproof not majroity; moves to next round")

		go st.nextRound(avp, manifest.Hash())

		return nil
	}
}

func (st *JoiningHandler) checkSuffrage(height base.Height) error {
	suf, found, err := st.nodeInConsensusNodes(st.local, height+1)

	switch {
	case err != nil:
	case suf == nil:
		return newBrokenSwitchContext(StateJoining, errors.Errorf("empty suffrage"))
	case !found:
		if lerr := st.leaveMemberlistf(time.Second); lerr != nil {
			st.Log().Error().Err(lerr).Msg("failed to leave memberilst; ignored")
		}

		st.Log().Debug().Msg("local not in consensus nodes; moves to syncing")

		return newSyncingSwitchContext(StateJoining, height)
	case suf.Exists(st.local.Address()) && suf.Len() < 2: //nolint:gomnd // local is alone in suffrage node
		st.Log().Debug().Msg("local alone in consensus nodes; will not wait new voteproof")

		st.waitFirstVoteproof = 0
	default:
		switch jerr := st.joinMemberlist(suf); {
		case jerr != nil:
			st.Log().Error().Err(jerr).Msg("failed to join memberlist")
		default:
			st.Log().Debug().Msg("joined to memberlist")
		}
	}

	switch {
	case errors.Is(err, storage.ErrNotFound):
		st.Log().Debug().Interface("height", height+1).Msg("suffrage not found; moves to syncing")

		return newSyncingSwitchContext(StateJoining, height)
	case err != nil:
		return err
	default:
		return nil
	}
}

// firstVoteproof handles the voteproof, which is received before joining
// handler. It will help to prevent voting stuck. It waits for given
// time, if no incoming voteproof, process last voteproof.
func (st *JoiningHandler) firstVoteproof(lvp base.Voteproof, manifest base.Manifest) {
	if lvp == nil {
		return
	}

	st.Log().Debug().Dur("wait", st.waitFirstVoteproof).Msg("will wait new voteproof")

	select {
	case <-st.ctx.Done():
		return
	case <-time.After(st.waitFirstVoteproof):
	}

	switch nlvp := st.lastVoteproofs().Cap(); {
	case nlvp == nil:
	case nlvp.Point().Compare(lvp.Point()) > 0:
		return
	}

	st.Log().Debug().Msg("last voteproof found for firstVoteproof")

	// NOTE if no new voteproof and last accept voteproof looks good, broadcast
	// next init ballot
	switch avp, ok := lvp.(base.ACCEPTVoteproof); {
	case !ok:
	case avp.Result() != base.VoteResultMajority:
	case avp.Point().Height() != manifest.Height():
	default:
		st.Log().Debug().Msg("no more new voteproof; prepare next block")

		go st.nextBlock(avp)

		return
	}

	var dsctx switchContext

	switch err := st.handleNewVoteproof(lvp); {
	case err == nil:
	case !errors.As(err, &dsctx):
		st.Log().Error().Err(err).Dict("voteproof", base.VoteproofLog(lvp)).
			Msg("failed last voteproof after enter; ignore")
	default:
		go st.switchState(dsctx)
	}
}

func (st *JoiningHandler) nextRound(vp base.Voteproof, prevBlock util.Hash) {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	var sctx switchContext
	var bl base.INITBallot

	switch i, err := st.prepareNextRound(vp, prevBlock, st.nodeInConsensusNodes); {
	case err == nil:
		if i == nil {
			return
		}

		bl = i
	case errors.As(err, &sctx):
		go st.switchState(sctx)

		return
	default:
		l.Debug().Err(err).Msg("failed to prepare next round; moves to broken state")

		go st.switchState(newBrokenSwitchContext(StateJoining, err))

		return
	}

	switch _, err := st.vote(bl); {
	case err == nil:
	case errors.Is(err, errFailedToVoteNotInConsensus):
		l.Error().Err(err).Msg("failed to vote init ballot for next round; moves to syncing state")

		go st.switchState(newSyncingSwitchContext(StateJoining, vp.Point().Height()-1))

		return
	default:
		l.Error().Err(err).Msg("failed to vote init ballot for next round; moves to broken state")

		go st.switchState(newBrokenSwitchContext(StateJoining, err))

		return
	}

	if err := st.broadcastINITBallot(
		bl,
		func(i int, _ time.Duration) time.Duration {
			if i < 1 {
				return time.Nanosecond
			}

			return st.params.IntervalBroadcastBallot()
		},
	); err != nil {
		l.Error().Err(err).Msg("failed to broadcast init ballot for next round")

		return
	}

	if err := st.timers.StartTimers([]util.TimerID{timerIDBroadcastINITBallot}, true); err != nil {
		l.Error().Err(err).Msg("failed to start timers for broadcasting init ballot for next round")

		return
	}

	l.Debug().Interface("ballot", bl).Msg("init ballot broadcasted for next round")
}

func (st *JoiningHandler) nextBlock(avp base.ACCEPTVoteproof) {
	point := avp.Point().Point.NextHeight()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Object("point", point).Logger()

	var sctx switchContext
	var bl base.INITBallot

	switch i, err := st.prepareNextBlock(avp, st.nodeInConsensusNodes); {
	case err == nil:
		if i == nil {
			return
		}

		bl = i
	case errors.As(err, &sctx):
		go st.switchState(sctx)

		return
	default:
		l.Debug().Err(err).Msg("failed to prepare next block; moves to broken state")

		go st.switchState(newBrokenSwitchContext(StateJoining, err))

		return
	}

	switch _, err := st.vote(bl); {
	case err == nil:
	case errors.Is(err, errFailedToVoteNotInConsensus):
		l.Error().Err(err).Msg("failed to vote init ballot for next round; moves to syncing state")

		go st.switchState(newSyncingSwitchContext(StateJoining, avp.Point().Height()-1))

		return
	default:
		l.Error().Err(err).Msg("failed to vote init ballot for next block; moves to broken")

		go st.switchState(newBrokenSwitchContext(StateJoining, err))

		return
	}

	if err := st.broadcastINITBallot(
		bl,
		func(i int, _ time.Duration) time.Duration {
			if i < 1 {
				return time.Nanosecond
			}

			return st.params.IntervalBroadcastBallot()
		},
	); err != nil {
		l.Error().Err(err).Msg("failed to broadcast init ballot for next block")

		return
	}

	if err := st.timers.StartTimers([]util.TimerID{
		timerIDBroadcastINITBallot,
	}, true); err != nil {
		l.Error().Err(err).Msg("failed to start timers for broadcasting init ballot for next block")

		return
	}

	l.Debug().Interface("ballot", bl).Msg("next init ballot broadcasted")
}

func (st *JoiningHandler) joinMemberlist(suf base.Suffrage) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-st.ctx.Done():
			return st.ctx.Err()
		case <-ticker.C:
			switch err := st.joinMemberlistf(st.ctx, suf); {
			case err == nil:
				return nil
			case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
				return nil
			}
		}
	}
}

type joiningSwitchContext struct {
	vp base.Voteproof
	baseSwitchContext
}

func newJoiningSwitchContext(from StateType, vp base.Voteproof) joiningSwitchContext {
	return joiningSwitchContext{
		baseSwitchContext: newBaseSwitchContext(StateJoining, switchContextOKFuncCheckFrom(from)),
		vp:                vp,
	}
}
