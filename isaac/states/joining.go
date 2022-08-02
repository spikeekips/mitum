package isaacstates

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type JoiningHandler struct {
	*baseHandler
	lastManifest         func() (base.Manifest, bool, error)
	joinMemberlist       func() error
	nodeInConsensusNodes isaac.NodeInConsensusNodesFunc
	waitFirstVoteproof   time.Duration
	newvoteproofLock     sync.Mutex
}

type NewJoiningHandlerType struct {
	*JoiningHandler
}

func NewNewJoiningHandlerType(
	local base.LocalNode,
	policy *isaac.NodePolicy,
	proposalSelector isaac.ProposalSelector,
	lastManifest func() (base.Manifest, bool, error),
	nodeInConsensusNodes isaac.NodeInConsensusNodesFunc,
	voteFunc func(base.Ballot) (bool, error),
	joinMemberlist func() error,
) *NewJoiningHandlerType {
	baseHandler := newBaseHandler(StateJoining, local, policy, proposalSelector)

	if voteFunc != nil {
		baseHandler.voteFunc = preventVotingWithEmptySuffrage(voteFunc, local, nodeInConsensusNodes)
	}

	return &NewJoiningHandlerType{
		JoiningHandler: &JoiningHandler{
			baseHandler:          baseHandler,
			lastManifest:         lastManifest,
			waitFirstVoteproof:   policy.IntervalBroadcastBallot()*2 + policy.WaitPreparingINITBallot(),
			nodeInConsensusNodes: nodeInConsensusNodes,
			joinMemberlist:       joinMemberlist,
		},
	}
}

func (h *NewJoiningHandlerType) new() (handler, error) {
	return &JoiningHandler{
		baseHandler:          h.baseHandler.new(),
		lastManifest:         h.lastManifest,
		waitFirstVoteproof:   h.waitFirstVoteproof,
		nodeInConsensusNodes: h.nodeInConsensusNodes,
		joinMemberlist:       h.joinMemberlist,
	}, nil
}

func (st *JoiningHandler) enter(i switchContext) (func(), error) {
	e := util.StringErrorFunc("failed to enter joining state")

	deferred, err := st.baseHandler.enter(i)
	if err != nil {
		return nil, e(err, "")
	}

	jctx, ok := i.(joiningSwitchContext)
	if !ok {
		return nil, e(nil, "invalid stateSwitchContext, not for joining state; %T", i)
	}

	if err := st.timers.StopTimersAll(); err != nil {
		return nil, e(err, "")
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

	switch suf, found, err := st.nodeInConsensusNodes(st.local, manifest.Height()+1); {
	case err != nil:
		return nil, e(err, "")
	case suf == nil:
		return nil, newBrokenSwitchContext(StateJoining, errors.Errorf("empty suffrage"))
	case !found:
		st.Log().Debug().Msg("local not in consensus nodes; moves to syncing")

		return nil, newSyncingSwitchContext(StateEmpty, manifest.Height())
	case suf.Len() < 2: //nolint:gomnd //...
		st.Log().Debug().Msg("local alone in consensus nodes; will not wait new voteproof")

		st.waitFirstVoteproof = 0
	default:
		// NOTE if not joined yet, join first
		if err := st.joinMemberlist(); err != nil {
			st.Log().Error().Err(err).Msg("failed to join memberlist; ignored")
		}

		st.Log().Debug().Msg("joined to memberlist")
	}

	return func() {
		deferred()

		go st.firstVoteproof(vp, manifest)
	}, nil
}

func (st *JoiningHandler) exit(sctx switchContext) (func(), error) {
	e := util.StringErrorFunc("failed to exit from joining state")

	deferred, err := st.baseHandler.exit(sctx)
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
	st.newvoteproofLock.Lock()
	defer st.newvoteproofLock.Unlock()

	e := util.StringErrorFunc("failed to handle new voteproof")

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	_, _ = st.baseHandler.setNewVoteproof(vp)

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

	switch err := st.newVoteproof(lvp); {
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

	switch i, err := st.prepareNextRound(vp, prevBlock); {
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
	case errors.Is(err, errNotInConsensusNodes):
		l.Error().Err(err).Msg("failed to vote init ballot for next round; moves to syncing state")

		go st.switchState(newSyncingSwitchContext(StateJoining, vp.Point().Height()-1))

		return
	default:
		l.Error().Err(err).Msg("failed to vote init ballot for next round; moves to broken state")

		go st.switchState(newBrokenSwitchContext(StateJoining, err))

		return
	}

	if err := st.broadcastINITBallot(bl, time.Nanosecond); err != nil {
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
	case errors.Is(err, errNotInConsensusNodes):
		l.Error().Err(err).Msg("failed to vote init ballot for next round; moves to syncing state")

		go st.switchState(newSyncingSwitchContext(StateJoining, avp.Point().Height()-1))

		return
	default:
		l.Error().Err(err).Msg("failed to vote init ballot for next block; moves to broken")

		go st.switchState(newBrokenSwitchContext(StateJoining, err))

		return
	}

	if err := st.broadcastINITBallot(bl, time.Nanosecond); err != nil {
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

type joiningSwitchContext struct {
	vp base.Voteproof
	baseSwitchContext
}

func newJoiningSwitchContext(from StateType, vp base.Voteproof) joiningSwitchContext {
	return joiningSwitchContext{
		baseSwitchContext: newBaseSwitchContext(from, StateJoining),
		vp:                vp,
	}
}
