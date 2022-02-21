package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type JoiningHandler struct {
	*baseStateHandler
	getLastManifest func() (base.Manifest, bool, error)
}

func NewJoiningHandler(
	local *LocalNode,
	policy Policy,
	proposalSelector ProposalSelector,
	getSuffrage func(base.Height) base.Suffrage,
	getLastManifest func() (base.Manifest, bool, error),
) *JoiningHandler {
	return &JoiningHandler{
		baseStateHandler: newBaseStateHandler(StateJoining, local, policy, proposalSelector, getSuffrage),
		getLastManifest:  getLastManifest,
	}
}

// BLOCK when stuck at init
// BLOCK when stuck at accept

func (st *JoiningHandler) enter(i stateSwitchContext) (func(), error) {
	// BLOCK stateSwitchContext has last voteproof and if not nil, process it.
	e := util.StringErrorFunc("failed to enter joining state")

	deferred, err := st.baseStateHandler.enter(i)
	if err != nil {
		return nil, e(err, "")
	}

	if _, ok := i.(joiningSwitchContext); !ok {
		return nil, e(nil, "invalid stateSwitchContext, not for joining state; %T", i)
	}

	if err := st.timers.StopTimersAll(); err != nil {
		return nil, e(err, "")
	}

	return func() {
		deferred()
	}, nil
}

func (st *JoiningHandler) exit(sctx stateSwitchContext) (func(), error) {
	e := util.StringErrorFunc("failed to exit from joining state")

	deferred, err := st.baseStateHandler.exit(sctx)
	if err != nil {
		return nil, e(err, "")
	}

	return func() {
		deferred()

		var timers []util.TimerID
		if sctx != nil {
			switch sctx.next() {
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
	e := util.StringErrorFunc("failed to handle new voteproof")

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	var manifest base.Manifest
	switch i, found, err := st.getLastManifest(); {
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
	}

	switch vp.Point().Stage() {
	case base.StageINIT:
		return st.newINITVoteproof(vp.(base.INITVoteproof), manifest)
	case base.StageACCEPT:
		return st.newACCEPTVoteproof(vp.(base.ACCEPTVoteproof), manifest)
	default:
		return e(nil, "invalid voteproof received, %T", vp)
	}
}

func (st *JoiningHandler) newINITVoteproof(ivp base.INITVoteproof, manifest base.Manifest) error {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(ivp)).Logger()

	switch expectedheight := manifest.Height() + 1; {
	case ivp.Point().Height() < expectedheight: // NOTE lower height; ignore
		return nil
	case ivp.Point().Height() > expectedheight: // NOTE higher height; moves to syncing state
		return newSyncingSwitchContext(StateJoining, ivp.Point().Height()-1)
	case ivp.Result() != base.VoteResultMajority:
		l.Debug().Msg("init voteproof not majroity; moves to next round")

		go st.nextRound(ivp, manifest.Hash())

		return nil
	case !ivp.Majority().(INITBallotFact).PreviousBlock().Equal(manifest.Hash()):
		l.Debug().Msg("previous block of init voteproof does tno match with last manifest; moves to syncing state")

		return newSyncingSwitchContext(StateJoining, ivp.Point().Height()-1)
	default:
		l.Debug().Msg("found valid init voteproof; moves to consensus state")

		return newConsensusSwitchContext(StateJoining, ivp)
	}
}

func (st *JoiningHandler) newACCEPTVoteproof(avp base.ACCEPTVoteproof, manifest base.Manifest) error {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Logger()

	switch expectedheight := manifest.Height() + 1; {
	case avp.Point().Height() < expectedheight: // NOTE lower height; ignore
		return nil
	case avp.Point().Height() > expectedheight: // NOTE higher height; moves to syncing state
		height := avp.Point().Height()
		if avp.Result() != base.VoteResultMajority {
			height = avp.Point().Height() - 1
		}

		return newSyncingSwitchContext(StateJoining, height)
	case avp.Result() == base.VoteResultMajority:
		return newSyncingSwitchContext(StateJoining, avp.Point().Height())
	default:
		l.Debug().Msg("init voteproof not majroity; moves to next round")

		go st.nextRound(avp, manifest.Hash())

		return nil
	}
}

type joiningSwitchContext struct {
	baseStateSwitchContext
}

func newJoiningSwitchContext(from StateType) joiningSwitchContext {
	return joiningSwitchContext{
		baseStateSwitchContext: newBaseStateSwitchContext(from, StateJoining),
	}
}
