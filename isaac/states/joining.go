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

type JoiningHandlerArgs struct {
	baseBallotHandlerArgs
	LastManifestFunc    func() (base.Manifest, bool, error)
	JoinMemberlistFunc  func(context.Context, base.Suffrage) error
	LeaveMemberlistFunc func(time.Duration) error
	WaitFirstVoteproof  time.Duration
}

func NewJoiningHandlerArgs(params *isaac.LocalParams) *JoiningHandlerArgs {
	return &JoiningHandlerArgs{
		baseBallotHandlerArgs: newBaseBallotHandlerArgs(),
		LastManifestFunc: func() (base.Manifest, bool, error) {
			return nil, false, util.ErrNotImplemented.Errorf("LastManifestFunc")
		},
		JoinMemberlistFunc: func(context.Context, base.Suffrage) error {
			return util.ErrNotImplemented.Errorf("JoinMemberlistFunc")
		},
		LeaveMemberlistFunc: func(time.Duration) error {
			return util.ErrNotImplemented.Errorf("LeaveMemberlistFunc")
		},
		WaitFirstVoteproof: params.IntervalBroadcastBallot()*2 + params.WaitPreparingINITBallot(),
	}
}

type JoiningHandler struct {
	baseBallotHandler
	args               *JoiningHandlerArgs
	waitFirstVoteproof time.Duration
	newvoteproofLock   sync.Mutex
}

type NewJoiningHandlerType struct {
	*JoiningHandler
}

func NewNewJoiningHandlerType(
	local base.LocalNode,
	params *isaac.LocalParams,
	args *JoiningHandlerArgs,
) *NewJoiningHandlerType {
	return &NewJoiningHandlerType{
		JoiningHandler: &JoiningHandler{
			baseBallotHandler:  newBaseBallotHandlerType(StateJoining, local, params, &args.baseBallotHandlerArgs),
			args:               args,
			waitFirstVoteproof: args.WaitFirstVoteproof,
		},
	}
}

func (h *NewJoiningHandlerType) new() (handler, error) {
	return &JoiningHandler{
		baseBallotHandler:  h.baseBallotHandler.new(),
		args:               h.args,
		waitFirstVoteproof: h.waitFirstVoteproof,
	}, nil
}

func (st *JoiningHandler) enter(from StateType, i switchContext) (func(), error) {
	e := util.StringErrorFunc("enter joining state")

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

	switch m, found, err := st.args.LastManifestFunc(); {
	case err != nil:
		return nil, e(err, "")
	case !found:
		return nil, e(nil, "last manifest not found")
	default:
		manifest = m
	}

	switch suf, err := st.checkSuffrage(manifest.Height()); {
	case err != nil:
		return nil, e(err, "")
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

	return func() {
		deferred()

		if err := st.timers.StopAllTimers(); err != nil {
			st.Log().Error().Err(err).Msg("failed to stop all timers")
		}

		go st.firstVoteproof(vp, manifest)
	}, nil
}

func (st *JoiningHandler) exit(sctx switchContext) (func(), error) {
	e := util.StringErrorFunc("exit from joining state")

	deferred, err := st.baseBallotHandler.exit(sctx)
	if err != nil {
		return nil, e(err, "")
	}

	return deferred, nil
}

func (st *JoiningHandler) newVoteproof(vp base.Voteproof) error {
	allowedConsensus := st.allowedConsensus()

	if !allowedConsensus {
		return newSyncingSwitchContextWithVoteproof(StateJoining, vp)
	}

	if _, _, isnew := st.baseBallotHandler.setNewVoteproof(vp); !isnew {
		return nil
	}

	st.Log().Debug().Func(base.VoteproofLogFunc("voteproof", vp)).Msg("new voteproof")

	if st.resolver != nil {
		st.resolver.Cancel(vp.Point())
	}

	var sctx switchContext

	switch err := st.handleNewVoteproof(vp); {
	case err == nil:
		if !allowedConsensus {
			return newSyncingSwitchContextWithVoteproof(StateJoining, vp)
		}

		return nil
	case errors.As(err, &sctx):
		if sctx.next() == StateConsensus {
			if _, err = st.checkSuffrage(vp.Point().Height().SafePrev()); err != nil {
				return err
			}
		}

		if !allowedConsensus {
			return newSyncingSwitchContextWithVoteproof(StateJoining, vp)
		}

		return sctx
	default:
		return err
	}
}

func (st *JoiningHandler) handleNewVoteproof(vp base.Voteproof) error {
	st.newvoteproofLock.Lock()
	defer st.newvoteproofLock.Unlock()

	e := util.StringErrorFunc("handle new voteproof")

	l := st.Log().With().Str("voteproof", vp.ID()).Logger()

	var manifest base.Manifest

	switch i, found, err := st.args.LastManifestFunc(); {
	case err != nil:
		err = e(err, "get last manifest")

		l.Error().Err(err).Msg("moves to syncing state")

		return newBrokenSwitchContext(StateJoining, err)
	case !found:
		l.Debug().Msg("empty last manifest; moves to syncing state")

		return newSyncingSwitchContextWithVoteproof(StateJoining, vp)
	default:
		manifest = i

		st.Log().Debug().Interface("last_manifest", manifest).Msg("new valid voteproof")
	}

	switch keep, err := st.checkStuckVoteproof(vp, manifest); {
	case err != nil:
		return err
	case !keep:
		return nil
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
	l := st.Log().With().Str("voteproof", ivp.ID()).Logger()

	var prevblock util.Hash
	if ivp.Majority() != nil {
		prevblock = ivp.Majority().(isaac.INITBallotFact).PreviousBlock() //nolint:forcetypeassert //...
	}

	switch expectedheight := manifest.Height() + 1; {
	case ivp.Point().Height() < expectedheight: // NOTE lower height; ignore
		return nil
	case ivp.Point().Height() > expectedheight:
		l.Debug().Msg("new init voteproof found; moves to syncing")

		return newSyncingSwitchContextWithVoteproof(StateJoining, ivp)
	case prevblock != nil && !prevblock.Equal(manifest.Hash()):
		l.Debug().Msg("previous block of init voteproof does tno match with last manifest; moves to syncing state")

		return newSyncingSwitchContextWithVoteproof(StateJoining, ivp)
	default:
		l.Debug().Msg("found valid init voteproof; moves to consensus state")

		sctx, err := newConsensusSwitchContext(StateJoining, ivp)
		if err != nil {
			return err
		}

		return sctx
	}
}

func (st *JoiningHandler) newACCEPTVoteproof(avp base.ACCEPTVoteproof, manifest base.Manifest) error {
	l := st.Log().With().Str("voteproof", avp.ID()).Logger()

	switch lastheight := manifest.Height(); {
	case avp.Point().Height() < lastheight+1: // NOTE lower height; ignore
		return nil
	case avp.Point().Height() > lastheight+1,
		avp.Point().Height() == lastheight+1 && avp.Result() == base.VoteResultMajority:
		l.Debug().Msg("new accept voteproof found; moves to syncing")

		return newSyncingSwitchContextWithVoteproof(StateJoining, avp)
	default:
		l.Debug().Msg("found valid accept voteproof; moves to consensus state")

		sctx, err := newConsensusSwitchContext(StateJoining, avp)
		if err != nil {
			return err
		}

		return sctx
	}
}

func (st *JoiningHandler) checkSuffrage(height base.Height) (base.Suffrage, error) {
	suf, found, err := st.args.NodeInConsensusNodesFunc(st.local, height)

	switch {
	case err != nil:
	case !found:
		if lerr := st.args.LeaveMemberlistFunc(time.Second); lerr != nil {
			st.Log().Error().Err(lerr).Msg("failed to leave memberilst; ignored")
		}

		st.Log().Debug().Msg("local not in consensus nodes; moves to syncing")

		return nil, newSyncingSwitchContext(StateJoining, height)
	case suf == nil:
		return nil, newBrokenSwitchContext(StateJoining, errors.Errorf("empty suffrage"))
	}

	switch {
	case errors.Is(err, storage.ErrNotFound):
		st.Log().Debug().Interface("height", height+1).Msg("suffrage not found; moves to syncing")

		return nil, newSyncingSwitchContext(StateJoining, height)
	case err != nil:
		return nil, err
	default:
		return suf, nil
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
		st.Log().Error().Err(err).Func(base.VoteproofLogFunc("voteproof", lvp)).
			Msg("failed last voteproof after enter; ignore")
	default:
		go st.switchState(dsctx)
	}
}

func (st *JoiningHandler) joinMemberlist(suf base.Suffrage) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-st.ctx.Done():
			return st.ctx.Err()
		case <-ticker.C:
			switch err := st.args.JoinMemberlistFunc(st.ctx, suf); {
			case err == nil:
				return nil
			case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
				return nil
			}
		}
	}
}

func (st *JoiningHandler) checkStuckVoteproof(
	vp base.Voteproof,
	lastManifest base.Manifest,
) (bool, error) {
	if _, ok := vp.(base.StuckVoteproof); !ok {
		return true, nil
	}

	l := st.Log().With().
		Interface("last_manifest", lastManifest).
		Logger()

	lastHeight := lastManifest.Height()

	switch {
	case vp.Point().Height() == lastHeight+1:
		l.Debug().
			Func(base.VoteproofLogFunc("init_voteproof", vp)).
			Msg("found valid stuck voteproof; moves to consensus state")

		sctx, err := newConsensusSwitchContext(StateJoining, vp)
		if err != nil {
			return false, err
		}

		return false, sctx
	case vp.Point().Height() > lastHeight+1:
		l.Debug().
			Func(base.VoteproofLogFunc("init_voteproof", vp)).
			Msg("higher init stuck voteproof; moves to syncing state")

		return false, newSyncingSwitchContextWithVoteproof(StateJoining, vp)
	default:
		return false, nil
	}
}

func (st *JoiningHandler) nextBlock(avp base.ACCEPTVoteproof) {
	point := avp.Point().Point.NextHeight()

	l := st.Log().With().Str("voteproof", avp.ID()).Object("point", point).Logger()

	var suf base.Suffrage

	var sctx switchContext

	switch i, err := st.localIsInConsensusNodes(avp.Point().Height()); {
	case errors.As(err, &sctx):
		go st.switchState(sctx)

		return
	case err != nil:
		l.Debug().Err(err).Msg("failed to prepare next block; moves to broken state")

		go st.switchState(newBrokenSwitchContext(StateConsensus, err))
	default:
		suf = i
	}

	if err := st.defaultPrepareNextBlockBallot(avp, []util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastACCEPTBallot,
	},
		suf,
		st.params.WaitPreparingINITBallot(),
	); err != nil {
		l.Debug().Err(err).Msg("failed to prepare next block ballot")

		return
	}
}

func (st *JoiningHandler) whenSetAllowConsensus(allow bool) { // revive:disable-line:flag-parameter
	st.baseBallotHandler.whenSetAllowConsensus(allow)

	if !allow {
		go st.switchState(emptySyncingSwitchContext(StateJoining))
	}
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

func (sctx joiningSwitchContext) voteproof() base.Voteproof {
	return sctx.vp
}
