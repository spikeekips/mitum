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
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

type voteproofWithErrchan struct {
	vp    base.Voteproof
	errch chan error
}

type voteproofHandlerArgs struct {
	baseBallotHandlerArgs
	ProposalProcessors           *isaac.ProposalProcessors
	GetManifestFunc              func(base.Height) (base.Manifest, error)
	WhenNewBlockSaved            func(base.BlockMap)
	WhenNewBlockConfirmed        func(base.Height)
	whenNewVoteproof             func(base.Voteproof) error
	prepareACCEPTBallot          func(base.INITVoteproof, base.Manifest, time.Duration) error
	prepareNextRoundBallot       func(base.Voteproof, util.Hash, []util.TimerID, base.Suffrage, time.Duration) error
	prepareSuffrageConfirmBallot func(base.Voteproof)
	prepareNextBlockBallot       func(base.ACCEPTVoteproof, []util.TimerID, base.Suffrage, time.Duration) error
	checkInState                 func(base.Voteproof) switchContext
	whenNewBlockSaved            func(base.BlockMap, base.ACCEPTVoteproof)
	stt                          StateType
}

func newVoteproofHandlerArgs() voteproofHandlerArgs {
	args := voteproofHandlerArgs{
		baseBallotHandlerArgs: newBaseBallotHandlerArgs(),
		GetManifestFunc: func(base.Height) (base.Manifest, error) {
			return nil, util.ErrNotImplemented.Errorf("GetManifestFunc")
		},
		WhenNewBlockSaved:     func(base.BlockMap) {},
		WhenNewBlockConfirmed: func(base.Height) {},

		whenNewVoteproof: func(base.Voteproof) error {
			return util.ErrNotImplemented.Errorf("newVoteproof")
		},
		prepareACCEPTBallot: func(base.INITVoteproof, base.Manifest, time.Duration) error {
			return util.ErrNotImplemented.Errorf("prepareACCEPTBallot")
		},
		prepareNextRoundBallot: func(base.Voteproof, util.Hash, []util.TimerID, base.Suffrage, time.Duration) error {
			return util.ErrNotImplemented.Errorf("prepareNextRoundBallot")
		},
		prepareNextBlockBallot: func(base.ACCEPTVoteproof, []util.TimerID, base.Suffrage, time.Duration) error {
			return util.ErrNotImplemented.Errorf("prepareNextRoundBallot")
		},
		whenNewBlockSaved: func(base.BlockMap, base.ACCEPTVoteproof) {},
	}

	args.checkInState = func(vp base.Voteproof) switchContext {
		return newSyncingSwitchContextWithVoteproof(args.stt, vp)
	}

	return args
}

type voteproofHandler struct {
	baseBallotHandler
	args                *voteproofHandlerArgs
	vpch                chan voteproofWithErrchan
	notallowconsensusch chan struct{}
	vplock              sync.Mutex
}

func newVoteproofHandler(
	stateType StateType,
	local base.LocalNode,
	params *isaac.LocalParams,
	args *voteproofHandlerArgs,
) *voteproofHandler {
	args.stt = stateType

	return &voteproofHandler{
		baseBallotHandler: newBaseBallotHandlerType(stateType, local, params, &args.baseBallotHandlerArgs),
		args:              args,
	}
}

func (st *voteproofHandler) new() *voteproofHandler {
	nst := &voteproofHandler{
		baseBallotHandler:   st.baseBallotHandler.new(),
		args:                st.args,
		vpch:                make(chan voteproofWithErrchan, 1<<6), // enough buffer
		notallowconsensusch: make(chan struct{}, 1<<6),             // enough buffer
	}

	nst.args.whenNewVoteproof = func(base.Voteproof) error { return nil }
	nst.args.prepareACCEPTBallot = nst.defaultPrepareACCEPTBallot
	nst.args.prepareNextRoundBallot = nst.defaultPrepareNextRoundBallot
	nst.args.prepareSuffrageConfirmBallot = nst.defaultPrepareSuffrageConfirmBallot
	nst.args.prepareNextBlockBallot = nst.defaultPrepareNextBlockBallot

	return nst
}

func (st *voteproofHandler) enter(from StateType, i switchContext) (func(), error) {
	e := util.StringErrorFunc("enter state")

	deferred, err := st.baseBallotHandler.enter(from, i)
	if err != nil {
		return nil, e(err, "")
	}

	var sctx voteproofSwitchContext
	var vp base.Voteproof

	switch j, ok := i.(consensusSwitchContext); {
	case !ok:
		return nil, e(nil, "invalid switchContext, not %T", i)
	case j.voteproof() == nil:
		return nil, e(nil, "invalid switchContext, empty voteproof")
	default:
		sctx = j
		vp = sctx.voteproof()
	}

	switch suf, found, err := st.args.NodeInConsensusNodesFunc(
		st.local, vp.Point().Height().SafePrev()); {
	case errors.Is(err, storage.ErrNotFound):
		st.Log().Debug().
			Dict("state_context", switchContextLog(sctx)).
			Interface("height", vp.Point().Height()).
			Msg("suffrage not found at entering state; moves to syncing state")

		return nil, newSyncingSwitchContextWithVoteproof(st.stt, vp)
	case err != nil:
		return nil, e(err, "")
	case suf == nil || suf.Len() < 1:
		return nil, e(nil, "empty suffrage of init voteproof")
	case !found:
		st.Log().Debug().
			Dict("state_context", switchContextLog(sctx)).
			Interface("height", vp.Point().Height()).
			Msg("local is not in consensus nodes at entering state; moves to syncing state")

		return nil, newSyncingSwitchContextWithVoteproof(st.stt, vp)
	}

	switch lvps, found := st.voteproofs(vp.Point()); {
	case !found:
		return nil, e(nil, "last voteproofs not found")
	default:
		st.vplock.Lock()

		return func() {
			deferred()

			go st.startch()

			defer st.vplock.Unlock()

			var nsctx switchContext

			switch err := st.newVoteproofWithLVPS(vp, lvps); {
			case err == nil:
			case !errors.As(err, &nsctx):
				st.Log().Error().Err(err).Msg("failed to process enter voteproof; moves to broken state")

				go st.switchState(newBrokenSwitchContext(st.stt, err))
			default:
				go st.switchState(nsctx)
			}
		}, nil
	}
}

func (st *voteproofHandler) exit(sctx switchContext) (func(), error) {
	e := util.StringErrorFunc("exit")

	deferred, err := st.baseBallotHandler.exit(sctx)
	if err != nil {
		return nil, e(err, "")
	}

	if err := st.args.ProposalProcessors.Cancel(); err != nil {
		return nil, e(err, "cancel proposal processors")
	}

	return deferred, nil
}

func (st *voteproofHandler) startch() {
end:
	for {
		var vperr voteproofWithErrchan

		select {
		case <-st.ctx.Done():
			return
		case vperr = <-st.vpch:
		case <-st.notallowconsensusch:
		}

		if sctx := st.args.checkInState(vperr.vp); sctx != nil {
			if vperr.vp != nil {
				vperr.errch <- sctx

				continue end
			}

			go st.switchState(sctx)

			continue end
		}

		if vperr.vp != nil {
			vperr.errch <- st.handleNewVoteproof(vperr.vp)
		}

		if sctx := st.args.checkInState(vperr.vp); sctx != nil {
			go st.switchState(sctx)

			continue end
		}
	}
}

func (st *voteproofHandler) processProposalFunc(ivp base.INITVoteproof) (func(context.Context) error, error) {
	facthash := ivp.BallotMajority().Proposal()
	l := st.Log().With().Stringer("fact", facthash).Logger()
	l.Debug().Msg("trying to process proposal")

	e := util.StringErrorFunc("process proposal")

	var process isaac.ProcessorProcessFunc

	switch i, err := st.processProposalInternal(ivp); {
	case err == nil:
		if i == nil {
			l.Debug().Msg("empty manifest; ignore")

			return nil, nil
		}

		process = i
	case errors.Is(err, context.Canceled),
		errors.Is(err, isaac.ErrNotProposalProcessorProcessed):
		// NOTE instead of moving next round, intended-wrong accept ballot.
		return func(ctx context.Context) error {
				return st.wrongACCEPTBallot(ctx, ivp)
			},
			nil
	default:
		err = e(err, "")

		l.Error().Err(err).Msg("failed to process proposal; moves to broken state")

		return nil, newBrokenSwitchContext(st.stt, err)
	}

	return func(ctx context.Context) error {
		manifest, err := process(ctx)

		switch {
		case errors.Is(err, context.Canceled),
			errors.Is(err, isaac.ErrNotProposalProcessorProcessed):
			if eerr := st.wrongACCEPTBallot(ctx, ivp); eerr != nil {
				return e(eerr, "")
			}

			return nil
		case err != nil:
			return e(err, "")
		case manifest == nil:
			return nil
		}

		eavp := st.lastVoteproofs().ACCEPT()

		if err := st.args.prepareACCEPTBallot(ivp, manifest, time.Nanosecond); err != nil {
			l.Error().Err(err).Msg("failed to prepare accept ballot")

			return e(err, "")
		}

		if eavp == nil || !eavp.Point().Point.Equal(ivp.Point().Point) {
			return nil
		}

		ll := l.With().Str("accept_voteproof_id", eavp.ID()).Logger()

		var sctx switchContext

		switch saved, err := st.handleACCEPTVoteproofAfterProcessingProposal(manifest, eavp); {
		case saved:
			ll.Debug().Msg("new block saved by accept voteproof after processing proposal")
		case err == nil:
			return nil
		case errors.As(err, &sctx):
		default:
			ll.Error().Err(err).Msg("failed to save new block by accept voteproof after processing proposal")

			sctx = newBrokenSwitchContext(st.stt, errors.Wrap(err, "save proposal"))
		}

		return sctx
	}, nil
}

func (st *voteproofHandler) processProposal(ivp base.INITVoteproof) (func(), error) {
	f, err := st.processProposalFunc(ivp)

	switch {
	case err != nil:
		return nil, err
	case f == nil:
		return func() {}, nil
	}

	return func() {
		var sctx switchContext

		switch err := f(st.ctx); {
		case err == nil:
		case errors.As(err, &sctx):
			go st.switchState(sctx)
		default:
			go st.switchState(newBrokenSwitchContext(st.stt, err))
		}
	}, nil
}

func (st *voteproofHandler) processProposalInternal(ivp base.INITVoteproof) (isaac.ProcessorProcessFunc, error) {
	e := util.StringErrorFunc("process proposal")

	facthash := ivp.BallotMajority().Proposal()

	var previous base.Manifest

	switch m, err := st.args.GetManifestFunc(ivp.Point().Height() - 1); {
	case err != nil:
		return nil, e(err, "")
	default:
		previous = m
	}

	switch process, err := st.args.ProposalProcessors.Process(st.ctx, ivp.Point().Point, facthash, previous, ivp); {
	case err != nil:
		return nil, e(err, "")
	case process == nil:
		return nil, nil
	default:
		return func(ctx context.Context) (base.Manifest, error) {
			switch manifest, err := process(ctx); {
			case err != nil:
				st.Log().Error().Err(err).Msg("failed to process proposal")

				if errors.Is(err, context.Canceled) {
					return nil, err
				}

				if err0 := st.args.ProposalProcessors.Cancel(); err0 != nil {
					return nil, e(err0, "cancel proposal processors")
				}

				return nil, err
			case manifest == nil:
				st.Log().Debug().Msg("empty manifest; already processed")

				return nil, nil
			default:
				st.Log().Debug().Msg("proposal processed")

				return manifest, nil
			}
		}, nil
	}
}

func (st *voteproofHandler) handleACCEPTVoteproofAfterProcessingProposal(
	manifest base.Manifest, avp base.ACCEPTVoteproof,
) (saved bool, _ error) {
	l := st.Log().With().Str("accept_voteproof", avp.ID()).Logger()

	switch { // NOTE check last accept voteproof is the execpted
	case avp.Result() != base.VoteResultMajority:
		if err := st.args.ProposalProcessors.Cancel(); err != nil {
			l.Error().Err(err).
				Msg("expected accept voteproof is not majority result; cancel processor, but failed")

			return false, err
		}

		l.Debug().Msg("expected accept voteproof is not majority result; ignore")

		return false, nil
	case !manifest.Hash().Equal(avp.BallotMajority().NewBlock()):
		if err := st.args.ProposalProcessors.Cancel(); err != nil {
			l.Error().Err(err).
				Msg("expected accept voteproof has different new block; cancel processor, but failed")

			return false, err
		}

		l.Debug().Msg("expected accept voteproof has different new block; moves to syncing")

		return false, newSyncingSwitchContextWithVoteproof(st.stt, avp)
	default:
		l.Debug().Msg("proposal processed and expected voteproof found")
	}

	var sctx switchContext

	switch i, err := st.saveBlock(avp); {
	case err == nil:
		saved = i
	case errors.As(err, &sctx):
	default:
		sctx = newBrokenSwitchContext(st.stt, errors.Wrap(err, "save proposal"))
	}

	return saved, sctx
}

func (st *voteproofHandler) newVoteproof(vp base.Voteproof) error {
	errch := make(chan error, 1)

	go func() {
		st.vpch <- voteproofWithErrchan{
			vp:    vp,
			errch: errch,
		}
	}()

	return <-errch
}

func (st *voteproofHandler) handleNewVoteproof(vp base.Voteproof) error {
	st.vplock.Lock()
	defer st.vplock.Unlock()

	switch lvps, v, isnew := st.baseBallotHandler.setNewVoteproof(vp); {
	case v == nil, !isnew:
		return nil
	default:
		return st.newVoteproofWithLVPS(vp, lvps)
	}
}

func (st *voteproofHandler) newVoteproofWithLVPS(vp base.Voteproof, lvps isaac.LastVoteproofs) error {
	if st.resolver != nil {
		st.resolver.Cancel(vp.Point())
	}

	e := util.StringErrorFunc("handle new voteproof")

	if err := st.args.whenNewVoteproof(vp); err != nil {
		return e(err, "")
	}

	switch keep, err := st.checkStuckVoteproof(vp, lvps); {
	case err != nil:
		return err
	case !keep:
		return nil
	}

	switch vp.Point().Stage() {
	case base.StageINIT:
		return st.newINITVoteproof(vp.(base.INITVoteproof), lvps) //nolint:forcetypeassert //...
	case base.StageACCEPT:
		return st.newACCEPTVoteproof(vp.(base.ACCEPTVoteproof), lvps) //nolint:forcetypeassert //...
	default:
		return e(nil, "invalid voteproof received, %T", vp)
	}
}

func (st *voteproofHandler) newINITVoteproof(ivp base.INITVoteproof, lvps isaac.LastVoteproofs) error {
	c := lvps.Cap()

	st.Log().Debug().
		Func(base.VoteproofLogFunc("init_voteproof", ivp)).
		Func(base.VoteproofLogFunc("last_voteproof", c)).
		Msg("new init voteproof received")

	switch c.Point().Stage() { //nolint:exhaustive //...
	case base.StageINIT:
		return st.newINITVoteproofWithLastINITVoteproof(ivp, lvps)
	case base.StageACCEPT:
		return st.newINITVoteproofWithLastACCEPTVoteproof(ivp, lvps)
	}

	return nil
}

func (st *voteproofHandler) newACCEPTVoteproof(avp base.ACCEPTVoteproof, lvps isaac.LastVoteproofs) error {
	lvp := lvps.Cap()

	st.Log().Debug().
		Func(base.VoteproofLogFunc("accept_voteproof", avp)).
		Func(base.VoteproofLogFunc("last_voteproof", lvp)).
		Msg("new accept voteproof received")

	switch lvp.Point().Stage() { //nolint:exhaustive //...
	case base.StageINIT:
		return st.newACCEPTVoteproofWithLastINITVoteproof(avp, lvps)
	case base.StageACCEPT:
		return st.newACCEPTVoteproofWithLastACCEPTVoteproof(avp, lvps)
	}

	return nil
}

func (st *voteproofHandler) newINITVoteproofWithLastINITVoteproof(
	ivp base.INITVoteproof, lvps isaac.LastVoteproofs,
) error {
	livp := lvps.Cap().(base.INITVoteproof) //nolint:forcetypeassert //...

	switch {
	case ivp.Point().Height() > livp.Point().Height(): // NOTE higher height; moves to syncing state
		st.Log().Debug().Msg("higher init voteproof; moves to syncing state")

		return newSyncingSwitchContextWithVoteproof(st.stt, ivp)
	case ivp.Result() != base.VoteResultMajority: // NOTE new init voteproof has same height, but higher round
		st.Log().Debug().Msg("new init voteproof draw; moves to next round")

		go st.nextRound(ivp, lvps.PreviousBlockForNextRound(ivp))

		return nil
	}

	lavp := lvps.ACCEPT()

	if lavp == nil {
		st.Log().Debug().Msg("empty last accept voteproof; moves to broken state")

		return newBrokenSwitchContext(st.stt, errors.Errorf("empty last accept voteproof"))
	}

	if m := lavp.BallotMajority(); m == nil || !ivp.BallotMajority().PreviousBlock().Equal(m.NewBlock()) {
		// NOTE local stored block is different with other nodes
		st.Log().Debug().
			Stringer("previous_block", ivp.BallotMajority().PreviousBlock()).
			Stringer("new_block", m.NewBlock()).
			Msg("previous block does not match with last accept voteproof; moves to syncing")

		return newSyncingSwitchContextWithVoteproof(st.stt, ivp)
	}

	switch keep, err := st.checkSuffrageVoting(ivp); {
	case err != nil:
		return err
	case !keep:
		return nil
	default:
		go st.whenNewBlockConfirmed(lavp)

		process, err := st.processProposal(ivp)
		if err != nil {
			return err
		}

		go process()

		return nil
	}
}

func (st *voteproofHandler) newINITVoteproofWithLastACCEPTVoteproof(
	ivp base.INITVoteproof, lvps isaac.LastVoteproofs,
) error {
	lavp := lvps.Cap().(base.ACCEPTVoteproof) //nolint:forcetypeassert //...

	switch expectedheight := lavp.Point().Height() + 1; {
	case ivp.Point().Height() > expectedheight:
		st.Log().Debug().Msg("higher init voteproof; moves to syncing state")

		return newSyncingSwitchContextWithVoteproof(st.stt, ivp)
	case ivp.Result() == base.VoteResultDraw:
		st.Log().Debug().Msg("new init voteproof draw; moves to next round")

		go st.nextRound(ivp, lvps.PreviousBlockForNextRound(ivp))

		return nil
	default:
		if m := lavp.BallotMajority(); m == nil || !ivp.BallotMajority().PreviousBlock().Equal(m.NewBlock()) {
			// NOTE local stored block is different with other nodes
			st.Log().Debug().
				Stringer("previous_block", ivp.BallotMajority().PreviousBlock()).
				Interface("majority", m).
				Msg("previous block does not match with last accept voteproof; moves to syncing")

			return newSyncingSwitchContextWithVoteproof(st.stt, ivp)
		}
	}

	// NOTE suffrage sign voting
	switch keep, err := st.checkSuffrageVoting(ivp); {
	case err != nil:
		return err
	case !keep:
		return nil
	default:
		go st.whenNewBlockConfirmed(lavp)

		process, err := st.processProposal(ivp)
		if err != nil {
			return err
		}

		go process()

		return nil
	}
}

func (st *voteproofHandler) newACCEPTVoteproofWithLastINITVoteproof(
	avp base.ACCEPTVoteproof, lvps isaac.LastVoteproofs,
) error {
	livp := lvps.Cap().(base.INITVoteproof) //nolint:forcetypeassert //...

	switch {
	case avp.Point().Point.Equal(livp.Point().Point): // NOTE expected accept voteproof
		if avp.Result() == base.VoteResultMajority {
			_, err := st.saveBlock(avp)

			return err
		}

		go st.nextRound(avp, lvps.PreviousBlockForNextRound(avp))

		return nil
	case avp.Point().Height() > livp.Point().Height():
	case avp.Result() == base.VoteResultDraw:
		go st.nextRound(avp, lvps.PreviousBlockForNextRound(avp))

		return nil
	}

	return newSyncingSwitchContextWithVoteproof(st.stt, avp)
}

func (st *voteproofHandler) newACCEPTVoteproofWithLastACCEPTVoteproof(
	avp base.ACCEPTVoteproof, lvps isaac.LastVoteproofs,
) error {
	lavp := lvps.Cap().(base.ACCEPTVoteproof) //nolint:forcetypeassert //...

	switch {
	case avp.Point().Height() > lavp.Point().Height():
		st.Log().Debug().Msg("higher accept voteproof; moves to syncing state")

		return newSyncingSwitchContextWithVoteproof(st.stt, avp)
	case avp.Result() == base.VoteResultDraw:
		st.Log().Debug().Msg("new accept voteproof draw; moves to next round")

		go st.nextRound(avp, lvps.PreviousBlockForNextRound(avp))

		return nil
	default:
		return newSyncingSwitchContextWithVoteproof(st.stt, avp)
	}
}

func (st *voteproofHandler) nextRound(vp base.Voteproof, previousBlock util.Hash) {
	point := vp.Point().Point.NextRound()

	l := st.Log().With().Str("voteproof", vp.ID()).Object("point", point).Logger()

	var suf base.Suffrage

	var sctx switchContext

	switch i, err := st.localIsInConsensusNodes(point.Height().SafePrev()); {
	case errors.As(err, &sctx):
		go st.switchState(sctx)

		return
	case err != nil:
		l.Debug().Err(err).Msg("failed to prepare next round; moves to broken state")

		go st.switchState(newBrokenSwitchContext(st.stt, err))
	default:
		suf = i
	}

	timerIDs := []util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastSuffrageConfirmBallot,
		timerIDBroadcastACCEPTBallot,
	}

	if err := st.args.prepareNextRoundBallot(
		vp, previousBlock,
		timerIDs,
		suf,
		st.params.WaitPreparingINITBallot(),
	); err != nil {
		l.Error().Err(err).Msg("next round ballot")

		return
	}
}

func (st *voteproofHandler) saveBlock(avp base.ACCEPTVoteproof) (bool, error) {
	facthash := avp.BallotMajority().Proposal()

	l := st.Log().With().Str("voteproof", avp.ID()).Logger()
	ll := l.With().Stringer("fact", facthash).Logger()

	ll.Debug().Msg("expected accept voteproof; trying to save proposal")

	switch bm, err := st.args.ProposalProcessors.Save(context.Background(), facthash, avp); {
	case err == nil:
		ll.Debug().Msg("processed proposal saved; moves to next block")

		go st.whenNewBlockSaved(bm, avp)
		go st.nextBlock(avp, []util.TimerID{
			timerIDBroadcastINITBallot,
			timerIDBroadcastSuffrageConfirmBallot,
			timerIDBroadcastACCEPTBallot,
		})

		return true, nil
	case errors.Is(err, isaac.ErrProcessorAlreadySaved):
		l.Debug().Msg("already saved")

		return false, nil
	case errors.Is(err, isaac.ErrNotProposalProcessorProcessed):
		l.Debug().Msg("no processed proposal; moves to syncing state")

		return false, newSyncingSwitchContextWithVoteproof(st.stt, avp)
	default:
		ll.Error().Err(err).Msg("failed to save proposal; moves to broken state")

		return false, newBrokenSwitchContext(st.stt, err)
	}
}

func (st *voteproofHandler) nextBlock(avp base.ACCEPTVoteproof, timerIDs []util.TimerID) {
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

	if err := st.args.prepareNextBlockBallot(avp, timerIDs, suf, st.params.WaitPreparingINITBallot()); err != nil {
		l.Debug().Err(err).Msg("failed to prepare next block ballot")

		return
	}
}

func (st *voteproofHandler) checkSuffrageVoting(ivp base.INITVoteproof) (bool, error) {
	if _, ok := ivp.(base.ExpelVoteproof); !ok {
		return true, nil
	}

	switch t := ivp.Majority().(type) {
	case isaac.INITBallotFact:
		go st.args.prepareSuffrageConfirmBallot(ivp)

		return false, nil
	case isaac.SuffrageConfirmBallotFact:
		return true, nil
	default:
		return false, errors.Errorf("expected SuffrageConfirmBallotFact, but %T", t)
	}
}

func (st *voteproofHandler) checkStuckVoteproof(
	vp base.Voteproof,
	lvps isaac.LastVoteproofs,
) (bool, error) {
	if _, ok := vp.(base.StuckVoteproof); !ok {
		return true, nil
	}

	lvp := lvps.Cap()

	expectedheight := lvp.Point().Height()

	if lvp.Point().Stage() == base.StageACCEPT {
		expectedheight++
	}

	switch {
	case vp.Point().Height() > expectedheight:
		st.Log().Debug().
			Func(base.VoteproofLogFunc("init_voteproof", vp)).
			Func(base.VoteproofLogFunc("last_voteproof", lvp)).
			Msg("higher init stuck voteproof; moves to syncing state")

		return false, newSyncingSwitchContextWithVoteproof(st.stt, vp)
	default:
		st.Log().Debug().
			Func(base.VoteproofLogFunc("init_voteproof", vp)).
			Func(base.VoteproofLogFunc("last_voteproof", lvp)).
			Msg("stuck voteproof; moves to next round")

		go st.nextRound(vp, lvps.PreviousBlockForNextRound(vp))

		return false, nil
	}
}

func (st *voteproofHandler) whenNewBlockSaved(bm base.BlockMap, vp base.ACCEPTVoteproof) {
	st.args.whenNewBlockSaved(bm, vp)

	if _, hasExpels := vp.(base.HasExpels); !hasExpels {
		st.args.WhenNewBlockConfirmed(vp.Point().Height())
	}

	st.args.WhenNewBlockSaved(bm)
}

func (st *voteproofHandler) whenNewBlockConfirmed(vp base.ACCEPTVoteproof) {
	if _, ok := vp.(base.HasExpels); ok {
		st.args.WhenNewBlockConfirmed(vp.Point().Height())
	}
}

func (st *voteproofHandler) wrongACCEPTBallot(_ context.Context, ivp base.INITVoteproof) error {
	dummy := isaac.NewManifest(
		ivp.Point().Height(),
		ivp.BallotMajority().PreviousBlock(),
		ivp.BallotMajority().Proposal(),
		valuehash.RandomSHA256(),
		valuehash.RandomSHA256(),
		valuehash.RandomSHA256(),
		localtime.Now(),
	)

	if err := st.args.prepareACCEPTBallot(ivp, dummy, time.Nanosecond); err != nil {
		return errors.WithMessage(err, "prepare intended wrong accept ballot")
	}

	return nil
}

func (st *voteproofHandler) setAllowConsensus(allow bool) { // revive:disable-line:flag-parameter
	st.baseBallotHandler.setAllowConsensus(allow)

	if !allow {
		st.notallowconsensusch <- struct{}{}
	}
}
