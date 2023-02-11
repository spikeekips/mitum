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

type ConsensusHandlerArgs struct {
	*baseBallotHandlerArgs
	ProposalProcessors    *isaac.ProposalProcessors
	GetManifestFunc       func(base.Height) (base.Manifest, error)
	WhenNewBlockSaved     func(base.Height)
	WhenNewBlockConfirmed func(base.Height)
}

func NewConsensusHandlerArgs() *ConsensusHandlerArgs {
	return &ConsensusHandlerArgs{
		baseBallotHandlerArgs: newBaseBallotHandlerArgs(),
		GetManifestFunc: func(base.Height) (base.Manifest, error) {
			return nil, util.ErrNotImplemented.Errorf("GetManifestFunc")
		},
		WhenNewBlockSaved:     func(base.Height) {},
		WhenNewBlockConfirmed: func(base.Height) {},
	}
}

type ConsensusHandler struct {
	*baseBallotHandler
	args   *ConsensusHandlerArgs
	vplock sync.Mutex
}

type NewConsensusHandlerType struct {
	*ConsensusHandler
}

func NewNewConsensusHandlerType(
	local base.LocalNode,
	params *isaac.LocalParams,
	args *ConsensusHandlerArgs,
) *NewConsensusHandlerType {
	baseBallotHandler := newBaseBallotHandler(StateConsensus, local, params, args.baseBallotHandlerArgs)

	return &NewConsensusHandlerType{
		ConsensusHandler: &ConsensusHandler{
			baseBallotHandler: baseBallotHandler,
			args:              args,
		},
	}
}

func (h *NewConsensusHandlerType) new() (handler, error) {
	return &ConsensusHandler{
		baseBallotHandler: h.baseBallotHandler.new(),
		args:              h.args,
	}, nil
}

func (st *ConsensusHandler) enter(from StateType, i switchContext) (func(), error) {
	e := util.StringErrorFunc("failed to enter consensus state")

	deferred, err := st.baseBallotHandler.enter(from, i)
	if err != nil {
		return nil, e(err, "")
	}

	var sctx consensusSwitchContext

	switch j, ok := i.(consensusSwitchContext); {
	case !ok:
		return nil, e(nil, "invalid stateSwitchContext, not for consensus state; %T", i)
	case j.vp == nil:
		return nil, e(nil, "invalid stateSwitchContext, empty init voteproof")
	default:
		sctx = j
	}

	switch suf, found, err := st.args.NodeInConsensusNodesFunc(
		st.local, sctx.vp.Point().Height()); {
	case errors.Is(err, storage.ErrNotFound):
		st.Log().Debug().
			Dict("state_context", switchContextLog(sctx)).
			Interface("height", sctx.vp.Point().Height()).
			Msg("suffrage not found at entering consensus state; moves to syncing state")

		return nil, newSyncingSwitchContext(StateConsensus, sctx.vp.Point().Height())
	case err != nil:
		return nil, e(err, "")
	case suf == nil || suf.Len() < 1:
		return nil, e(nil, "empty suffrage of init voteproof")
	case !found:
		st.Log().Debug().
			Dict("state_context", switchContextLog(sctx)).
			Interface("height", sctx.vp.Point().Height()).
			Msg("local is not in consensus nodes at entering consensus state; moves to syncing state")

		return nil, newSyncingSwitchContext(StateConsensus, sctx.vp.Point().Height())
	}

	switch lvps, found := st.voteproofs(sctx.vp.Point()); {
	case !found:
		return nil, e(nil, "last voteproofs not found")
	default:
		st.vplock.Lock()

		return func() {
			deferred()

			defer st.vplock.Unlock()

			var nsctx switchContext

			switch err := st.newVoteproofWithLVPS(sctx.vp, lvps); {
			case err == nil:
			case !errors.As(err, &nsctx):
				st.Log().Error().Err(err).Msg("failed to process enter voteproof; moves to broken state")

				go st.switchState(newBrokenSwitchContext(StateConsensus, err))
			default:
				go st.switchState(nsctx)
			}
		}, nil
	}
}

func (st *ConsensusHandler) exit(sctx switchContext) (func(), error) {
	e := util.StringErrorFunc("failed to exit from consensus state")

	deferred, err := st.baseBallotHandler.exit(sctx)
	if err != nil {
		return nil, e(err, "")
	}

	if err := st.args.ProposalProcessors.Cancel(); err != nil {
		return nil, e(err, "failed to cancel proposal processors")
	}

	return deferred, nil
}

func (st *ConsensusHandler) processProposalFunc(ivp base.INITVoteproof) (func(context.Context) error, error) {
	facthash := ivp.BallotMajority().Proposal()
	l := st.Log().With().Stringer("fact", facthash).Logger()
	l.Debug().Msg("trying to process proposal")

	e := util.StringErrorFunc("failed to process proposal")

	var process isaac.ProcessorProcessFunc

	switch i, err := st.processProposalInternal(ivp); {
	case err == nil:
		if i == nil {
			l.Debug().Msg("empty manifest; ignore")

			return nil, nil
		}

		process = i
	case errors.Is(err, isaac.ErrNotProposalProcessorProcessed):
		// NOTE instead of moving next round, intended-wrong accept ballot.
		return func(context.Context) error {
				dummy := isaac.NewManifest(
					ivp.Point().Height(),
					ivp.BallotMajority().PreviousBlock(),
					valuehash.RandomSHA256(),
					valuehash.RandomSHA256(),
					valuehash.RandomSHA256(),
					valuehash.RandomSHA256(),
					localtime.Now(),
				)

				if err = st.prepareACCEPTBallot(ivp, dummy, time.Nanosecond); err != nil {
					return errors.WithMessage(err, "failed to prepare intended wrong accept ballot")
				}

				return nil
			},
			nil
	default:
		err = e(err, "")

		l.Error().Err(err).Msg("failed to process proposal; moves to broken state")

		return nil, newBrokenSwitchContext(StateConsensus, err)
	}

	return func(ctx context.Context) error {
		manifest, err := process(ctx)

		switch {
		case err != nil:
			return e(err, "")
		case manifest == nil:
			return nil
		}

		eavp := st.lastVoteproofs().ACCEPT()

		if err := st.prepareACCEPTBallot(ivp, manifest, time.Nanosecond); err != nil {
			l.Error().Err(err).Msg("failed to prepare accept ballot")

			return e(err, "")
		}

		if eavp == nil || !eavp.Point().Point.Equal(ivp.Point().Point) {
			return nil
		}

		ll := l.With().Dict("accept_voteproof", base.VoteproofLog(eavp)).Logger()

		var sctx switchContext

		switch saved, err := st.handleACCEPTVoteproofAfterProcessingProposal(manifest, eavp); {
		case saved:
			ll.Debug().Msg("new block saved by accept voteproof after processing proposal")
		case err == nil:
			return nil
		case errors.As(err, &sctx):
		default:
			ll.Error().Err(err).Msg("failed to save new block by accept voteproof after processing proposal")

			sctx = newBrokenSwitchContext(StateConsensus, errors.Wrap(err, "failed to save proposal"))
		}

		return sctx
	}, nil
}

func (st *ConsensusHandler) processProposal(ivp base.INITVoteproof) (func(), error) {
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
			go st.switchState(newBrokenSwitchContext(StateConsensus, err))
		}
	}, nil
}

func (st *ConsensusHandler) processProposalInternal(ivp base.INITVoteproof) (isaac.ProcessorProcessFunc, error) {
	e := util.StringErrorFunc("failed to process proposal")

	facthash := ivp.BallotMajority().Proposal()

	var previous base.Manifest

	switch m, err := st.args.GetManifestFunc(ivp.Point().Height() - 1); {
	case err != nil:
		return nil, e(err, "")
	default:
		previous = m
	}

	switch process, err := st.args.ProposalProcessors.Process(st.ctx, facthash, previous, ivp); {
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
					return nil, e(err0, "failed to cancel proposal processors")
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

func (st *ConsensusHandler) handleACCEPTVoteproofAfterProcessingProposal(
	manifest base.Manifest, avp base.ACCEPTVoteproof,
) (saved bool, _ error) {
	l := st.Log().With().Dict("accept_voteproof", base.VoteproofLog(avp)).Logger()

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

		return false, newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	default:
		l.Debug().Msg("proposal processed and expected voteproof found")
	}

	var sctx switchContext

	switch i, err := st.saveBlock(avp); {
	case err == nil:
		saved = i
	case errors.As(err, &sctx):
	default:
		sctx = newBrokenSwitchContext(StateConsensus, errors.Wrap(err, "failed to save proposal"))
	}

	return saved, sctx
}

func (st *ConsensusHandler) prepareINITBallot(
	bl base.INITBallot,
	ids []util.TimerID,
	initialWait time.Duration,
	interval func(int, time.Duration) time.Duration,
) error {
	go func() {
		<-time.After(initialWait)

		switch _, err := st.vote(bl); {
		case err == nil:
		case errors.Is(err, errFailedToVoteNotInConsensus):
			st.Log().Debug().Err(err).Msg("failed to vote init ballot; moves to syncing state")

			go st.switchState(newSyncingSwitchContext(StateConsensus, bl.Point().Height()-1))
		default:
			st.Log().Debug().Err(err).Msg("failed to vote init ballot; moves to broken state")

			go st.switchState(newBrokenSwitchContext(StateConsensus, err))
		}
	}()

	if err := st.broadcastINITBallot(bl, interval); err != nil {
		return err
	}

	return st.timers.StartTimers(ids, true)
}

func (st *ConsensusHandler) newVoteproof(vp base.Voteproof) error {
	st.vplock.Lock()
	defer st.vplock.Unlock()

	switch lvps, v, isnew := st.baseBallotHandler.setNewVoteproof(vp); {
	case v == nil, !isnew:
		return nil
	default:
		return st.newVoteproofWithLVPS(vp, lvps)
	}
}

func (st *ConsensusHandler) newVoteproofWithLVPS(vp base.Voteproof, lvps LastVoteproofs) error {
	if st.resolver != nil {
		st.resolver.Cancel(vp.Point())
	}

	e := util.StringErrorFunc("failed to handle new voteproof")

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

func (st *ConsensusHandler) newINITVoteproof(ivp base.INITVoteproof, lvps LastVoteproofs) error {
	c := lvps.Cap()
	l := st.Log().With().
		Dict("init_voteproof", base.VoteproofLog(ivp)).
		Dict("last_voteproof", base.VoteproofLog(c)).
		Logger()

	l.Debug().Msg("new init voteproof received")

	switch c.Point().Stage() { //nolint:exhaustive //...
	case base.StageINIT:
		return st.newINITVoteproofWithLastINITVoteproof(ivp, lvps)
	case base.StageACCEPT:
		return st.newINITVoteproofWithLastACCEPTVoteproof(ivp, lvps)
	}

	return nil
}

func (st *ConsensusHandler) newACCEPTVoteproof(avp base.ACCEPTVoteproof, lvps LastVoteproofs) error {
	lvp := lvps.Cap()
	l := st.Log().With().
		Dict("accept_voteproof", base.VoteproofLog(avp)).
		Dict("last_voteproof", base.VoteproofLog(lvp)).
		Logger()

	l.Debug().Msg("new accept voteproof received")

	switch lvp.Point().Stage() { //nolint:exhaustive //...
	case base.StageINIT:
		return st.newACCEPTVoteproofWithLastINITVoteproof(avp, lvps)
	case base.StageACCEPT:
		return st.newACCEPTVoteproofWithLastACCEPTVoteproof(avp, lvps)
	}

	return nil
}

func (st *ConsensusHandler) newINITVoteproofWithLastINITVoteproof(
	ivp base.INITVoteproof, lvps LastVoteproofs,
) error {
	livp := lvps.Cap().(base.INITVoteproof) //nolint:forcetypeassert //...

	l := st.Log().With().
		Dict("last_init_voteproof", base.VoteproofLog(livp)).
		Logger()

	switch {
	case ivp.Point().Height() > livp.Point().Height(): // NOTE higher height; moves to syncing state
		l.Debug().Msg("higher init voteproof; moves to syncing state")

		return newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1)
	case ivp.Result() != base.VoteResultMajority: // NOTE new init voteproof has same height, but higher round
		l.Debug().Msg("new init voteproof draw; moves to next round")

		go st.nextRound(ivp, lvps.PreviousBlockForNextRound(ivp))

		return nil
	}

	lavp := lvps.ACCEPT()

	ll := st.Log().With().
		Dict("init_voteproof", base.VoteproofLog(ivp)).
		Dict("last_accept_voteproof", base.VoteproofLog(lavp)).
		Logger()

	if lavp == nil {
		ll.Debug().Msg("empty last accept voteproof; moves to broken state")

		return newBrokenSwitchContext(StateConsensus, errors.Errorf("empty last accept voteproof"))
	}

	if m := lavp.BallotMajority(); m == nil || !ivp.BallotMajority().PreviousBlock().Equal(m.NewBlock()) {
		// NOTE local stored block is different with other nodes
		ll.Debug().
			Stringer("previous_block", ivp.BallotMajority().PreviousBlock()).
			Stringer("new_block", m.NewBlock()).
			Msg("previous block does not match with last accept voteproof; moves to syncing")

		return newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1)
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

func (st *ConsensusHandler) newINITVoteproofWithLastACCEPTVoteproof(
	ivp base.INITVoteproof, lvps LastVoteproofs,
) error {
	lavp := lvps.Cap().(base.ACCEPTVoteproof) //nolint:forcetypeassert //...

	l := st.Log().With().
		Dict("init_voteproof", base.VoteproofLog(ivp)).
		Dict("last_accept_voteproof", base.VoteproofLog(lavp)).
		Logger()

	switch expectedheight := lavp.Point().Height() + 1; {
	case ivp.Point().Height() > expectedheight:
		l.Debug().Msg("higher init voteproof; moves to syncing state")

		return newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1)
	case ivp.Result() == base.VoteResultDraw:
		l.Debug().Msg("new init voteproof draw; moves to next round")

		go st.nextRound(ivp, lvps.PreviousBlockForNextRound(ivp))

		return nil
	default:
		if m := lavp.BallotMajority(); m == nil || !ivp.BallotMajority().PreviousBlock().Equal(m.NewBlock()) {
			// NOTE local stored block is different with other nodes
			l.Debug().
				Stringer("previous_block", ivp.BallotMajority().PreviousBlock()).
				Interface("majority", m).
				Msg("previous block does not match with last accept voteproof; moves to syncing")

			return newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1)
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

func (st *ConsensusHandler) newACCEPTVoteproofWithLastINITVoteproof(
	avp base.ACCEPTVoteproof, lvps LastVoteproofs,
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

	return newSyncingSwitchContext(StateConsensus, avp.Point().Height())
}

func (st *ConsensusHandler) newACCEPTVoteproofWithLastACCEPTVoteproof(
	avp base.ACCEPTVoteproof, lvps LastVoteproofs,
) error {
	lavp := lvps.Cap().(base.ACCEPTVoteproof) //nolint:forcetypeassert //...

	l := st.Log().With().
		Dict("last_accept_voteproof", base.VoteproofLog(lavp)).
		Logger()

	switch {
	case avp.Point().Height() > lavp.Point().Height():
		l.Debug().Msg("higher accept voteproof; moves to syncing state")

		return newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	case avp.Result() == base.VoteResultDraw:
		l.Debug().Msg("new accept voteproof draw; moves to next round")

		go st.nextRound(avp, lvps.PreviousBlockForNextRound(avp))

		return nil
	default:
		return newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	}
}

func (st *ConsensusHandler) nextRound(vp base.Voteproof, previousBlock util.Hash) {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	started := time.Now()

	if previousBlock == nil {
		l.Debug().Msg("failed to find previous block from last voteproofs; ignore to move next round")

		return
	}

	var sctx switchContext
	var bl base.INITBallot

	switch i, err := st.baseBallotHandler.makeNextRoundBallot(vp, previousBlock, st.args.NodeInConsensusNodesFunc); {
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

		go st.switchState(newBrokenSwitchContext(StateConsensus, err))

		return
	}

	initialWait := time.Nanosecond
	if d := time.Since(started); d < st.params.WaitPreparingNextRoundINITBallot() {
		initialWait = st.params.WaitPreparingNextRoundINITBallot() - d
	}

	if err := st.prepareINITBallot(
		bl,
		[]util.TimerID{
			timerIDBroadcastINITBallot,
			timerIDBroadcastSuffrageConfirmBallot,
			timerIDBroadcastACCEPTBallot,
		},
		initialWait,
		func(i int, _ time.Duration) time.Duration {
			lvp := st.lastVoteproofs().Cap()
			if bl.Point().Compare(lvp.Point()) < 0 &&
				bl.Point().Height() == lvp.Point().Height() &&
				lvp.Point().Stage() == base.StageINIT &&
				lvp.Result() == base.VoteResultMajority {
				return 0
			}

			if i < 1 {
				return initialWait
			}

			return st.params.IntervalBroadcastBallot()
		},
	); err != nil {
		l.Error().Err(err).Msg("failed to prepare init ballot for next round")

		return
	}

	l.Debug().Interface("ballot", bl).Msg("init ballot broadcasted for next round")
}

func (st *ConsensusHandler) prepareNextBlock(avp base.ACCEPTVoteproof) {
	point := avp.Point().Point.NextHeight()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Object("point", point).Logger()

	started := time.Now()

	var sctx switchContext
	var bl base.INITBallot

	switch i, err := st.baseBallotHandler.makeNextBlockBallot(avp, st.args.NodeInConsensusNodesFunc); {
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

		go st.switchState(newBrokenSwitchContext(StateConsensus, err))

		return
	}

	initialWait := time.Nanosecond
	if d := time.Since(started); d < st.params.WaitPreparingINITBallot() {
		initialWait = st.params.WaitPreparingINITBallot() - d
	}

	if err := st.prepareINITBallot(
		bl,
		[]util.TimerID{
			timerIDBroadcastINITBallot,
			timerIDBroadcastSuffrageConfirmBallot,
			timerIDBroadcastACCEPTBallot,
		},
		initialWait,
		func(i int, _ time.Duration) time.Duration {
			if i < 1 {
				return initialWait
			}

			return st.params.IntervalBroadcastBallot()
		},
	); err != nil {
		l.Error().Err(err).Msg("failed to prepare init ballot for next block")

		return
	}

	l.Debug().Interface("ballot", bl).Msg("next init ballot broadcasted")
}

func (st *ConsensusHandler) saveBlock(avp base.ACCEPTVoteproof) (bool, error) {
	facthash := avp.BallotMajority().Proposal()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Logger()
	ll := l.With().Stringer("fact", facthash).Logger()

	ll.Debug().Msg("expected accept voteproof; trying to save proposal")

	switch err := st.args.ProposalProcessors.Save(context.Background(), facthash, avp); {
	case err == nil:
		ll.Debug().Msg("processed proposal saved; moves to next block")

		go st.whenNewBlockSaved(avp)
		go st.prepareNextBlock(avp)

		return true, nil
	case errors.Is(err, isaac.ErrProcessorAlreadySaved):
		l.Debug().Msg("already saved")

		return false, nil
	case errors.Is(err, isaac.ErrNotProposalProcessorProcessed):
		l.Debug().Msg("no processed proposal; moves to syncing state")

		return false, newSyncingSwitchContext(StateConsensus, avp.Point().Height())
	default:
		ll.Error().Err(err).Msg("failed to save proposal; moves to broken state")

		return false, newBrokenSwitchContext(StateConsensus, err)
	}
}

func (st *ConsensusHandler) checkSuffrageVoting(ivp base.INITVoteproof) (bool, error) {
	if _, ok := ivp.(base.WithdrawVoteproof); !ok {
		return true, nil
	}

	switch t := ivp.Majority().(type) {
	case isaac.INITBallotFact:
		go st.prepareSuffrageConfirmBallot(ivp)

		return false, nil
	case isaac.SuffrageConfirmBallotFact:
		return true, nil
	default:
		return false, errors.Errorf("expected SuffrageConfirmBallotFact, but %T", t)
	}
}

func (st *ConsensusHandler) checkStuckVoteproof(
	vp base.Voteproof,
	lvps LastVoteproofs,
) (bool, error) {
	if _, ok := vp.(base.StuckVoteproof); !ok {
		return true, nil
	}

	lvp := lvps.Cap()

	expectedheight := lvp.Point().Height()

	l := st.Log().With().
		Dict("init_voteproof", base.VoteproofLog(vp)).
		Dict("last_voteproof", base.VoteproofLog(lvp)).
		Logger()

	if lvp.Point().Stage() == base.StageACCEPT {
		expectedheight++
	}

	switch {
	case vp.Point().Height() > expectedheight:
		l.Debug().Msg("higher init stuck voteproof; moves to syncing state")

		return false, newSyncingSwitchContext(StateConsensus, vp.Point().Height()-1)
	default:
		l.Debug().Msg("stuck voteproof; moves to next round")

		go st.nextRound(vp, lvps.PreviousBlockForNextRound(vp))

		return false, nil
	}
}

func (st *ConsensusHandler) whenNewBlockSaved(vp base.ACCEPTVoteproof) {
	if _, hasWithdraws := vp.(base.HasWithdraws); !hasWithdraws {
		st.args.WhenNewBlockConfirmed(vp.Point().Height())
	}

	st.args.WhenNewBlockSaved(vp.Point().Height())
}

func (st *ConsensusHandler) whenNewBlockConfirmed(vp base.ACCEPTVoteproof) {
	if _, ok := vp.(base.HasWithdraws); ok {
		st.args.WhenNewBlockConfirmed(vp.Point().Height())
	}
}

type consensusSwitchContext struct {
	vp base.Voteproof
	baseSwitchContext
}

func newConsensusSwitchContext(from StateType, vp base.Voteproof) (consensusSwitchContext, error) {
	switch {
	case vp == nil:
		return consensusSwitchContext{}, errors.Errorf(
			"invalid voteproof for consensus switch context; empty init voteproof")
	case vp.Point().Stage() == base.StageINIT:
	case vp.Point().Stage() == base.StageACCEPT:
		if vp.Result() != base.VoteResultDraw {
			return consensusSwitchContext{}, errors.Errorf(
				"invalid voteproof for consensus switch context; vote result is not draw, %T", vp.Result())
		}
	}

	return consensusSwitchContext{
		baseSwitchContext: newBaseSwitchContext(StateConsensus, switchContextOKFuncCheckFrom(from)),
		vp:                vp,
	}, nil
}
