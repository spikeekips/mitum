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
	"github.com/spikeekips/mitum/util/logging"
)

type (
	// ProposalSelectFunc fetchs proposal from selected proposer
	ProposalSelectFunc func(_ context.Context, _ base.Point, wait time.Duration) (
		base.ProposalSignFact, error)
	SuffrageVotingFindFunc func(context.Context, base.Height, base.Suffrage) ([]base.SuffrageWithdrawOperation, error)
)

type baseBallotHandlerArgs struct {
	ProposalSelectFunc       ProposalSelectFunc
	NodeInConsensusNodesFunc isaac.NodeInConsensusNodesFunc
	VoteFunc                 func(base.Ballot) (bool, error)
	SuffrageVotingFindFunc   SuffrageVotingFindFunc
	WhenEmptyMembersFunc     func()
}

func newBaseBallotHandlerArgs() *baseBallotHandlerArgs {
	return &baseBallotHandlerArgs{
		NodeInConsensusNodesFunc: func(base.Node, base.Height) (base.Suffrage, bool, error) {
			return nil, false, util.ErrNotImplemented.Errorf("NodeInConsensusNodesFunc")
		},
		VoteFunc: func(base.Ballot) (bool, error) {
			return false, util.ErrNotImplemented.Errorf("VoteFunc")
		},
		SuffrageVotingFindFunc: func(context.Context, base.Height, base.Suffrage) (
			[]base.SuffrageWithdrawOperation, error,
		) {
			return nil, util.ErrNotImplemented.Errorf("SuffrageVotingFindFunc")
		},
		WhenEmptyMembersFunc: func() {},
	}
}

type baseBallotHandler struct {
	*baseHandler
	args              *baseBallotHandlerArgs
	ballotBroadcaster BallotBroadcaster
	voteFunc          func(base.Ballot) (bool, error)
	resolver          BallotStuckResolver
}

func newBaseBallotHandler(
	state StateType,
	local base.LocalNode,
	params *isaac.LocalParams,
	args *baseBallotHandlerArgs,
) *baseBallotHandler {
	args.VoteFunc = preventVotingWithEmptySuffrage(
		local,
		args.VoteFunc,
		args.NodeInConsensusNodesFunc,
	)

	h := &baseBallotHandler{
		baseHandler: newBaseHandler(state, local, params),
		args:        args,
		voteFunc:    func(base.Ballot) (bool, error) { return false, errors.Errorf("not voted") },
	}

	h.whenEmptyMembersFunc = args.WhenEmptyMembersFunc

	return h
}

func (st *baseBallotHandler) new() *baseBallotHandler {
	return &baseBallotHandler{
		baseHandler:       st.baseHandler.new(),
		args:              st.args,
		resolver:          st.resolver,
		ballotBroadcaster: st.ballotBroadcaster,
	}
}

func (st *baseBallotHandler) setStates(sts *States) {
	st.baseHandler.setStates(sts)
	st.resolver = sts.args.BallotStuckResolver
	st.ballotBroadcaster = sts.args.BallotBroadcaster
}

func (st *baseBallotHandler) makeNextRoundBallot(
	ctx context.Context,
	vp base.Voteproof,
	prevBlock util.Hash,
	suf base.Suffrage,
	initialWait time.Duration,
) (base.INITBallot, error) {
	bl, err := st.makeINITBallot(
		ctx,
		vp.Point().Point.NextRound(),
		prevBlock,
		vp,
		suf,
		initialWait,
	)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to prepare next round init ballot")
	}

	return bl, nil
}

func (st *baseBallotHandler) makeNextBlockBallot(
	ctx context.Context,
	avp base.ACCEPTVoteproof,
	suf base.Suffrage,
	initialWait time.Duration,
) (base.INITBallot, error) {
	bl, err := st.makeINITBallot(
		ctx,
		avp.Point().Point.NextHeight(),
		avp.BallotMajority().NewBlock(),
		avp,
		suf,
		initialWait,
	)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to prepare next block init ballot")
	}

	return bl, nil
}

func (st *baseBallotHandler) makeINITBallot(
	ctx context.Context,
	point base.Point,
	prevBlock util.Hash,
	vp base.Voteproof,
	suf base.Suffrage,
	initialWait time.Duration,
) (base.INITBallot, error) {
	e := util.StringErrorFunc("failed to prepare next block")

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Object("point", point).Logger()

	var pr base.ProposalSignFact

	switch i, err := st.args.ProposalSelectFunc(ctx, point, initialWait); {
	case err == nil:
		pr = i

		l.Debug().Interface("proposal", pr).Msg("proposal selected")
	case errors.Is(err, context.Canceled):
		l.Debug().Err(err).Msg("canceled to select proposal; ignore")

		return nil, nil
	default:
		l.Error().Err(err).Msg("failed to select proposal")

		return nil, e(err, "")
	}

	switch bl, found, err := st.ballotBroadcaster.Ballot(point, base.StageINIT, false); {
	case err != nil:
		return nil, e(err, "")
	case found:
		l.Debug().Msg("init ballot found in ballot pool")

		return bl.(base.INITBallot), nil //nolint:forcetypeassert //...
	}

	// NOTE collect suffrage withdraw operations
	withdraws, withdrawfacts, err := st.findWithdraws(point.Height(), suf)
	if err != nil {
		return nil, err
	}

	// NOTE broadcast next init ballot
	fact := isaac.NewINITBallotFact(
		point,
		prevBlock,
		pr.Fact().Hash(),
		withdrawfacts,
	)
	sf := isaac.NewINITBallotSignFact(fact)

	if err := sf.NodeSign(st.local.Privatekey(), st.params.NetworkID(), st.local.Address()); err != nil {
		return nil, e(err, "failed to make next init ballot")
	}

	bl := isaac.NewINITBallot(vp, sf, withdraws)

	return bl, nil
}

func (st *baseBallotHandler) prepareACCEPTBallot(
	ivp base.INITVoteproof,
	manifest base.Manifest,
	initialWait time.Duration,
) error {
	e := util.StringErrorFunc("failed to prepare accept ballot")

	bl, err := st.makeACCEPTBallot(ivp, manifest)
	if err != nil {
		return e(err, "")
	}

	go func() {
		<-time.After(initialWait)

		switch _, err := st.vote(bl); {
		case err == nil:
		case errors.Is(err, errFailedToVoteNotInConsensus):
			st.Log().Debug().Err(err).Msg("failed to vote accept ballot; moves to syncing state")

			go st.switchState(newSyncingSwitchContext(StateConsensus, ivp.Point().Height()-1))
		default:
			st.Log().Error().Err(err).Msg("failed to vote accept ballot; moves to broken state")

			go st.switchState(newBrokenSwitchContext(StateConsensus, err))
		}
	}()

	if err := st.broadcastACCEPTBallot(bl, initialWait); err != nil {
		return e(err, "failed to broadcast accept ballot")
	}

	if err := st.timers.StopOthers([]util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastSuffrageConfirmBallot,
		timerIDBroadcastACCEPTBallot,
	}); err != nil {
		return e(err, "failed to start timers for broadcasting accept ballot")
	}

	return nil
}

func (st *baseBallotHandler) makeACCEPTBallot(
	ivp base.INITVoteproof,
	manifest base.Manifest,
) (base.ACCEPTBallot, error) {
	switch bl, found, err := st.ballotBroadcaster.Ballot(ivp.Point().Point, base.StageACCEPT, false); {
	case err != nil:
		return nil, err
	case found:
		st.Log().Debug().Dict("voteproof", base.VoteproofLog(ivp)).Msg("accept ballot found in ballot pool")

		return bl.(base.ACCEPTBallot), nil //nolint:forcetypeassert //...
	}

	// NOTE add SuffrageWithdrawOperations into ballot from init voteproof
	var withdrawfacts []util.Hash
	var withdraws []base.SuffrageWithdrawOperation

	if i, ok := ivp.(base.WithdrawVoteproof); ok {
		withdraws = i.Withdraws()

		withdrawfacts = make([]util.Hash, len(withdraws))

		for i := range withdraws {
			withdrawfacts[i] = withdraws[i].WithdrawFact().Hash()
		}
	}

	afact := isaac.NewACCEPTBallotFact(
		ivp.Point().Point,
		ivp.BallotMajority().Proposal(),
		manifest.Hash(),
		withdrawfacts,
	)
	signfact := isaac.NewACCEPTBallotSignFact(afact)

	if err := signfact.NodeSign(st.local.Privatekey(), st.params.NetworkID(), st.local.Address()); err != nil {
		return nil, err
	}

	bl := isaac.NewACCEPTBallot(ivp, signfact, withdraws)

	return bl, nil
}

func (st *baseBallotHandler) prepareSuffrageConfirmBallot(vp base.Voteproof) {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	if _, ok := vp.(base.WithdrawVoteproof); !ok {
		l.Error().Msg("expected WithdrawVoteproof for suffrage sign voting")

		return
	}

	bl, err := st.makeSuffrageConfirmBallot(vp)
	if err != nil {
		l.Error().Err(err).Msg("failed to prepare suffrage confirm ballot")

		return
	}

	go func() {
		switch _, err := st.vote(bl); {
		case err == nil:
		case errors.Is(err, errFailedToVoteNotInConsensus):
			st.Log().Debug().Err(err).Msg("failed to vote suffrage confirm ballot; moves to syncing state")

			go st.switchState(newSyncingSwitchContext(StateConsensus, bl.Point().Height()-1))
		default:
			st.Log().Debug().Err(err).Msg("failed to vote suffrage confirm ballot; moves to broken state")

			go st.switchState(newBrokenSwitchContext(StateConsensus, err))
		}
	}()

	if err := st.broadcastSuffrageConfirmBallot(bl); err != nil {
		l.Error().Err(err).Msg("failed to prepare suffrage confirm ballot")

		return
	}

	l.Debug().Interface("ballot", bl).Msg("suffrage confirm ballot broadcasted")
}

func (st *baseBallotHandler) makeSuffrageConfirmBallot(vp base.Voteproof) (base.INITBallot, error) {
	switch bl, found, err := st.ballotBroadcaster.Ballot(vp.Point().Point, base.StageINIT, true); {
	case err != nil:
		return nil, err
	case found:
		st.Log().Debug().
			Dict("voteproof", base.VoteproofLog(vp)).
			Object("point", vp.Point().Point).
			Msg("init suffrage confirm ballot found in ballot pool")

		return bl.(base.INITBallot), nil //nolint:forcetypeassert //...
	}

	ifact := vp.Majority().(isaac.INITBallotFact) //nolint:forcetypeassert //...
	withdrawfacts := ifact.WithdrawFacts()

	fact := isaac.NewSuffrageConfirmBallotFact(
		vp.Point().Point,
		ifact.PreviousBlock(),
		ifact.Proposal(),
		withdrawfacts,
	)

	sf := isaac.NewINITBallotSignFact(fact)

	if err := sf.NodeSign(st.local.Privatekey(), st.params.NetworkID(), st.local.Address()); err != nil {
		go st.switchState(
			newBrokenSwitchContext(st.stt, errors.WithMessage(err, "failed to make suffrage confirm ballot")),
		)

		return nil, err
	}

	bl := isaac.NewINITBallot(vp, sf, nil)

	return bl, nil
}

func (st *baseBallotHandler) broadcastACCEPTBallot(bl base.Ballot, initialWait time.Duration) error {
	if initialWait < 1 {
		initialWait = time.Nanosecond //revive:disable-line:modifies-parameter
	}

	return broadcastBallot(
		bl,
		st.timers,
		timerIDBroadcastACCEPTBallot,
		st.ballotBroadcaster.Broadcast,
		st.Logging,
		func(i uint64) time.Duration {
			if i < 1 {
				return initialWait
			}

			return st.params.IntervalBroadcastBallot()
		},
	)
}

func (st *baseBallotHandler) broadcastSuffrageConfirmBallot(bl base.INITBallot) error {
	if err := broadcastBallot(
		bl,
		st.timers,
		timerIDBroadcastSuffrageConfirmBallot,
		st.ballotBroadcaster.Broadcast,
		st.Logging,
		func(i uint64) time.Duration {
			lvp := st.lastVoteproofs().Cap()
			if lvp.Point().Height() > bl.Point().Height() {
				return 0
			}

			if i < 1 {
				return time.Nanosecond
			}

			return st.params.IntervalBroadcastBallot()
		},
	); err != nil {
		return err
	}

	return st.timers.StopOthers(
		[]util.TimerID{
			timerIDBroadcastINITBallot,
			timerIDBroadcastSuffrageConfirmBallot,
			timerIDBroadcastACCEPTBallot,
		},
	)
}

func (st *baseBallotHandler) vote(bl base.Ballot) (bool, error) {
	voted, err := st.args.VoteFunc(bl)
	if err != nil {
		return voted, err
	}

	if voted && st.resolver != nil {
		_ = st.resolver.NewPoint(st.ctx, bl.Point())
	}

	return voted, nil
}

func (st *baseBallotHandler) findWithdraws(height base.Height, suf base.Suffrage) (
	withdraws []base.SuffrageWithdrawOperation,
	withdrawfacts []util.Hash,
	_ error,
) {
	ops, err := st.args.SuffrageVotingFindFunc(context.Background(), height, suf)
	if err != nil {
		return nil, nil, err
	}

	if len(ops) < 1 {
		return nil, nil, nil
	}

	withdrawfacts = make([]util.Hash, len(ops))

	for i := range ops {
		withdrawfacts[i] = ops[i].WithdrawFact().Hash()
	}

	withdraws = ops

	return withdraws, withdrawfacts, nil
}

func (st *baseBallotHandler) localIsInConsensusNodes(height base.Height) (base.Suffrage, error) {
	l := st.Log().With().Interface("height", height).Logger()

	switch suf, found, err := st.args.NodeInConsensusNodesFunc(st.local, height); {
	case errors.Is(err, storage.ErrNotFound):
		return nil, newSyncingSwitchContext(StateConsensus, height-1)
	case err != nil:
		return nil, err
	case suf == nil || suf.Len() < 1:
		l.Debug().Msg("empty suffrage of next block; moves to broken state")

		return nil, util.ErrNotFound.Errorf("empty suffrage")
	case !found:
		l.Debug().Msg("local is not in consensus nodes at next block; moves to syncing state")

		return nil, newSyncingSwitchContext(StateConsensus, height-1)
	default:
		l.Debug().
			Bool("in_suffrage", suf.ExistsPublickey(st.local.Address(), st.local.Publickey())).
			Msg("local is in consensus nodes and is in suffrage?")

		return suf, nil
	}
}

func (st *baseBallotHandler) timerINITBallot(
	newballotf func(context.Context) base.INITBallot,
	voteError func(error),
	initialWait time.Duration,
) error {
	started := time.Now()
	wait := time.Nanosecond

	var bl base.INITBallot

	var createOnce sync.Once
	donech := make(chan base.INITBallot, 1)

	var ballotCreated bool

	if _, err := st.timers.New(
		timerIDBroadcastINITBallot,
		func(uint64) time.Duration {
			if bl == nil {
				return time.Millisecond * 100 //nolint:gomnd // short enough
			}

			if !ballotCreated {
				ballotCreated = true

				return wait
			}

			return st.params.IntervalBroadcastBallot()
		},
		func(tctx context.Context, i uint64) (bool, error) {
			if bl != nil {
				if err := st.ballotBroadcaster.Broadcast(bl); err != nil {
					st.Log().Error().Err(err).Msg("failed to broadcast ballot; keep going")

					return true, nil
				}

				st.Log().Debug().Msg("ballot broadcasted")

				return true, nil
			}

			createOnce.Do(func() {
				go func() {
					donech <- newballotf(tctx)
				}()
			})

			select {
			case <-tctx.Done():
				return false, nil
			case bl = <-donech:
				if bl == nil {
					return false, nil
				}

				if d := time.Since(started); d < initialWait {
					wait = initialWait - d
				}

				go func() {
					<-time.After(wait)

					if _, err := st.vote(bl); err != nil {
						voteError(err)
					}
				}()
			}

			return true, nil
		},
	); err != nil {
		return err
	}

	return nil
}

func (st *baseBallotHandler) prepareNextBlock(
	avp base.ACCEPTVoteproof,
	timerIDs []util.TimerID,
) {
	point := avp.Point().Point.NextHeight()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Object("point", point).Logger()

	var suf base.Suffrage

	var sctx switchContext

	switch i, err := st.localIsInConsensusNodes(point.Height()); {
	case errors.As(err, &sctx):
		go st.switchState(sctx)

		return
	case err != nil:
		l.Debug().Err(err).Msg("failed to prepare next block; moves to broken state")

		go st.switchState(newBrokenSwitchContext(StateConsensus, err))
	default:
		suf = i
	}

	if err := st.timerINITBallot(
		func(ctx context.Context) base.INITBallot {
			bl, err := st.makeNextBlockBallot(ctx, avp, suf, st.params.WaitPreparingINITBallot())
			if err != nil {
				go st.switchState(newBrokenSwitchContext(StateConsensus, err))

				return nil
			}

			return bl
		},
		func(err error) {
			switch {
			case err == nil:
			case errors.Is(err, errFailedToVoteNotInConsensus):
				st.Log().Debug().Err(err).Msg("failed to vote init ballot; moves to syncing state")

				go st.switchState(newSyncingSwitchContext(StateConsensus, point.Height()-1))
			default:
				st.Log().Debug().Err(err).Msg("failed to vote init ballot; moves to broken state")

				go st.switchState(newBrokenSwitchContext(StateConsensus, err))
			}
		},
		st.params.WaitPreparingINITBallot(),
	); err != nil {
		l.Error().Err(err).Msg("failed to prepare init ballot for next block")

		return
	}

	if err := st.timers.StopOthers(timerIDs); err != nil {
		l.Error().Err(err).Msg("failed to start timers for next block")

		return
	}
}

var errFailedToVoteNotInConsensus = util.NewMError("failed to vote; local not in consensus nodes")

func preventVotingWithEmptySuffrage(
	local base.Node,
	voteFunc func(base.Ballot) (bool, error),
	nodeInConsensusNodes isaac.NodeInConsensusNodesFunc,
) func(base.Ballot) (bool, error) {
	return func(bl base.Ballot) (bool, error) {
		e := util.StringErrorFunc("failed to vote")

		switch suf, found, err := nodeInConsensusNodes(local, bl.Point().Height()); {
		case err != nil:
			if !errors.Is(err, storage.ErrNotFound) {
				return false, e(err, "")
			}
		case suf == nil || len(suf.Nodes()) < 1:
			return false, e(nil, "empty suffrage")
		case !found:
			return false, e(errFailedToVoteNotInConsensus.Errorf("ballot=%q", bl.Point()), "")
		}

		return voteFunc(bl)
	}
}

func broadcastBallot(
	bl base.Ballot,
	timers *util.SimpleTimers,
	timerid util.TimerID,
	broadcastBallotFunc func(base.Ballot) error,
	log *logging.Logging,
	interval func(uint64) time.Duration,
) error {
	l := log.Log().With().
		Stringer("ballot_hash", bl.SignFact().Fact().Hash()).
		Logger()
	l.Debug().Interface("ballot", bl).Object("point", bl.Point()).Msg("trying to broadcast ballot")

	_, err := timers.New(
		timerid,
		interval,
		func(_ context.Context, i uint64) (bool, error) {
			if err := broadcastBallotFunc(bl); err != nil {
				l.Error().Err(err).Msg("failed to broadcast ballot; keep going")

				return true, nil
			}

			l.Debug().Msg("ballot broadcasted")

			return true, nil
		},
	)

	return errors.WithMessage(err, "failed to broadcast ballot")
}
