package isaacstates

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
)

type SuffrageVotingFindFunc func(context.Context, base.Height, base.Suffrage) ([]base.SuffrageExpelOperation, error)

type baseBallotHandlerArgs struct {
	ProposalSelectFunc         isaac.ProposalSelectFunc
	NodeInConsensusNodesFunc   isaac.NodeInConsensusNodesFunc
	VoteFunc                   func(base.Ballot) (bool, error)
	SuffrageVotingFindFunc     SuffrageVotingFindFunc
	IntervalBroadcastBallot    func() time.Duration
	WaitPreparingINITBallot    func() time.Duration
	MinWaitNextBlockINITBallot func() time.Duration
	NewINITBallotFactFunc      func(
		_ context.Context,
		point base.Point,
		previousBlock util.Hash,
		proposal base.ProposalSignFact,
		expelfacts []util.Hash,
	) (base.INITBallotFact, error)
	IsEmptyProposalNoBlockFunc func() bool
	IsEmptyProposalFunc        func(context.Context, base.ProposalSignFact) (bool, error)
}

func newBaseBallotHandlerArgs() baseBallotHandlerArgs {
	return baseBallotHandlerArgs{
		NodeInConsensusNodesFunc: func(base.Node, base.Height) (base.Suffrage, bool, error) {
			return nil, false, util.ErrNotImplemented.Errorf("NodeInConsensusNodesFunc")
		},
		VoteFunc: func(base.Ballot) (bool, error) {
			return false, util.ErrNotImplemented.Errorf("VoteFunc")
		},
		SuffrageVotingFindFunc: func(context.Context, base.Height, base.Suffrage) (
			[]base.SuffrageExpelOperation, error,
		) {
			return nil, util.ErrNotImplemented.Errorf("SuffrageVotingFindFunc")
		},
		IntervalBroadcastBallot: func() time.Duration {
			return isaac.DefaultntervalBroadcastBallot
		},
		WaitPreparingINITBallot: func() time.Duration {
			return isaac.DefaultWaitPreparingINITBallot
		},
		NewINITBallotFactFunc: func(
			_ context.Context,
			point base.Point,
			previousBlock util.Hash,
			proposal base.ProposalSignFact,
			expelfacts []util.Hash,
		) (base.INITBallotFact, error) {
			return isaac.NewINITBallotFact(
				point,
				previousBlock,
				proposal.Fact().Hash(),
				expelfacts,
			), nil
		},
		IsEmptyProposalNoBlockFunc: func() bool { return false },
		IsEmptyProposalFunc:        func(context.Context, base.ProposalSignFact) (bool, error) { return false, nil },
		MinWaitNextBlockINITBallot: func() time.Duration { return time.Second * 2 },
	}
}

func (args *baseBallotHandlerArgs) IsEmptyProposalNoBlock() bool {
	return args.IsEmptyProposalNoBlockFunc()
}

func (args *baseBallotHandlerArgs) IsEmptyProposal(ctx context.Context, pr base.ProposalSignFact) (bool, error) {
	return args.IsEmptyProposalFunc(ctx, pr)
}

type baseBallotHandler struct {
	*baseHandler
	args              *baseBallotHandlerArgs
	ballotBroadcaster BallotBroadcaster
	voteFunc          func(base.Ballot) (bool, error)
	resolver          BallotStuckResolver
}

func newBaseBallotHandlerType(
	state StateType,
	networkID base.NetworkID,
	local base.LocalNode,
	args *baseBallotHandlerArgs,
) baseBallotHandler {
	args.VoteFunc = preventVotingWithEmptySuffrage(
		local,
		args.VoteFunc,
		args.NodeInConsensusNodesFunc,
	)

	return baseBallotHandler{
		baseHandler: newBaseHandlerType(state, networkID, local),
		args:        args,
		voteFunc:    func(base.Ballot) (bool, error) { return false, errors.Errorf("not voted") },
	}
}

func (st baseBallotHandler) new() baseBallotHandler {
	return baseBallotHandler{
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
	st.bbt = sts.bbt.Clone().SetBroadcasterFunc(func(ctx context.Context, bl base.Ballot) error {
		_ = st.broadcastBallot(ctx, bl)

		return nil
	})
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
		return nil, errors.WithMessage(err, "prepare next round init ballot")
	}

	return bl, nil
}

func (st *baseBallotHandler) makeNextBlockBallot(
	ctx context.Context,
	avp base.ACCEPTVoteproof,
	suf base.Suffrage,
	initialWait time.Duration,
) (base.INITBallot, error) {
	if wait := st.args.MinWaitNextBlockINITBallot(); wait < st.args.WaitPreparingINITBallot() {
		st.Log().Debug().Dur("wait", wait).Msg("wait for next block init balllot")

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
			initialWait -= wait //revive:disable-line:modifies-parameter
		}
	}

	bl, err := st.makeINITBallot(
		ctx,
		avp.Point().Point.NextHeight(),
		avp.BallotMajority().NewBlock(),
		avp,
		suf,
		initialWait,
	)
	if err != nil {
		return nil, errors.WithMessage(err, "prepare next block init ballot")
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
	e := util.StringError("prepare init ballot")

	l := st.Log().With().Str("voteproof", vp.ID()).Object("point", point).Logger()

	switch bl, found, err := st.ballotBroadcaster.Ballot(point, base.StageINIT, false); {
	case err != nil:
		return nil, e.Wrap(err)
	case found:
		l.Debug().Msg("init ballot found in ballot pool")

		return bl.(base.INITBallot), nil //nolint:forcetypeassert //...
	}

	var pr base.ProposalSignFact

	switch i, err := st.requestProposal(ctx, point, prevBlock, initialWait); {
	case err != nil:
		return nil, e.Wrap(err)
	default:
		pr = i
	}

	// NOTE collect suffrage expel operations
	expels, expelfacts, err := st.findExpels(point.Height(), suf)
	if err != nil {
		return nil, err
	}

	// NOTE broadcast next init ballot
	var fact base.INITBallotFact

	switch i, err := st.args.NewINITBallotFactFunc(
		ctx,
		point,
		prevBlock,
		pr,
		expelfacts,
	); {
	case err != nil:
		return nil, e.Wrap(err)
	default:
		fact = i
	}

	sf := isaac.NewINITBallotSignFact(fact)

	if err := sf.NodeSign(st.local.Privatekey(), st.networkID, st.local.Address()); err != nil {
		return nil, e.WithMessage(err, "make next init ballot")
	}

	bl := isaac.NewINITBallot(vp, sf, expels)

	return bl, nil
}

func (st *baseBallotHandler) defaultPrepareACCEPTBallot(
	ivp base.INITVoteproof,
	newBlock util.Hash,
	initialWait time.Duration,
	fact base.ACCEPTBallotFact,
) error {
	e := util.StringError("prepare accept ballot")

	bl, err := st.makeACCEPTBallot(ivp, newBlock, fact)
	if err != nil {
		return e.Wrap(err)
	}

	go func() {
		<-time.After(initialWait)

		switch _, err := st.vote(bl); {
		case err == nil:
		case errors.Is(err, errFailedToVoteNotInConsensus):
			st.Log().Debug().Err(err).Msg("failed to vote accept ballot; moves to syncing state")

			go st.switchState(newSyncingSwitchContextWithVoteproof(StateConsensus, ivp))
		default:
			st.Log().Error().Err(err).Msg("failed to vote accept ballot; moves to broken state")

			go st.switchState(newBrokenSwitchContext(StateConsensus, err))
		}
	}()

	if err := st.bbt.ACCEPT(bl, initialWait); err != nil {
		return e.WithMessage(err, "broadcast accept ballot")
	}

	return nil
}

func (st *baseBallotHandler) makeACCEPTBallot(
	ivp base.INITVoteproof,
	newBlock util.Hash,
	fact base.ACCEPTBallotFact,
) (base.ACCEPTBallot, error) {
	switch bl, found, err := st.ballotBroadcaster.Ballot(ivp.Point().Point, base.StageACCEPT, false); {
	case err != nil:
		return nil, err
	case found:
		st.Log().Debug().Str("voteproof", ivp.ID()).Msg("accept ballot found in ballot pool")

		return bl.(base.ACCEPTBallot), nil //nolint:forcetypeassert //...
	}

	var expels []base.SuffrageExpelOperation
	afact := fact

	if fact == nil {
		// NOTE add SuffrageExpelOperations into ballot from init voteproof
		var expelfacts []util.Hash

		if i, ok := ivp.(base.ExpelVoteproof); ok {
			expels = i.Expels()

			expelfacts = make([]util.Hash, len(expels))

			for i := range expels {
				expelfacts[i] = expels[i].ExpelFact().Hash()
			}
		}

		afact = isaac.NewACCEPTBallotFact(
			ivp.Point().Point,
			ivp.BallotMajority().Proposal(),
			newBlock,
			expelfacts,
		)
	}

	signfact := isaac.NewACCEPTBallotSignFact(afact)

	if err := signfact.NodeSign(st.local.Privatekey(), st.networkID, st.local.Address()); err != nil {
		return nil, err
	}

	bl := isaac.NewACCEPTBallot(ivp, signfact, expels)

	return bl, nil
}

func (st *baseBallotHandler) defaultPrepareSuffrageConfirmBallot(vp base.Voteproof) {
	l := st.Log().With().Str("voteproof", vp.ID()).Logger()

	if _, ok := vp.(base.ExpelVoteproof); !ok {
		l.Error().Msg("expected ExpelVoteproof for suffrage sign voting")

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

			go st.switchState(newSyncingSwitchContextWithVoteproof(StateConsensus, vp))
		default:
			st.Log().Debug().Err(err).Msg("failed to vote suffrage confirm ballot; moves to broken state")

			go st.switchState(newBrokenSwitchContext(StateConsensus, err))
		}
	}()

	if err := st.bbt.SuffrageConfirm(bl, 0); err != nil {
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
			Str("voteproof", vp.ID()).
			Object("point", vp.Point().Point).
			Msg("init suffrage confirm ballot found in ballot pool")

		return bl.(base.INITBallot), nil //nolint:forcetypeassert //...
	}

	ifact := vp.Majority().(isaac.INITBallotFact) //nolint:forcetypeassert //...
	expelfacts := ifact.ExpelFacts()

	fact := isaac.NewSuffrageConfirmBallotFact(
		vp.Point().Point,
		ifact.PreviousBlock(),
		ifact.Proposal(),
		expelfacts,
	)

	sf := isaac.NewINITBallotSignFact(fact)

	if err := sf.NodeSign(st.local.Privatekey(), st.networkID, st.local.Address()); err != nil {
		go st.switchState(
			newBrokenSwitchContext(st.stt, errors.WithMessage(err, "make suffrage confirm ballot")),
		)

		return nil, err
	}

	bl := isaac.NewINITBallot(vp, sf, nil)

	return bl, nil
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

func (st *baseBallotHandler) findExpels(height base.Height, suf base.Suffrage) (
	expels []base.SuffrageExpelOperation,
	expelfacts []util.Hash,
	_ error,
) {
	ops, err := st.args.SuffrageVotingFindFunc(context.Background(), height, suf)
	if err != nil {
		return nil, nil, err
	}

	if len(ops) < 1 {
		return nil, nil, nil
	}

	expelfacts = make([]util.Hash, len(ops))

	for i := range ops {
		expelfacts[i] = ops[i].ExpelFact().Hash()
	}

	expels = ops

	return expels, expelfacts, nil
}

func (st *baseBallotHandler) localIsInConsensusNodes(height base.Height) (base.Suffrage, error) {
	l := st.Log().With().Interface("height", height).Logger()

	switch suf, found, err := st.args.NodeInConsensusNodesFunc(st.local, height); {
	case errors.Is(err, storage.ErrNotFound):
		return nil, newSyncingSwitchContext(StateConsensus, height)
	case err != nil:
		return nil, err
	case !found:
		l.Debug().Msg("local is not in consensus nodes at next block; moves to syncing state")

		return nil, newSyncingSwitchContext(StateConsensus, height)
	case suf == nil || suf.Len() < 1:
		l.Debug().Msg("empty suffrage of next block; moves to broken state")

		return nil, util.ErrNotFound.Errorf("empty suffrage")
	default:
		l.Debug().
			Bool("in_suffrage", suf.ExistsPublickey(st.local.Address(), st.local.Publickey())).
			Msg("local is in consensus nodes and is in suffrage?")

		return suf, nil
	}
}

func (st *baseBallotHandler) prepareINITBallot(
	newballotf func(context.Context) base.INITBallot,
	voteError func(error),
	initialWait time.Duration,
) error {
	started := time.Now()

	bl := newballotf(st.ctx)
	if bl == nil {
		return nil
	}

	wait := time.Nanosecond
	if d := time.Since(started); d < initialWait {
		wait = initialWait - d
	}

	go func() {
		if wait > 0 {
			<-time.After(wait)
		}

		if _, err := st.vote(bl); err != nil {
			voteError(err)
		}
	}()

	return st.bbt.INIT(bl, wait)
}

func (st *baseBallotHandler) defaultPrepareNextBlockBallot(
	avp base.ACCEPTVoteproof,
	suf base.Suffrage,
	wait time.Duration,
) error {
	point := avp.Point().Point.NextHeight()

	l := st.Log().With().Str("voteproof", avp.ID()).Object("point", point).Logger()

	if err := st.prepareINITBallot(
		func(ctx context.Context) base.INITBallot {
			switch bl, err := st.makeNextBlockBallot(ctx, avp, suf, wait); {
			case errors.Is(err, context.Canceled):
				return nil
			case err != nil:
				go st.switchState(newBrokenSwitchContext(StateConsensus, err))

				return nil
			default:
				return bl
			}
		},
		func(err error) {
			switch {
			case err == nil:
			case errors.Is(err, context.Canceled):
			case errors.Is(err, errFailedToVoteNotInConsensus):
				l.Debug().Err(err).Msg("failed to vote init ballot; moves to syncing state")

				go st.switchState(newSyncingSwitchContextWithVoteproof(StateConsensus, avp))
			default:
				l.Debug().Err(err).Msg("failed to vote init ballot; moves to broken state")

				go st.switchState(newBrokenSwitchContext(StateConsensus, err))
			}
		},
		st.args.WaitPreparingINITBallot(),
	); err != nil {
		l.Error().Err(err).Msg("failed to prepare init ballot for next block")

		return err
	}

	return nil
}

func (st *baseBallotHandler) defaultPrepareNextRoundBallot(
	vp base.Voteproof,
	previousBlock util.Hash,
	suf base.Suffrage,
	wait time.Duration,
) error {
	point := vp.Point().Point.NextRound()

	l := st.Log().With().Str("voteproof", vp.ID()).Object("point", point).Logger()

	if err := st.prepareINITBallot(
		func(ctx context.Context) base.INITBallot {
			switch bl, err := st.makeNextRoundBallot(ctx, vp, previousBlock, suf, wait); {
			case errors.Is(err, context.Canceled):
				return nil
			case err != nil:
				go st.switchState(newBrokenSwitchContext(StateConsensus, err))

				return nil
			default:
				return bl
			}
		},
		func(err error) {
			switch {
			case err == nil:
			case errors.Is(err, context.Canceled):
			case errors.Is(err, errFailedToVoteNotInConsensus):
				st.Log().Debug().Err(err).Msg("failed to vote init ballot; moves to syncing state")

				go st.switchState(newSyncingSwitchContextWithVoteproof(StateConsensus, vp))
			default:
				st.Log().Debug().Err(err).Msg("failed to vote init ballot; moves to broken state")

				go st.switchState(newBrokenSwitchContext(StateConsensus, err))
			}
		},
		st.args.WaitPreparingINITBallot(),
	); err != nil {
		l.Error().Err(err).Msg("failed to prepare init ballot for next block")

		return err
	}

	return nil
}

func (st *baseBallotHandler) requestProposal(
	ctx context.Context,
	point base.Point,
	previousBlock util.Hash,
	initialWait time.Duration,
) (base.ProposalSignFact, error) {
	l := st.Log().With().
		Object("point", point).
		Stringer("previous_block", previousBlock).
		Stringer("initial_wait", initialWait).
		Logger()

	started := time.Now()
	defer func() {
		l.Debug().Stringer("elapsed", time.Since(started)).Msg("proposal selection done")
	}()

	switch pr, err := st.args.ProposalSelectFunc(ctx, point, previousBlock, initialWait); {
	case err == nil:
		l.Debug().Interface("proposal", pr).Msg("proposal selected")

		return pr, nil
	default:
		l.Error().Err(err).Stringer("initial_wait", initialWait).Msg("failed to select proposal")

		return nil, err
	}
}

func (st *baseBallotHandler) broadcastBallot(ctx context.Context, ballot base.Ballot) error {
	go func() {
		if err := st.sendBallotToHandoverY(ctx, ballot); err != nil {
			st.Log().Error().Err(err).Msg("failed to send ballot to handover y")
		}
	}()

	return st.ballotBroadcaster.Broadcast(ballot)
}

func (st *baseBallotHandler) sendBallotToHandoverY(ctx context.Context, ballot base.Ballot) error {
	broker := st.handoverXBroker()
	if broker == nil {
		return nil
	}

	return broker.sendBallot(ctx, ballot)
}

var errFailedToVoteNotInConsensus = util.NewIDError("vote; local not in consensus nodes")

func preventVotingWithEmptySuffrage(
	local base.Node,
	voteFunc func(base.Ballot) (bool, error),
	nodeInConsensusNodes isaac.NodeInConsensusNodesFunc,
) func(base.Ballot) (bool, error) {
	return func(bl base.Ballot) (bool, error) {
		e := util.StringError("vote")

		switch suf, found, err := nodeInConsensusNodes(local, bl.Point().Height().SafePrev()); {
		case err != nil:
			if !errors.Is(err, storage.ErrNotFound) {
				return false, e.Wrap(err)
			}
		case suf == nil || len(suf.Nodes()) < 1:
			return false, e.Errorf("empty suffrage")
		case !found:
			return false, e.Wrap(errFailedToVoteNotInConsensus.Errorf("ballot=%q", bl.Point()))
		}

		return voteFunc(bl)
	}
}

func newEmptyProposalINITBallotFactFunc(args interface {
	IsEmptyProposalNoBlock() bool
	IsEmptyProposal(context.Context, base.ProposalSignFact) (bool, error)
}) func(
	_ context.Context,
	_ base.Point,
	previousBlock util.Hash,
	_ base.ProposalSignFact,
	expelfacts []util.Hash,
) (base.INITBallotFact, error) {
	return func(
		ctx context.Context,
		point base.Point,
		previousBlock util.Hash,
		proposal base.ProposalSignFact,
		expelfacts []util.Hash,
	) (base.INITBallotFact, error) {
		if len(expelfacts) > 0 || !args.IsEmptyProposalNoBlock() {
			return nil, nil
		}

		if isempty, err := args.IsEmptyProposal(ctx, proposal); err != nil || !isempty {
			return nil, err
		}

		// NOTE empty-proposal-init-ballot-fact
		return isaac.NewEmptyProposalINITBallotFact(
			point,
			previousBlock,
			proposal.Fact().Hash(),
		), nil
	}
}
