package basicstates

import (
	"context"
	"time"

	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/base/ballot"
	"github.com/spikeekips/mitum/base/block"
	"github.com/spikeekips/mitum/base/prprocessor"
	"github.com/spikeekips/mitum/base/seal"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
)

var BlockPrefixFailedProcessProposal = []byte("processproposalfailed")

type ConsensusState struct {
	*logging.Logging
	*BaseState
	storage       storage.Storage
	blockFS       *storage.BlockFS
	policy        *isaac.LocalPolicy
	nodepool      *network.Nodepool
	suffrage      base.Suffrage
	proposalMaker *isaac.ProposalMaker
	pps           *prprocessor.Processors
}

func NewConsensusState(
	st storage.Storage,
	blockFS *storage.BlockFS,
	policy *isaac.LocalPolicy,
	nodepool *network.Nodepool,
	suffrage base.Suffrage,
	proposalMaker *isaac.ProposalMaker,
	pps *prprocessor.Processors,
) *ConsensusState {
	return &ConsensusState{
		Logging: logging.NewLogging(func(c logging.Context) logging.Emitter {
			return c.Str("module", "basic-consensus-state")
		}),
		BaseState:     NewBaseState(base.StateConsensus),
		storage:       st,
		blockFS:       blockFS,
		policy:        policy,
		nodepool:      nodepool,
		suffrage:      suffrage,
		proposalMaker: proposalMaker,
		pps:           pps,
	}
}

// Enter starts consensus state with the voteproof,
// - height of last init voteproof + 1
func (st *ConsensusState) Enter(sctx StateSwitchContext) (func() error, error) {
	callback := EmptySwitchFunc
	if i, err := st.BaseState.Enter(sctx); err != nil {
		return nil, err
	} else if i != nil {
		callback = i
	}

	if sctx.Voteproof() == nil {
		return nil, xerrors.Errorf("consensus state not allowed to enter without voteproof")
	} else if stage := sctx.Voteproof().Stage(); stage != base.StageINIT {
		return nil, xerrors.Errorf("consensus state not allowed to enter with init voteproof, not %v", stage)
	}

	l := isaac.LoggerWithVoteproof(sctx.Voteproof(), st.Log())

	if lvp := st.LastINITVoteproof(); lvp == nil {
		return nil, xerrors.Errorf("empty last init voteproof")
	} else if base.CompareVoteproof(sctx.Voteproof(), lvp) != 0 {
		h := sctx.Voteproof().Height()
		lh := lvp.Height()
		if h != lh+1 { // NOTE tolerate none-expected voteproof
			l.Error().Err(
				xerrors.Errorf("wrong height of voteproof, %v than last init voteproof, %v or %v + 1", h, lh, lh),
			).Msg("wrong incoming voteproof for consensus state, but enter the consensus state")

			return nil, nil
		}
	}

	return func() error {
		if err := callback(); err != nil {
			return err
		}

		return st.ProcessVoteproof(sctx.Voteproof())
	}, nil
}

func (st *ConsensusState) Exit(sctx StateSwitchContext) (func() error, error) {
	callback := EmptySwitchFunc
	if i, err := st.BaseState.Exit(sctx); err != nil {
		return nil, err
	} else if i != nil {
		callback = i
	}

	return func() error {
		if err := callback(); err != nil {
			return err
		}

		return st.Timers().StopTimers([]localtime.TimerID{
			TimerIDBroadcastINITBallot,
			TimerIDBroadcastProposal,
			TimerIDFindProposal,
		})
	}, nil
}

func (st *ConsensusState) ProcessVoteproof(voteproof base.Voteproof) error {
	if voteproof.Result() == base.VoteResultDraw { // NOTE moves to next round
		isaac.LoggerWithVoteproof(voteproof, st.Log()).Debug().Msg("draw voteproof found; moves to next round")

		return st.nextRound(voteproof)
	}

	switch s := voteproof.Stage(); s {
	case base.StageINIT:
		return st.newINITVoteproof(voteproof)
	case base.StageACCEPT:
		return st.newACCEPTVoteproof(voteproof)
	default:
		return util.IgnoreError.Errorf("unsupported voteproof stage, %v found", s)
	}
}

// ProcessProposal processes incoming proposal, not from local
func (st *ConsensusState) ProcessProposal(proposal ballot.Proposal) error {
	if err := st.broadcastProposal(proposal); err != nil {
		return err
	}

	started := time.Now()
	if voteproof, newBlock, ok := st.processProposal(proposal); newBlock != nil {
		if st.suffrage.NumberOfActing() > 1 && ok {
			go func() {
				if err := st.broadcastSIGNBallot(proposal, newBlock); err != nil {
					st.Log().Error().Err(err).Msg("failed to broadcast sign ballot")
				}
			}()
		}

		initialDelay := st.policy.WaitBroadcastingACCEPTBallot() - (time.Since(started))
		if initialDelay < 0 {
			initialDelay = time.Nanosecond
		}

		return st.broadcastACCEPTBallot(newBlock, proposal.Hash(), voteproof, initialDelay)
	} else {
		return nil
	}
}

func (st *ConsensusState) newINITVoteproof(voteproof base.Voteproof) error {
	if err := st.Timers().StopTimers([]localtime.TimerID{TimerIDBroadcastProposal}); err != nil {
		return err
	}

	l := isaac.LoggerWithVoteproof(voteproof, st.Log())

	l.Debug().Msg("processing new init voteproof; propose proposal")

	var proposal ballot.Proposal

	var actingSuffrage base.ActingSuffrage
	if i, err := st.suffrage.Acting(voteproof.Height(), voteproof.Round()); err != nil {
		l.Error().Err(err).Msg("failed to get acting suffrage")

		return err
	} else {
		actingSuffrage = i
	}

	// NOTE find proposal first
	if i, err := st.findProposal(voteproof.Height(), voteproof.Round(), actingSuffrage.Proposer()); err != nil {
		return err
	} else if i != nil {
		l.Debug().Msg("proposal found in local")

		proposal = i
	}

	if proposal == nil && actingSuffrage.Proposer().Equal(st.nodepool.Local().Address()) {
		if i, err := st.prepareProposal(voteproof.Height(), voteproof.Round(), voteproof); err != nil {
			return err
		} else if i != nil {
			proposal = i
		}
	}

	if proposal != nil { // NOTE if proposer, broadcast proposal
		return st.ProcessProposal(proposal)
	}

	// NOTE wait new proposal
	return st.whenProposalTimeout(voteproof, actingSuffrage.Proposer())
}

func (st *ConsensusState) newACCEPTVoteproof(voteproof base.Voteproof) error {
	if err := st.processACCEPTVoteproof(voteproof); err != nil {
		return err
	}

	return st.broadcastNewINITBallot(voteproof)
}

func (st *ConsensusState) processACCEPTVoteproof(voteproof base.Voteproof) error {
	var fact ballot.ACCEPTBallotFact
	if f, ok := voteproof.Majority().(ballot.ACCEPTBallotFact); !ok {
		return xerrors.Errorf("needs ACCEPTBallotFact: fact=%T", voteproof.Majority())
	} else {
		fact = f
	}

	l := isaac.LoggerWithVoteproof(voteproof, st.Log().WithLogger(func(ctx logging.Context) logging.Emitter {
		return ctx.Hinted("proposal_hash", fact.Proposal()).
			Dict("block", logging.Dict().
				Hinted("hash", fact.NewBlock()).Hinted("height", voteproof.Height()).Hinted("round", voteproof.Round()))
	}))

	s := time.Now()

	if proposal, err := st.processProposalOfACCEPTVoteproof(voteproof); err != nil {
		return xerrors.Errorf("failed to process proposal of accept voteproof: %w", err)
	} else if proposal != nil {
		if err := st.broadcastProposal(proposal); err != nil {
			return err
		}
	}

	l.Debug().Msg("trying to store new block")
	var newBlock block.Block
	{
		var err error

		// NOTE no timeout to store block
		if result := <-st.pps.Save(context.Background(), fact.Proposal(), voteproof); result.Err != nil {
			err = result.Err
		} else if newBlock = result.Block; newBlock == nil {
			err = xerrors.Errorf("failed to process Proposal; empty Block returned")
		}

		if err != nil {
			l.Error().Err(err).Msg("failed to save block from accept voteproof; moves to syncing")

			// NOTE if failed to save block, moves to syncing
			return NewStateSwitchContext(base.StateConsensus, base.StateSyncing).
				SetVoteproof(voteproof).
				SetError(err)
		}
	}

	l.Info().Dur("elapsed", time.Since(s)).Msg("new block stored")

	return st.NewBlocks([]block.Block{newBlock})
}

func (st *ConsensusState) processProposalOfACCEPTVoteproof(voteproof base.Voteproof) (ballot.Proposal, error) {
	var fact ballot.ACCEPTBallotFact
	if f, ok := voteproof.Majority().(ballot.ACCEPTBallotFact); !ok {
		return nil, xerrors.Errorf("needs ACCEPTBallotFact: fact=%T", voteproof.Majority())
	} else {
		fact = f
	}

	l := isaac.LoggerWithVoteproof(voteproof, st.Log().WithLogger(func(ctx logging.Context) logging.Emitter {
		return ctx.Hinted("proposal_hash", fact.Proposal()).
			Dict("block", logging.Dict().
				Hinted("hash", fact.NewBlock()).Hinted("height", voteproof.Height()).Hinted("round", voteproof.Round()))
	}))

	// NOTE if proposal is not yet processed, process first.

	l.Debug().Msg("checking processing state of proposal")
	switch s := st.pps.CurrentState(fact.Proposal()); s {
	case prprocessor.Preparing, prprocessor.Prepared, prprocessor.Saving:
		l.Debug().Msg("processing proposal of accept voteproof")

		return nil, nil
	case prprocessor.Saved:
		l.Debug().Msg("already proposal of accept voteproof processed")

		return nil, nil
	default:
		l.Debug().Str("state", s.String()).Msg("proposal of accept voteproof not yet processed, process it")
	}

	var proposal ballot.Proposal
	switch i, found, err := st.storage.Seal(fact.Proposal()); {
	case err != nil:
		return nil, xerrors.Errorf("failed to find proposal of accept voteproof in local: %w", err)
	case !found:
		return nil, xerrors.Errorf("proposal of accept voteproof not found in local")
	default:
		if j, ok := i.(ballot.Proposal); !ok {
			return nil, xerrors.Errorf("proposal of accept voteproof is not proposal, %T", i)
		} else {
			proposal = j
		}
	}

	if _, _, ok := st.processProposal(proposal); ok {
		return proposal, nil
	}

	switch s := st.pps.CurrentState(proposal.Hash()); s {
	case prprocessor.Preparing, prprocessor.Prepared, prprocessor.Saving:
		l.Debug().Msg("processing proposal of accept voteproof")

		return proposal, nil
	case prprocessor.Saved:
		l.Debug().Msg("block saved from proposal of accept voteproof")

		return proposal, nil
	default:
		return proposal, xerrors.Errorf("failed to process proposal of accept voteproof")
	}
}

func (st *ConsensusState) findProposal(
	height base.Height,
	round base.Round,
	proposer base.Address,
) (ballot.Proposal, error) {
	switch i, found, err := st.storage.Proposal(height, round, proposer); {
	case err != nil:
		return nil, err
	case !found:
		return nil, nil
	default:
		return i, nil
	}
}

func (st *ConsensusState) processProposal(proposal ballot.Proposal) (base.Voteproof, valuehash.Hash, bool) {
	l := isaac.LoggerWithBallot(proposal, st.Log())

	l.Debug().Msg("processing proposal")

	voteproof := st.LastINITVoteproof()

	// NOTE if last init voteproof is not for proposal, voteproof of proposal
	// will be used.
	if pvp := proposal.Voteproof(); pvp.Height() != voteproof.Height() || pvp.Round() != voteproof.Round() {
		voteproof = pvp
	}

	started := time.Now()

	var newBlock valuehash.Hash
	if result := <-st.pps.NewProposal(context.Background(), proposal, voteproof); result.Err != nil {
		if xerrors.Is(result.Err, util.IgnoreError) {
			return nil, nil, false
		}

		newBlock = valuehash.RandomSHA256WithPrefix(BlockPrefixFailedProcessProposal)
		l.Debug().Err(result.Err).Dur("elapsed", time.Since(started)).Hinted("new_block", newBlock).
			Msg("proposal processging failed; random block hash will be used")

		return voteproof, newBlock, false
	} else {
		newBlock = result.Block.Hash()

		l.Debug().Dur("elapsed", time.Since(started)).Hinted("new_block", newBlock).Msg("proposal processed")

		return voteproof, newBlock, true
	}
}

func (st *ConsensusState) broadcastProposal(proposal ballot.Proposal) error {
	st.Log().Debug().Msg("broadcasting proposal")

	timer, err := localtime.NewCallbackTimer(TimerIDBroadcastProposal, func(int) (bool, error) {
		if err := st.BroadcastSeals(proposal, false); err != nil {
			st.Log().Error().Err(err).Msg("failed to broadcast proposal")
		}

		return true, nil
	}, 0)
	if err != nil {
		return err
	} else {
		timer.SetInterval(func(i int) time.Duration {
			if i < 1 {
				return time.Nanosecond
			}

			return st.policy.IntervalBroadcastingProposal()
		})
	}

	if err := st.Timers().SetTimer(timer); err != nil {
		return err
	}

	return st.Timers().StartTimers([]localtime.TimerID{
		TimerIDBroadcastProposal,
		TimerIDBroadcastINITBallot,
		TimerIDBroadcastACCEPTBallot,
	}, true)
}

func (st *ConsensusState) prepareProposal(
	height base.Height,
	round base.Round,
	voteproof base.Voteproof,
) (ballot.Proposal, error) {
	l := isaac.LoggerWithVoteproof(voteproof, st.Log()).WithLogger(func(ctx logging.Context) logging.Emitter {
		return ctx.Hinted("height", height).Hinted("round", round)
	})

	l.Debug().Msg("local is proposer; preparing proposal")

	if i, err := st.proposalMaker.Proposal(height, round, voteproof); err != nil {
		return nil, err
	} else if err := st.storage.NewProposal(i); err != nil { // NOTE save proposal
		if xerrors.Is(err, storage.DuplicatedError) {
			return i, nil
		}

		return nil, xerrors.Errorf("failed to save proposal: %w", err)
	} else {
		seal.LogEventWithSeal(i, l.Debug(), true).Msg("proposal made")

		return i, nil
	}
}

func (st *ConsensusState) broadcastSIGNBallot(proposal ballot.Proposal, newBlock valuehash.Hash) error {
	st.Log().Debug().Msg("broadcasting sign ballot")

	if i, err := st.suffrage.Acting(proposal.Height(), proposal.Round()); err != nil {
		return err
	} else if !i.Exists(st.nodepool.Local().Address()) {
		return nil
	}

	// NOTE not like broadcasting ACCEPT Ballot, SIGN Ballot will be broadcasted
	// withtout waiting.
	sb := ballot.NewSIGNBallotV0(
		st.nodepool.Local().Address(),
		proposal.Height(),
		proposal.Round(),
		proposal.Hash(),
		newBlock,
	)
	if err := sb.Sign(st.nodepool.Local().Privatekey(), st.policy.NetworkID()); err != nil {
		return err
	} else if err := st.BroadcastSeals(sb, true); err != nil {
		return err
	} else {
		return nil
	}
}

func (st *ConsensusState) broadcastACCEPTBallot(
	newBlock,
	proposal valuehash.Hash,
	voteproof base.Voteproof,
	initialDelay time.Duration,
) error {
	baseBallot := ballot.NewACCEPTBallotV0(
		st.nodepool.Local().Address(),
		voteproof.Height(),
		voteproof.Round(),
		proposal,
		newBlock,
		voteproof,
	)

	if err := baseBallot.Sign(st.nodepool.Local().Privatekey(), st.policy.NetworkID()); err != nil {
		return xerrors.Errorf("failed to re-sign accept ballot: %w", err)
	}

	l := isaac.LoggerWithBallot(baseBallot, st.Log().WithLogger(func(ctx logging.Context) logging.Emitter {
		return ctx.Hinted("new_block", newBlock)
	}))

	l.Debug().Dur("initial_delay", initialDelay).Msg("start timer to broadcast accept ballot")

	timer, err := localtime.NewCallbackTimer(TimerIDBroadcastACCEPTBallot, func(i int) (bool, error) {
		if err := st.BroadcastSeals(baseBallot, i == 0); err != nil {
			l.Error().Err(err).Msg("failed to broadcast accept ballot")
		}

		return true, nil
	}, 0)
	if err != nil {
		return err
	} else {
		timer = timer.SetInterval(func(i int) time.Duration {
			// NOTE at 1st time, wait duration, after then, periodically
			// broadcast ACCEPT Ballot.
			if i < 1 {
				return initialDelay
			}

			return st.policy.IntervalBroadcastingACCEPTBallot()
		})
	}

	if err := st.Timers().SetTimer(timer); err != nil {
		return err
	}

	return st.Timers().StartTimers([]localtime.TimerID{
		TimerIDBroadcastACCEPTBallot,
		TimerIDBroadcastProposal,
	}, true)
}

func (st *ConsensusState) broadcastNewINITBallot(voteproof base.Voteproof) error {
	if s := voteproof.Stage(); s != base.StageACCEPT {
		return xerrors.Errorf("for broadcastNewINITBallot, should be accept voteproof, not %v", s)
	}

	var baseBallot ballot.INITBallotV0
	if b, err := NextINITBallotFromACCEPTVoteproof(st.storage, st.blockFS, st.nodepool.Local(), voteproof); err != nil {
		return err
	} else if err := b.Sign(st.nodepool.Local().Privatekey(), st.policy.NetworkID()); err != nil {
		return xerrors.Errorf("failed to re-sign new init ballot: %w", err)
	} else {
		baseBallot = b
	}

	l := st.Log().WithLogger(func(ctx logging.Context) logging.Emitter {
		return ctx.Hinted("height", baseBallot.Height()).
			Hinted("round", baseBallot.Round()).(logging.Context)
	})
	l.Debug().Msg("broadcasting new init ballot")

	timer, err := localtime.NewCallbackTimer(TimerIDBroadcastINITBallot, func(i int) (bool, error) {
		if err := st.BroadcastSeals(baseBallot, i == 0); err != nil {
			l.Error().Err(err).Msg("failed to broadcast new init ballot")
		}

		return true, nil
	}, st.policy.IntervalBroadcastingINITBallot())
	if err != nil {
		return err
	}

	if err := st.Timers().SetTimer(timer); err != nil {
		return err
	}

	return st.Timers().StartTimers([]localtime.TimerID{
		TimerIDBroadcastINITBallot,
		TimerIDBroadcastProposal,
	}, true)
}

func (st *ConsensusState) whenProposalTimeout(voteproof base.Voteproof, proposer base.Address) error {
	if s := voteproof.Stage(); s != base.StageINIT {
		return xerrors.Errorf("for whenProposalTimeout, should be init voteproof, not %v", s)
	}

	l := st.Log().WithLogger(func(ctx logging.Context) logging.Emitter {
		return ctx.Hinted("height", voteproof.Height()).Hinted("round", voteproof.Round()).(logging.Context)
	})
	l.Debug().Msg("waiting new proposal; if timed out, will move to next round")

	var baseBallot ballot.INITBallotV0
	if b, err := NextINITBallotFromINITVoteproof(st.storage, st.blockFS, st.nodepool.Local(), voteproof); err != nil {
		return err
	} else if err := b.Sign(st.nodepool.Local().Privatekey(), st.policy.NetworkID()); err != nil {
		return xerrors.Errorf("failed to re-sign next init ballot: %w", err)
	} else {
		baseBallot = b
	}

	if timer, err := localtime.NewCallbackTimer(TimerIDBroadcastINITBallot, func(i int) (bool, error) {
		if err := st.BroadcastSeals(baseBallot, i == 0); err != nil {
			l.Error().Err(err).Msg("failed to broadcast next init ballot")
		}

		return true, nil
	}, 0); err != nil {
		return err
	} else {
		timer = timer.SetInterval(func(i int) time.Duration {
			// NOTE at 1st time, wait timeout duration, after then, periodically
			// broadcast INIT Ballot.
			if i < 1 {
				return st.policy.TimeoutWaitingProposal()
			}

			return st.policy.IntervalBroadcastingINITBallot()
		})
		if err := st.Timers().SetTimer(timer); err != nil {
			return err
		}
	}

	if timer, err := localtime.NewCallbackTimer(TimerIDFindProposal, func(int) (bool, error) {
		if i, err := st.findProposal(voteproof.Height(), voteproof.Round(), proposer); err == nil && i != nil {
			l.Debug().Msg("proposal found in local")

			go st.NewProposal(i)
		}

		return true, nil
	}, time.Second); err != nil { // NOTE periodically find proposal
		return err
	} else if err := st.Timers().SetTimer(timer); err != nil {
		return err
	}

	return st.Timers().StartTimers([]localtime.TimerID{
		TimerIDBroadcastINITBallot,
		TimerIDBroadcastProposal,
		TimerIDFindProposal,
	}, true)
}

func (st *ConsensusState) nextRound(voteproof base.Voteproof) error {
	l := st.Log().WithLogger(func(ctx logging.Context) logging.Emitter {
		return ctx.Str("stage", voteproof.Stage().String()).
			Hinted("height", voteproof.Height()).
			Hinted("round", voteproof.Round()).(logging.Context)
	})
	l.Debug().Msg("starting next round")

	var baseBallot ballot.INITBallotV0
	{
		var err error
		switch s := voteproof.Stage(); s {
		case base.StageINIT:
			baseBallot, err = NextINITBallotFromINITVoteproof(st.storage, st.blockFS, st.nodepool.Local(), voteproof)
		case base.StageACCEPT:
			baseBallot, err = NextINITBallotFromACCEPTVoteproof(st.storage, st.blockFS, st.nodepool.Local(), voteproof)
		}
		if err != nil {
			return err
		}
	}

	if err := baseBallot.Sign(st.nodepool.Local().Privatekey(), st.policy.NetworkID()); err != nil {
		return xerrors.Errorf("failed to re-sign next round init ballot: %w", err)
	}

	timer, err := localtime.NewCallbackTimer(TimerIDBroadcastINITBallot, func(i int) (bool, error) {
		if err := st.BroadcastSeals(baseBallot, i == 0); err != nil {
			l.Error().Err(err).Msg("failed to broadcast next round init ballot")
		}

		return true, nil
	}, 0)
	if err != nil {
		return err
	} else {
		timer = timer.SetInterval(func(i int) time.Duration {
			if i < 1 {
				return time.Nanosecond
			}

			return st.policy.IntervalBroadcastingINITBallot()
		})
	}

	if err := st.Timers().SetTimer(timer); err != nil {
		return err
	}

	return st.Timers().StartTimers([]localtime.TimerID{
		TimerIDBroadcastINITBallot,
	}, true)
}
