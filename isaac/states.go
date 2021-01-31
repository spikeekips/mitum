package isaac

import (
	"sync"
	"time"

	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/base/ballot"
	"github.com/spikeekips/mitum/base/block"
	"github.com/spikeekips/mitum/base/seal"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/errors"
	"github.com/spikeekips/mitum/util/logging"
)

var FailedToActivateHandler = errors.NewError("failed to activate handler")

type ConsensusStates struct {
	sync.RWMutex
	*logging.Logging
	*util.FunctionDaemon
	local         *Local
	ballotbox     *Ballotbox
	suffrage      base.Suffrage
	states        map[base.State]StateHandler
	activeHandler StateHandler
	stateChan     chan *StateChangeContext
	sealChan      chan seal.Seal
	voteproofChan chan base.Voteproof
	stopHooks     []func() error
	livp          base.Voteproof
	errChan       chan error
}

func NewConsensusStates(
	local *Local,
	ballotbox *Ballotbox,
	suffrage base.Suffrage,
	booting, joining, consensus, syncing, broken StateHandler,
) (*ConsensusStates, error) {
	var livp base.Voteproof
	if vp, found, err := local.BlockFS().LastVoteproof(base.StageINIT); err != nil {
		return nil, err
	} else if found {
		livp = vp
	}

	css := &ConsensusStates{
		Logging: logging.NewLogging(func(c logging.Context) logging.Emitter {
			return c.Str("module", "consensus-states")
		}),
		local:     local,
		ballotbox: ballotbox,
		suffrage:  suffrage,
		states: map[base.State]StateHandler{
			base.StateBooting:   booting,
			base.StateJoining:   joining,
			base.StateConsensus: consensus,
			base.StateSyncing:   syncing,
			base.StateBroken:    broken,
		},
		stateChan:     make(chan *StateChangeContext),
		voteproofChan: make(chan base.Voteproof, 100),
		sealChan:      make(chan seal.Seal),
		livp:          livp,
		errChan:       make(chan error, 100),
	}
	css.FunctionDaemon = util.NewFunctionDaemon(css.start, false)

	return css, nil
}

func (css *ConsensusStates) SetLogger(l logging.Logger) logging.Logger {
	_ = css.Logging.SetLogger(l)
	_ = css.FunctionDaemon.SetLogger(l)

	for _, handler := range css.states {
		if handler == nil {
			continue
		}

		_ = handler.(logging.SetLogger).SetLogger(l)
	}

	return css.Log()
}

func (css *ConsensusStates) ErrChan() <-chan error {
	return css.errChan
}

func (css *ConsensusStates) Start() error {
	css.Log().Debug().Msg("trying to start")
	defer css.Log().Debug().Msg("started")

	for state, handler := range css.states {
		if handler == nil {
			css.Log().Warn().Hinted("state_handler", state).Msg("empty state handler found")
			continue
		}

		handler.SetStateChan(css.stateChan)
		handler.SetSealChan(css.sealChan)

		css.Log().Debug().Hinted("state_handler", state).Msg("state handler registered")
	}

	if err := css.FunctionDaemon.Start(); err != nil {
		return err
	}

	go css.cleanBallotbox()

	go func() {
		css.stateChan <- NewStateChangeContext(base.StateStopped, base.StateBooting, nil, nil)
	}()

	go css.handleNewVoteproof()

	return nil
}

func (css *ConsensusStates) Stop() error {
	css.Lock()
	defer css.Unlock()

	if err := css.FunctionDaemon.Stop(); err != nil {
		if xerrors.Is(err, util.DaemonAlreadyStoppedError) {
			return nil
		}

		return err
	}

	for _, h := range css.stopHooks {
		if err := h(); err != nil {
			return err
		}
	}

	if css.activeHandler != nil {
		ctx := NewStateChangeContext(css.activeHandler.State(), base.StateStopped, nil, nil)
		if err := css.activeHandler.Deactivate(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (css *ConsensusStates) cleanBallotbox() {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for range ticker.C {
		var height base.Height
		switch m, found, err := css.local.Storage().LastManifest(); {
		case !found:
			continue
		case err != nil:
			css.Log().Error().Err(err).Msg("something wrong to clean Ballotbox; failed to get last manifest")

			continue
		default:
			height = m.Height() - 3
		}

		if height < 1 {
			continue
		}

		if err := css.ballotbox.Clean(height); err != nil {
			css.Log().Error().Err(err).Msg("something wrong to clean Ballotbox")
		}
	}
}

func (css *ConsensusStates) start(stopChan chan struct{}) error {
	errChan := make(chan error)
	stateStopChan := make(chan struct{})
	go css.startStateChan(stateStopChan, errChan)

	sealStopChan := make(chan struct{})
	go css.startSealChan(sealStopChan)

	var err error
	select {
	case err = <-errChan:
	case <-stopChan:
		stateStopChan <- struct{}{}
	}

	sealStopChan <- struct{}{}

	go func() {
		css.errChan <- err
	}()

	return err
}

func (css *ConsensusStates) startStateChan(stopChan chan struct{}, errChan chan<- error) {
end:
	for {
		select {
		case <-stopChan:
			break end
		case ctx := <-css.stateChan:
			if err := css.activateHandler(ctx); err != nil {
				css.Log().Error().Err(err).Msg("failed to activate handler")

				go func(err error) {
					errChan <- err
				}(err)

				break end
			}
		}
	}
}

func (css *ConsensusStates) startSealChan(stopChan chan struct{}) {
end:
	for {
		select {
		case <-stopChan:
			break end
		case sl := <-css.sealChan:
			go css.broadcastSeal(sl, nil)
		}
	}
}

func (css *ConsensusStates) ActivateHandler(ctx *StateChangeContext) {
	css.stateChan <- ctx
}

func (css *ConsensusStates) activateHandler(ctx *StateChangeContext) error {
	css.Lock()
	defer css.Unlock()

	var current base.State
	if css.activeHandler != nil {
		current = css.activeHandler.State()
	}

	css.Log().Debug().
		Str("current", current.String()).
		Hinted("states", ctx).
		HintedVerbose("voteproof", ctx.Voteproof(), true).
		Msg("trying to change state")

	if !css.canChangeState(ctx) {
		return nil
	}

	var toHandler StateHandler
	if h, found := css.states[ctx.toState]; !found {
		return FailedToActivateHandler.Errorf("unknown state found: %s", ctx.toState)
	} else {
		toHandler = h
	}

	if toHandler == nil {
		return FailedToActivateHandler.Errorf("state handler not registered: %s", ctx.toState)
	}

	var livp base.Voteproof = css.livp
	if css.activeHandler != nil {
		livp = css.activeHandler.LastINITVoteproof()
	}

	if css.activeHandler != nil {
		if err := css.activeHandler.Deactivate(ctx); err != nil {
			return FailedToActivateHandler.Wrap(
				xerrors.Errorf("failed to deactivate previous handler: %w", err),
			)
		}
	}

	_ = toHandler.SetLastINITVoteproof(livp)
	if err := toHandler.Activate(ctx); err != nil {
		return FailedToActivateHandler.Wrap(err)
	}

	css.activeHandler = toHandler

	css.Log().Info().
		Hinted("states", ctx).
		Hinted("new_handler", toHandler.State()).
		Msg("state changed")

	return nil
}

func (css *ConsensusStates) canChangeState(ctx *StateChangeContext) bool {
	if css.activeHandler == nil {
		return true
	}

	if css.activeHandler.State() != ctx.fromState {
		css.Log().Debug().Msgf("not from active handler, %s", ctx.fromState)

		return false
	}

	if css.activeHandler.State() == ctx.toState {
		css.Log().Debug().Msgf("handler, %s already activated", ctx.toState)

		return false
	}

	return true
}

// ActiveHandler returns the current activated handler.
func (css *ConsensusStates) ActiveHandler() StateHandler {
	css.RLock()
	defer css.RUnlock()

	return css.activeHandler
}

func (css *ConsensusStates) broadcastSeal(sl seal.Seal, errChan chan<- error) {
	l := loggerWithSeal(sl, css.Log())
	l.Debug().Msg("trying to broadcast seal")

	go func() {
		if err := css.newSeal(sl); err != nil {
			l.Error().Err(err).Msg("failed to send ballot to local")
		}
	}()

	css.local.Nodes().Traverse(func(m network.Node) bool {
		go func(n network.Node) {
			lt := l.WithLogger(func(ctx logging.Context) logging.Emitter {
				return ctx.Hinted("target_node", n.Address())
			})

			if err := n.Channel().SendSeal(sl); err != nil {
				lt.Error().Err(err).Msg("failed to broadcast")

				if errChan != nil {
					errChan <- err

					return
				}
			}

			lt.Debug().Msgf("seal broadcasted: %T", sl)
		}(m)

		return true
	})
}

func (css *ConsensusStates) processNewVoteproof(voteproof base.Voteproof) error {
	l := loggerWithVoteproof(voteproof, css.Log())

	var ctx *StateToBeChangeError
	if c, err := css.checkNewVoteproof(voteproof); err != nil {
		return err
	} else {
		ctx = c
	}

	l.Debug().Msg("new voteproof")

	if css.ActiveHandler() == nil {
		return nil
	}

	if ctx == nil {
		return css.ActiveHandler().NewVoteproof(voteproof)
	}

	if css.ActiveHandler().State() == ctx.ToState {
		go func(ctx *StateToBeChangeError) {
			if err := css.ActiveHandler().NewVoteproof(ctx.Voteproof); err != nil {
				css.Log().Error().Err(err).Msg("failed to newVoteproof for handler")
			}
		}(ctx)

		return nil
	}

	go func() {
		css.stateChan <- NewStateChangeContext(
			css.ActiveHandler().State(), ctx.ToState, ctx.Voteproof, ctx.Ballot,
		)
	}()

	return nil
}

func (css *ConsensusStates) checkNewVoteproof(voteproof base.Voteproof) (*StateToBeChangeError, error) {
	css.Lock()
	defer css.Unlock()

	var lvp base.Voteproof
	if css.activeHandler != nil {
		lvp = css.activeHandler.LastINITVoteproof()
	}

	var vpc *VoteproofConsensusStateChecker
	if v, err := NewVoteproofConsensusStateChecker(css.local.Storage(), lvp, voteproof, css); err != nil {
		return nil, err
	} else {
		vpc = v
		_ = vpc.SetLogger(css.Log())
	}

	var ctx *StateToBeChangeError
	if err := util.NewChecker("voteproof-validation-checker", []util.CheckerFunc{
		vpc.CheckHeight,
		vpc.CheckINITVoteproof,
		vpc.CheckACCEPTVoteproof,
	}).Check(); err != nil {
		switch {
		case xerrors.As(err, &ctx):
		case xerrors.Is(err, util.IgnoreError):
			return nil, nil
		case err != nil:
			return nil, err
		}
	}

	if voteproof.Stage() == base.StageINIT {
		if css.activeHandler != nil {
			if err := css.activeHandler.SetLastINITVoteproof(voteproof); err != nil {
				css.Log().Error().Err(err).Msg("ignore to set the last init voteproof")
			}
		}
	}

	return ctx, nil
}

// NewSeal receives Seal and hand it over to handler;
func (css *ConsensusStates) NewSeal(sl seal.Seal) {
	go func() {
		if err := css.newSeal(sl); err != nil {
			css.Log().Error().Err(err).Msg("failed to handle new seal")
		}
	}()
}

func (css *ConsensusStates) newSeal(sl seal.Seal) error {
	l := loggerWithSeal(sl, css.Log()).WithLogger(func(ctx logging.Context) logging.Emitter {
		return ctx.Hinted("handler", css.ActiveHandler().State())
	})

	seal.LogEventWithSeal(sl, l.Debug(), l.IsVerbose()).Msg("seal received")

	if _, ok := sl.(ballot.Ballot); !ok {
		if err := css.local.Storage().NewSeals([]seal.Seal{sl}); err != nil {
			if !xerrors.Is(err, storage.DuplicatedError) {
				return err
			}
		}
	}

	if err := css.processSeal(sl); err != nil {
		l.Error().Err(err).Msg("activated handler failed to process seal")

		return err
	}

	return nil
}

func (css *ConsensusStates) processSeal(sl seal.Seal) error {
	if css.ActiveHandler() == nil {
		return xerrors.Errorf("no activated handler")
	}

	switch t := sl.(type) {
	case ballot.Proposal:
		if err := css.validateProposal(t); err != nil {
			return err
		}
	case ballot.Ballot:
		if err := css.validateBallot(t); err != nil {
			return err
		}
	default:
		return nil
	}

	return css.ActiveHandler().NewSeal(sl)
}

func (css *ConsensusStates) validateBallot(blt ballot.Ballot) error {
	if !blt.Stage().CanVote() {
		return nil
	}

	css.Log().Debug().HintedVerbose("ballot", blt, css.Log().IsVerbose()).Msg("ballot received; will vote")

	if err := css.vote(blt); err != nil {
		return xerrors.Errorf("failed to vote: %w", err)
	}

	return nil
}

func (css *ConsensusStates) validateProposal(proposal ballot.Proposal) error {
	pvc := NewProposalValidationChecker(css.local, css.suffrage, proposal, css.lastINITVoteproof())
	_ = pvc.SetLogger(css.Log())

	return util.NewChecker("proposal-validation-checker", []util.CheckerFunc{
		pvc.IsKnown,
		pvc.CheckSigning,
		pvc.IsProposer,
		pvc.SaveProposal,
		pvc.IsOldOrHigher,
	}).Check()
}

func (css *ConsensusStates) vote(blt ballot.Ballot) error {
	l := loggerWithSeal(blt, css.Log())

	voteproof, err := css.ballotbox.Vote(blt)
	if err != nil {
		l.Verbose().Err(err).Msg("failed to vote")

		return err
	}

	if !voteproof.IsFinished() {
		l.Verbose().Msg("voted, but not yet finished")

		return nil
	}

	if voteproof.IsClosed() {
		l.Verbose().Msg("voted, but closed")

		return nil
	}

	l.Verbose().Interface("voteproof", voteproof).Msg("voted and will process")

	go func() {
		css.voteproofChan <- voteproof
	}()

	return nil
}

func (css *ConsensusStates) lastINITVoteproof() base.Voteproof {
	css.RLock()
	defer css.RUnlock()

	if css.activeHandler == nil {
		return nil
	}

	return css.activeHandler.LastINITVoteproof()
}

func (css *ConsensusStates) StateHandler(state base.State) StateHandler {
	return css.states[state]
}

func checkBlockWithINITVoteproof(manifest block.Manifest, voteproof base.Voteproof) error {
	if manifest == nil {
		return nil
	}

	// check init ballot fact.PreviousBlock with local block
	fact, ok := voteproof.Majority().(ballot.INITBallotFact)
	if !ok {
		return xerrors.Errorf("needs INITTBallotFact: fact=%T", voteproof.Majority())
	}

	if !fact.PreviousBlock().Equal(manifest.Hash()) {
		return xerrors.Errorf(
			"INIT Voteproof of ACCEPT Ballot has different PreviousBlock with local: previousBlock=%s local=%s",

			fact.PreviousBlock(),
			manifest.Hash(),
		)
	}

	return nil
}

func (css *ConsensusStates) handleNewVoteproof() {
	for voteproof := range css.voteproofChan {
		l := loggerWithVoteproof(voteproof, css.Log())
		l.Debug().HintedVerbose("voteproof", voteproof, true).Msg("new voteproof")

		if err := css.processNewVoteproof(voteproof); err != nil {
			l.Error().Err(err).
				Hinted("height", voteproof.Height()).
				Hinted("round", voteproof.Round()).
				Hinted("stage", voteproof.Stage()).
				Msg("failed to handle new voteproof")
		}
	}
}
