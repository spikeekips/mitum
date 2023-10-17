package isaacstates

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"golang.org/x/exp/slices"
)

var (
	ErrIgnoreSwitchingState = util.NewIDError("switch state, but ignored")
	errIgnoreNewVoteproof   = util.NewIDError("new voteproof; ignored")
)

type (
	NewHandoverXBrokerFunc func(context.Context, quicstream.ConnInfo) (*HandoverXBroker, error)
	NewHandoverYBrokerFunc func(context.Context, quicstream.ConnInfo) (*HandoverYBroker, error)
)

type StatesArgs struct {
	Ballotbox               *Ballotbox
	BallotStuckResolver     BallotStuckResolver
	LastVoteproofsHandler   *isaac.LastVoteproofsHandler
	IsInSyncSourcePoolFunc  func(base.Address) bool
	BallotBroadcaster       BallotBroadcaster
	WhenStateSwitchedFunc   func(StateType)
	IntervalBroadcastBallot func() time.Duration
	WhenNewVoteproof        func(base.Voteproof)
	NewHandoverXBroker      NewHandoverXBrokerFunc
	NewHandoverYBroker      NewHandoverYBrokerFunc
	// AllowConsensus decides to enter Consensus states. If false, States enters
	// Syncing state instead of Consensus state.
	AllowConsensus bool
}

func NewStatesArgs() *StatesArgs {
	return &StatesArgs{
		LastVoteproofsHandler:  isaac.NewLastVoteproofsHandler(),
		IsInSyncSourcePoolFunc: func(base.Address) bool { return false },
		WhenStateSwitchedFunc:  func(StateType) {},
		NewHandoverXBroker: func(context.Context, quicstream.ConnInfo) (*HandoverXBroker, error) {
			return nil, util.ErrNotImplemented.Errorf("NewHandoverXBroker")
		},
		NewHandoverYBroker: func(context.Context, quicstream.ConnInfo) (*HandoverYBroker, error) {
			return nil, util.ErrNotImplemented.Errorf("NewHandoverYBroker")
		},
		IntervalBroadcastBallot: func() time.Duration {
			return isaac.DefaultntervalBroadcastBallot
		},
		WhenNewVoteproof: func(base.Voteproof) {},
	}
}

type States struct {
	cs handler
	*logging.Logging
	local       base.LocalNode
	args        *StatesArgs
	statech     chan switchContext
	vpch        chan voteproofWithErrchan
	newHandlers map[StateType]newHandler
	*util.ContextDaemon
	bbt              *ballotBroadcastTimers
	allowedConsensus *util.Locked[bool]
	handoverXBroker  *util.Locked[*HandoverXBroker]
	handoverYBroker  *util.Locked[*HandoverYBroker]
	networkID        base.NetworkID
	stateLock        sync.RWMutex
}

func NewStates(networkID base.NetworkID, local base.LocalNode, args *StatesArgs) (*States, error) {
	timers, err := util.NewSimpleTimers(1<<4, time.Millisecond*33) //nolint:gomnd //...
	if err != nil {
		return nil, err
	}

	st := &States{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "states")
		}),
		local:            local,
		networkID:        networkID,
		args:             args,
		statech:          make(chan switchContext),
		vpch:             make(chan voteproofWithErrchan),
		newHandlers:      map[StateType]newHandler{},
		cs:               nil,
		allowedConsensus: util.NewLocked(args.AllowConsensus),
		handoverXBroker:  util.EmptyLocked[*HandoverXBroker](),
		handoverYBroker:  util.EmptyLocked[*HandoverYBroker](),
	}

	st.bbt = newBallotBroadcastTimers(
		timers,
		func(_ context.Context, bl base.Ballot) error {
			_ = st.args.BallotBroadcaster.Broadcast(bl)

			return nil
		},
		st.args.IntervalBroadcastBallot(),
	)

	if st.args.Ballotbox != nil {
		st.args.Ballotbox.SetNewBallotFunc(st.mimicBallotFunc())
	}

	st.ContextDaemon = util.NewContextDaemon(st.start)

	return st, nil
}

func (st *States) SetHandler(state StateType, h newHandler) *States {
	if st.ContextDaemon.IsStarted() {
		panic("can not set state newHandler; already started")
	}

	if i, ok := (interface{})(h).(interface{ setStates(*States) }); ok {
		i.setStates(st)
	}

	st.newHandlers[state] = h

	if l, ok := (interface{})(h).(logging.SetLogging); ok {
		_ = l.SetLogging(st.Logging)
	}

	return st
}

func (st *States) SetLogging(l *logging.Logging) *logging.Logging {
	for i := range st.newHandlers {
		if j, ok := (interface{})(st.newHandlers[i]).(logging.SetLogging); ok {
			_ = j.SetLogging(l)
		}
	}

	return st.Logging.SetLogging(l)
}

func (st *States) SetWhenStateSwitched(f func(StateType)) {
	st.args.WhenStateSwitchedFunc = f
}

func (st *States) Hold() error {
	current := st.current()
	if current == nil {
		return nil
	}

	st.Log().Debug().Msg("states holded")

	return st.switchState(newStoppedSwitchContext(current.state(), nil))
}

func (st *States) AskMoveState(sctx switchContext) error {
	var nsctx switchContext

	switch err := st.checkStateSwitchContext(sctx, st.current()); {
	case err == nil:
		nsctx = sctx
	case errors.Is(err, ErrIgnoreSwitchingState):
		return nil
	case errors.As(err, &nsctx):
	default:
		return err
	}

	go func() {
		st.statech <- nsctx
	}()

	return nil
}

func (st *States) Current() StateType {
	return st.current().state()
}

func (st *States) start(ctx context.Context) error {
	if st.bbt != nil {
		defer func() {
			_ = st.bbt.Stop()
		}()
	}

	defer st.Log().Debug().Msg("states stopped")

	if err := st.bbt.Start(ctx); err != nil {
		return err
	}

	// NOTE set stopped as current
	switch newHandler, found := st.newHandlers[StateStopped]; {
	case !found:
		return errors.Errorf("find stopped handler")
	default:
		h, err := newHandler.new()
		if err != nil {
			return errors.WithMessage(err, "create stopped new handler")
		}

		if _, err := h.enter(StateEmpty, nil); err != nil {
			return errors.Errorf("enter stopped handler")
		}

		st.cs = h
	}

	// NOTE entering to booting at starting
	if err := st.ensureSwitchState(newBootingSwitchContext(StateStopped)); err != nil {
		return errors.Wrap(err, "enter booting state")
	}

	serr := st.startStatesSwitch(ctx)

	// st.cleanHandovers()

	// NOTE exit current
	switch current := st.current(); {
	case current == nil:
		return serr
	default:
		if err := st.switchState(newStoppedSwitchContext(current.state(), serr)); err != nil {
			st.Log().Error().Err(err).Msg("failed to switch to stopped; ignored")
		}

		return serr
	}
}

func (st *States) startStatesSwitch(ctx context.Context) error {
	var resolvervpch <-chan base.Voteproof

	switch {
	case st.args.BallotStuckResolver == nil:
		resolvervpch = make(chan base.Voteproof)
	default:
		resolvervpch = st.args.BallotStuckResolver.Voteproof()
	}

end:
	for {
		var vp base.Voteproof
		var errch chan error

		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "states stopped by context")
		case sctx := <-st.statech:
			if err := st.ensureSwitchState(sctx); err != nil {
				return err
			}
		case vp = <-st.args.Ballotbox.Voteproof():
			// NOTE voteproof from ballotbox ignored under not allowed consensus
			if !st.AllowedConsensus() {
				continue end
			}
		case vp = <-resolvervpch:
		case vperr := <-st.vpch:
			vp = vperr.vp
			errch = vperr.errch
		}

		if vp != nil {
			err := st.newVoteproof(vp)
			if errch != nil {
				errch <- err
			}

			if err != nil {
				return err
			}
		}
	}
}

func (st *States) newVoteproof(vp base.Voteproof) error {
	var sctx switchContext

	var current handler

	switch current = st.current(); {
	case current == nil:
		return nil
	case !st.args.LastVoteproofsHandler.IsNew(vp):
		return nil
	}

	switch err := st.voteproofToCurrent(vp, current); {
	case err == nil:
		st.args.WhenNewVoteproof(vp)

		return nil
	case errors.Is(err, errIgnoreNewVoteproof):
		return nil
	case !errors.As(err, &sctx):
		st.Log().Error().Err(err).
			Func(base.VoteproofLogFunc("voteproof", vp)).Msg("failed to handle voteproof")

		return err
	}

	if sctx != nil {
		if err := st.ensureSwitchState(sctx); err != nil {
			return err
		}
	}

	return nil
}

func (st *States) current() handler {
	st.stateLock.RLock()
	defer st.stateLock.RUnlock()

	return st.cs
}

func (st *States) ensureSwitchState(sctx switchContext) error {
	var n int

	movetobroken := func(from StateType, err error) switchContext {
		st.Log().Error().Err(err).Msg("failed to switch state; will move to broken")

		n = 0

		return newBrokenSwitchContext(from, err)
	}

	nsctx := sctx
end:
	for {
		if n > 3 { //nolint:gomnd //...
			st.Log().Warn().Msg("suspicious infinite loop in switch states; > 3; will move to broken")

			nsctx = movetobroken(nsctx.from(), nsctx)

			continue
		}

		n++

		var rsctx switchContext

		switch err := st.switchState(nsctx); {
		case err == nil:
			if nsctx.next() == StateStopped {
				return errors.Wrap(nsctx, "states stopped")
			}

			if err = st.checkOutOfHandoverX(nsctx); err != nil {
				return err
			}

			return nil
		case !errors.As(err, &rsctx):
			if nsctx.next() == StateBroken {
				st.Log().Error().Err(err).Msg("failed to switch to broken; will stop switching")

				return errors.Wrap(err, "switch to broken")
			}

			nsctx = movetobroken(nsctx.from(), err)

			continue end
		default:
			nsctx = rsctx
		}
	}
}

func (st *States) switchState(sctx switchContext) error {
	e := util.StringError("switch state")

	current := st.current()
	nsctx := sctx

	var asctx switchContext

	switch err := st.checkStateSwitchContext(nsctx, current); {
	case err == nil:
	case errors.Is(err, ErrIgnoreSwitchingState):
		return nil
	case errors.As(err, &asctx):
		nsctx = asctx
	default:
		return err
	}

	l := st.stateSwitchContextLog(nsctx, current)

	cdefer, ndefer, err := st.exitAndEnter(nsctx, current)
	if err != nil {
		switch {
		case errors.Is(err, ErrIgnoreSwitchingState):
			l.Debug().Msg("switching state ignored")

			return nil
		case isSwitchContextError(err):
			return err
		default:
			l.Error().Err(err).Msg("failed to switch(locked)")

			return e.Wrap(err)
		}
	}

	go func() {
		if cdefer != nil {
			cdefer()
		}

		if ndefer != nil {
			ndefer()
		}
	}()

	st.args.WhenStateSwitchedFunc(nsctx.next())

	l.Debug().Msg("state switched")

	return nil
}

func (st *States) exitAndEnter(sctx switchContext, current handler) (func(), func(), error) {
	st.stateLock.Lock()
	defer st.stateLock.Unlock()

	e := util.StringError("switch state")
	l := st.stateSwitchContextLog(sctx, current)

	var cdefer, ndefer func()

	// NOTE if switching to broken, error during exiting from current handler
	// will not be ignored
	if current != nil {
		switch i, err := current.exit(sctx); {
		case err == nil:
			l.Debug().Msg("exited")

			cdefer = i
		case sctx.next() == StateBroken:
			l.Error().Err(err).Msg("failed to exit current state, but next is broken state; error will be ignored")
		default:
			if errors.Is(err, ErrIgnoreSwitchingState) {
				l.Debug().Err(err).Msg("current state ignores switching state")

				return nil, nil, err
			}

			l.Error().Err(err).Msg("failed to exit state")

			return nil, nil, e.WithMessage(err, "exit current state")
		}
	}

	nextHandler, err := st.newHandlers[sctx.next()].new()
	if err != nil {
		return nil, nil, e.WithMessage(err, "create new handler, %q", sctx.next())
	}

	ndefer, err = nextHandler.enter(current.state(), sctx)
	if err != nil {
		var nsctx switchContext

		if errors.As(err, &nsctx) {
			l.Debug().Dict("next_next_state", switchContextLog(nsctx)).
				Msg("failed to enter; another switch context")

			st.cs = nextHandler

			return nil, nil, err
		}

		l.Error().Err(err).Msg("failed to enter state")

		return nil, nil, e.WithMessage(err, "enter next state")
	}

	st.cs = nextHandler

	return cdefer, ndefer, nil
}

func (st *States) voteproofToCurrent(vp base.Voteproof, current handler) error {
	st.Log().Debug().Interface("voteproof", vp).Msg("new voteproof")

	if err := current.newVoteproof(vp); err != nil {
		return errors.WithMessage(err, "send voteproof to current")
	}

	return nil
}

func (st *States) checkStateSwitchContext(sctx switchContext, current handler) error {
	next := sctx.next()
	nsctx := sctx

	if current != nil && current.state() == StateStopped {
		switch nsctx.next() {
		case StateBooting, StateBroken:
		default:
			return ErrIgnoreSwitchingState.Errorf("state stopped, next should be StateBooting or StateBroken")
		}
	}

	switch _, found := st.newHandlers[next]; {
	case current == nil:
		return nil
	case !found:
		return errors.Errorf("unknown next state, %q", next)
	case next == current.state():
		return ErrIgnoreSwitchingState.Errorf("same next state")
	case nsctx.from() != current.state():
		return ErrIgnoreSwitchingState.Errorf("current != from")
	case next == StateBroken: // NOTE prevent new broken state, which comes from ater handler exit
		return nil
	}

	if err := st.checkHandoverStateSwitchContext(nsctx, current.state()); err != nil {
		return err
	}

	l := st.Log().With().Dict("previous_next_state", switchContextLog(nsctx)).Logger()

	switch {
	case st.AllowedConsensus():
		if next == StateHandover {
			return ErrIgnoreSwitchingState.Errorf(
				"next state is handover, but allowed to enter consensus states; ignore")
		}
	case current.state() == StateHandover:
	case next == StateConsensus, next == StateJoining:
		if current.state() == StateSyncing {
			return ErrIgnoreSwitchingState.Errorf("not allowed to enter consensus states; keep syncing")
		}

		switch vsctx, ok := nsctx.(voteproofSwitchContext); {
		case ok:
			nsctx = newSyncingSwitchContextWithVoteproof(current.state(), vsctx.voteproof())
		default:
			nsctx = emptySyncingSwitchContext(current.state())
		}

		l.Debug().
			Dict("next_state", switchContextLog(nsctx)).
			Msg("not allowed to enter consensus states; moves to syncing state")
	}

	return nsctx
}

func (st *States) stateSwitchContextLog(sctx switchContext, current handler) zerolog.Logger {
	return st.Log().With().
		Stringer("current_state", handlerLog(current)).
		Dict("next_state", switchContextLog(sctx)).Logger()
}

func (st *States) voteproofs(point base.StagePoint) (isaac.LastVoteproofs, bool) {
	return st.args.LastVoteproofsHandler.Voteproofs(point)
}

func (st *States) lastVoteproof() isaac.LastVoteproofs {
	return st.args.LastVoteproofsHandler.Last()
}

func (st *States) setLastVoteproof(vp base.Voteproof) bool {
	return st.args.LastVoteproofsHandler.Set(vp)
}

// mimicBallotFunc mimics incoming ballot when node can not broadcast ballot; this will
// prevent to be gussed by the other nodes, local node is dead.
// - ballot signer should be in sync sources
func (st *States) mimicBallotFunc() func(base.Ballot) {
	mimicBallotf := st.mimicBallot()

	votef := func(bl base.Ballot) error {
		return nil
	}

	if st.args.Ballotbox != nil {
		votef = func(bl base.Ballot) error {
			_, err := st.args.Ballotbox.Vote(bl)

			return err
		}
	}

	logger := logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
		return lctx.Str("module", "states-mimic-ballot")
	})
	l := logger.SetLogging(st.Logging).Log()

	return func(bl base.Ballot) {
		switch s := st.current().state(); {
		case s != StateSyncing && s != StateBroken:
			return
		case !st.AllowedConsensus(),
			bl.SignFact().Node().Equal(st.local.Address()):
			return
		case !st.args.IsInSyncSourcePoolFunc(bl.SignFact().Node()):
			return
		case st.filterMimicBallot(bl):
			return
		}

		switch newbl, found, err := st.args.BallotBroadcaster.Ballot(
			bl.Point().Point,
			bl.Point().Stage(),
			isaac.IsSuffrageConfirmBallotFact(bl.SignFact().Fact()),
		); {
		case err != nil:
			l.Error().Err(err).Interface("ballot", bl).Msg("mimic ballot")

			return
		case !found:
		case !newbl.SignFact().Node().Equal(bl.SignFact().Node()):
			return
		default:
			_ = st.args.BallotBroadcaster.Broadcast(newbl)

			return
		}

		var newbl base.Ballot

		switch i, err := mimicBallotf(bl); {
		case err != nil:
			l.Error().Err(err).Interface("ballot", bl).Msg("mimic ballot")

			return
		default:
			newbl = i
		}

		ll := l.With().Interface("ballot", bl).Interface("new_ballot", newbl).Logger()

		go func() {
			if err := votef(newbl); err != nil {
				ll.Error().Err(err).Msg("failed to vote mimic ballot")
			}
		}()

		_ = st.args.BallotBroadcaster.Broadcast(newbl)

		ll.Debug().Msg("mimic ballot broadcasted")
	}
}

func (st *States) mimicBallot() func(base.Ballot) (base.Ballot, error) {
	var lock sync.Mutex

	return func(bl base.Ballot) (base.Ballot, error) {
		lock.Lock()
		defer lock.Unlock()

		switch i, err := st.signMimicBallot(bl); {
		case err != nil:
			return nil, err
		default:
			return i, nil
		}
	}
}

func (st *States) signMimicBallot(bl base.Ballot) (base.Ballot, error) {
	var expels []base.SuffrageExpelOperation

	if w, ok := bl.(base.HasExpels); ok {
		expels = w.Expels()
	}

	return mimicBallot(
		st.networkID,
		st.local,
		bl.SignFact().Fact().(base.BallotFact), //nolint:forcetypeassert //...
		expels,
		bl.Voteproof(),
	)
}

func (st *States) filterMimicBallot(bl base.Ballot) bool {
	l := st.Log().With().Interface("ballot", bl).Logger()

	// NOTE if local is in expels, ignore
	switch w, ok := bl.(base.HasExpels); {
	case !ok:
	default:
		if slices.IndexFunc(w.Expels(), func(i base.SuffrageExpelOperation) bool {
			return i.ExpelFact().Node().Equal(st.local.Address())
		}) >= 0 {
			l.Debug().Msg("local in expels; ignore")

			return true
		}
	}

	if w, ok := bl.Voteproof().(base.HasExpels); ok {
		if slices.IndexFunc(w.Expels(), func(i base.SuffrageExpelOperation) bool {
			return i.ExpelFact().Node().Equal(st.local.Address())
		}) >= 0 {
			l.Debug().Msg("local in expels voteproof; ignore")

			return true
		}
	}

	return false
}

func (st *States) AllowedConsensus() bool {
	i, _ := st.allowedConsensus.Value()

	return i
}

func (st *States) SetAllowConsensus(allow bool) bool { // revive:disable-line:flag-parameter
	st.stateLock.RLock()
	defer st.stateLock.RUnlock()

	isset := st.setAllowConsensus(allow)

	if isset {
		switch current := st.current(); {
		case current == nil:
		case current.state() == StateJoining, current.state() == StateConsensus:
			st.Log().Debug().Stringer("current", current.state()).Bool("allow", allow).Msg("set allow consensus")

			current.whenSetAllowConsensus(allow) // NOTE if not allowed, exits from consensus state
		}

		if broker := st.HandoverXBroker(); broker != nil && !allow {
			broker.cancel(errors.Errorf("not allowed consensus"))
			st.cleanHandovers()

			st.Log().Debug().Msg("not allowed consensus, handover x broker canceled")
		}

		if broker := st.HandoverYBroker(); broker != nil && allow {
			broker.cancel(errors.Errorf("allowed consensus"))
			st.cleanHandovers()

			st.Log().Debug().Msg("allowed consensus, handover y broker canceled")
		}
	}

	return isset
}

func (st *States) setAllowConsensus(allow bool) bool { // revive:disable-line:flag-parameter
	var isset bool

	_, _ = st.allowedConsensus.Set(func(prev bool, isempty bool) (bool, error) {
		if prev == allow {
			return false, util.ErrLockedSetIgnore.WithStack()
		}

		isset = true

		return allow, nil
	})

	return isset
}

func (st *States) checkHandoverStateSwitchContext(sctx switchContext, current StateType) error {
	next := sctx.next()

	broker := st.HandoverYBroker()

	if broker == nil {
		if next == StateHandover {
			return ErrIgnoreSwitchingState.Errorf("not under handover; ignore")
		}

		return nil
	}

	if !broker.IsAsked() {
		return ErrIgnoreSwitchingState.Errorf("handover y not yet asked")
	}

	switch {
	case current == StateHandover:
	case next == StateConsensus, next == StateJoining:
		st.Log().Debug().Dict("previous_next_state", switchContextLog(sctx)).
			Dict("next_state", switchContextLog(sctx)).
			Msg("trying to enter consensus states, but under handover y; moves to handover state")

		return newHandoverSwitchContextFromOther(sctx)
	}

	return nil
}

func (st *States) checkOutOfHandoverX(sctx switchContext) error {
	broker := st.HandoverXBroker()
	if broker == nil {
		return nil
	}

	if i, _ := broker.isFinishedLocked.Value(); i {
		return nil
	}

	switch sctx.next() {
	case StateBooting, StateConsensus, StateJoining:
		return nil
	default:
		_ = broker.finish(nil, nil)

		return nil
	}
}

func mimicBallot(
	networkID base.NetworkID,
	local base.LocalNode,
	fact base.BallotFact,
	expels []base.SuffrageExpelOperation,
	voteproof base.Voteproof,
) (base.Ballot, error) {
	var newbl base.Ballot

	switch t := fact.(type) {
	case isaac.SuffrageConfirmBallotFact:
		sf := isaac.NewINITBallotSignFact(t)

		if err := sf.NodeSign(local.Privatekey(), networkID, local.Address()); err != nil {
			return nil, err
		}

		newbl = isaac.NewINITBallot(voteproof, sf, nil)
	case base.INITBallotFact:
		if _, ok := t.(isaac.EmptyProposalINITBallotFact); ok {
			t = isaac.NewEmptyProposalINITBallotFact(t.Point().Point, t.PreviousBlock(), t.Proposal())
		}

		sf := isaac.NewINITBallotSignFact(t)

		if err := sf.NodeSign(local.Privatekey(), networkID, local.Address()); err != nil {
			return nil, err
		}

		newbl = isaac.NewINITBallot(voteproof, sf, expels)
	case base.ACCEPTBallotFact:
		if _, ok := t.(isaac.EmptyOperationsACCEPTBallotFact); ok {
			t = isaac.NewEmptyOperationsACCEPTBallotFact(t.Point().Point, t.Proposal())
		}

		sf := isaac.NewACCEPTBallotSignFact(t)

		if err := sf.NodeSign(local.Privatekey(), networkID, local.Address()); err != nil {
			return nil, err
		}

		newbl = isaac.NewACCEPTBallot( //nolint:forcetypeassert //...
			voteproof.(base.INITVoteproof),
			sf,
			expels,
		)
	default:
		return nil, errors.Errorf("unknown ballot, %T", fact)
	}

	return newbl, nil
}

type voteproofWithErrchan struct {
	vp    base.Voteproof
	errch chan error
}

func emptyVoteproofWithErrchan(vp base.Voteproof) voteproofWithErrchan {
	return voteproofWithErrchan{vp: vp}
}
