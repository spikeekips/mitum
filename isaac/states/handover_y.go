package isaacstates

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type HandoverYBrokerArgs struct {
	SendMessageFunc func(context.Context, quicstream.ConnInfo, HandoverMessage) error
	NewDataFunc     func(HandoverMessageDataType, interface{}) error
	// WhenFinished is called when handover process is finished.
	WhenFinished func(base.INITVoteproof, quicstream.ConnInfo) error
	WhenCanceled func(error, quicstream.ConnInfo)
	SyncDataFunc func(
		context.Context, quicstream.ConnInfo, chan<- struct{}) error //revive:disable-line:nested-structs
	AskRequestFunc           AskHandoverFunc
	NetworkID                base.NetworkID
	MaxEnsureSendFailure     uint64
	RetrySendMessageInterval time.Duration
	MaxEnsureAsk             uint64
}

func NewHandoverYBrokerArgs(networkID base.NetworkID) *HandoverYBrokerArgs {
	return &HandoverYBrokerArgs{
		NetworkID: networkID,
		SendMessageFunc: func(context.Context, quicstream.ConnInfo, HandoverMessage) error {
			return ErrHandoverCanceled.Errorf("SendFunc not implemented")
		},
		NewDataFunc: func(HandoverMessageDataType, interface{}) error {
			return util.ErrNotImplemented.Errorf("NewData")
		},
		WhenFinished: func(base.INITVoteproof, quicstream.ConnInfo) error { return nil },
		WhenCanceled: func(error, quicstream.ConnInfo) {},
		AskRequestFunc: func(context.Context, quicstream.ConnInfo) (string, bool, error) {
			return "", false, util.ErrNotImplemented.Errorf("AskFunc")
		},
		SyncDataFunc:             func(context.Context, quicstream.ConnInfo, chan<- struct{}) error { return nil },
		MaxEnsureSendFailure:     9,                     //nolint:gomnd //...
		RetrySendMessageInterval: time.Millisecond * 33, //nolint:gomnd //...
		MaxEnsureAsk:             9,                     //nolint:gomnd //...
	}
}

// HandoverYBroker handles handover processes of non-consensus node.
type HandoverYBroker struct {
	*logging.Logging
	args          *HandoverYBrokerArgs
	ctxFunc       func() context.Context
	cancel        func(error)
	newVoteprooff func(base.Voteproof) error
	// whenFinished is called when handover process is finished. If
	// INITVoteproof is nil, moves to Syncing state. HandoverYBroker will be
	// automatically canceled.
	whenFinishedf    func(base.INITVoteproof) error
	whenCanceledf    func(error)
	cancelByMessage  func(HandoverMessageCancel)
	stop             func()
	id               *util.Locked[string]
	isReadyToAsk     *util.Locked[bool]
	isDataSynced     *util.Locked[bool]
	isFinishedLocked *util.Locked[bool]
	sendFailureCount *util.Locked[uint64]
	connInfo         quicstream.ConnInfo // NOTE x conn info
	maxEnsureAsk     uint64
	receivelock      sync.Mutex
}

func NewHandoverYBroker(
	ctx context.Context,
	args *HandoverYBrokerArgs,
	connInfo quicstream.ConnInfo,
) *HandoverYBroker {
	hctx, cancel := context.WithCancel(ctx)

	var cancelOnce sync.Once

	broker := &HandoverYBroker{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "handover-y-broker")
		}),
		args:             args,
		connInfo:         connInfo,
		ctxFunc:          func() context.Context { return hctx },
		id:               util.EmptyLocked[string](),
		whenFinishedf:    func(base.INITVoteproof) error { return nil },
		whenCanceledf:    func(error) {},
		isReadyToAsk:     util.NewLocked(false),
		isDataSynced:     util.NewLocked(false),
		isFinishedLocked: util.NewLocked(false),
		sendFailureCount: util.NewLocked[uint64](0),
		maxEnsureAsk:     args.MaxEnsureAsk,
	}

	cancelf := func(err error) {
		cancel()

		broker.whenCanceled(err)
	}

	syncdatactx, syncdatacancel := context.WithCancel(hctx)

	broker.cancel = func(err error) {
		cancelOnce.Do(func() {
			defer broker.Log().Error().Err(err).Msg("canceled")

			syncdatacancel()

			if id := broker.ID(); len(id) > 0 {
				go func() {
					_ = broker.retrySendMessage(
						context.Background(), NewHandoverMessageCancel(id, err), 3) //nolint:gomnd //...
				}()
			}

			cancelf(err)
		})
	}

	broker.cancelByMessage = func(i HandoverMessageCancel) {
		cancelOnce.Do(func() {
			defer broker.Log().Debug().Interface("handover_message", i).Msg("canceled by message")

			syncdatacancel()

			cancelf(ErrHandoverCanceled.Errorf("canceled by message"))
		})
	}

	broker.stop = func() {
		cancelOnce.Do(func() {
			defer broker.Log().Debug().Msg("stopped")

			syncdatacancel()

			cancel()
		})
	}

	go func() {
		readych := make(chan struct{}, 1)
		donech := make(chan struct{}, 1)

		go func() {
			if err := broker.args.SyncDataFunc(syncdatactx, connInfo, readych); err != nil {
				broker.Log().Error().Err(err).Msg("failed to sync data")

				broker.cancel(err)

				return
			}

			donech <- struct{}{}
		}()

		select {
		case <-syncdatactx.Done():
			return
		case <-readych:
			broker.Log().Debug().Msg("ready to ask")

			_ = broker.isReadyToAsk.SetValue(true)
		}

		select {
		case <-syncdatactx.Done():
			return
		case <-donech:
			broker.Log().Debug().Msg("data synced")

			_ = broker.isDataSynced.SetValue(true)
		}
	}()

	return broker
}

func (broker *HandoverYBroker) ID() string {
	id, _ := broker.id.Value()

	return id
}

func (broker *HandoverYBroker) ConnInfo() quicstream.ConnInfo {
	return broker.connInfo
}

func (broker *HandoverYBroker) IsAsked() bool {
	id, _ := broker.id.Value()

	return len(id) > 0
}

func (broker *HandoverYBroker) Ask() (canMoveConsensus, isAsked bool, _ error) {
	if err := broker.isCanceled(); err != nil {
		return false, false, err
	}

	if i, _ := broker.isFinishedLocked.Value(); i {
		return false, false, ErrHandoverStopped.Errorf("finished")
	}

	if synced, _ := broker.isReadyToAsk.Value(); !synced {
		return false, false, nil
	}

	var id string

	if _, err := broker.id.Set(func(prev string, isempty bool) (string, error) {
		switch {
		case !isempty:
			id = prev

			return "", util.ErrLockedSetIgnore
		case broker.maxEnsureAsk < 1:
			return "", errors.Errorf("over max ask")
		}

		switch i, j, err := broker.ask(); {
		case err != nil:
			broker.maxEnsureAsk--

			if broker.maxEnsureAsk < 1 {
				return "", err
			}

			return "", util.ErrLockedSetIgnore
		default:
			id = i
			canMoveConsensus = j

			return i, nil
		}
	}); err != nil {
		broker.cancel(err)

		broker.Log().Error().Err(err).Msg("failed to ask")

		return false, false, ErrHandoverCanceled.Wrap(err)
	}

	broker.Log().Debug().Str("id", id).Msg("handover asked")

	return canMoveConsensus, len(id) > 0, nil
}

func (broker *HandoverYBroker) ask() (id string, canMoveConsensus bool, _ error) {
	switch i, j, err := broker.args.AskRequestFunc(broker.ctxFunc(), broker.connInfo); {
	case err != nil:
		return "", false, err
	case len(i) < 1:
		return "", false, errors.Errorf("empty handover id")
	default:
		return i, j, nil
	}
}

func (broker *HandoverYBroker) isCanceled() error {
	if err := broker.ctxFunc().Err(); err != nil {
		return ErrHandoverCanceled.Wrap(err)
	}

	return nil
}

func (broker *HandoverYBroker) sendStagePoint(ctx context.Context, point base.StagePoint) error {
	if err := broker.isCanceled(); err != nil {
		return err
	}

	if i, _ := broker.isFinishedLocked.Value(); i {
		return ErrHandoverStopped.Errorf("finished")
	}

	if ok, _ := broker.isDataSynced.Value(); !ok {
		return nil
	}

	id := broker.ID()
	if len(id) < 1 {
		return errors.Errorf("not yet asked")
	}

	if err := broker.endureSendMessage(ctx, newHandoverMessageChallengeStagePoint(id, point)); err != nil {
		broker.cancel(err)

		return ErrHandoverCanceled.Wrap(err)
	}

	return nil
}

func (broker *HandoverYBroker) sendBlockMap(ctx context.Context, point base.StagePoint, m base.BlockMap) error {
	if err := broker.isCanceled(); err != nil {
		return err
	}

	if i, _ := broker.isFinishedLocked.Value(); i {
		return ErrHandoverStopped.Errorf("finished")
	}

	if ok, _ := broker.isDataSynced.Value(); !ok {
		return nil
	}

	id := broker.ID()
	if len(id) < 1 {
		return errors.Errorf("not yet asked")
	}

	if err := broker.endureSendMessage(ctx, newHandoverMessageChallengeBlockMap(id, point, m)); err != nil {
		broker.cancel(err)

		return ErrHandoverCanceled.Wrap(err)
	}

	return nil
}

func (broker *HandoverYBroker) Receive(i interface{}) error {
	broker.receivelock.Lock()
	defer broker.receivelock.Unlock()

	if err := broker.isCanceled(); err != nil {
		return err
	}

	if j, _ := broker.isFinishedLocked.Value(); j {
		return ErrHandoverStopped.Errorf("finished")
	}

	switch err := broker.receiveInternal(i); {
	case err == nil:
	case errors.Is(err, errHandoverIgnore):
	case errors.Is(err, ErrHandoverCanceled):
		return err
	default:
		broker.cancel(err)

		return ErrHandoverCanceled.Wrap(err)
	}

	return nil
}

func (broker *HandoverYBroker) receiveInternal(i interface{}) error {
	id := broker.ID()
	if len(id) < 1 {
		return errors.Errorf("not yet asked")
	}

	if m, ok := i.(HandoverMessage); ok {
		if id != m.HandoverID() {
			return errors.Errorf("id not matched")
		}
	}

	if iv, ok := i.(util.IsValider); ok {
		if err := iv.IsValid(broker.args.NetworkID); err != nil {
			return err
		}
	}

	if msg, ok := i.(HandoverMessageCancel); ok {
		broker.cancelByMessage(msg)

		return ErrHandoverCanceled.Errorf("canceled by message")
	}

	switch t := i.(type) {
	case HandoverMessageData:
		return broker.receiveData(t)
	case HandoverMessageChallengeResponse:
		return broker.receiveChallengeResponse(t)
	case HandoverMessageFinish:
		return broker.receiveFinish(t)
	default:
		return errHandoverIgnore.Errorf("X sent unknown message, %T", i)
	}
}

func (broker *HandoverYBroker) receiveData(i HandoverMessageData) error {
	switch t := i.DataType(); t {
	case HandoverMessageDataTypeVoteproof:
		if vp, err := i.LoadVoteproofData(); err != nil {
			return errHandoverIgnore.Wrap(err)
		} else { //revive:disable-line:indent-error-flow
			return broker.newVoteproof(vp)
		}
	case HandoverMessageDataTypeINITVoteproof:
		if pr, vp, err := i.LoadINITVoteproofData(); err != nil {
			return errHandoverIgnore.Wrap(err)
		} else { //revive:disable-line:indent-error-flow
			if pr != nil {
				if err := broker.args.NewDataFunc(HandoverMessageDataTypeProposal, pr); err != nil {
					return err
				}
			}

			return broker.newVoteproof(vp)
		}
	default:
		return broker.args.NewDataFunc(t, i.Data())
	}
}

func (broker *HandoverYBroker) receiveChallengeResponse(hc HandoverMessageChallengeResponse) error {
	broker.Log().Debug().Interface("handover_message", hc).Msg("receive HandoverMessageChallengeResponse")

	return hc.Err()
}

func (broker *HandoverYBroker) receiveFinish(hc HandoverMessageFinish) error {
	broker.Log().Debug().
		Bool("has_voteproof", hc.INITVoteproof() != nil).
		Msg("receive HandoverMessageFinish")

	var err error

	_, _ = broker.isFinishedLocked.Set(func(i, _ bool) (bool, error) {
		if i {
			return false, util.ErrLockedSetIgnore.WithStack()
		}

		if pr := hc.Proposal(); pr != nil {
			err = broker.args.NewDataFunc(HandoverMessageDataTypeProposal, pr)
		}

		err = util.JoinErrors(err, broker.whenFinished(hc.INITVoteproof()))

		return true, nil
	})

	l := broker.Log().With().Interface("handover_message", hc).Logger()

	if err != nil {
		l.Error().Err(err).Msg("received HandoverMessageFinish")
	} else {
		l.Debug().Msg("received HandoverMessageFinish")
	}

	return err
}

func (broker *HandoverYBroker) newVoteproof(vp base.Voteproof) error {
	if err := broker.newVoteprooff(vp); err != nil {
		return err
	}

	return broker.args.NewDataFunc(HandoverMessageDataTypeVoteproof, vp)
}

func (broker *HandoverYBroker) whenFinished(vp base.INITVoteproof) error {
	err := broker.whenFinishedf(vp)

	return util.JoinErrors(err, broker.args.WhenFinished(vp, broker.connInfo))
}

func (broker *HandoverYBroker) whenCanceled(err error) {
	broker.whenCanceledf(err)
	broker.args.WhenCanceled(err, broker.connInfo)
}

func (broker *HandoverYBroker) patchStates(st *States) error {
	broker.newVoteprooff = func(vp base.Voteproof) error {
		go func() {
			st.vpch <- emptyVoteproofWithErrchan(vp)
		}()

		return nil
	}

	broker.whenFinishedf = func(vp base.INITVoteproof) error {
		switch current := st.current(); {
		case current == nil:
		case vp == nil:
			go func() {
				// NOTE moves to syncing
				_ = st.AskMoveState(newSyncingSwitchContextWithVoteproof(current.state(), nil))
			}()
		default:
			go func() {
				st.vpch <- emptyVoteproofWithErrchan(handoverFinishedVoteporof{INITVoteproof: vp})
			}()
		}

		return nil
	}

	broker.whenCanceledf = func(error) {
		switch current := st.current(); {
		case current == nil:
			go st.cleanHandovers()
		default:
			go func() {
				st.cleanHandovers()

				err := st.AskMoveState(emptySyncingSwitchContext(current.state()))
				if err != nil {
					panic(err)
				}
			}()
		}
	}

	return nil
}

func (broker *HandoverYBroker) retrySendMessage(ctx context.Context, msg HandoverMessage, limit int) error {
	return retryHandoverSendMessageFunc(
		ctx,
		limit,
		broker.args.RetrySendMessageInterval,
		broker.args.SendMessageFunc,
		broker.connInfo,
		msg,
	)
}

func (broker *HandoverYBroker) endureSendMessage(ctx context.Context, msg HandoverMessage) error {
	return endureHandoverSendMessageFunc(
		ctx,
		broker.sendFailureCount,
		broker.args.MaxEnsureSendFailure,
		broker.args.SendMessageFunc,
		broker.connInfo,
		msg,
	)
}
