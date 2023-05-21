package isaacstates

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type HandoverYBrokerArgs struct {
	SendMessageFunc func(context.Context, quicstream.UDPConnInfo, HandoverMessage) error
	NewDataFunc     func(HandoverMessageDataType, interface{}) error
	// WhenFinished is called when handover process is finished.
	WhenFinished   func(base.INITVoteproof, quicstream.UDPConnInfo) error
	WhenCanceled   func(error, quicstream.UDPConnInfo)
	AskRequestFunc AskHandoverFunc
	NetworkID      base.NetworkID
}

func NewHandoverYBrokerArgs(networkID base.NetworkID) *HandoverYBrokerArgs {
	return &HandoverYBrokerArgs{
		NetworkID: networkID,
		SendMessageFunc: func(context.Context, quicstream.UDPConnInfo, HandoverMessage) error {
			return ErrHandoverCanceled.Errorf("SendFunc not implemented")
		},
		NewDataFunc: func(HandoverMessageDataType, interface{}) error {
			return util.ErrNotImplemented.Errorf("NewData")
		},
		WhenFinished: func(base.INITVoteproof, quicstream.UDPConnInfo) error { return nil },
		WhenCanceled: func(error, quicstream.UDPConnInfo) {},
		AskRequestFunc: func(context.Context, quicstream.UDPConnInfo) (string, bool, error) {
			return "", false, util.ErrNotImplemented.Errorf("AskFunc")
		},
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
	whenFinishedf   func(base.INITVoteproof) error
	whenCanceledf   func(error)
	cancelByMessage func()
	stop            func()
	lastpoint       *util.Locked[base.StagePoint]
	id              *util.Locked[string]
	connInfo        quicstream.UDPConnInfo // NOTE x conn info
	receivelock     sync.Mutex
}

func NewHandoverYBroker(
	ctx context.Context,
	args *HandoverYBrokerArgs,
	connInfo quicstream.UDPConnInfo,
) *HandoverYBroker {
	hctx, cancel := context.WithCancel(ctx)

	var cancelOnce sync.Once

	h := &HandoverYBroker{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "handover-y-broker")
		}),
		args:          args,
		connInfo:      connInfo,
		ctxFunc:       func() context.Context { return hctx },
		lastpoint:     util.EmptyLocked[base.StagePoint](),
		id:            util.EmptyLocked[string](),
		whenFinishedf: func(base.INITVoteproof) error { return nil },
		whenCanceledf: func(error) {},
	}

	cancelf := func(err error) {
		cancel()

		h.whenCanceled(err)
	}

	h.cancel = func(err error) {
		cancelOnce.Do(func() {
			defer h.Log().Debug().Err(err).Msg("canceled")

			if id := h.ID(); len(id) > 0 {
				_ = h.sendMessage(ctx, NewHandoverMessageCancel(id))
			}

			cancelf(err)
		})
	}

	h.cancelByMessage = func() {
		cancelOnce.Do(func() {
			defer h.Log().Debug().Msg("canceled by message")

			cancelf(ErrHandoverCanceled.Errorf("canceled by message"))
		})
	}

	h.stop = func() {
		cancelOnce.Do(func() {
			defer h.Log().Debug().Msg("stopped")

			cancel()
		})
	}

	return h
}

func (h *HandoverYBroker) ID() string {
	id, _ := h.id.Value()

	return id
}

func (h *HandoverYBroker) ConnInfo() quicstream.UDPConnInfo {
	return h.connInfo
}

func (h *HandoverYBroker) IsAsked() bool {
	id, _ := h.id.Value()

	return len(id) > 0
}

func (h *HandoverYBroker) Ask() (canMoveConsensus bool, _ error) {
	var id string

	if _, err := h.id.Set(func(_ string, isempty bool) (string, error) {
		if !isempty {
			return "", util.ErrLockedSetIgnore
		}

		switch i, j, err := h.args.AskRequestFunc(h.ctxFunc(), h.connInfo); {
		case err != nil:
			return "", err
		case len(i) < 1:
			return "", errors.Errorf("empty handover id")
		default:
			id = i
			canMoveConsensus = j

			return i, nil
		}
	}); err != nil {
		h.cancel(err)

		h.Log().Error().Err(err).Msg("failed to ask")

		return false, ErrHandoverCanceled.Wrap(err)
	}

	h.Log().Debug().Str("id", id).Msg("asked")

	return canMoveConsensus, nil
}

func (h *HandoverYBroker) isCanceled() error {
	if err := h.ctxFunc().Err(); err != nil {
		return ErrHandoverCanceled.Wrap(err)
	}

	return nil
}

func (h *HandoverYBroker) sendStagePoint(ctx context.Context, point base.StagePoint) error {
	if err := h.isCanceled(); err != nil {
		return err
	}

	id := h.ID()
	if len(id) < 1 {
		return errors.Errorf("not yet asked")
	}

	_ = h.lastpoint.SetValue(point)

	if err := h.sendMessage(ctx, newHandoverMessageChallengeStagePoint(id, point)); err != nil {
		h.cancel(err)

		return ErrHandoverCanceled.Wrap(err)
	}

	return nil
}

func (h *HandoverYBroker) sendBlockMap(ctx context.Context, point base.StagePoint, m base.BlockMap) error {
	if err := h.isCanceled(); err != nil {
		return err
	}

	id := h.ID()
	if len(id) < 1 {
		return errors.Errorf("not yet asked")
	}

	_ = h.lastpoint.SetValue(point)

	if err := h.sendMessage(ctx, newHandoverMessageChallengeBlockMap(id, point, m)); err != nil {
		h.cancel(err)

		return ErrHandoverCanceled.Wrap(err)
	}

	return nil
}

func (h *HandoverYBroker) Receive(i interface{}) error {
	h.receivelock.Lock()
	defer h.receivelock.Unlock()

	if err := h.isCanceled(); err != nil {
		return err
	}

	switch err := h.receiveInternal(i); {
	case err == nil:
	case errors.Is(err, errHandoverIgnore):
	case errors.Is(err, ErrHandoverCanceled):
		return err
	default:
		h.cancel(err)

		return ErrHandoverCanceled.Wrap(err)
	}

	return nil
}

func (h *HandoverYBroker) receiveInternal(i interface{}) error {
	id := h.ID()
	if len(id) < 1 {
		return errors.Errorf("not yet asked")
	}

	if m, ok := i.(HandoverMessage); ok {
		if id != m.HandoverID() {
			return errors.Errorf("id not matched")
		}
	}

	if iv, ok := i.(util.IsValider); ok {
		if err := iv.IsValid(h.args.NetworkID); err != nil {
			return err
		}
	}

	if _, ok := i.(HandoverMessageCancel); ok {
		h.cancelByMessage()

		return ErrHandoverCanceled.Errorf("canceled by message")
	}

	switch t := i.(type) {
	case HandoverMessageData:
		return h.receiveData(t)
	case HandoverMessageChallengeResponse:
		return h.receiveReadyResponse(t)
	case HandoverMessageFinish:
		return h.receiveFinish(t)
	default:
		return errHandoverIgnore.Errorf("X sent unknown message, %T", i)
	}
}

func (h *HandoverYBroker) receiveData(i HandoverMessageData) error {
	switch t := i.DataType(); t {
	case HandoverMessageDataTypeVoteproof:
		if vp, err := i.LoadVoteproofData(); err != nil {
			return errHandoverIgnore.Wrap(err)
		} else { //revive:disable-line:indent-error-flow
			return h.newVoteproof(vp)
		}
	case HandoverMessageDataTypeINITVoteproof:
		if pr, vp, err := i.LoadINITVoteproofData(); err != nil {
			return errHandoverIgnore.Wrap(err)
		} else { //revive:disable-line:indent-error-flow
			if pr != nil {
				if err := h.args.NewDataFunc(HandoverMessageDataTypeProposal, pr); err != nil {
					return err
				}
			}

			return h.newVoteproof(vp)
		}
	default:
		return h.args.NewDataFunc(t, i.Data())
	}
}

func (h *HandoverYBroker) receiveReadyResponse(hc HandoverMessageChallengeResponse) error {
	h.Log().Debug().Interface("message", hc).Msg("receive HandoverMessageReadyResponse")

	if hc.Err() != nil {
		return hc.Err()
	}

	return h.lastpoint.Get(func(prev base.StagePoint, isempty bool) error {
		switch {
		case isempty:
			return errors.Errorf("unknown ready response message received")
		case !hc.Point().Equal(prev):
			return errors.Errorf("ready response message point not matched")
		default:
			return nil
		}
	})
}

func (h *HandoverYBroker) receiveFinish(hc HandoverMessageFinish) error {
	defer h.stop()

	var err error

	if pr := hc.Proposal(); pr != nil {
		err = h.args.NewDataFunc(HandoverMessageDataTypeProposal, pr)
	}

	err = util.JoinErrors(err, h.whenFinished(hc.INITVoteproof()))

	l := h.Log().With().Interface("message", hc).Logger()

	if err != nil {
		l.Error().Err(err).Msg("receive HandoverMessageFinish")
	} else {
		l.Debug().Msg("receive HandoverMessageFinish")
	}

	return err
}

func (h *HandoverYBroker) newVoteproof(vp base.Voteproof) error {
	if err := h.newVoteprooff(vp); err != nil {
		return err
	}

	return h.args.NewDataFunc(HandoverMessageDataTypeVoteproof, vp)
}

func (h *HandoverYBroker) whenFinished(vp base.INITVoteproof) error {
	err := h.whenFinishedf(vp)

	return util.JoinErrors(err, h.args.WhenFinished(vp, h.connInfo))
}

func (h *HandoverYBroker) whenCanceled(err error) {
	h.whenCanceledf(err)
	h.args.WhenCanceled(err, h.connInfo)
}

func (h *HandoverYBroker) patchStates(st *States) error {
	h.newVoteprooff = func(vp base.Voteproof) error {
		vperr := newVoteproofWithErrchan(vp)

		go func() {
			st.vpch <- vperr
		}()

		<-vperr.errch

		return nil
	}

	h.whenFinishedf = func(vp base.INITVoteproof) error {
		switch current := st.current(); {
		case current == nil:
		case vp == nil:
			st.cleanHandovers()
			_ = st.SetAllowConsensus(true)

			_ = st.args.Ballotbox.Count()

			go func() {
				// NOTE moves to syncing
				err := st.AskMoveState(newSyncingSwitchContextWithVoteproof(current.state(), vp))
				if err != nil {
					panic(err)
				}
			}()
		default:
			vperr := newVoteproofWithErrchan(vp)

			go func() {
				st.vpch <- vperr
			}()

			<-vperr.errch
		}

		return nil
	}

	h.whenCanceledf = func(error) {
		st.cleanHandovers()

		if current := st.current(); current != nil {
			go func() {
				err := st.AskMoveState(emptySyncingSwitchContext(current.state()))
				if err != nil {
					panic(err)
				}
			}()
		}
	}

	return nil
}

func (h *HandoverYBroker) sendMessage(ctx context.Context, msg HandoverMessage) error {
	return h.args.SendMessageFunc(ctx, h.connInfo, msg)
}
