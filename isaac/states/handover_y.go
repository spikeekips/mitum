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
	SendFunc func(context.Context, interface{}) error
	NewData  func(HandoverMessageDataType, interface{}) error
	// WhenFinished is called when handover process is finished.
	WhenFinished func(base.INITVoteproof) error
	WhenCanceled func(error)
	NetworkID    base.NetworkID
}

// FIXME WhenCanceled; left memberlist

func NewHandoverYBrokerArgs(networkID base.NetworkID) *HandoverYBrokerArgs {
	return &HandoverYBrokerArgs{
		NetworkID: networkID,
		SendFunc: func(context.Context, interface{}) error {
			return ErrHandoverCanceled.Errorf("SendFunc not implemented")
		},
		NewData: func(HandoverMessageDataType, interface{}) error {
			return util.ErrNotImplemented.Errorf("NewData")
		},
		WhenFinished: func(base.INITVoteproof) error { return nil },
		WhenCanceled: func(error) {},
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
	isReady         *util.Locked[bool]
	connInfo        quicstream.UDPConnInfo // NOTE x conn info
	id              string
	receivelock     sync.Mutex
}

func NewHandoverYBroker(
	ctx context.Context,
	args *HandoverYBrokerArgs,
	id string,
	connInfo quicstream.UDPConnInfo,
) *HandoverYBroker {
	hctx, cancel := context.WithCancel(ctx)

	var cancelOnce sync.Once

	h := &HandoverYBroker{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "handover-y-broker").Str("id", id)
		}),
		args:          args,
		id:            id,
		connInfo:      connInfo,
		ctxFunc:       func() context.Context { return hctx },
		lastpoint:     util.EmptyLocked[base.StagePoint](),
		isReady:       util.EmptyLocked[bool](),
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

			_ = args.SendFunc(ctx, newHandoverMessageCancel(id))

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
	return h.id
}

func (h *HandoverYBroker) ConnInfo() quicstream.UDPConnInfo {
	return h.connInfo
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

	_ = h.lastpoint.SetValue(point)

	if err := h.args.SendFunc(ctx, newHandoverMessageChallengeStagePoint(h.id, point)); err != nil {
		h.cancel(err)

		return ErrHandoverCanceled.Wrap(err)
	}

	return nil
}

func (h *HandoverYBroker) sendBlockMap(ctx context.Context, point base.StagePoint, m base.BlockMap) error {
	if err := h.isCanceled(); err != nil {
		return err
	}

	_ = h.lastpoint.SetValue(point)

	if err := h.args.SendFunc(ctx, newHandoverMessageChallengeBlockMap(h.id, point, m)); err != nil {
		h.cancel(err)

		return ErrHandoverCanceled.Wrap(err)
	}

	return nil
}

func (h *HandoverYBroker) receive(i interface{}) error {
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
	if id, ok := i.(HandoverMessage); ok {
		if h.ID() != id.HandoverID() {
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
				if err := h.args.NewData(HandoverMessageDataTypeProposal, pr); err != nil {
					return err
				}
			}

			return h.newVoteproof(vp)
		}
	default:
		return h.args.NewData(t, i.Data())
	}
}

func (h *HandoverYBroker) receiveReadyResponse(hc HandoverMessageChallengeResponse) error {
	h.Log().Debug().Interface("message", hc).Msg("receive HandoverMessageReadyResponse")

	switch {
	case hc.Err() != nil:
		return hc.Err()
	case !hc.OK():
		_ = h.isReady.SetValue(false)
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
		err = h.args.NewData(HandoverMessageDataTypeProposal, pr)
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

	return h.args.NewData(HandoverMessageDataTypeVoteproof, vp)
}

func (h *HandoverYBroker) whenFinished(vp base.INITVoteproof) error {
	err := h.whenFinishedf(vp)

	return util.JoinErrors(err, h.args.WhenFinished(vp))
}

func (h *HandoverYBroker) whenCanceled(err error) {
	h.whenCanceledf(err)
	h.args.WhenCanceled(err)
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
			st.cleanHandoverBrokers()
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
		st.cleanHandoverBrokers()

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
