package isaacstates

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type HandoverYBrokerArgs struct {
	SendFunc     func(context.Context, interface{}) error
	NewVoteproof func(base.Voteproof) error
	// WhenFinished is called when handover process is finished. If
	// INITVoteproof is nil, moves to Syncing state. HandoverYBroker will be
	// automatically canceled.
	NewData      func(interface{}) error
	WhenFinished func(base.INITVoteproof) error
	WhenCanceled func(error)
	NetworkID    base.NetworkID
}

func NewHandoverYBrokerArgs(networkID base.NetworkID) *HandoverYBrokerArgs {
	return &HandoverYBrokerArgs{
		NetworkID: networkID,
		SendFunc: func(context.Context, interface{}) error {
			return util.ErrNotImplemented.Errorf("SendFunc")
		},
		NewVoteproof: func(base.Voteproof) error { return util.ErrNotImplemented.Errorf("NewVoteproof") },
		NewData:      func(interface{}) error { return util.ErrNotImplemented.Errorf("NewData") },
		WhenFinished: func(base.INITVoteproof) error { return util.ErrNotImplemented.Errorf("WhenFinished") },
		WhenCanceled: func(error) {},
	}
}

// HandoverYBroker handles handover processes of non-consensus node.
type HandoverYBroker struct {
	*logging.Logging
	args            *HandoverYBrokerArgs
	ctxFunc         func() context.Context
	cancel          func(error)
	cancelByMessage func()
	stop            func()
	lastpoint       *util.Locked[base.StagePoint]
	isReady         *util.Locked[bool]
	id              string
	receivelock     sync.Mutex
}

func NewHandoverYBroker(ctx context.Context, args *HandoverYBrokerArgs, id string) *HandoverYBroker {
	hctx, cancel := context.WithCancel(ctx)

	var cancelOnce sync.Once

	h := &HandoverYBroker{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "handover-y-broker").Str("id", id)
		}),
		args:      args,
		id:        id,
		ctxFunc:   func() context.Context { return hctx },
		lastpoint: util.EmptyLocked(base.StagePoint{}),
		isReady:   util.EmptyLocked(false),
	}

	h.cancel = func(err error) {
		cancelOnce.Do(func() {
			defer h.Log().Debug().Err(err).Msg("canceled")

			_ = args.SendFunc(ctx, newHandoverMessageCancel(id))

			cancel()

			args.WhenCanceled(err)
		})
	}

	h.cancelByMessage = func() {
		cancelOnce.Do(func() {
			defer h.Log().Debug().Msg("canceled by message")

			cancel()

			args.WhenCanceled(errHandoverCanceled.Errorf("canceled by message"))
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

func (h *HandoverYBroker) isCanceled() error {
	if err := h.ctxFunc().Err(); err != nil {
		return errHandoverCanceled.Wrap(err)
	}

	return nil
}

func (h *HandoverYBroker) sendStagePoint(ctx context.Context, point base.StagePoint) error {
	if err := h.isCanceled(); err != nil {
		return err
	}

	if err := h.args.SendFunc(ctx, newHandoverMessageChallengeStagePoint(h.id, point)); err != nil {
		return errHandoverIgnore.Wrap(err)
	}

	if err := h.sendReady(ctx, point); err != nil {
		return errHandoverIgnore.Wrap(err)
	}

	return nil
}

func (h *HandoverYBroker) sendBlockMap(ctx context.Context, point base.StagePoint, m base.BlockMap) error {
	if err := h.isCanceled(); err != nil {
		return err
	}

	if err := h.args.SendFunc(ctx, newHandoverMessageChallengeBlockMap(h.id, point, m)); err != nil {
		return errHandoverIgnore.Wrap(err)
	}

	if err := h.sendReady(ctx, point); err != nil {
		return errHandoverIgnore.Wrap(err)
	}

	return nil
}

func (h *HandoverYBroker) sendReady(ctx context.Context, point base.StagePoint) error {
	if err := h.isCanceled(); err != nil {
		return err
	}

	switch isReady, isempty := h.isReady.Value(); {
	case isempty, !isReady:
	default:
		return nil
	}

	hc := newHandoverMessageReady(h.id, point)

	_ = h.lastpoint.SetValue(point)

	h.Log().Debug().Interface("message", hc).Msg("sent HandoverMessageReady")

	if err := h.args.SendFunc(ctx, hc); err != nil {
		return errHandoverIgnore.Wrap(err)
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
	case errors.Is(err, errHandoverCanceled):
		return err
	default:
		h.cancel(err)

		return errHandoverCanceled.Wrap(err)
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

		return errHandoverCanceled.Errorf("canceled by message")
	}

	switch t := i.(type) {
	case HandoverMessageData:
		return h.receiveData(t)
	case HandoverMessageReadyResponse:
		return h.receiveReadyResponse(t)
	case HandoverMessageFinish:
		return h.receiveFinish(t)
	default:
		return errHandoverIgnore.Errorf("X sent unknown message, %T", i)
	}
}

func (h *HandoverYBroker) receiveData(i HandoverMessageData) error {
	switch t := i.Data().(type) {
	case base.Voteproof:
		return h.args.NewVoteproof(t)
	default:
		return h.args.NewData(t)
	}
}

func (h *HandoverYBroker) receiveReadyResponse(hc HandoverMessageReadyResponse) error {
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
	err := h.args.WhenFinished(hc.INITVoteproof())

	h.Log().Debug().Interface("message", hc).Err(err).Msg("receive HandoverMessageFinish")

	if err != nil {
		return err
	}

	h.stop()

	return nil
}
