package isaacstates

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
)

type (
	StartHandoverYFunc func(context.Context, base.Address, quicstream.UDPConnInfo) error
	CheckHandoverFunc  func(context.Context, base.Address, quicstream.UDPConnInfo) error
	AskHandoverFunc    func(context.Context, quicstream.UDPConnInfo) (
		handoverid string, canMoveConsensus bool, _ error)
	AskHandoverReceivedFunc func(context.Context, base.Address, quicstream.UDPConnInfo) (
		handoverid string, canMoveConsensus bool, _ error)
	CheckHandoverXFunc func(context.Context) error
)

func (st *States) HandoverXBroker() *HandoverXBroker {
	v, _ := st.handoverXBroker.Value()

	switch {
	case v == nil:
	default:
		if err := v.isCanceled(); err != nil {
			return nil
		}
	}

	return v
}

func (st *States) NewHandoverXBroker(connInfo quicstream.UDPConnInfo) (handoverid string, _ error) {
	_, err := st.handoverXBroker.Set(func(_ *HandoverXBroker, isempty bool) (*HandoverXBroker, error) {
		switch {
		case !isempty:
			return nil, errors.Errorf("already under handover x")
		case !st.AllowedConsensus():
			return nil, errors.Errorf("not allowed consensus")
		case st.HandoverYBroker() != nil:
			return nil, errors.Errorf("under handover y")
		}

		broker, err := st.args.NewHandoverXBroker(context.Background(), connInfo)
		if err != nil {
			st.Log().Error().Err(err).Msg("failed new handover x broker")

			return nil, err
		}

		if err := broker.patchStates(st); err != nil {
			return nil, err
		}

		handoverid = broker.ID()

		st.Log().Debug().Str("id", handoverid).Msg("handover x broker created")

		return broker, nil
	})

	return handoverid, err
}

func (st *States) CancelHandoverXBroker() error {
	return st.handoverXBroker.Empty(func(broker *HandoverXBroker, isempty bool) error {
		if isempty {
			return nil
		}

		broker.cancel(ErrHandoverCanceled.Errorf("canceled"))

		return nil
	})
}

func (st *States) HandoverYBroker() *HandoverYBroker {
	v, _ := st.handoverYBroker.Value()

	switch {
	case v == nil:
	default:
		if err := v.isCanceled(); err != nil {
			return nil
		}
	}

	return v
}

func (st *States) NewHandoverYBroker(connInfo quicstream.UDPConnInfo) error {
	_, err := st.handoverYBroker.Set(func(_ *HandoverYBroker, isempty bool) (*HandoverYBroker, error) {
		switch {
		case !isempty:
			return nil, errors.Errorf("already under handover y")
		case st.AllowedConsensus():
			return nil, errors.Errorf("allowed consensus")
		case st.HandoverXBroker() != nil:
			return nil, errors.Errorf("under handover x")
		}

		broker, err := st.args.NewHandoverYBroker(context.Background(), connInfo)
		if err != nil {
			st.Log().Error().Err(err).Msg("failed new handover y broker")

			return nil, err
		}

		if err := broker.patchStates(st); err != nil {
			return nil, err
		}

		st.Log().Debug().Msg("handover y broker created")

		return broker, nil
	})

	return err
}

func (st *States) CancelHandoverYBroker() error {
	return st.handoverYBroker.Empty(func(broker *HandoverYBroker, isempty bool) error {
		if isempty {
			return nil
		}

		broker.cancel(ErrHandoverCanceled.Errorf("canceled"))

		return nil
	})
}

func (st *States) cleanHandovers() {
	_ = st.handoverXBroker.Empty(func(i *HandoverXBroker, isempty bool) error {
		if !isempty {
			st.Log().Debug().Msg("handover x broker canceled")
		}

		return nil
	})

	_ = st.handoverYBroker.Empty(func(i *HandoverYBroker, isempty bool) error {
		if !isempty {
			st.Log().Debug().Msg("handover y broker canceled")
		}

		return nil
	})
}

func NewStartHandoverYFunc(
	local base.Address,
	localci quicstream.UDPConnInfo,
	isAllowedConsensus func() bool,
	isHandoverStarted func() bool,
	checkX func(context.Context, base.Address, quicstream.UDPConnInfo) error,
	addSyncSource func(base.Address, quicstream.UDPConnInfo) error,
	startHandoverY func(quicstream.UDPConnInfo) error,
) StartHandoverYFunc {
	return func(ctx context.Context, node base.Address, xci quicstream.UDPConnInfo) error {
		e := util.StringError("check handover y")

		switch {
		case !local.Equal(node):
			return e.Errorf("address not matched")
		case localci.Addr().String() == xci.Addr().String():
			return e.Errorf("same conn info")
		case isAllowedConsensus():
			return e.Errorf("allowed consensus")
		case isHandoverStarted():
			return e.Errorf("handover already started")
		}

		if err := checkX(ctx, node, xci); err != nil {
			return e.WithMessage(err, "check x")
		}

		if err := addSyncSource(node, xci); err != nil {
			return e.WithMessage(err, "add sync source")
		}

		if err := startHandoverY(xci); err != nil {
			return e.WithMessage(err, "start handover y")
		}

		return nil
	}
}

func NewCheckHandoverFunc(
	local base.Address,
	localci quicstream.UDPConnInfo,
	isAllowedConsensus func() bool,
	isHandoverStarted func() bool,
	isJoinedMemberlist func() (bool, error),
	currentState func() StateType,
) CheckHandoverFunc {
	return func(ctx context.Context, node base.Address, yci quicstream.UDPConnInfo) error {
		e := util.StringError("check handover x")

		switch {
		case !local.Equal(node):
			return e.Errorf("address not matched")
		case localci.Addr().String() == yci.Addr().String():
			return e.Errorf("same conn info")
		case !isAllowedConsensus():
			return e.Errorf("not allowed consensus")
		case isHandoverStarted():
			return e.Errorf("handover already started")
		}

		switch joined, err := isJoinedMemberlist(); {
		case err != nil:
			return e.Wrap(err)
		case !joined:
			return e.Errorf("x not joined memberlist")
		}

		switch currentState() {
		case StateSyncing, StateConsensus, StateJoining, StateBooting:
			return nil
		case StateHandover:
			return e.Errorf("x is under handover x")
		default:
			return e.Errorf("not valid state")
		}
	}
}

func NewAskHandoverReceivedFunc(
	local base.Address,
	localci quicstream.UDPConnInfo,
	isAllowedConsensus func() bool,
	isHandoverStarted func() bool,
	isJoinedMemberlist func(quicstream.UDPConnInfo) (bool, error),
	currentState func() StateType,
	setNotAllowConsensus func(),
	startHandoverX func(quicstream.UDPConnInfo) (handoverid string, _ error),
) AskHandoverReceivedFunc {
	return func(ctx context.Context, node base.Address, yci quicstream.UDPConnInfo) (string, bool, error) {
		e := util.StringError("ask handover")

		switch {
		case !local.Equal(node):
			return "", false, e.Errorf("address not matched")
		case localci.Addr().String() == yci.Addr().String():
			return "", false, e.Errorf("same conn info")
		case isHandoverStarted():
			return "", false, e.Errorf("handover already started")
		case !isAllowedConsensus():
			return "", true, nil
		}

		switch joined, err := isJoinedMemberlist(yci); {
		case err != nil:
			return "", false, e.Wrap(err)
		case !joined:
			return "", false, e.Errorf("y not joined memberlist")
		}

		switch currentState() {
		case StateConsensus, StateJoining, StateBooting:
		default:
			setNotAllowConsensus()

			return "", true, nil
		}

		id, err := startHandoverX(yci)

		return id, false, err
	}
}

func NewCheckHandoverXFunc(
	isAllowedConsensus func() bool,
	isHandoverStarted func() bool,
	isJoinedMemberlist func() (bool, error),
	currentState func() StateType,
) CheckHandoverXFunc {
	return func(ctx context.Context) error {
		e := util.StringError("check only handover x")

		switch {
		case !isAllowedConsensus():
			return e.Errorf("not allowed consensus")
		case isHandoverStarted():
			return e.Errorf("handover already started")
		}

		switch joined, err := isJoinedMemberlist(); {
		case err != nil:
			return e.Wrap(err)
		case !joined:
			return e.Errorf("not joined memberlist")
		}

		switch currentState() {
		case StateSyncing, StateConsensus, StateJoining, StateBooting:
			return nil
		case StateHandover:
			return e.Errorf("x is under handover x")
		default:
			return e.Errorf("not valid state")
		}
	}
}

func NewAskHandoverFunc(
	local base.Address,
	joinMemberlist func(context.Context, quicstream.UDPConnInfo) error,
	sendAsk func(context.Context, base.Address, quicstream.UDPConnInfo) (string, bool, error),
) AskHandoverFunc {
	return func(ctx context.Context, ci quicstream.UDPConnInfo) (string, bool, error) {
		e := util.StringError("ask handover to x")

		if err := joinMemberlist(ctx, ci); err != nil {
			return "", false, e.WithMessage(err, "join memberlist")
		}

		<-time.After(time.Second * 6)

		handoverid, canMoveConsensus, err := sendAsk(ctx, local, ci)

		return handoverid, canMoveConsensus, e.WithMessage(err, "ask")
	}
}

func NewHandoverXFinishedFunc(
	leftMemberlist func() error,
) func(base.INITVoteproof) error {
	return func(base.INITVoteproof) error {
		return util.StringError("handover x finished").
			WithMessage(leftMemberlist(), "left memberlist")
	}
}

func NewHandoverYFinishedFunc(
	leftMemberlist func() error,
	removeSyncSource func(quicstream.UDPConnInfo) error,
) func(base.INITVoteproof, quicstream.UDPConnInfo) error {
	return func(_ base.INITVoteproof, xci quicstream.UDPConnInfo) error {
		lch := make(chan error)
		rch := make(chan error)

		go func() { lch <- leftMemberlist() }()
		go func() { rch <- removeSyncSource(xci) }()

		return util.StringError("handover y finished").Wrap(
			util.JoinErrors(<-lch, <-rch),
		)
	}
}

func NewHandoverYCanceledFunc(
	leftMemberlist func() error,
	removeSyncSource func(quicstream.UDPConnInfo) error,
) func(error, quicstream.UDPConnInfo) {
	return func(_ error, xci quicstream.UDPConnInfo) {
		_ = leftMemberlist()
		_ = removeSyncSource(xci)
	}
}
