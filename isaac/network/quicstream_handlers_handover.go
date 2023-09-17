package isaacnetwork

import (
	"context"
	"io"
	"net"

	"github.com/spikeekips/mitum/base"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

func QuicstreamHandlerStartHandover(
	aclhandler quicstreamheader.HandlerFunc[StartHandoverHeader],
	f isaacstates.StartHandoverYFunc,
) quicstreamheader.Handler[StartHandoverHeader] {
	return aclhandler(func(
		ctx context.Context, addr net.Addr, broker *quicstreamheader.HandlerBroker, header StartHandoverHeader,
	) (context.Context, error) {
		err := f(ctx, header.Address(), header.ConnInfo())

		return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
	})
}

func QuicstreamHandlerCancelHandover(
	aclhandler quicstreamheader.HandlerFunc[CancelHandoverHeader],
	f func() error,
) quicstreamheader.Handler[CancelHandoverHeader] {
	return aclhandler(func(
		ctx context.Context, addr net.Addr, broker *quicstreamheader.HandlerBroker, header CancelHandoverHeader,
	) (context.Context, error) {
		err := f()

		return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
	})
}

func QuicstreamHandlerCheckHandover(
	aclhandler quicstreamheader.HandlerFunc[CheckHandoverHeader],
	f isaacstates.CheckHandoverFunc,
) quicstreamheader.Handler[CheckHandoverHeader] {
	return aclhandler(func(
		ctx context.Context, addr net.Addr, broker *quicstreamheader.HandlerBroker, header CheckHandoverHeader,
	) (context.Context, error) {
		err := f(ctx, header.Address(), header.ConnInfo())

		return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
	})
}

func QuicstreamHandlerAskHandover(
	local base.Node,
	networkID base.NetworkID,
	f isaacstates.AskHandoverReceivedFunc,
) quicstreamheader.Handler[AskHandoverHeader] {
	return func(
		ctx context.Context, addr net.Addr, broker *quicstreamheader.HandlerBroker, header AskHandoverHeader,
	) (context.Context, error) {
		if err := QuicstreamHandlerVerifyNode(
			ctx, addr, broker,
			local.Publickey(), networkID,
		); err != nil {
			return ctx, err
		}

		id, canMoveConsensus, err := f(ctx, header.Address(), header.ConnInfo())

		return ctx, broker.WriteResponseHead(ctx, NewAskHandoverResponseHeader(canMoveConsensus, err, id))
	}
}

func QuicstreamHandlerHandoverMessage(
	networkID base.NetworkID,
	f func(isaacstates.HandoverMessage) error,
) quicstreamheader.Handler[HandoverMessageHeader] {
	return func(
		ctx context.Context, addr net.Addr, broker *quicstreamheader.HandlerBroker, header HandoverMessageHeader,
	) (context.Context, error) {
		e := util.StringError("handover message")

		var msg isaacstates.HandoverMessage

		switch _, _, body, err := broker.ReadBodyErr(ctx); {
		case err != nil:
			return ctx, e.Wrap(err)
		case body == nil:
			return ctx, e.Errorf("empty body")
		default:
			switch i, err := io.ReadAll(body); {
			case err != nil:
				return ctx, e.Wrap(err)
			default:
				if err = encoder.Decode(broker.Encoder, i, &msg); err != nil {
					return ctx, e.Wrap(err)
				}

				if msg == nil {
					return ctx, e.Errorf("empty handover message")
				}

				if err = msg.IsValid(networkID); err != nil {
					return ctx, e.Wrap(err)
				}
			}
		}

		err := f(msg)

		return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
	}
}

func QuicstreamHandlerCheckHandoverX(
	local base.Node,
	networkID base.NetworkID,
	f isaacstates.CheckHandoverXFunc,
) quicstreamheader.Handler[CheckHandoverXHeader] {
	return func(
		ctx context.Context, addr net.Addr, broker *quicstreamheader.HandlerBroker, header CheckHandoverXHeader,
	) (context.Context, error) {
		err := QuicstreamHandlerVerifyNode(
			ctx, addr, broker,
			local.Publickey(), networkID,
		)

		if err == nil {
			err = f(ctx)
		}

		return ctx, broker.WriteResponseHeadOK(ctx, err == nil, err)
	}
}
