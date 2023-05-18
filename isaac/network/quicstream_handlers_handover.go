package isaacnetwork

import (
	"context"
	"io"
	"net"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

func QuicstreamHandlerStartHandover(
	local base.Node,
	networkID base.NetworkID,
	f isaacstates.StartHandoverYFunc,
) quicstreamheader.Handler[StartHandoverHeader] {
	return func(
		ctx context.Context, addr net.Addr, broker *quicstreamheader.HandlerBroker, header StartHandoverHeader,
	) error {
		err := quicstreamHandlerVerifyNode(
			ctx, addr, broker,
			local.Publickey(), networkID,
		)

		if err == nil {
			err = f(ctx, header.Address(), header.ConnInfo())
		}

		return broker.WriteResponseHeadOK(ctx, err == nil, err)
	}
}

func QuicstreamHandlerCancelHandover(
	local base.Node,
	networkID base.NetworkID,
	f func() error,
) quicstreamheader.Handler[CancelHandoverHeader] {
	return func(
		ctx context.Context, addr net.Addr, broker *quicstreamheader.HandlerBroker, header CancelHandoverHeader,
	) error {
		err := quicstreamHandlerVerifyNode(
			ctx, addr, broker,
			local.Publickey(), networkID,
		)

		if err == nil {
			err = f()
		}

		return broker.WriteResponseHeadOK(ctx, err == nil, err)
	}
}

func QuicstreamHandlerCheckHandover(
	local base.Node,
	networkID base.NetworkID,
	f isaacstates.CheckHandoverFunc,
) quicstreamheader.Handler[CheckHandoverHeader] {
	return func(
		ctx context.Context, addr net.Addr, broker *quicstreamheader.HandlerBroker, header CheckHandoverHeader,
	) error {
		err := quicstreamHandlerVerifyNode(
			ctx, addr, broker,
			local.Publickey(), networkID,
		)

		if err == nil {
			err = f(ctx, header.Address(), header.ConnInfo())
		}

		return broker.WriteResponseHeadOK(ctx, err == nil, err)
	}
}

func QuicstreamHandlerAskHandover(
	local base.Node,
	networkID base.NetworkID,
	localConnInfo quicstream.UDPConnInfo,
	f isaacstates.AskHandoverReceivedFunc,
) quicstreamheader.Handler[AskHandoverHeader] {
	return func(
		ctx context.Context, addr net.Addr, broker *quicstreamheader.HandlerBroker, header AskHandoverHeader,
	) error {
		switch {
		case !header.Address().Equal(local.Address()):
			return errors.Errorf("node address not matched")
		case localConnInfo.Addr().String() == header.ConnInfo().Addr().String():
			return errors.Errorf("same node conn info")
		}

		if err := quicstreamHandlerVerifyNode(
			ctx, addr, broker,
			local.Publickey(), networkID,
		); err != nil {
			return err
		}

		id, canMoveConsensus, err := f(ctx, header.Address(), header.ConnInfo())

		return broker.WriteResponseHead(ctx, NewAskHandoverResponseHeader(canMoveConsensus, err, id))
	}
}

func QuicstreamHandlerHandoverMessage(
	f func(isaacstates.HandoverMessage) error,
) quicstreamheader.Handler[HandoverMessageHeader] {
	return func(
		ctx context.Context, addr net.Addr, broker *quicstreamheader.HandlerBroker, header HandoverMessageHeader,
	) error {
		e := util.StringError("handover message")

		var msg isaacstates.HandoverMessage

		switch _, _, body, err := broker.ReadBodyErr(ctx); {
		case err != nil:
			return e.Wrap(err)
		case body == nil:
			return e.Errorf("empty body")
		default:
			switch i, err := io.ReadAll(body); {
			case err != nil:
				return e.Wrap(err)
			default:
				if err = encoder.Decode(broker.Encoder, i, &msg); err != nil {
					return e.Wrap(err)
				}

				if msg == nil {
					return e.Errorf("empty handover messag")
				}

				if err = msg.IsValid(nil); err != nil {
					return e.Wrap(err)
				}
			}
		}

		err := f(msg)

		return broker.WriteResponseHeadOK(ctx, err == nil, err)
	}
}

func QuicstreamHandlerCheckHandoverX(
	local base.Node,
	networkID base.NetworkID,
	f isaacstates.CheckHandoverXFunc,
) quicstreamheader.Handler[CheckHandoverXHeader] {
	return func(
		ctx context.Context, addr net.Addr, broker *quicstreamheader.HandlerBroker, header CheckHandoverXHeader,
	) error {
		err := quicstreamHandlerVerifyNode(
			ctx, addr, broker,
			local.Publickey(), networkID,
		)

		if err == nil {
			err = f(ctx, header.Address())
		}

		return broker.WriteResponseHeadOK(ctx, err == nil, err)
	}
}
