package launchcmd

import (
	"context"
	"os"

	"github.com/pkg/errors"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util/encoder"
)

type NetworkClientNodeInfoCommand struct { //nolint:govet //...
	BaseNetworkClientCommand
}

func (cmd *NetworkClientNodeInfoCommand) Run(pctx context.Context) error {
	if err := cmd.Prepare(pctx); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	stream, _, err := cmd.Client.Dial(ctx, cmd.Remote.ConnInfo())
	if err != nil {
		return err
	}

	defer func() {
		_ = cmd.Client.Close()
	}()

	header := isaacnetwork.NewNodeInfoRequestHeader()
	header.SetClientID(cmd.ClientID)

	return stream(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		if err := broker.WriteRequestHead(ctx, header); err != nil {
			return err
		}

		var enc encoder.Encoder

		switch renc, rh, err := broker.ReadResponseHead(ctx); {
		case err != nil:
			return err
		case rh.Err() != nil:
			return rh.Err()
		case !rh.OK():
			return errors.Errorf("not ok")
		default:
			enc = renc
		}

		switch bodyType, bodyLenght, r, err := broker.ReadBodyErr(ctx); {
		case err != nil:
			return err
		case bodyType == quicstreamheader.EmptyBodyType,
			bodyType == quicstreamheader.FixedLengthBodyType && bodyLenght < 1:
			return errors.Errorf("empty body")
		default:
			var v interface{}

			if err := enc.StreamDecoder(r).Decode(&v); err != nil {
				return err
			}

			return cmd.Print(v, os.Stdout)
		}
	})
}
