package launchcmd

import (
	"context"
	"os"

	"github.com/pkg/errors"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util/encoder"
)

type NetworkClientNodeInfoCommand struct { //nolint:govet //...
	BaseNetworkClientCommand
}

func (cmd *NetworkClientNodeInfoCommand) Run(pctx context.Context) error {
	if err := cmd.Prepare(pctx); err != nil {
		return err
	}

	ci, _ := cmd.Remote.ConnInfo()

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	broker, err := cmd.Client.Broker(ctx, ci)
	if err != nil {
		return err
	}

	defer func() {
		_ = broker.Close()
	}()

	header := isaacnetwork.NewNodeInfoRequestHeader()
	if err := broker.WriteRequestHead(ctx, header); err != nil {
		return err
	}

	var enc encoder.Encoder

	switch renc, rh, err := broker.ReadResponseHead(ctx); {
	case err != nil:
		return err
	case !rh.OK():
		return errors.Errorf("not ok")
	case rh.Err() != nil:
		return rh.Err()
	default:
		enc = renc
	}

	switch dataFormat, bodyLenght, r, err := broker.ReadBody(ctx); {
	case err != nil:
		return err
	case dataFormat == quicstream.EmptyDataFormat, dataFormat == quicstream.LengthedDataFormat && bodyLenght < 1:
		return errors.Errorf("empty body")
	default:
		var v interface{}

		if err := enc.StreamDecoder(r).Decode(&v); err != nil {
			return err
		}

		return cmd.Print(v, os.Stdout)
	}
}
