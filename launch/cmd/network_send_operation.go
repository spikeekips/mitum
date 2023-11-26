package launchcmd

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util/encoder"
)

type NetworkClientSendOperationCommand struct { //nolint:govet //...
	BaseNetworkClientCommand
	Input    string `arg:"" name:"input" help:"input; default is stdin" default:"-"`
	IsString bool   `name:"input.is-string" help:"input is string, not file"`
}

func (cmd *NetworkClientSendOperationCommand) Run(pctx context.Context) error {
	if err := cmd.Prepare(pctx); err != nil {
		return err
	}

	defer func() {
		_ = cmd.Client.Close()
	}()

	var op base.Operation

	switch i, err := launch.LoadInputFlag(cmd.Input, !cmd.IsString); {
	case err != nil:
		return err
	case len(i) < 1:
		return errors.Errorf("empty input")
	default:
		cmd.Log.Debug().
			Str("input", string(i)).
			Msg("input")

		if err := encoder.Decode(cmd.JSONEncoder, i, &op); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	switch sent, err := cmd.Client.SendOperation(ctx, cmd.Remote.ConnInfo(), op); {
	case err != nil:
		cmd.Log.Error().Err(err).Msg("not sent")

		return err
	case !sent:
		cmd.Log.Error().Msg("not sent")
	default:
		cmd.Log.Info().Msg("sent")
	}

	return nil
}
