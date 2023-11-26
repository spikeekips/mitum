package launchcmd

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicstream"
)

type HandoverCommands struct {
	//revive:disable:line-length-limit
	Start  StartHandoverCommand  `cmd:"" name:"start" help:"start handover"`
	Cancel CancelHandoverCommand `cmd:"" name:"cancel" help:"cancel handover"`
	Check  CheckHandoverCommand  `cmd:"" name:"check" help:"check current consensus node for handover"`
	//revive:enable:line-length-limit
}

type baseHandoverCommand struct { //nolint:govet //...
	BaseNetworkClientCommand
	Privatekey string `arg:"" name:"privatekey" help:"privatekey string"`
	yci        quicstream.ConnInfo
	priv       base.Privatekey
}

func (cmd *baseHandoverCommand) run(pctx context.Context) (func(), error) {
	if err := cmd.Prepare(pctx); err != nil {
		return nil, err
	}

	deferred := func() {
		_ = cmd.Client.Close()
	}

	cmd.yci = cmd.Remote.ConnInfo()

	switch key, err := launch.DecodePrivatekey(cmd.Privatekey, cmd.JSONEncoder); {
	case err != nil:
		return nil, err
	default:
		cmd.priv = key
	}

	return deferred, nil
}

type StartHandoverCommand struct { //nolint:govet //...
	Node launch.AddressFlag  `arg:"" name:"node" help:"node address"`
	X    launch.ConnInfoFlag `arg:"" help:"current consensus node" placeholder:"ConnInfo"`
	baseHandoverCommand
}

func (cmd *StartHandoverCommand) Run(pctx context.Context) error {
	switch deferred, err := cmd.baseHandoverCommand.run(pctx); {
	case err != nil:
		return err
	default:
		defer deferred()
	}

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	isStarted, err := cmd.Client.StartHandover(
		ctx,
		cmd.yci,
		cmd.priv,
		base.NetworkID(cmd.NetworkID),
		cmd.Node.Address(),
		cmd.X.ConnInfo(),
	)

	switch {
	case err != nil:
		return err
	case !isStarted:
		return errors.Errorf("not started")
	default:
		cmd.Log.Info().Msg("hanoover started")

		return nil
	}
}

type CancelHandoverCommand struct { //nolint:govet //...
	baseHandoverCommand
}

func (cmd *CancelHandoverCommand) Run(pctx context.Context) error {
	switch deferred, err := cmd.baseHandoverCommand.run(pctx); {
	case err != nil:
		return err
	default:
		defer deferred()
	}

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	isCanceled, err := cmd.Client.CancelHandover(
		ctx,
		cmd.yci,
		cmd.priv,
		base.NetworkID(cmd.NetworkID),
	)

	switch {
	case err != nil:
		return err
	case !isCanceled:
		return errors.Errorf("not canceled")
	default:
		cmd.Log.Info().Msg("hanoover canceled")

		return nil
	}
}

type CheckHandoverCommand struct { //nolint:govet //...
	Node launch.AddressFlag `arg:"" name:"node" help:"node address"`
	baseHandoverCommand
}

func (cmd *CheckHandoverCommand) Run(pctx context.Context) error {
	switch deferred, err := cmd.baseHandoverCommand.run(pctx); {
	case err != nil:
		return err
	default:
		defer deferred()
	}

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	ok, err := cmd.Client.CheckHandoverX(
		ctx,
		cmd.yci,
		cmd.priv,
		base.NetworkID(cmd.NetworkID),
		cmd.Node.Address(),
	)

	switch {
	case err != nil:
		return err
	case !ok:
		return errors.Errorf("handover is not available")
	default:
		cmd.Log.Info().Msg("ok")

		return nil
	}
}
