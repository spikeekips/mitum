package launchcmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicstream"
)

type HandoverCommands struct {
	//revive:disable:line-length-limit
	Start  StartHandoverCommand     `cmd:"" name:"start" help:"start handover"`
	Cancel CancelHandoverCommand    `cmd:"" name:"cancel" help:"cancel handover"`
	Check  CheckHandoverCommand     `cmd:"" name:"check" help:"check current consensus node for handover"`
	Log    LastHandoverYLogsCommand `cmd:"" name:"log" help:"last handover y logs"`
	//revive:enable:line-length-limit
}

type baseHandoverCommand struct { //nolint:govet //...
	KeyString string `arg:"" name:"privatekey" help:"privatekey string"`
	BaseNetworkClientCommand
	yci  quicstream.UDPConnInfo
	priv base.Privatekey
}

func (cmd *baseHandoverCommand) run(pctx context.Context) (func(), error) {
	if err := cmd.Prepare(pctx); err != nil {
		return nil, err
	}

	deferred := func() {
		_ = cmd.Client.Close()
	}

	cmd.yci, _ = cmd.Remote.ConnInfo()

	switch key, err := launch.DecodePrivatekey(cmd.KeyString, cmd.Encoder); {
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

	xci, _ := cmd.X.ConnInfo()

	isStarted, err := cmd.Client.StartHandover(
		ctx,
		cmd.yci,
		cmd.priv,
		base.NetworkID(cmd.NetworkID),
		cmd.Node.Address(),
		xci,
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

type LastHandoverYLogsCommand struct { //nolint:govet //...
	baseHandoverCommand
}

func (cmd *LastHandoverYLogsCommand) Run(pctx context.Context) error {
	switch deferred, err := cmd.baseHandoverCommand.run(pctx); {
	case err != nil:
		return err
	default:
		defer deferred()
	}

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	return cmd.Client.LastHandoverYLogs(
		ctx,
		cmd.yci,
		cmd.priv,
		base.NetworkID(cmd.NetworkID),
		func(b json.RawMessage) bool {
			_, _ = fmt.Fprintln(os.Stdout, string(b))

			return true
		},
	)
}
