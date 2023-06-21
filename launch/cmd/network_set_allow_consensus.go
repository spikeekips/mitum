package launchcmd

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/launch"
)

type NetworkClientSetAllowConsensusCommand struct { //nolint:govet //...
	KeyString string `arg:"" name:"privatekey" help:"privatekey string"`
	Allow     string `arg:"" name:"allow" help:"{allow, not-allow}"`
	BaseNetworkClientCommand
}

func (cmd *NetworkClientSetAllowConsensusCommand) Run(pctx context.Context) error {
	if err := cmd.Prepare(pctx); err != nil {
		return err
	}

	var priv base.Privatekey

	switch key, err := launch.DecodePrivatekey(cmd.KeyString, cmd.Encoder); {
	case err != nil:
		return err
	default:
		priv = key
	}

	var allow bool

	switch cmd.Allow {
	case "allow", "not-allow":
		allow = cmd.Allow == "allow"
	default:
		return errors.Errorf(`wrong allow value, should be "allow" or "not-allow"`)
	}

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	l := cmd.Log.With().Bool("allow", allow).Logger()

	isset, err := cmd.Client.SetAllowConsensus(ctx, cmd.Remote.ConnInfo(), priv, base.NetworkID(cmd.NetworkID), allow)

	switch {
	case err != nil:
		return err
	case !isset:
		err = errors.Errorf("not set")
	case allow:
		l.Info().Msg("allowed")
	default:
		l.Info().Msg("not allowed")
	}

	return err
}
