package launchcmd

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
)

type NetworkClientSetAllowConsensusCommand struct { //nolint:govet //...
	KeyString string `arg:"" name:"privatekey" help:"privatekey string"`
	Allow     string `arg:"" name:"allow" help:"{allow, not-allow}" default:""`
	BaseNetworkClientCommand
}

func (cmd *NetworkClientSetAllowConsensusCommand) Run(pctx context.Context) error {
	if err := cmd.Prepare(pctx); err != nil {
		return err
	}

	var priv base.Privatekey

	switch key, err := base.DecodePrivatekeyFromString(cmd.KeyString, cmd.Encoder); {
	case err != nil:
		return err
	default:
		if err := key.IsValid(nil); err != nil {
			return err
		}

		priv = key
	}

	var allow bool

	switch cmd.Allow {
	case "allow", "not-allow":
		allow = cmd.Allow == "allow"
	default:
		return errors.Errorf(`wrong allow value, should be "allow" or "not-allow"`)
	}

	ci, _ := cmd.Remote.ConnInfo()

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	l := cmd.Log.With().Bool("allow", allow).Logger()

	isset, err := cmd.Client.SetAllowConsensus(ctx, ci, priv, base.NetworkID(cmd.NetworkID), allow)

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
