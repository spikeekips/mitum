package launchcmd

import (
	"context"
	"os"
	"strings"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

type NetworkClientLastBlockMapCommand struct { //nolint:govet //...
	BaseNetworkClientCommand
	Hash string `arg:"" name:"manifest hash" help:"manifest hash" default:""`
}

func (cmd *NetworkClientLastBlockMapCommand) Run(pctx context.Context) error {
	if err := cmd.Prepare(pctx); err != nil {
		return err
	}

	defer func() {
		_ = cmd.Client.Close()
	}()

	var h util.Hash

	if len(strings.TrimSpace(cmd.Hash)) > 0 {
		switch i, err := valuehash.NewBytesFromString(cmd.Hash); {
		case err != nil:
			return err
		default:
			h = i
		}
	}

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	switch m, found, err := cmd.Client.LastBlockMap(ctx, cmd.Remote.ConnInfo(), h); {
	case err != nil:
		cmd.Log.Error().Err(err).Msg("failed to get last blockmap")

		return err
	case !found:
		cmd.Log.Error().Msg("not found")

		return nil
	case h != nil && m == nil:
		cmd.Log.Info().Msg("no new last blockmap")

		return nil
	default:
		return cmd.Print(m, os.Stdout)
	}
}
