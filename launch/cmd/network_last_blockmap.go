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

	var h util.Hash

	if len(strings.TrimSpace(cmd.Hash)) > 0 {
		h = valuehash.NewBytesFromString(cmd.Hash)
	}

	ci, _ := cmd.Remote.ConnInfo()

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	switch m, found, err := cmd.Client.LastBlockMap(ctx, ci, h); {
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
