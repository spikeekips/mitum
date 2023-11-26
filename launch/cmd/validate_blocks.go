package launchcmd

import (
	"context"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var PNameValidateBlocks = ps.Name("validate-blocks")

type ValidateBlocksCommand struct { //nolint:govet //...
	launch.DesignFlag
	launch.PrivatekeyFlags
	HeightRange     launch.RangeFlag `name:"range" help:"<from>-<to>" default:""`
	log             *zerolog.Logger
	launch.DevFlags `embed:"" prefix:"dev."`
	fromHeight      base.Height
	toHeight        base.Height
	prevblockmap    base.BlockMap
}

func (cmd *ValidateBlocksCommand) Run(pctx context.Context) error {
	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		return err
	}

	cmd.fromHeight, cmd.toHeight = base.NilHeight, base.NilHeight

	if h := cmd.HeightRange.From(); h != nil {
		cmd.fromHeight = base.Height(*h)

		if err := cmd.fromHeight.IsValid(nil); err != nil {
			return errors.WithMessagef(err, "invalid from height; from=%d", *h)
		}
	}

	if h := cmd.HeightRange.To(); h != nil {
		cmd.toHeight = base.Height(*h)

		if err := cmd.toHeight.IsValid(nil); err != nil {
			return errors.WithMessagef(err, "invalid to height; to=%d", *h)
		}

		if cmd.fromHeight > cmd.toHeight {
			return errors.Errorf("from height is higher than to; from=%d to=%d", cmd.fromHeight, cmd.toHeight)
		}
	}

	log.Log().Debug().
		Interface("design", cmd.DesignFlag).
		Interface("privatekey", cmd.PrivatekeyFlags).
		Interface("dev", cmd.DevFlags).
		Interface("from_height", cmd.fromHeight).
		Interface("to_height", cmd.toHeight).
		Msg("flags")

	cmd.log = log.Log()

	nctx := util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
		launch.DesignFlagContextKey:      cmd.DesignFlag,
		launch.DevFlagsContextKey:        cmd.DevFlags,
		launch.PrivatekeyFlagsContextKey: cmd.PrivatekeyFlags,
	})

	pps := ps.NewPS("cmd-validate-blocks")
	_ = pps.SetLogging(log)

	_ = pps.
		AddOK(launch.PNameEncoder, launch.PEncoder, nil).
		AddOK(launch.PNameDesign, launch.PLoadDesign, nil, launch.PNameEncoder).
		AddOK(launch.PNameLocal, launch.PLocal, nil, launch.PNameDesign).
		AddOK(launch.PNameStorage, launch.PStorage, launch.PCloseStorage, launch.PNameLocal)

	_ = pps.POK(launch.PNameEncoder).
		PostAddOK(launch.PNameAddHinters, launch.PAddHinters)

	_ = pps.POK(launch.PNameDesign).
		PostAddOK(launch.PNameCheckDesign, launch.PCheckDesign)

	_ = pps.POK(launch.PNameStorage).
		PreAddOK(launch.PNameCheckLocalFS, launch.PCheckLocalFS).
		PreAddOK(launch.PNameLoadDatabase, launch.PLoadDatabase).
		PostAddOK(launch.PNameCheckLeveldbStorage, launch.PCheckLeveldbStorage).
		PostAddOK(launch.PNameLoadFromDatabase, launch.PLoadFromDatabase).
		PostAddOK(launch.PNameCheckBlocksOfStorage, launch.PCheckBlocksOfStorage).
		PostAddOK(launch.PNameNodeInfo, launch.PNodeInfo).
		PostAddOK(PNameValidateBlocks, cmd.pValidateBlocks)

	cmd.log.Debug().Interface("process", pps.Verbose()).Msg("process ready")

	nctx, err := pps.Run(nctx)
	defer func() {
		cmd.log.Debug().Interface("process", pps.Verbose()).Msg("process will be closed")

		if _, err = pps.Close(nctx); err != nil {
			cmd.log.Error().Err(err).Msg("failed to close")
		}
	}()

	return err
}

func (cmd *ValidateBlocksCommand) pValidateBlocks(pctx context.Context) (context.Context, error) {
	e := util.StringError("validate blocks")

	var encs *encoder.Encoders
	var design launch.NodeDesign
	var local base.LocalNode
	var isaacparams *isaac.Params
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		launch.EncodersContextKey, &encs,
		launch.DesignContextKey, &design,
		launch.LocalContextKey, &local,
		launch.ISAACParamsContextKey, &isaacparams,
		launch.CenterDatabaseContextKey, &db,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	var last base.Height

	root := launch.LocalFSDataDirectory(design.Storage.Base)

	switch fromHeight, toHeight, i, err := checkLastHeight(pctx, root, cmd.fromHeight, cmd.toHeight); {
	case err != nil:
		return pctx, e.Wrap(err)
	default:
		cmd.fromHeight = fromHeight
		cmd.toHeight = toHeight
		last = i

		cmd.log.Debug().
			Interface("from_height", cmd.fromHeight).
			Interface("to_height", cmd.toHeight).
			Interface("last", last).
			Msg("heights checked")
	}

	if cmd.fromHeight > base.GenesisHeight {
		switch i, found, err := db.BlockMap(cmd.fromHeight - 1); {
		case err != nil:
			return pctx, err
		case !found:
			return pctx, errors.Errorf("previous blockmap not found for from height, %d", cmd.fromHeight-1)
		default:
			cmd.prevblockmap = i
		}
	}

	cmd.log.Debug().Interface("height", last).Msg("last height found in source")

	if err := isaacblock.ValidateBlocksFromStorage(
		root, cmd.fromHeight, last, encs.Default(), isaacparams.NetworkID(), db,
		cmd.whenBlockDone,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	return pctx, nil
}

func (cmd *ValidateBlocksCommand) whenBlockDone(m base.BlockMap, err error) error {
	l := cmd.log.With().Interface("blockmap", m).Logger()

	switch {
	case err != nil:
		l.Error().Err(err).Msg("failed to validate block")
	default:
		l.Debug().Msg("block validated")
	}

	return nil
}
