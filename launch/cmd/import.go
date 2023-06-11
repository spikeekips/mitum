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

var (
	PNameImportBlocks = ps.Name("import-blocks")
	PNameCheckStorage = ps.Name("check-blocks")
)

type ImportCommand struct { //nolint:govet //...
	// revive:disable:line-length-limit
	launch.DesignFlag
	Source          string           `arg:"" name:"source directory" help:"block data directory to import" type:"existingdir"`
	HeightRange     launch.RangeFlag `name:"range" help:"<from>-<to>" default:""`
	Vault           string           `name:"vault" help:"privatekey path of vault"`
	Do              bool             `name:"do" help:"really do import"`
	log             *zerolog.Logger
	launch.DevFlags `embed:"" prefix:"dev."`
	fromHeight      base.Height
	toHeight        base.Height
	prevblockmap    base.BlockMap
	// revive:enable:line-length-limit
}

func (cmd *ImportCommand) Run(pctx context.Context) error {
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
		Interface("vault", cmd.Vault).
		Interface("dev", cmd.DevFlags).
		Str("source", cmd.Source).
		Interface("from_height", cmd.fromHeight).
		Interface("to_height", cmd.toHeight).
		Bool("do", cmd.Do).
		Msg("flags")

	cmd.log = log.Log()

	nctx := context.WithValue(pctx, launch.DesignFlagContextKey, cmd.DesignFlag)
	nctx = context.WithValue(nctx, launch.DevFlagsContextKey, cmd.DevFlags)
	nctx = context.WithValue(nctx, launch.VaultContextKey, cmd.Vault)

	pps := launch.DefaultImportPS()
	_ = pps.SetLogging(log)

	_ = pps.AddOK(PNameImportBlocks, cmd.importBlocks, nil, launch.PNameStorage)

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

func (cmd *ImportCommand) importBlocks(pctx context.Context) (context.Context, error) {
	e := util.StringError("import blocks")

	var encs *encoder.Encoders
	var enc encoder.Encoder
	var design launch.NodeDesign
	var local base.LocalNode
	var isaacparams *isaac.Params
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		launch.EncodersContextKey, &encs,
		launch.EncoderContextKey, &enc,
		launch.DesignContextKey, &design,
		launch.LocalContextKey, &local,
		launch.ISAACParamsContextKey, &isaacparams,
		launch.CenterDatabaseContextKey, &db,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	var last base.Height

	switch i, err := cmd.checkHeights(pctx); {
	case err != nil:
		return pctx, e.Wrap(err)
	default:
		last = i
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

	if err := cmd.validateSourceBlocks(last, enc, isaacparams); err != nil {
		return pctx, e.Wrap(err)
	}

	if !cmd.Do {
		cmd.log.Debug().Msg("to import really blocks, `--do`")

		return pctx, nil
	}

	if err := launch.ImportBlocks(
		cmd.Source,
		design.Storage.Base,
		cmd.fromHeight,
		last,
		encs,
		enc,
		db,
		isaacparams,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	if err := cmd.validateImported(last, enc, design, isaacparams, db); err != nil {
		return pctx, e.Wrap(err)
	}

	return pctx, nil
}

func (cmd *ImportCommand) checkHeights(pctx context.Context) (base.Height, error) {
	var last base.Height

	var encs *encoder.Encoders
	var enc encoder.Encoder
	var isaacparams *isaac.Params
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		launch.EncodersContextKey, &encs,
		launch.EncoderContextKey, &enc,
		launch.ISAACParamsContextKey, &isaacparams,
		launch.CenterDatabaseContextKey, &db,
	); err != nil {
		return last, err
	}

	switch fromHeight, toHeight, i, err := checkLastHeight(pctx, cmd.Source, cmd.fromHeight, cmd.toHeight); {
	case err != nil:
		return last, err
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

	switch i, found, err := db.LastBlockMap(); {
	case err != nil:
		return last, err
	case !found:
	case cmd.fromHeight < base.GenesisHeight:
		cmd.fromHeight = i.Manifest().Height() + 1
	case i.Manifest().Height() != cmd.fromHeight-1:
		return last, errors.Errorf(
			"from height should be same with last height + 1; from=%d last=%d", cmd.fromHeight, i.Manifest().Height())
	}

	cmd.log.Debug().
		Interface("from_height", cmd.fromHeight).
		Interface("to_height", cmd.toHeight).
		Interface("last", last).
		Msg("heights checked")

	return last, nil
}

func (cmd *ImportCommand) validateSourceBlocks(
	last base.Height,
	enc encoder.Encoder,
	params *isaac.Params,
) error {
	e := util.StringError("validate source blocks")

	d := last - cmd.fromHeight

	if err := util.BatchWork(
		context.Background(),
		uint64(d.Int64())+1,
		333, //nolint:gomnd //...
		func(context.Context, uint64) error {
			return nil
		},
		func(_ context.Context, i, _ uint64) error {
			height := base.Height(int64(i) + cmd.fromHeight.Int64())

			return isaacblock.ValidateBlockFromLocalFS(height, cmd.Source, enc, params.NetworkID(), nil, nil, nil)
		},
	); err != nil {
		return e.Wrap(err)
	}

	cmd.log.Debug().Msg("source blocks validated")

	return nil
}

func (cmd *ImportCommand) validateImported(
	last base.Height,
	enc encoder.Encoder,
	design launch.NodeDesign,
	params *isaac.Params,
	db isaac.Database,
) error {
	e := util.StringError("validate imported")

	root := launch.LocalFSDataDirectory(design.Storage.Base)

	if err := isaacblock.ValidateBlocksFromStorage(
		root, cmd.fromHeight, last, enc, params.NetworkID(), db, nil); err != nil {
		return e.Wrap(err)
	}

	cmd.log.Debug().Msg("imported blocks validated")

	return nil
}

func checkLastHeight(pctx context.Context, source string, fromHeight, toHeight base.Height) (
	base.Height,
	base.Height,
	base.Height,
	error,
) {
	var last base.Height

	var encs *encoder.Encoders
	var enc encoder.Encoder
	var isaacparams *isaac.Params
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		launch.EncodersContextKey, &encs,
		launch.EncoderContextKey, &enc,
		launch.ISAACParamsContextKey, &isaacparams,
		launch.CenterDatabaseContextKey, &db,
	); err != nil {
		return fromHeight, toHeight, last, err
	}

	lastlocalheight := base.NilHeight

	switch i, found, err := db.LastBlockMap(); {
	case err != nil:
		return fromHeight, toHeight, last, err
	case !found:
	default:
		lastlocalheight = i.Manifest().Height()
	}

	if toHeight > base.NilHeight && toHeight <= lastlocalheight {
		return fromHeight, toHeight, last, errors.Errorf(
			"to height should be higher than last; to=%d last=%d", toHeight, lastlocalheight)
	}

	nfromHeight := fromHeight

	switch {
	case nfromHeight < base.GenesisHeight:
		nfromHeight = base.GenesisHeight
	case nfromHeight > base.NilHeight:
		switch _, found, err := db.BlockMap(nfromHeight - 1); {
		case err != nil:
			return nfromHeight, toHeight, last, err
		case !found:
			return nfromHeight, toHeight, last, errors.Errorf(
				"previous blockmap not found for from height, %d", nfromHeight-1)
		}
	}

	switch i, found, err := isaacblock.FindLastHeightFromLocalFS(source, enc, isaacparams.NetworkID()); {
	case err != nil:
		return nfromHeight, toHeight, last, err
	case !found, i < base.GenesisHeight:
		return nfromHeight, toHeight, last, errors.Errorf("last height not found in source")
	case i < toHeight:
		return nfromHeight, toHeight, last, errors.Errorf("last is lower than to height; last=%d to=%d", i, toHeight)
	case toHeight > base.NilHeight:
		last = toHeight
	default:
		last = i
	}

	switch {
	case nfromHeight > last:
		return nfromHeight, toHeight, last, errors.Errorf(
			"from height is higher than to; from=%d to=%d", nfromHeight, last)
	case nfromHeight < base.GenesisHeight:
		nfromHeight = base.GenesisHeight
	}

	return nfromHeight, toHeight, last, nil
}
