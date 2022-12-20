package launchcmd

import (
	"context"
	"path/filepath"

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

var PNameImportBlocks = ps.Name("import-blocks")

type ImportCommand struct { //nolint:govet //...
	launch.DesignFlag
	From            string `arg:"" name:"from directory" help:"block data directory to import" type:"existingdir"`
	Vault           string `name:"vault" help:"privatekey path of vault"`
	log             *zerolog.Logger
	launch.DevFlags `embed:"" prefix:"dev."`
}

func (cmd *ImportCommand) Run(pctx context.Context) error {
	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		return err
	}

	cmd.log = log.Log()

	//revive:disable:modifies-parameter
	pctx = context.WithValue(pctx, launch.DesignFlagContextKey, cmd.DesignFlag)
	pctx = context.WithValue(pctx, launch.DevFlagsContextKey, cmd.DevFlags)
	pctx = context.WithValue(pctx, launch.VaultContextKey, cmd.Vault)
	//revive:enable:modifies-parameter

	pps := launch.DefaultINITPS()
	_ = pps.SetLogging(log)

	_ = pps.POK(launch.PNameDesign).
		PostRemoveOK(launch.PNameGenesisDesign)

	_ = pps.
		RemoveOK(launch.PNameGenerateGenesis)

	_ = pps.AddOK(PNameImportBlocks, cmd.importBlocks, nil, launch.PNameStorage)

	cmd.log.Debug().Interface("process", pps.Verbose()).Msg("process ready")

	pctx, err := pps.Run(pctx) //revive:disable-line:modifies-parameter
	defer func() {
		cmd.log.Debug().Interface("process", pps.Verbose()).Msg("process will be closed")

		if _, err = pps.Close(pctx); err != nil {
			cmd.log.Error().Err(err).Msg("failed to close")
		}
	}()

	return err
}

func (cmd *ImportCommand) importBlocks(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to import blocks")

	last, err := launch.LastHeightOfLocalFS(ctx, cmd.From)
	if err != nil {
		return ctx, e(err, "")
	}

	var encs *encoder.Encoders
	var enc encoder.Encoder
	var design launch.NodeDesign
	var local base.LocalNode
	var params *isaac.LocalParams
	var db isaac.Database
	var perm isaac.PermanentDatabase

	if err := util.LoadFromContextOK(ctx,
		launch.EncodersContextKey, &encs,
		launch.EncoderContextKey, &enc,
		launch.DesignContextKey, &design,
		launch.LocalContextKey, &local,
		launch.LocalParamsContextKey, &params,
		launch.CenterDatabaseContextKey, &db,
		launch.PermanentDatabaseContextKey, &perm,
	); err != nil {
		return ctx, e(err, "")
	}

	if err := launch.ImportBlocks(
		cmd.From,
		design.Storage.Base,
		base.GenesisHeight,
		last,
		encs,
		enc,
		db,
		perm,
		params,
	); err != nil {
		return ctx, e(err, "")
	}

	if err := cmd.validateImported(last, enc, design, params, perm); err != nil {
		return ctx, e(err, "")
	}

	return ctx, nil
}

func (cmd *ImportCommand) validateImported(
	last base.Height,
	enc encoder.Encoder,
	design launch.NodeDesign,
	params *isaac.LocalParams,
	perm isaac.PermanentDatabase,
) error {
	e := util.StringErrorFunc("failed to validate imported")

	root := launch.LocalFSDataDirectory(design.Storage.Base)

	switch h, found, err := isaacblock.FindHighestDirectory(root); {
	case err != nil:
		return e(err, "")
	case !found:
		return util.ErrNotFound.Errorf("height directories not found")
	default:
		rel, err := filepath.Rel(root, h)
		if err != nil {
			return e(err, "")
		}

		switch found, err := isaacblock.HeightFromDirectory(rel); {
		case err != nil:
			return e(err, "")
		case found != last:
			return util.ErrNotFound.Errorf("last height not found; found=%d last=%d", found, last)
		}
	}

	if err := cmd.validateImportedBlockMaps(root, last, enc, params); err != nil {
		return e(err, "")
	}

	if err := cmd.validateImportedBlocks(root, last, enc, params, perm); err != nil {
		return e(err, "")
	}

	return nil
}

func (cmd *ImportCommand) validateImportedBlockMaps(
	root string,
	last base.Height,
	enc encoder.Encoder,
	params *isaac.LocalParams,
) error {
	e := util.StringErrorFunc("failed to validate imported BlockMaps")

	if err := base.BatchValidateMaps(
		context.Background(),
		nil,
		last,
		333, //nolint:gomnd //...
		func(_ context.Context, height base.Height) (base.BlockMap, error) {
			reader, err := isaacblock.NewLocalFSReaderFromHeight(root, height, enc)
			if err != nil {
				return nil, err
			}

			switch m, found, err := reader.BlockMap(); {
			case err != nil:
				return nil, err
			case !found:
				return nil, util.ErrNotFound.Call()
			default:
				if err := m.IsValid(params.NetworkID()); err != nil {
					return nil, err
				}

				return m, nil
			}
		},
		func(m base.BlockMap) error {
			return nil
		},
	); err != nil {
		return e(err, "")
	}

	cmd.log.Debug().Msg("imported BlockMaps validated")

	return nil
}

func (*ImportCommand) validateImportedBlocks(
	root string,
	last base.Height,
	enc encoder.Encoder,
	params *isaac.LocalParams,
	perm isaac.PermanentDatabase,
) error {
	e := util.StringErrorFunc("failed to validate imported blocks")

	if err := util.BatchWork(
		context.Background(),
		uint64(last.Int64())+1,
		333, //nolint:gomnd //...
		func(context.Context, uint64) error {
			return nil
		},
		func(_ context.Context, i, _ uint64) error {
			return launch.ValidateBlockFromLocalFS(
				base.Height(int64(i)), root, enc, params.NetworkID(), perm)
		},
	); err != nil {
		return e(err, "")
	}

	return nil
}
