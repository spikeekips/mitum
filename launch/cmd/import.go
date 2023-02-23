package launchcmd

import (
	"context"
	"path/filepath"

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

var PNameImportBlocks = ps.Name("import-blocks")

type ImportCommand struct { //nolint:govet //...
	// revive:disable:line-length-limit
	launch.DesignFlag
	Source          string            `arg:"" name:"source directory" help:"block data directory to import" type:"existingdir"`
	FromHeight      launch.HeightFlag `name:"from-height" help:"import from height" default:"0"`
	Vault           string            `name:"vault" help:"privatekey path of vault"`
	log             *zerolog.Logger
	launch.DevFlags `embed:"" prefix:"dev."`
	fromHeight      base.Height
	prevblockmap    base.BlockMap
	// revive:enable:line-length-limit
}

func (cmd *ImportCommand) Run(pctx context.Context) error {
	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		return err
	}

	cmd.fromHeight = cmd.FromHeight.Height()
	if cmd.fromHeight < base.GenesisHeight {
		cmd.fromHeight = base.GenesisHeight
	}

	log.Log().Debug().
		Interface("design", cmd.DesignFlag).
		Interface("vault", cmd.Vault).
		Interface("dev", cmd.DevFlags).
		Str("source", cmd.Source).
		Interface("from_height", cmd.fromHeight).
		Msg("flags")

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

	last := base.NilHeight

	switch i, err := launch.PLastHeightOfLocalFS(ctx, cmd.Source); {
	case err != nil:
		return ctx, e(err, "")
	case i < base.GenesisHeight:
		return ctx, e(nil, "last height not found in source")
	case i < cmd.fromHeight:
		return ctx, e(nil, "last, %d is lower than --from-height, %d", last, cmd.fromHeight)
	default:
		last = i
	}

	cmd.log.Debug().Interface("height", last).Msg("last height found in source")

	var encs *encoder.Encoders
	var enc encoder.Encoder
	var design launch.NodeDesign
	var local base.LocalNode
	var params *isaac.LocalParams
	var db isaac.Database

	if err := util.LoadFromContextOK(ctx,
		launch.EncodersContextKey, &encs,
		launch.EncoderContextKey, &enc,
		launch.DesignContextKey, &design,
		launch.LocalContextKey, &local,
		launch.LocalParamsContextKey, &params,
		launch.CenterDatabaseContextKey, &db,
	); err != nil {
		return ctx, e(err, "")
	}

	if cmd.fromHeight > base.GenesisHeight {
		switch i, found, err := db.BlockMap(cmd.fromHeight - 1); {
		case err != nil:
			return ctx, err
		case !found:
			return ctx, errors.Errorf("previous blockmap not found for from height, %d", cmd.fromHeight-1)
		default:
			cmd.prevblockmap = i
		}
	}

	if err := cmd.validateSourceBlocks(last, enc, params); err != nil {
		return ctx, e(err, "")
	}

	if err := launch.ImportBlocks(
		cmd.Source,
		design.Storage.Base,
		cmd.fromHeight,
		last,
		encs,
		enc,
		db,
		params,
	); err != nil {
		return ctx, e(err, "")
	}

	if err := cmd.validateImported(last, enc, design, params, db); err != nil {
		return ctx, e(err, "")
	}

	return ctx, nil
}

func (cmd *ImportCommand) validateSourceBlocks(
	last base.Height,
	enc encoder.Encoder,
	params *isaac.LocalParams,
) error {
	e := util.StringErrorFunc("failed to validate source blocks")

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
		return e(err, "")
	}

	cmd.log.Debug().Msg("source blocks validated")

	return nil
}

func (cmd *ImportCommand) validateImported(
	last base.Height,
	enc encoder.Encoder,
	design launch.NodeDesign,
	params *isaac.LocalParams,
	db isaac.Database,
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

	if err := cmd.validateImportedBlocks(root, last, enc, params, db); err != nil {
		return e(err, "")
	}

	return nil
}

func (cmd *ImportCommand) validateImportedBlocks(
	root string,
	last base.Height,
	enc encoder.Encoder,
	params *isaac.LocalParams,
	db isaac.Database,
) error {
	e := util.StringErrorFunc("failed to validate imported blocks")

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

			return isaacblock.ValidateBlockFromLocalFS(height, root, enc, params.NetworkID(),
				func(m base.BlockMap) error {
					switch i, found, err := db.BlockMap(m.Manifest().Height()); {
					case err != nil:
						return err
					case !found:
						return util.ErrNotFound.Errorf("blockmap not found in database; %q", m.Manifest().Height())
					default:
						if err := base.IsEqualBlockMap(m, i); err != nil {
							return err
						}

						return nil
					}
				},
				func(op base.Operation) error {
					switch found, err := db.ExistsKnownOperation(op.Hash()); {
					case err != nil:
						return err
					case !found:
						return util.ErrNotFound.Errorf("operation not found in database; %q", op.Hash())
					default:
						return nil
					}
				},
				func(st base.State) error {
					switch rst, found, err := db.State(st.Key()); {
					case err != nil:
						return err
					case !found:
						return util.ErrNotFound.Errorf("state not found in State")
					case !base.IsEqualState(st, rst):
						return errors.Errorf("states does not match")
					}

					ops := st.Operations()
					for j := range ops {
						switch found, err := db.ExistsInStateOperation(ops[j]); {
						case err != nil:
							return err
						case !found:
							return util.ErrNotFound.Errorf("operation of state not found in database")
						}
					}

					return nil
				},
			)
		},
	); err != nil {
		return e(err, "")
	}

	cmd.log.Debug().Msg("imported blocks validated")

	return nil
}
