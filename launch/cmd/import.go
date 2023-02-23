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

var (
	PNameImportBlocks = ps.Name("import-blocks")
	PNameCheckStorage = ps.Name("check-blocks")
)

type ImportCommand struct { //nolint:govet //...
	// revive:disable:line-length-limit
	launch.DesignFlag
	Source          string           `arg:"" name:"source directory" help:"block data directory to import" type:"existingdir"`
	HeightRange     launch.RangeFlag `name:"range" help:"<from>-<to>" default:"0-"`
	Vault           string           `name:"vault" help:"privatekey path of vault"`
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
		Msg("flags")

	cmd.log = log.Log()

	//revive:disable:modifies-parameter
	pctx = context.WithValue(pctx, launch.DesignFlagContextKey, cmd.DesignFlag)
	pctx = context.WithValue(pctx, launch.DevFlagsContextKey, cmd.DevFlags)
	pctx = context.WithValue(pctx, launch.VaultContextKey, cmd.Vault)
	//revive:enable:modifies-parameter

	pps := launch.DefaultImportPS()
	_ = pps.SetLogging(log)

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

func (cmd *ImportCommand) importBlocks(pctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to import blocks")

	var encs *encoder.Encoders
	var enc encoder.Encoder
	var design launch.NodeDesign
	var local base.LocalNode
	var params *isaac.LocalParams
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		launch.EncodersContextKey, &encs,
		launch.EncoderContextKey, &enc,
		launch.DesignContextKey, &design,
		launch.LocalContextKey, &local,
		launch.LocalParamsContextKey, &params,
		launch.CenterDatabaseContextKey, &db,
	); err != nil {
		return pctx, e(err, "")
	}

	var last base.Height

	switch i, err := cmd.checkHeights(pctx); {
	case err != nil:
		return pctx, e(err, "")
	default:
		last = i
	}

	cmd.log.Debug().Interface("height", last).Msg("last height found in source")

	if err := cmd.validateSourceBlocks(last, enc, params); err != nil {
		return pctx, e(err, "")
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
		return pctx, e(err, "")
	}

	if err := cmd.validateImported(last, enc, design, params, db); err != nil {
		return pctx, e(err, "")
	}

	return pctx, nil
}

func (cmd *ImportCommand) checkHeights(pctx context.Context) (base.Height, error) {
	var last base.Height

	var encs *encoder.Encoders
	var enc encoder.Encoder
	var params *isaac.LocalParams
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		launch.EncodersContextKey, &encs,
		launch.EncoderContextKey, &enc,
		launch.LocalParamsContextKey, &params,
		launch.CenterDatabaseContextKey, &db,
	); err != nil {
		return last, err
	}

	lastlocalheight := base.NilHeight

	switch i, found, err := db.LastBlockMap(); {
	case err != nil:
		return last, err
	case !found:
	case cmd.fromHeight < base.GenesisHeight:
		cmd.fromHeight = i.Manifest().Height() + 1

		lastlocalheight = i.Manifest().Height()
	case i.Manifest().Height() != cmd.fromHeight-1:
		return last, errors.Errorf(
			"from height should be same with last height + 1; from=%d last=%d", cmd.fromHeight, i.Manifest().Height())
	default:
		lastlocalheight = i.Manifest().Height()
	}

	if cmd.toHeight > base.NilHeight && cmd.toHeight <= lastlocalheight {
		return last, errors.Errorf("to height should be higher than last; to=%d last=%d", cmd.toHeight, lastlocalheight)
	}

	switch {
	case cmd.fromHeight < base.GenesisHeight:
		cmd.fromHeight = base.GenesisHeight
	case cmd.fromHeight > base.NilHeight:
		switch i, found, err := db.BlockMap(cmd.fromHeight - 1); {
		case err != nil:
			return last, err
		case !found:
			return last, errors.Errorf("previous blockmap not found for from height, %d", cmd.fromHeight-1)
		default:
			cmd.prevblockmap = i
		}
	}

	switch i, found, err := isaacblock.FindLastHeightFromLocalFS(cmd.Source, enc, params.NetworkID()); {
	case err != nil:
		return last, err
	case !found, i < base.GenesisHeight:
		return last, errors.Errorf("last height not found in source")
	case i < cmd.toHeight:
		return last, errors.Errorf("last is lower than to height; last=%d to=%d", i, cmd.toHeight)
	case cmd.toHeight > base.NilHeight:
		last = cmd.toHeight
	default:
		last = i
	}

	if cmd.fromHeight > last {
		return last, errors.Errorf("from height is higher than to; from=%d to=%d", cmd.fromHeight, last)
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
						return util.ErrNotFound.Errorf("blockmap not found in database; %d", m.Manifest().Height())
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
