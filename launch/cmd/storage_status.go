package launchcmd

import (
	"context"
	"os"
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

var PNameStorageStatus = ps.Name("storage-status")

type StorageStatusCommand struct { //nolint:govet //...
	launch.DesignFlag
	launch.PrivatekeyFlags
	log             *zerolog.Logger
	launch.DevFlags `embed:"" prefix:"dev."`
	readers         *isaac.BlockItemReaders
}

func (cmd *StorageStatusCommand) Run(pctx context.Context) (err error) {
	var log *logging.Logging
	if err = util.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		return err
	}

	log.Log().Debug().
		Interface("design", cmd.DesignFlag).
		Interface("privatekey", cmd.PrivatekeyFlags).
		Interface("dev", cmd.DevFlags).
		Msg("flags")

	cmd.log = log.Log()

	pps := ps.NewPS("cmd-status")
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
		PreAddOK(launch.PNameBlockReadersDecompressFunc, launch.PBlockReadersDecompressFunc).
		PreAddOK(launch.PNameBlockReaders, launch.PBlockReaders).
		PreAddOK(launch.PNameCheckLocalFS, cmd.pCheckLocalFS).
		PostAddOK(PNameStorageStatus, cmd.pStorageStatus)

	nctx := util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
		launch.DesignFlagContextKey:      cmd.DesignFlag,
		launch.DevFlagsContextKey:        cmd.DevFlags,
		launch.PrivatekeyFlagsContextKey: cmd.PrivatekeyFlags,
	})

	cmd.log.Debug().Interface("process", pps.Verbose()).Msg("process ready")

	nctx, err = pps.Run(nctx)
	defer func() {
		cmd.log.Debug().Interface("process", pps.Verbose()).Msg("process will be closed")

		if _, err = pps.Close(nctx); err != nil {
			cmd.log.Error().Err(err).Msg("failed to close")
		}
	}()

	return err
}

func (cmd *StorageStatusCommand) pCheckLocalFS(pctx context.Context) (context.Context, error) {
	switch nctx, err := launch.PCheckLocalFS(pctx); {
	case err == nil:
		cmd.log.Info().Msg("local fs checked")

		return nctx, nil
	case errors.Is(err, os.ErrNotExist), errors.Is(err, util.ErrNotFound):
		cmd.log.Error().Err(err).Msg("failed to load node info")

		return nctx, nil
	default:
		return nctx, err
	}
}

func (cmd *StorageStatusCommand) pStorageStatus(pctx context.Context) (context.Context, error) {
	e := util.StringError("storage status")

	var design launch.NodeDesign
	var encs *encoder.Encoders
	var isaacparams *isaac.Params
	var newReaders func(string) *isaac.BlockItemReaders

	if err := util.LoadFromContextOK(pctx,
		launch.DesignContextKey, &design,
		launch.EncodersContextKey, &encs,
		launch.ISAACParamsContextKey, &isaacparams,
		launch.NewBlockReadersFuncContextKey, &newReaders,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	var fsnodeinfo launch.NodeInfo
	if err := util.LoadFromContext(pctx,
		launch.FSNodeInfoContextKey, &fsnodeinfo,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	localfsroot := launch.LocalFSDataDirectory(design.Storage.Base)
	dbroot := launch.LocalFSDatabaseDirectory(design.Storage.Base)

	cmd.log.Info().
		Str("base", design.Storage.Base).
		Str("local_fs", localfsroot).
		Str("database", dbroot).
		Msg("config")

	cmd.log.Info().Interface("node_info", fsnodeinfo).Msg("local fs information")

	cmd.readers = newReaders(localfsroot)

	if err := cmd.localfs(localfsroot, isaacparams.NetworkID()); err != nil {
		return pctx, e.Wrap(err)
	}

	if err := cmd.database(dbroot); err != nil {
		return pctx, e.Wrap(err)
	}

	return pctx, nil
}

func (cmd *StorageStatusCommand) localfs(root string, networkID base.NetworkID) error {
	// NOTE last block
	switch last, found, err := isaacblock.FindLastHeightFromLocalFS(cmd.readers, networkID); {
	case err != nil:
		return err
	case !found:
		cmd.log.Info().Msg("last block not found")
	default:
		cmd.log.Info().Interface("last_height", last).Msg("last block found")
	}

	// NOTE number of directories and files
	// NOTE disk usage

	var countFiles, countDirs, diskusage uint64

	if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		switch {
		case info.IsDir():
			countDirs++
		default:
			countFiles++
			diskusage += uint64(info.Size())
		}

		return nil
	}); err != nil {
		return errors.WithStack(err)
	}

	cmd.log.Info().
		Uint64("files", countFiles).
		Uint64("directories", countDirs).
		Uint64("disk_usage", diskusage).
		Msg("local fs files and directories")

	return nil
}

func (cmd *StorageStatusCommand) database(root string) error {
	var countFiles, countDirs, diskusage uint64

	if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		switch {
		case info.IsDir():
			countDirs++
		default:
			countFiles++
			diskusage += uint64(info.Size())
		}

		return nil
	}); err != nil {
		return errors.WithStack(err)
	}

	cmd.log.Info().
		Uint64("files", countFiles).
		Uint64("directories", countDirs).
		Uint64("disk_usage", diskusage).
		Msg("database files and directories")

	return nil
}
