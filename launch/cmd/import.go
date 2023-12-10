package launchcmd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
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
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	PNameImportBlocks = ps.Name("import-blocks")
	PNameCheckStorage = ps.Name("check-blocks")
)

type ImportCommand struct { //nolint:govet //...
	// revive:disable:line-length-limit
	launch.DesignFlag
	Source      string           `arg:"" name:"source directory" help:"block data directory to import" type:"existingdir"`
	HeightRange launch.RangeFlag `name:"range" help:"<from>-<to>" default:""`
	launch.PrivatekeyFlags
	Do              bool   `name:"do" help:"really do import"`
	CacheDirectory  string `name:"cache-directory" help:"directory for remote block item file"`
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

	cmd.log = log.Log()

	if err := cmd.prepare(); err != nil {
		return err
	}

	defer func() {
		_ = os.RemoveAll(cmd.CacheDirectory)
	}()

	log.Log().Debug().
		Interface("design", cmd.DesignFlag).
		Interface("privatekey", cmd.PrivatekeyFlags).
		Interface("dev", cmd.DevFlags).
		Str("source", cmd.Source).
		Interface("from_height", cmd.fromHeight).
		Interface("to_height", cmd.toHeight).
		Bool("do", cmd.Do).
		Str("cache_directory", cmd.CacheDirectory).
		Msg("flags")

	nctx := util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
		launch.DesignFlagContextKey:      cmd.DesignFlag,
		launch.DevFlagsContextKey:        cmd.DevFlags,
		launch.PrivatekeyFlagsContextKey: cmd.PrivatekeyFlags,
	})

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

func (cmd *ImportCommand) prepare() error {
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

	var checkCacheDirectory func() error

	switch {
	case len(cmd.CacheDirectory) < 1:
		checkCacheDirectory = func() error {
			i, err := os.MkdirTemp("", "mitum-import-")
			if err != nil {
				return errors.WithStack(err)
			}

			cmd.CacheDirectory = i

			return nil
		}
	default:
		checkCacheDirectory = func() error {
			switch fi, err := os.Stat(cmd.CacheDirectory); {
			case os.IsNotExist(err):
				if err = os.MkdirAll(filepath.Clean(cmd.CacheDirectory), 0o700); err != nil {
					return errors.WithStack(err)
				}
			case err != nil:
				return errors.WithStack(err)
			case !fi.IsDir():
				return errors.Errorf("cache directory is not directory")
			}

			return nil
		}
	}

	return checkCacheDirectory()
}

func (cmd *ImportCommand) importBlocks(pctx context.Context) (context.Context, error) {
	e := util.StringError("import blocks")

	var encs *encoder.Encoders
	var design launch.NodeDesign
	var local base.LocalNode
	var isaacparams *isaac.Params
	var db isaac.Database
	var newReaders func(string) *isaac.BlockItemReaders
	var fromRemotes isaac.RemotesBlockItemReadFunc

	if err := util.LoadFromContextOK(pctx,
		launch.EncodersContextKey, &encs,
		launch.DesignContextKey, &design,
		launch.LocalContextKey, &local,
		launch.ISAACParamsContextKey, &isaacparams,
		launch.CenterDatabaseContextKey, &db,
		launch.NewBlockReadersFuncContextKey, &newReaders,
		launch.RemotesBlockItemReaderFuncContextKey, &fromRemotes,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	sourceReaders := newReaders(cmd.Source)

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

	if err := cmd.validateSourceBlocks(
		cmd.loadItemFile(sourceReaders, fromRemotes, cmd.Do),
		last,
		isaacparams.NetworkID(),
	); err != nil {
		return pctx, e.Wrap(err)
	}

	if !cmd.Do {
		cmd.log.Debug().Msg("to import really blocks, `--do`")

		return pctx, nil
	}

	if err := launch.ImportBlocks(
		sourceReaders,
		func(ctx context.Context,
			uri url.URL,
			compressFormat string,
			callback func(_ io.Reader, compressFormat string) error,
		) (known, found bool, _ error) {
			switch found, err := cmd.readTempRemoteItemFile(uri, compressFormat, callback); {
			case err != nil:
				return false, false, err
			case found:
				return true, true, nil
			default:
				return fromRemotes(ctx, uri, compressFormat, callback)
			}
		},
		newReaders(launch.LocalFSDataDirectory(design.Storage.Base)),
		cmd.fromHeight,
		last,
		encs,
		db,
		isaacparams,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	importedReaders := newReaders(launch.LocalFSDataDirectory(design.Storage.Base))

	if err := cmd.validateImported(importedReaders, last, isaacparams, db); err != nil {
		return pctx, e.Wrap(err)
	}

	return pctx, nil
}

func (cmd *ImportCommand) checkHeights(pctx context.Context) (base.Height, error) {
	var last base.Height

	var isaacparams *isaac.Params
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
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
	itemf isaac.BlockItemReadersItemFunc,
	last base.Height,
	networkID base.NetworkID,
) error {
	// NOTE if cmd.Do is true, save the remote item files in temp directory.
	e := util.StringError("validate source blocks")

	d := last - cmd.fromHeight

	if err := util.BatchWork(
		context.Background(),
		d.Int64()+1,
		333, //nolint:gomnd //...
		func(context.Context, uint64) error {
			return nil
		},
		func(_ context.Context, i, _ uint64) error {
			height := base.Height(int64(i) + cmd.fromHeight.Int64())

			return isaacblock.ValidateBlockFromLocalFS(
				itemf,
				height,
				networkID,
				nil, nil, nil,
			)
		},
	); err != nil {
		return e.Wrap(err)
	}

	cmd.log.Debug().Msg("source blocks validated")

	return nil
}

func (cmd *ImportCommand) loadItemFile( //revive:disable-line:flag-parameter
	sourceReaders *isaac.BlockItemReaders,
	fromRemotes isaac.RemotesBlockItemReadFunc,
	saveTemp bool,
) isaac.BlockItemReadersItemFunc {
	return isaac.BlockItemReadersItemFuncWithRemote(
		sourceReaders,
		fromRemotes,
		func(itemfile base.BlockItemFile, ir isaac.BlockItemReader, f isaac.BlockItemReaderCallbackFunc) error {
			if isaac.IsInLocalBlockItemFile(itemfile.URI()) {
				return f(ir)
			}

			if !saveTemp {
				return f(ir)
			}

			buf := bytes.NewBuffer(nil)
			defer func() {
				buf.Reset()
			}()

			if _, err := ir.Reader().Tee(buf, nil); err != nil {
				return err
			}

			if err := f(ir); err != nil {
				return err
			}

			return cmd.writeTempRemoteItemFile(itemfile.URI(), itemfile.CompressFormat(), buf)
		},
	)
}

func (cmd *ImportCommand) validateImported(
	importedReaders *isaac.BlockItemReaders,
	last base.Height,
	params *isaac.Params,
	db isaac.Database,
) error {
	e := util.StringError("validate imported")

	if err := isaacblock.ValidateBlocksFromStorage(
		importedReaders.Item, cmd.fromHeight, last, params.NetworkID(), db, nil); err != nil {
		return e.Wrap(err)
	}

	cmd.log.Debug().Msg("imported blocks validated")

	return nil
}

func (cmd *ImportCommand) tempRemoteItemFileName(uri url.URL, compressFormat string) string {
	h := valuehash.NewSHA256(util.ConcatBytesSlice(
		[]byte(fmt.Sprintf("%v", uri)),
		[]byte(compressFormat),
	))

	return filepath.Join(cmd.CacheDirectory, util.DelmSplitStrings(h.String(), "/", 32)) //nolint:gomnd //...
}

func (cmd *ImportCommand) writeTempRemoteItemFile(
	uri url.URL,
	compressFormat string,
	r io.Reader,
) error {
	p := cmd.tempRemoteItemFileName(uri, compressFormat)

	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return errors.WithStack(err)
	}

	switch f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600); {
	case err != nil:
		return errors.WithStack(err)
	default:
		defer func() {
			_ = f.Close()
		}()

		if _, err := io.Copy(f, r); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (cmd *ImportCommand) readTempRemoteItemFile(
	uri url.URL,
	compressFormat string,
	f func(io.Reader, string) error,
) (bool, error) {
	switch i, err := os.Open(cmd.tempRemoteItemFileName(uri, compressFormat)); {
	case os.IsNotExist(err):
		return false, nil
	case err != nil:
		return false, errors.WithStack(err)
	default:
		defer func() {
			_ = i.Close()
		}()

		return true, f(i, compressFormat)
	}
}

func checkLastHeight(pctx context.Context, root string, fromHeight, toHeight base.Height) (
	base.Height,
	base.Height,
	base.Height,
	error,
) {
	var last base.Height

	var encs *encoder.Encoders
	var isaacparams *isaac.Params
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		launch.EncodersContextKey, &encs,
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

	if toHeight > base.NilHeight && toHeight > lastlocalheight {
		return fromHeight, toHeight, last, errors.Errorf(
			"to height higher than last; to=%d last=%d", toHeight, lastlocalheight)
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

	switch i, _, found, err := isaacblock.FindHighestDirectory(root); {
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
