package launchcmd

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"

	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type NetworkClientBlockItemFilesCommand struct { //nolint:govet //...
	BaseNetworkClientCommand
	Privatekey         string            `arg:"" name:"privatekey" help:"privatekey string"`
	Height             launch.HeightFlag `arg:""`
	OutputDirectory    string            `arg:"" name:"output directory" default:""`
	DownloadRemoteItem bool              `name:"download-remote-item"`
	DownloadAllItems   bool              `name:"download-all-items"`
	priv               base.Privatekey
	remoteItemf        isaac.RemotesBlockItemReadFunc
	decompressf        util.DecompressReaderFunc
}

func (cmd *NetworkClientBlockItemFilesCommand) Run(
	kctx *kong.Context, pctx context.Context, //revive:disable-line:context-as-argument
) error {
	defer func() {
		_ = cmd.Client.Close()
	}()

	if err := cmd.prepare(pctx); err != nil {
		return err
	}

	if err := cmd.download(kctx, pctx); err != nil {
		return err
	}

	cmd.Log.Debug().Msg("done")

	return nil
}

func (cmd *NetworkClientBlockItemFilesCommand) prepare(pctx context.Context) error {
	if len(cmd.OutputDirectory) > 0 {
		switch fi, err := os.Stat(cmd.OutputDirectory); {
		case os.IsNotExist(err):
		case err != nil:
			return errors.WithStack(err)
		case !fi.IsDir():
			return errors.Errorf("output directory, %q not directory", cmd.OutputDirectory)
		}
	}

	if err := cmd.Prepare(pctx); err != nil {
		return err
	}

	switch i, err := launch.DecodePrivatekey(cmd.Privatekey, cmd.Encoders.JSON()); {
	case err != nil:
		return err
	default:
		cmd.Log.Debug().Interface("publickey", i.Publickey()).Msg("privatekey loaded from somewhere")

		cmd.priv = i
	}

	switch {
	case cmd.DownloadRemoteItem:
		cmd.remoteItemf = isaac.NewDefaultRemotesBlockItemReadFunc()
	default:
		cmd.remoteItemf = func(
			_ context.Context,
			uri url.URL,
			compressFormat string,
			_ func(_ io.Reader, compressFormat string) error,
		) (bool, bool, error) {
			cmd.Log.Debug().
				Stringer("uri", &uri).
				Str("compress_format", compressFormat).
				Msg("remote item found, but skip")

			return true, true, nil
		}
	}

	cmd.decompressf = util.DecompressReaderFunc(util.DefaultDecompressReaderFunc)

	cmd.Log.Debug().
		Interface("height", cmd.Height.Height()).
		Str("output_directory", cmd.OutputDirectory).
		Bool("download_remote_item", cmd.DownloadRemoteItem).
		Bool("download_all_items", cmd.DownloadAllItems).
		Msg("flags")

	return nil
}

func (cmd *NetworkClientBlockItemFilesCommand) download(
	kctx *kong.Context,
	pctx context.Context, //revive:disable-line:context-as-argument
) error {
	var bfiles base.BlockItemFiles

	switch i, err := cmd.downloadBlockItemFiles(kctx, pctx); {
	case err != nil:
		return err
	default:
		bfiles = i
	}

	if cmd.DownloadAllItems {
		if err := cmd.downloadBlockItems(kctx, pctx, bfiles); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *NetworkClientBlockItemFilesCommand) downloadBlockItemFiles(
	kctx *kong.Context,
	pctx context.Context, //revive:disable-line:context-as-argument
) (
	bfiles base.BlockItemFiles,
	_ error,
) {
	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	buf := bytes.NewBuffer(nil)
	defer buf.Reset()

	out := io.MultiWriter(os.Stdout, buf)

	switch found, err := cmd.Client.BlockItemFiles(
		ctx,
		cmd.Remote.ConnInfo(),
		cmd.Height.Height(),
		cmd.priv,
		base.NetworkID(cmd.NetworkID),
		func(r io.Reader) error {
			_, err := io.Copy(out, r)

			return errors.WithStack(err)
		},
	); {
	case err != nil:
		return nil, err
	case !found:
		kctx.Errorf(util.ErrNotFound.Errorf("block item files").Error())
		kctx.Exit(2)
	}

	if len(cmd.OutputDirectory) > 0 {
		obuf := bytes.NewBuffer(nil)
		defer obuf.Reset()

		or := io.TeeReader(buf, obuf)

		fname := isaac.BlockItemFilesPath(cmd.OutputDirectory, cmd.Height.Height())
		if err := cmd.saveFile(or, fname); err != nil {
			return nil, err
		}

		buf = obuf
	}

	if err := encoder.DecodeReader(cmd.Encoders.JSON(), buf, &bfiles); err != nil {
		return nil, err
	}

	return bfiles, nil
}

func (cmd *NetworkClientBlockItemFilesCommand) downloadBlockItems(
	kctx *kong.Context,
	pctx context.Context, //revive:disable-line:context-as-argument
	bfiles base.BlockItemFiles,
) error {
	// NOTE remove height directory
	if err := os.RemoveAll(filepath.Join(
		cmd.OutputDirectory, isaac.BlockHeightDirectory(cmd.Height.Height()))); err != nil {
		return errors.WithStack(err)
	}

	m := bfiles.Items()

	worker, err := util.NewBaseJobWorker(pctx, int64(len(m)))
	if err != nil {
		return err
	}

	defer worker.Close()

	for t := range m {
		t := t

		if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
			switch found, err := cmd.downloadBlockItem(ctx, t, m[t]); {
			case err != nil:
				return err
			case !found:
				kctx.Errorf(util.ErrNotFound.Errorf("block item file, %q", t.String()).Error())
				kctx.Exit(2)
			}

			return nil
		}); err != nil {
			return err
		}
	}

	worker.Done()

	return worker.Wait()
}

func (cmd *NetworkClientBlockItemFilesCommand) downloadBlockItem(
	pctx context.Context, t base.BlockItemType, bf base.BlockItemFile,
) (bool, error) {
	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	if !isaac.IsInLocalBlockItemFile(bf.URI()) {
		return true, cmd.downloadRemoteItemFile(ctx, t, bf.URI(), bf.CompressFormat())
	}

	cmd.Log.Debug().
		Interface("item_file", bf).
		Msg("local fs item found")

	switch found, err := cmd.Client.BlockItem(
		ctx,
		cmd.Remote.ConnInfo(),
		cmd.Height.Height(),
		t,
		func(r io.Reader, _ url.URL, _ string) error {
			if r == nil {
				return util.ErrNotFound.Errorf("block item file, %q", t.String())
			}

			return cmd.saveItemFile(r, bf.URI().Path)
		},
	); {
	case err != nil, !found:
		return found, err
	default:
		return true, nil
	}
}

func (cmd *NetworkClientBlockItemFilesCommand) saveFile(r io.Reader, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil && !os.IsExist(err) {
		return errors.WithStack(err)
	}

	switch f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600); {
	case err != nil:
		return errors.WithStack(err)
	default:
		defer func() {
			_ = f.Close()
		}()

		if _, err := io.Copy(f, r); err != nil {
			return errors.WithStack(err)
		}

		cmd.Log.Debug().Str("file", path).Msg("saved")

		return nil
	}
}

func (cmd *NetworkClientBlockItemFilesCommand) saveItemFile(r io.Reader, name string) error {
	return cmd.saveFile(
		r,
		filepath.Join(
			cmd.OutputDirectory,
			isaac.BlockHeightDirectory(cmd.Height.Height()),
			name,
		),
	)
}

func (cmd *NetworkClientBlockItemFilesCommand) downloadRemoteItemFile(
	ctx context.Context,
	t base.BlockItemType,
	uri url.URL,
	compressFormat string,
) error {
	switch known, found, err := cmd.remoteItemf(ctx, uri, compressFormat,
		func(r io.Reader, compressFormat string) error {
			cmd.Log.Debug().
				Stringer("uri", &uri).
				Str("compress_format", compressFormat).
				Msg("remote item found")

			var fname string

			rr := util.NewBufferedResetReader(r)
			defer func() {
				_ = rr.Close()
			}()

			var dr io.Reader

			switch i, err := cmd.decompressf(compressFormat); {
			case err != nil:
				return err
			default:
				j, err := i(rr)
				if err != nil {
					return err
				}

				dr = j
			}

			switch _, _, enchint, err := isaac.LoadBlockItemFileBaseHeader(dr); {
			case errors.Is(err, io.EOF):
				return util.ErrNotFound.Errorf("item file header, %q", t)
			case err != nil:
				return err
			default:
				j, err := isaacblock.BlockItemFileName(t, enchint.Type(), compressFormat)
				if err != nil {
					return err
				}

				fname = j
			}

			rr.Reset()

			return cmd.saveItemFile(rr, fname)
		},
	); {
	case err != nil:
		return err
	case !known:
		return errors.Errorf("unknown uri=%q, compress_format=%q", &uri, compressFormat)
	case !found:
		return util.ErrNotFound.Errorf("uri=%q, compress_format=%q", &uri, compressFormat)
	default:
		return nil
	}
}

type NetworkClientBlockItemFileCommand struct { //nolint:govet //...
	BaseNetworkClientCommand
	Height   launch.HeightFlag  `arg:""`
	Item     base.BlockItemType `arg:"item" help:"item type"`
	Validate bool               `name:"validate" negatable:"" help:"validate by default" default:"true"`
}

func (cmd *NetworkClientBlockItemFileCommand) Run(
	kctx *kong.Context, pctx context.Context, //revive:disable-line:context-as-argument
) error {
	if err := cmd.Item.IsValid(nil); err != nil {
		return err
	}

	if err := cmd.Prepare(pctx); err != nil {
		return err
	}

	defer func() {
		_ = cmd.Client.Close()
	}()

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	switch found, err := cmd.downloadItem(ctx); {
	case err != nil:
		return err
	case !found:
		kctx.Errorf(util.ErrNotFound.Errorf("block item file").Error())
		kctx.Exit(2)
	}

	return nil
}

func (cmd *NetworkClientBlockItemFileCommand) downloadItem(ctx context.Context) (bool, error) {
	switch found, err := cmd.Client.BlockItem(
		ctx,
		cmd.Remote.ConnInfo(),
		cmd.Height.Height(),
		cmd.Item,
		func(r io.Reader, uri url.URL, compressFormat string) error {
			if r == nil {
				cmd.Log.Info().
					Str("compress_format", compressFormat).
					Stringer("uri", &uri).
					Msg("remote uri found")

				return nil
			}

			cmd.Log.Info().
				Str("compress_format", compressFormat).
				Msg("found")

			_, err := io.Copy(os.Stdout, r)

			return errors.WithStack(err)
		},
	); {
	case err != nil, !found:
		return false, err
	default:
		return true, nil
	}
}
