package process

import (
	"context"
	"strings"
	"syscall"

	"github.com/spikeekips/mitum/launch/config"
	"github.com/spikeekips/mitum/launch/pm"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/storage/localfs"
	mongodbstorage "github.com/spikeekips/mitum/storage/mongodb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/cache"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"golang.org/x/xerrors"
)

const (
	ProcessNameStorage = "storage"
	ProcessNameBlockFS = "blockfs"
)

var (
	ProcessorBlockFS pm.Process
	ProcessorStorage pm.Process
)

func init() {
	if i, err := pm.NewProcess(
		ProcessNameStorage,
		[]string{
			ProcessNameConfig,
		},
		ProcessMongodbStorage,
	); err != nil {
		panic(err)
	} else {
		ProcessorStorage = i
	}

	if i, err := pm.NewProcess(
		ProcessNameBlockFS,
		[]string{
			ProcessNameStorage,
		},
		ProcessBlockFS,
	); err != nil {
		panic(err)
	} else {
		ProcessorBlockFS = i
	}
}

func ProcessBlockFS(ctx context.Context) (context.Context, error) {
	var l config.LocalNode
	var conf config.BlockFS
	if err := config.LoadConfigContextValue(ctx, &l); err != nil {
		return ctx, err
	} else {
		conf = l.Storage().BlockFS()
	}

	var st storage.Storage
	if err := LoadStorageContextValue(ctx, &st); err != nil {
		return ctx, err
	}

	var enc *jsonenc.Encoder
	if err := config.LoadJSONEncoderContextValue(ctx, &enc); err != nil {
		return ctx, err
	}

	if conf.WideOpen() {
		syscall.Umask(0)
		localfs.DefaultFilePermission = 0o644
		localfs.DefaultDirectoryPermission = 0o755
	}

	var blockFS *storage.BlockFS
	if fs, err := localfs.NewFS(conf.Path(), true); err != nil {
		return nil, err
	} else {
		blockFS = storage.NewBlockFS(fs, enc)
		if err := blockFS.Initialize(); err != nil {
			return nil, err
		}
	}

	var forceCreate bool
	if err := LoadGenesisBlockForceCreateContextValue(ctx, &forceCreate); err != nil {
		if !xerrors.Is(err, util.ContextValueNotFoundError) {
			return ctx, err
		}
	}

	if !forceCreate {
		if m, found, err := st.LastManifest(); err != nil {
			return ctx, err
		} else if found {
			if err := blockFS.SetLast(m.Height()); err != nil {
				return ctx, err
			}
		}
	}

	return context.WithValue(ctx, ContextValueBlockFS, blockFS), nil
}

func ProcessMongodbStorage(ctx context.Context) (context.Context, error) {
	var l config.LocalNode
	var conf config.MainStorage
	if err := config.LoadConfigContextValue(ctx, &l); err != nil {
		return ctx, err
	} else {
		conf = l.Storage().Main()
	}

	if !strings.EqualFold(conf.URI().Scheme, "mongodb") {
		return ctx, nil
	}

	var ca cache.Cache
	if c, err := cache.NewCacheFromURI(conf.Cache().String()); err != nil {
		return ctx, err
	} else {
		ca = c
	}

	var encs *encoder.Encoders
	if err := config.LoadEncodersContextValue(ctx, &encs); err != nil {
		return ctx, err
	}

	if st, err := mongodbstorage.NewStorageFromURI(conf.URI().String(), encs, ca); err != nil {
		return ctx, err
	} else if err := st.Initialize(); err != nil {
		return ctx, err
	} else {
		return context.WithValue(ctx, ContextValueStorage, st), nil
	}
}
