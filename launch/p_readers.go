package launch

import (
	"context"

	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameBlockReadersDecompressFunc      = ps.Name("block-readers-decompress-func")
	PNameBlockReaders                    = ps.Name("block-readers")
	PNameRemotesBlockItemReaderFunc      = ps.Name("remotes-block-item-reader-func")
	BlockReadersDecompressFuncContextKey = util.ContextKey("block-readers-decompress-func")
	NewBlockReadersFuncContextKey        = util.ContextKey("new-block-readers-func")
	BlockReadersContextKey               = util.ContextKey("block-readers") // FIXME renem BlockReaders -> BlockItemReaders
	RemotesBlockItemReaderFuncContextKey = util.ContextKey("remotes-block-item-reader-func")
)

func PBlockReaders(pctx context.Context) (context.Context, error) {
	var log *logging.Logging
	var design NodeDesign
	var encs *encoder.Encoders
	var decompress util.DecompressReaderFunc

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		BlockReadersDecompressFuncContextKey, &decompress,
	); err != nil {
		return pctx, err
	}

	args := isaac.NewBlockItemReadersArgs()
	args.DecompressReaderFunc = decompress

	newReaders := func(root string) *isaac.BlockItemReaders {
		readers := isaac.NewBlockItemReaders(root, encs, args)
		_ = readers.Add(isaacblock.LocalFSWriterHint, isaacblock.NewDefaultItemReaderFunc(1<<6)) //nolint:gomnd //...

		_ = readers.SetLogging(log)

		return readers
	}

	readers := newReaders(LocalFSDataDirectory(design.Storage.Base))
	_ = readers.SetLogging(log)

	return util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
		NewBlockReadersFuncContextKey: newReaders,
		BlockReadersContextKey:        readers,
	}), nil
}

func PBlockReadersDecompressFunc(pctx context.Context) (context.Context, error) {
	return context.WithValue(pctx,
		BlockReadersDecompressFuncContextKey,
		util.DecompressReaderFunc(util.DefaultDecompressReaderFunc),
	), nil
}

func PRemotesBlockItemReaderFunc(pctx context.Context) (context.Context, error) {
	return context.WithValue(pctx,
		RemotesBlockItemReaderFuncContextKey,
		isaac.NewDefaultRemotesBlockItemReadFunc(true),
	), nil
}
