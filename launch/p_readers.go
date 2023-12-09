package launch

import (
	"context"

	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameBlockReadersDecompressFunc      = ps.Name("block-readers-decompress-func")
	PNameBlockReaders                    = ps.Name("block-readers")
	PNameRemotesBlockItemReaderFunc      = ps.Name("remotes-block-item-reader-func")
	BlockReadersDecompressFuncContextKey = util.ContextKey("block-readers-decompress-func")
	NewBlockReadersFuncContextKey        = util.ContextKey("new-block-readers-func")
	BlockReadersContextKey               = util.ContextKey("block-readers")
	RemotesBlockItemReaderFuncContextKey = util.ContextKey("remotes-block-item-reader-func")
)

func PBlockReaders(pctx context.Context) (context.Context, error) {
	var design NodeDesign
	var encs *encoder.Encoders
	var decompress util.DecompressReaderFunc

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
		BlockReadersDecompressFuncContextKey, &decompress,
	); err != nil {
		return pctx, err
	}

	newReaders := func(root string) *isaac.BlockItemReaders {
		readers := isaac.NewBlockItemReaders(root, encs, decompress)
		_ = readers.Add(isaacblock.LocalFSWriterHint, isaacblock.NewDefaultItemReaderFunc(1<<6)) //nolint:gomnd //...

		return readers
	}

	readers := newReaders(LocalFSDataDirectory(design.Storage.Base))

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
