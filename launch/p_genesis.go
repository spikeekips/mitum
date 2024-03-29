package launch

import (
	"context"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var PNameGenerateGenesis = ps.Name("generate-genesis")

var NodeEventLogger EventLoggerName = "node"

func PGenerateGenesis(pctx context.Context) (context.Context, error) {
	e := util.StringError("generate genesis block")

	var log *logging.Logging
	var design NodeDesign
	var genesisDesign GenesisDesign
	var encs *encoder.Encoders
	var local base.LocalNode
	var isaacparams *isaac.Params
	var db isaac.Database
	var fsnodeinfo NodeInfo
	var eventLogging *EventLogging
	var newReaders func(context.Context, string, *isaac.BlockItemReadersArgs) (*isaac.BlockItemReaders, error)

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
		GenesisDesignContextKey, &genesisDesign,
		EncodersContextKey, &encs,
		LocalContextKey, &local,
		ISAACParamsContextKey, &isaacparams,
		CenterDatabaseContextKey, &db,
		FSNodeInfoContextKey, &fsnodeinfo,
		EventLoggingContextKey, &eventLogging,
		NewBlockItemReadersFuncContextKey, &newReaders,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	var el zerolog.Logger

	switch i, found := eventLogging.Logger(NodeEventLogger); {
	case !found:
		return pctx, errors.Errorf("node event logger not found")
	default:
		el = i
	}

	root := LocalFSDataDirectory(design.Storage.Base)

	var readers *isaac.BlockItemReaders

	switch i, err := newReaders(pctx, root, nil); {
	case err != nil:
		return pctx, err
	default:
		defer i.Close()

		readers = i
	}

	g := NewGenesisBlockGenerator(
		local,
		isaacparams.NetworkID(),
		encs,
		db,
		root,
		genesisDesign.Facts,
		func() (base.BlockMap, bool, error) {
			return isaac.BlockItemReadersDecode[base.BlockMap](
				readers.Item,
				base.GenesisHeight,
				base.BlockItemMap,
				nil,
			)
		},
	)
	_ = g.SetLogging(log)

	if _, err := g.Generate(); err != nil {
		return pctx, e.Wrap(err)
	}

	el.Debug().Interface("node_info", fsnodeinfo).Msg("node initialized")

	return pctx, nil
}
