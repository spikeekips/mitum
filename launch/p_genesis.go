package launch

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var PNameGenerateGenesis = ps.Name("generate-genesis")

func PGenerateGenesis(pctx context.Context) (context.Context, error) {
	e := util.StringError("generate genesis block")

	var log *logging.Logging
	var design NodeDesign
	var genesisDesign GenesisDesign
	var enc encoder.Encoder
	var local base.LocalNode
	var isaacparams *isaac.Params
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
		GenesisDesignContextKey, &genesisDesign,
		EncoderContextKey, &enc,
		LocalContextKey, &local,
		ISAACParamsContextKey, &isaacparams,
		CenterDatabaseContextKey, &db,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	g := NewGenesisBlockGenerator(
		local,
		isaacparams.NetworkID(),
		enc,
		db,
		LocalFSDataDirectory(design.Storage.Base),
		genesisDesign.Facts,
	)
	_ = g.SetLogging(log)

	if _, err := g.Generate(); err != nil {
		return pctx, e.Wrap(err)
	}

	return pctx, nil
}
