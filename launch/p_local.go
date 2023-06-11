package launch

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameLocal            = ps.Name("local")
	LocalContextKey       = util.ContextKey("local")
	LocalParamsContextKey = util.ContextKey("local-params")
	ISAACParamsContextKey = util.ContextKey("isaac-params")
)

func PLocal(pctx context.Context) (context.Context, error) {
	e := util.StringError("load local")

	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, LoggingContextKey, &log); err != nil {
		return pctx, e.Wrap(err)
	}

	var design NodeDesign
	if err := util.LoadFromContextOK(pctx, DesignContextKey, &design); err != nil {
		return pctx, e.Wrap(err)
	}

	local, err := LocalFromDesign(design)
	if err != nil {
		return pctx, e.Wrap(err)
	}

	log.Log().Debug().Interface("local", local).Msg("local loaded")

	return context.WithValue(pctx, LocalContextKey, local), nil
}

func LocalFromDesign(design NodeDesign) (base.LocalNode, error) {
	local := isaac.NewLocalNode(design.Privatekey, design.Address)

	if err := local.IsValid(nil); err != nil {
		return nil, err
	}

	return local, nil
}
