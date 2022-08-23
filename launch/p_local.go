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
	LocalContextKey       = ps.ContextKey("local")
	LocalParamsContextKey = ps.ContextKey("local-params")
)

func PLocal(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to load local")

	var log *logging.Logging
	if err := ps.LoadFromContextOK(ctx, LoggingContextKey, &log); err != nil {
		return ctx, e(err, "")
	}

	var design NodeDesign
	if err := ps.LoadsFromContextOK(ctx, DesignContextKey, &design); err != nil {
		return ctx, e(err, "")
	}

	local, err := LocalFromDesign(design)
	if err != nil {
		return ctx, e(err, "")
	}

	log.Log().Info().Interface("local", local).Msg("local loaded")

	ctx = context.WithValue(ctx, LocalContextKey, local) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func LocalFromDesign(design NodeDesign) (base.LocalNode, error) {
	local := isaac.NewLocalNode(design.Privatekey, design.Address)

	if err := local.IsValid(nil); err != nil {
		return nil, err
	}

	return local, nil
}
