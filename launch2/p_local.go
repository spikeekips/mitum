package launch2

import (
	"context"

	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameLocal           = ps.PName("local")
	LocalContextKey      = ps.ContextKey("local")
	NodePolicyContextKey = ps.ContextKey("node-policy")
)

func PLocal(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to load local")

	var log *logging.Logging
	if err := ps.LoadFromContextOK(ctx, LoggingContextKey, &log); err != nil {
		return ctx, e(err, "")
	}

	var design launch.NodeDesign
	if err := ps.LoadsFromContextOK(ctx, DesignContextKey, &design); err != nil {
		return ctx, e(err, "")
	}

	local, err := launch.LocalFromDesign(design)
	if err != nil {
		return ctx, e(err, "")
	}

	log.Log().Info().Interface("local", local).Msg("local loaded")

	ctx = context.WithValue(ctx, LocalContextKey, local) //revive:disable-line:modifies-parameter

	nodepolicy, err := NodePolicyFromDesign(design)
	if err != nil {
		return ctx, e(err, "")
	}

	ctx = context.WithValue(ctx, NodePolicyContextKey, nodepolicy) //revive:disable-line:modifies-parameter

	log.Log().Info().Interface("node_policy", nodepolicy).Msg("node policy loaded")

	return ctx, nil
}

func NodePolicyFromDesign(design launch.NodeDesign) (*isaac.NodePolicy, error) {
	return isaac.DefaultNodePolicy(design.NetworkID), nil
}
