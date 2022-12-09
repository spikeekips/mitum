package launch

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameSyncSourceChecker      = ps.Name("sync-source-checker")
	PNameStartSyncSourceChecker = ps.Name("start-sync-source-checker")
	SyncSourceCheckerContextKey = util.ContextKey("sync-source-checker")
	SyncSourcePoolContextKey    = util.ContextKey("sync-source-pool")
)

func PSyncSourceChecker(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare SyncSourceChecker")

	var log *logging.Logging
	var enc encoder.Encoder
	var design NodeDesign
	var local base.LocalNode
	var params *isaac.LocalParams
	var client *isaacnetwork.QuicstreamClient

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		EncoderContextKey, &enc,
		DesignContextKey, &design,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		QuicstreamClientContextKey, &client,
	); err != nil {
		return ctx, e(err, "")
	}

	sources := make([]isaacnetwork.SyncSource, len(design.SyncSources))
	copy(sources, design.SyncSources)

	switch {
	case len(sources) < 1:
		log.Log().Warn().Msg("empty initial sync sources; connected memberlist members will be used")
	default:
		log.Log().Debug().Interface("sync_sources", sources).Msg("initial sync sources found")
	}

	syncSourcePool := isaac.NewSyncSourcePool(nil)

	syncSourceChecker := isaacnetwork.NewSyncSourceChecker(
		local,
		params.NetworkID(),
		client,
		params.SyncSourceCheckerInterval(),
		enc,
		sources,
		func(called int64, ncis []isaac.NodeConnInfo, err error) {
			syncSourcePool.UpdateFixed(ncis)

			if err != nil {
				log.Log().Error().Err(err).
					Interface("node_conninfo", ncis).
					Msg("failed to check sync sources")

				return
			}

			log.Log().Debug().
				Int64("called", called).
				Interface("node_conninfo", ncis).
				Msg("sync sources updated")
		},
	)
	_ = syncSourceChecker.SetLogging(log)

	ctx = context.WithValue(ctx, //revive:disable-line:modifies-parameter
		SyncSourceCheckerContextKey, syncSourceChecker)
	ctx = context.WithValue(ctx, //revive:disable-line:modifies-parameter
		SyncSourcePoolContextKey, syncSourcePool)

	return ctx, nil
}

func PStartSyncSourceChecker(ctx context.Context) (context.Context, error) {
	var syncSourceChecker *isaacnetwork.SyncSourceChecker
	if err := util.LoadFromContextOK(ctx, SyncSourceCheckerContextKey, &syncSourceChecker); err != nil {
		return ctx, err
	}

	return ctx, syncSourceChecker.Start()
}

func PCloseSyncSourceChecker(ctx context.Context) (context.Context, error) {
	var syncSourceChecker *isaacnetwork.SyncSourceChecker
	if err := util.LoadFromContextOK(ctx,
		SyncSourceCheckerContextKey, &syncSourceChecker,
	); err != nil {
		return ctx, err
	}

	if err := syncSourceChecker.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
		return ctx, err
	}

	return ctx, nil
}
