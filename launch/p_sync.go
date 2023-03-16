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

func PSyncSourceChecker(pctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("prepare SyncSourceChecker")

	var log *logging.Logging
	var enc encoder.Encoder
	var design NodeDesign
	var local base.LocalNode
	var params *isaac.LocalParams
	var client *isaacnetwork.QuicstreamClient

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		EncoderContextKey, &enc,
		DesignContextKey, &design,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		QuicstreamClientContextKey, &client,
	); err != nil {
		return pctx, e(err, "")
	}

	sources := append([]isaacnetwork.SyncSource{}, design.SyncSources...)

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
		func(called int64, ncis []isaac.NodeConnInfo, _ error) {
			syncSourcePool.UpdateFixed(ncis)

			log.Log().Debug().
				Int64("called", called).
				Interface("node_conninfo", ncis).
				Msg("sync sources updated")
		},
	)
	_ = syncSourceChecker.SetLogging(log)

	pctx = context.WithValue(pctx, //revive:disable-line:modifies-parameter
		SyncSourceCheckerContextKey, syncSourceChecker)
	pctx = context.WithValue(pctx, //revive:disable-line:modifies-parameter
		SyncSourcePoolContextKey, syncSourcePool)

	return pctx, nil
}

func PStartSyncSourceChecker(pctx context.Context) (context.Context, error) {
	var syncSourceChecker *isaacnetwork.SyncSourceChecker
	if err := util.LoadFromContextOK(pctx, SyncSourceCheckerContextKey, &syncSourceChecker); err != nil {
		return pctx, err
	}

	return pctx, syncSourceChecker.Start(context.Background())
}

func PCloseSyncSourceChecker(pctx context.Context) (context.Context, error) {
	var syncSourceChecker *isaacnetwork.SyncSourceChecker
	if err := util.LoadFromContextOK(pctx,
		SyncSourceCheckerContextKey, &syncSourceChecker,
	); err != nil {
		return pctx, err
	}

	if err := syncSourceChecker.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
		return pctx, err
	}

	return pctx, nil
}
