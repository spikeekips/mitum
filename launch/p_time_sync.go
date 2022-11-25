package launch

import (
	"context"
	"time"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var DefaultTimeSyncerInterval = time.Minute * 10

var (
	PNameTimeSyncer      = ps.Name("time-syncer")
	TimeSyncerContextKey = util.ContextKey("time-syncer")
)

func PStartTimeSyncer(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare time syncer")

	var log *logging.Logging
	var design NodeDesign

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
	); err != nil {
		return ctx, e(err, "")
	}

	if len(design.TimeServer) < 1 {
		log.Log().Debug().Msg("no time server given")

		return ctx, nil
	}

	ts, err := localtime.NewTimeSyncer(design.TimeServer, design.TimeServerPort, DefaultTimeSyncerInterval)
	if err != nil {
		return ctx, e(err, "")
	}

	_ = ts.SetLogging(log)

	if err := ts.Start(); err != nil {
		return ctx, e(err, "")
	}

	return context.WithValue(ctx, TimeSyncerContextKey, ts), nil
}

func PCloseTimeSyncer(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to stop time syncer")

	var ts *localtime.TimeSyncer

	if err := util.LoadFromContextOK(ctx, TimeSyncerContextKey, &ts); err != nil {
		return ctx, e(err, "")
	}

	if err := ts.Stop(); err != nil {
		return ctx, e(err, "")
	}

	return ctx, nil
}
