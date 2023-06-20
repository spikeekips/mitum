package localtime

import (
	"context"
	"sync"
	"time"

	"github.com/beevik/ntp"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var (
	allowedTimeSyncOffset     = time.Millisecond * 500
	minTimeSyncCheckInterval  = time.Minute * 10
	timeServerQueryingTimeout = time.Second * 5
	defaultTimeSyncer         *TimeSyncer
)

// TimeSyncer tries to sync time to time server.
type TimeSyncer struct {
	*logging.Logging
	*util.ContextDaemon
	host     string
	port     int
	offset   time.Duration
	interval time.Duration
	sync.RWMutex
}

// NewTimeSyncer creates new TimeSyncer
func NewTimeSyncer(server string, port int, interval time.Duration) (*TimeSyncer, error) {
	ts := &TimeSyncer{
		Logging: logging.NewLogging(func(c zerolog.Context) zerolog.Context {
			return c.Str("module", "time-syncer").
				Str("server", server).
				Int("port", port).
				Stringer("interval", interval)
		}),
		host:     server,
		port:     port,
		interval: interval,
	}

	ts.ContextDaemon = util.NewContextDaemon(ts.schedule)

	return ts, ts.check()
}

// Start starts TimeSyncer
func (ts *TimeSyncer) Start(ctx context.Context) error {
	ts.Log().Debug().Msg("started")

	if ts.interval < minTimeSyncCheckInterval {
		ts.Log().Warn().
			Stringer("check_interval", ts.interval).
			Stringer("min_ceck_interval", minTimeSyncCheckInterval).
			Msg("interval too short")
	}

	return ts.ContextDaemon.Start(ctx)
}

func (ts *TimeSyncer) schedule(ctx context.Context) error {
	defer ts.Log().Debug().Msg("stopped")

	ticker := time.NewTicker(ts.interval)
	defer ticker.Stop()

end:
	for {
		select {
		case <-ctx.Done():
			break end
		case <-ticker.C:
			started := time.Now()
			if err := ts.check(); err != nil {
				ts.Log().Error().Err(err).Stringer("elapsed", time.Since(started)).Msg("failed to check sync time")
			} else {
				ts.Log().Debug().Stringer("elapsed", time.Since(started)).Msg("time sync checked")
			}
		}
	}

	return nil
}

// Offset returns the latest time offset.
func (ts *TimeSyncer) Offset() time.Duration {
	ts.RLock()
	defer ts.RUnlock()

	return ts.offset
}

func (ts *TimeSyncer) setOffset(d time.Duration) {
	ts.Lock()
	defer ts.Unlock()

	ts.offset = d
}

func (ts *TimeSyncer) check() error {
	e := util.StringError("sync time")

	option := ntp.QueryOptions{Timeout: timeServerQueryingTimeout}

	if ts.port > 0 {
		option.Port = ts.port
	}

	response, err := ntp.QueryWithOptions(ts.host, option)
	if err != nil {
		return e.WithMessage(err, "query")
	}

	if err := response.Validate(); err != nil {
		return e.WithMessage(err, "invalid response")
	}

	offset := ts.Offset()
	defer ts.Log().Debug().Interface("response", response).Stringer("offset", offset).Msg("time checked")

	switch diff := offset - response.ClockOffset; {
	case diff == 0:
		return nil
	case diff > 0:
		if diff < allowedTimeSyncOffset {
			return nil
		}
	case diff < 0:
		if diff > allowedTimeSyncOffset*-1 {
			return nil
		}
	}

	ts.setOffset(response.ClockOffset)

	return nil
}

// SetDefaultTimeSyncer sets the global TimeSyncer.
func SetDefaultTimeSyncer(syncer *TimeSyncer) {
	defaultTimeSyncer = syncer
}

// Now returns the tuned Time with TimeSyncer.Offset().
func Now() time.Time {
	if defaultTimeSyncer == nil {
		return time.Now()
	}

	return time.Now().Add(defaultTimeSyncer.Offset())
}

func Within(base, target time.Time, d time.Duration) bool {
	switch {
	case d <= 0:
		return base.Equal(target)
	case target.After(base.Add(d)):
	case target.Before(base.Add(d * -1)):
	default:
		return true
	}

	return false
}
