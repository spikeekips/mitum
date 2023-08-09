package launch

import (
	"context"
	"io"
	"net"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
	"golang.org/x/sync/singleflight"
)

var (
	PNameRateLimiterContextKey = ps.Name("network-ratelimiter")
	RateLimiterContextKey      = util.ContextKey("network-ratelimiter")
)

func PNetworkRateLimiter(pctx context.Context) (context.Context, error) {
	e := util.StringError("ratelimiter")

	var log *logging.Logging
	var design NodeDesign
	var handlers *quicstream.PrefixHandler
	var watcher *isaac.LastConsensusNodesWatcher

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
		QuicstreamHandlersContextKey, &handlers,
		LastConsensusNodesWatcherContextKey, &watcher,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	args := NewRateLimitHandlerArgs()
	args.Rules = design.LocalParams.Network.RateLimit().RateLimiterRules
	args.Rules.SetIsInConsensusNodesFunc(rateLimiterIsInConsensusNodesFunc(watcher))

	var rateLimitHandler *RateLimitHandler

	switch i, err := NewRateLimitHandler(args); {
	case err != nil:
		return pctx, e.Wrap(err)
	default:
		rateLimitHandler = i
	}

	rslog := logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
		return zctx.Str("module", "ratelimit")
	}).SetLogging(log)

	handlers.SetHandlerFunc(func(handler quicstream.Handler) quicstream.Handler {
		return func(ctx context.Context, addr net.Addr, r io.Reader, w io.WriteCloser) (context.Context, error) {
			ctx, err := handler(ctx, addr, r, w)
			if ctx == nil {
				return ctx, err
			}

			if result, ok := ctx.Value(RateLimiterResultContextKey).(func() RateLimiterResult); ok {
				if allowed, ok := ctx.Value(RateLimiterResultAllowedContextKey).(bool); ok && !allowed {
					rslog.Log().Debug().Func(func(e *zerolog.Event) {
						e.Interface("result", result())
					}).Msg("not allowed")
				}

				rslog.Log().Trace().Func(func(e *zerolog.Event) {
					e.Interface("result", result())
				}).Msg("checked")
			}

			return ctx, err
		}
	})

	return context.WithValue(pctx, RateLimiterContextKey, rateLimitHandler), nil
}

func rateLimiterIsInConsensusNodesFunc(
	watcher *isaac.LastConsensusNodesWatcher,
) func() (util.Hash, func(base.Address) bool, error) {
	var sg singleflight.Group
	var sgf singleflight.Group

	return func() (util.Hash, func(base.Address) bool, error) {
		// NOTE suffrage and candidates
		i, err, _ := util.SingleflightDo[[2]interface{}](&sg, "", func() (r [2]interface{}, _ error) {
			var proof base.SuffrageProof
			var candidates base.State

			switch i, j, err := watcher.Last(); {
			case err != nil:
				return r, err
			case i == nil:
				return r, errors.Errorf("proof not found")
			default:
				proof = i
				candidates = j
			}

			return [2]interface{}{
				proof.State().Hash(),
				func(node base.Address) bool {
					exists, _, _ := util.SingleflightDo[bool](&sgf, node.String(), func() (bool, error) {
						_, exists, _ := isaac.IsNodeAddressInLastConsensusNodes(node, proof, candidates)

						return exists, nil
					})

					return exists
				},
			}, nil
		})

		if err != nil {
			return nil, nil, err
		}

		return i[0].(util.Hash), i[1].(func(base.Address) bool), nil //nolint:forcetypeassert //...
	}
}
