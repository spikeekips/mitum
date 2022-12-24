package isaacstates

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type ballotStuckResolver struct {
	*logging.Logging
	cancelf                *util.Locked[func()]
	findMissingBallotsf    func(context.Context, base.StagePoint) ([]base.Address, error)
	requestMissingBallotsf func(context.Context, base.StagePoint, []base.Address) error
	voteSuffrageVotingf    func(context.Context, base.Point, []base.Address) error
	withdrawsVoteprooff    func(context.Context, base.Point) (base.Voteproof, error)
	nextroundf             func(context.Context, base.Point, base.Voteproof) error
	point                  base.StagePoint
	wait                   time.Duration
	interval               time.Duration
}

func newBallotStuckResolver(
	wait,
	interval time.Duration,
	findMissingBallotsf func(context.Context, base.StagePoint) ([]base.Address, error),
	requestMissingBallotsf func(context.Context, base.StagePoint, []base.Address) error,
	voteSuffrageVotingf func(context.Context, base.Point, []base.Address) error,
	withdrawsVoteprooff func(context.Context, base.Point) (base.Voteproof, error),
	nextroundf func(context.Context, base.Point, base.Voteproof) error,
) *ballotStuckResolver {
	return &ballotStuckResolver{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "ballot-stuck-resolver")
		}),
		point:                  base.ZeroStagePoint,
		cancelf:                util.EmptyLocked(func() {}),
		wait:                   wait,
		interval:               interval,
		findMissingBallotsf:    findMissingBallotsf,
		requestMissingBallotsf: requestMissingBallotsf,
		voteSuffrageVotingf:    voteSuffrageVotingf,
		withdrawsVoteprooff:    withdrawsVoteprooff,
		nextroundf:             nextroundf,
	}
}

func (c *ballotStuckResolver) newPoint(ctx context.Context, point base.StagePoint) bool {
	var started bool

	_, _ = c.cancelf.Set(func(previous func(), isempty bool) (func(), error) {
		if point.Compare(c.point) < 1 {
			return nil, util.ErrLockedSetIgnore.Call()
		}

		c.point = point

		if !isempty {
			previous() // NOTE cancel previous wait
		}

		wctx, wcancel := context.WithTimeout(ctx, c.wait) //nolint:govet //...
		sctx, cancel := context.WithCancel(ctx)

		l := c.Log().With().Interface("point", point).Logger()

		l.Debug().Msg("new point started")

		go func() {
			defer func() {
				wcancel()
				cancel()
			}()

			select {
			case <-sctx.Done():
				return
			case <-wctx.Done():
				if !errors.Is(wctx.Err(), context.DeadlineExceeded) {
					return
				}
			}

			l.Debug().Msg("found stuck")

			switch err := c.start(sctx, point); {
			case err == nil:
			case errors.Is(err, context.Canceled),
				errors.Is(err, context.DeadlineExceeded):
			default:
				l.Error().Err(err).Msg("failed to gather ballots")
			}
		}()

		started = true

		return cancel, nil
	})

	return started
}

func (c *ballotStuckResolver) start(ctx context.Context, point base.StagePoint) error {
	l := c.Log().With().Interface("point", point).Logger()

	l.Debug().Msg("start")

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	tick := -1

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			tick++

			ll := l.With().Int("tick", tick).Logger()

			ll.Debug().Msg("trying to gather missing ballots")

			// NOTE find nodes of missing ballots
			switch nomore, err := c.gatherMissingBallots(ctx, point); {
			case err != nil:
				return errors.WithMessage(err, "failed to find missing ballot nodes")
			case nomore:
				l.Debug().Msg("no more to request missing ballots; cancel")

				return nil
			}

			if tick < 3 { //nolint:gomnd //...
				continue
			}

			ll.Debug().Msg("trying suffrage voting")

			var vp base.Voteproof

			switch i, nomore, err := c.suffrageVoting(ctx, point); {
			case err != nil:
				return errors.WithMessage(err, "failed suffrage voting")
			case nomore:
				l.Debug().Msg("no more suffrage voting; cancel")

				return nil
			case i == nil:
				continue
			default:
				vp = i
			}

			ll.Debug().Msg("trying next round")

			// NOTE enough withdraw operations, moves to next round
			if err := c.nextroundf(ctx, point.Point, vp); err != nil {
				return errors.WithMessage(err, "failed to broadcast next round ballot")
			}

			l.Debug().Msg("start next round")

			return nil
		}
	}
}

func (c *ballotStuckResolver) clean() {
	_ = c.cancelf.Empty(func(cancel func(), _ bool) error {
		c.point = base.ZeroStagePoint

		if cancel != nil {
			cancel()
		}

		return nil
	})
}

func (c *ballotStuckResolver) cancel(point base.StagePoint) {
	_ = c.cancelf.Empty(func(cancel func(), _ bool) error {
		if point.Compare(c.point) < 0 {
			return util.ErrLockedSetIgnore.Call()
		}

		if cancel != nil {
			cancel()
		}

		return nil
	})
}

func (c *ballotStuckResolver) gatherMissingBallots(ctx context.Context, point base.StagePoint) (nomore bool, _ error) {
	nodes, err := c.findMissingBallotsf(ctx, point)

	switch {
	case err != nil:
		return false, err
	case len(nodes) < 1:
		return true, nil
	}

	return false, c.requestMissingBallotsf(ctx, point, nodes)
}

func (c *ballotStuckResolver) suffrageVoting(
	ctx context.Context,
	point base.StagePoint,
) (_ base.Voteproof, nomore bool, _ error) {
	var nodes []base.Address

	switch i, err := c.findMissingBallotsf(ctx, point); {
	case err != nil:
		return nil, false, err
	case len(i) < 1:
		return nil, true, nil
	default:
		nodes = i
	}

	if err := c.voteSuffrageVotingf(ctx, point.Point, nodes); err != nil {
		return nil, false, err
	}

	vp, err := c.withdrawsVoteprooff(ctx, point.Point)

	return vp, false, err
}
