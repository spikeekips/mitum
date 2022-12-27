package isaacstates

import (
	"context"
	"math"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
)

type BallotStuckResolver interface {
	NewPoint(context.Context, base.StagePoint) bool
	Voteproof() <-chan base.Voteproof
	Clean()
	Cancel(base.StagePoint)
}

type DefaultBallotStuckResolver struct {
	*logging.Logging
	cancelf                *util.Locked[func()]
	findMissingBallotsf    func(context.Context, base.StagePoint, bool) ([]base.Address, bool, error)
	requestMissingBallotsf func(context.Context, base.StagePoint, []base.Address) error
	voteSuffrageVotingf    func(context.Context, base.Point, []base.Address) (base.Voteproof, error)
	vpch                   chan base.Voteproof
	point                  base.StagePoint
	wait                   time.Duration
	interval               time.Duration
}

func NewDefaultBallotStuckResolver(
	wait,
	interval time.Duration,
	findMissingBallotsf func(context.Context, base.StagePoint, bool) ([]base.Address, bool, error),
	requestMissingBallotsf func(context.Context, base.StagePoint, []base.Address) error,
	voteSuffrageVotingf func(context.Context, base.Point, []base.Address) (base.Voteproof, error),
) *DefaultBallotStuckResolver {
	return &DefaultBallotStuckResolver{
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
		vpch:                   make(chan base.Voteproof, math.MaxUint16),
	}
}

func (c *DefaultBallotStuckResolver) NewPoint(ctx context.Context, point base.StagePoint) bool {
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

func (c *DefaultBallotStuckResolver) Voteproof() <-chan base.Voteproof {
	return c.vpch
}

func (c *DefaultBallotStuckResolver) Clean() {
	_ = c.cancelf.Empty(func(cancel func(), _ bool) error {
		c.point = base.ZeroStagePoint

		if cancel != nil {
			cancel()
		}

		return nil
	})
}

func (c *DefaultBallotStuckResolver) Cancel(point base.StagePoint) {
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

func (c *DefaultBallotStuckResolver) start(ctx context.Context, point base.StagePoint) error {
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
			switch nomore, ok, err := c.gatherMissingBallots(ctx, point); {
			case err != nil:
				return errors.WithMessage(err, "failed to find missing ballot nodes")
			case nomore:
				l.Debug().Msg("no more to request missing ballots; cancel")

				return nil
			case !ok:
				tick--

				continue
			}

			if tick < 3 { //nolint:gomnd //...
				continue
			}

			ll.Debug().Msg("trying suffrage voting")

			switch vp, nomore, err := c.suffrageVoting(ctx, point); {
			case err != nil:
				return errors.WithMessage(err, "failed suffrage voting")
			case nomore:
				l.Debug().Msg("no more suffrage voting; cancel")

				return nil
			case vp == nil:
				continue
			default:
				ll.Debug().Msg("voteproof for next round")

				go func() {
					c.vpch <- vp
				}()
			}

			return nil
		}
	}
}

func (c *DefaultBallotStuckResolver) gatherMissingBallots(
	ctx context.Context,
	point base.StagePoint,
) (nomore, ok bool, _ error) {
	nodes, ok, err := c.findMissingBallotsf(ctx, point, false)

	c.Log().Debug().Interface("nodes", nodes).Bool("ok", ok).Msg("missing nodes checked")

	switch {
	case err != nil:
		return false, false, err
	case !ok:
		return false, false, nil
	case len(nodes) < 1:
		return true, true, nil
	}

	return false, true, c.requestMissingBallotsf(ctx, point, nodes)
}

func (c *DefaultBallotStuckResolver) suffrageVoting(
	ctx context.Context,
	point base.StagePoint,
) (_ base.Voteproof, nomore bool, _ error) {
	var nodes []base.Address

	switch i, ok, err := c.findMissingBallotsf(ctx, point, true); {
	case err != nil:
		return nil, false, err
	case !ok:
		return nil, false, nil
	case len(i) < 1:
		return nil, true, nil
	default:
		nodes = i
	}

	switch vp, err := c.voteSuffrageVotingf(ctx, point.Point, nodes); {
	case err != nil:
		return nil, false, err
	default:
		return vp, false, nil
	}
}

func FindMissingBallotsFromBallotboxFunc(
	local base.Address,
	params *isaac.LocalParams,
	getSuffragef isaac.GetSuffrageByBlockHeight,
	ballotbox *Ballotbox,
) func(context.Context, base.StagePoint, bool) ([]base.Address, bool, error) {
	return func(_ context.Context, point base.StagePoint, checkalone bool) ([]base.Address, bool, error) {
		_ = ballotbox.Count(params.Threshold())

		// NOTE check local is in **last** suffrage, if not, return empty nodes
		// and true ok.
		var suf base.Suffrage

		switch i, found, err := getSuffragef(point.Height()); {
		case err != nil:
			return nil, false, err
		case !found:
			return nil, false, nil
		case !i.Exists(local):
			return nil, true, nil
		default:
			suf = i
		}

		// NOTE if nodes are entire suffrage nodes except local, local seems to
		// be out of network. it returns empty nodes and false ok.
		switch nodes, ok, err := ballotbox.missingNodes(point, params.Threshold()); {
		case err != nil:
			return nil, false, err
		case !ok:
			return nil, false, nil
		case !checkalone:
			return nodes, true, nil
		case suf.Len()-1 == len(nodes): // NOTE all nodes missing except local
			return nil, false, nil
		default:
			return nodes, true, nil
		}
	}
}

func RequestMissingBallots(
	localci quicstream.UDPConnInfo,
	broadcastf func(string, []byte, chan struct{}) error,
) func(context.Context, base.StagePoint, []base.Address) error {
	return func(ctx context.Context, point base.StagePoint, nodes []base.Address) error {
		m := NewMissingBallotsRequestsMessage(point, nodes, localci)

		switch b, err := util.MarshalJSON(m); {
		case err != nil:
			return errors.WithMessage(err, "failed to marshal MissingBallotsRequestsMessage")
		default:
			if err := broadcastf(util.UUID().String(), b, nil); err != nil {
				return errors.WithMessage(err, "failed to broadcast MissingBallotsRequestsMessage")
			}

			return nil
		}
	}
}

func VoteSuffrageVotingFunc(
	local base.LocalNode,
	params *isaac.LocalParams,
	ballotbox *Ballotbox,
	sv *isaac.SuffrageVoting,
	getSuffragef isaac.GetSuffrageByBlockHeight,
) func(context.Context, base.StagePoint, []base.Address) (base.Voteproof, error) {
	return func(ctx context.Context, point base.StagePoint, nodes []base.Address) (base.Voteproof, error) {
		_ = ballotbox.Count(params.Threshold())

		var suf base.Suffrage

		switch i, found, err := getSuffragef(point.Height()); {
		case err != nil:
			return nil, err
		case !found:
			return nil, nil
		default:
			suf = i
		}

		for i := range nodes {
			node := nodes[i]

			fact := isaac.NewSuffrageWithdrawFact(node, point.Height(), point.Height()+1, "no ballot")

			op := isaac.NewSuffrageWithdrawOperation(fact)

			if err := op.NodeSign(local.Privatekey(), params.NetworkID(), local.Address()); err != nil {
				return nil, errors.WithMessage(err, "failed to node sign SuffrageWithdrawOperation")
			}

			if _, err := sv.Vote(op); err != nil {
				return nil, errors.WithMessage(err, "failed to vote SuffrageWithdrawOperation")
			}
		}

		var withdraws []base.SuffrageWithdrawOperation

		switch i, err := sv.Find(ctx, point.Height(), suf); {
		case err != nil:
			return nil, err
		default:
			withdraws = util.FilterSlice(i, func(j base.SuffrageWithdrawOperation) bool {
				return util.InSliceFunc(nodes, func(k base.Address) bool {
					return j.WithdrawFact().Node().Equal(k)
				}) >= 0
			})
		}

		switch {
		case len(withdraws) < 1:
			return nil, nil
		case len(nodes)+len(withdraws) < suf.Len():
			return nil, nil
		}

		return ballotbox.voteproofWithWithdraws(point, params.Threshold(), withdraws)
	}
}

var MissingBallotsRequestsMessageHint = hint.MustNewHint("missing-ballots-request-message-v0.0.1")

type MissingBallotsRequestMessage struct {
	nodes []base.Address
	ci    quicstream.UDPConnInfo
	hint.BaseHinter
	point base.StagePoint
}

func NewMissingBallotsRequestsMessage(
	point base.StagePoint,
	nodes []base.Address,
	ci quicstream.UDPConnInfo,
) MissingBallotsRequestMessage {
	return MissingBallotsRequestMessage{
		BaseHinter: hint.NewBaseHinter(MissingBallotsRequestsMessageHint),
		point:      point,
		nodes:      nodes,
		ci:         ci,
	}
}

func (m MissingBallotsRequestMessage) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid MissingBallotsRequestsMessage")

	if err := m.BaseHinter.IsValid(MissingBallotsRequestsMessageHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if len(m.nodes) < 1 {
		return e.Errorf("empty nodes")
	}

	if err := m.point.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := m.ci.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (m MissingBallotsRequestMessage) ConnInfo() quicstream.UDPConnInfo {
	return m.ci
}

func (m MissingBallotsRequestMessage) Point() base.StagePoint {
	return m.point
}

func (m MissingBallotsRequestMessage) Nodes() []base.Address {
	return m.nodes
}

type missingBallotsRequestsMessageJSONMarshaler struct {
	Nodes []base.Address         `json:"nodes"`
	CI    quicstream.UDPConnInfo `json:"conn_info"` //nolint:tagliatelle //...
	hint.BaseHinter
	Point base.StagePoint `json:"point"`
}

func (m MissingBallotsRequestMessage) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(missingBallotsRequestsMessageJSONMarshaler{
		BaseHinter: m.BaseHinter,
		Point:      m.point,
		Nodes:      m.nodes,
		CI:         m.ci,
	})
}

type missingBallotsRequestsMessageJSONUnmarshaler struct {
	Nodes []string               `json:"nodes"`
	CI    quicstream.UDPConnInfo `json:"conn_info"` //nolint:tagliatelle //...
	hint.BaseHinter
	Point base.StagePoint `json:"point"`
}

func (m *MissingBallotsRequestMessage) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode MissingBallotsRequestsMessage")

	var u missingBallotsRequestsMessageJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	m.point = u.Point
	m.nodes = make([]base.Address, len(u.Nodes))

	for i := range u.Nodes {
		switch j, err := base.DecodeAddress(u.Nodes[i], enc); {
		case err != nil:
			return e(err, "failed to decode node")
		default:
			m.nodes[i] = j
		}
	}

	m.ci = u.CI

	return nil
}
