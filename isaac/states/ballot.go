package isaacstates

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util/logging"
)

type BallotBroadcaster interface {
	Broadcast(base.Ballot) error
	Ballot(_ base.Point, _ base.Stage, isSuffrageConfirm bool) (base.Ballot, bool, error)
}

type DefaultBallotBroadcaster struct {
	*logging.Logging
	local         base.Address
	pool          isaac.BallotPool
	broadcastFunc func(base.Ballot) error
	sync.Mutex
}

func NewDefaultBallotBroadcaster(
	local base.Address,
	pool isaac.BallotPool,
	broadcastFunc func(base.Ballot) error,
) *DefaultBallotBroadcaster {
	return &DefaultBallotBroadcaster{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "ballot-broadcaster")
		}),
		local:         local,
		pool:          pool,
		broadcastFunc: broadcastFunc,
	}
}

func (bb *DefaultBallotBroadcaster) Ballot(
	point base.Point,
	stage base.Stage,
	isSuffrageConfirm bool,
) (base.Ballot, bool, error) {
	return bb.pool.Ballot(point, stage, isSuffrageConfirm)
}

func (bb *DefaultBallotBroadcaster) Broadcast(bl base.Ballot) error {
	l := bb.Log().With().Interface("ballot", bl).Logger()

	if err := bb.set(bl); err != nil {
		l.Error().Err(err).Msg("failed to set ballot")

		return err
	}

	if err := bb.broadcastFunc(bl); err != nil {
		l.Error().Err(err).Msg("failed to broadcast ballot; keep going")

		return err
	}

	l.Debug().Msg("ballot broadcasted")

	return nil
}

func (bb *DefaultBallotBroadcaster) set(bl base.Ballot) error {
	bb.Lock()
	defer bb.Unlock()

	if !bl.SignFact().Node().Equal(bb.local) {
		return nil
	}

	if _, err := bb.pool.SetBallot(bl); err != nil {
		return errors.WithMessage(err, "set ballot to pool")
	}

	return nil
}
