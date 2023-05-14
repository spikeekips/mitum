//go:build test
// +build test

package isaacstates

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type DummyBallotPool struct {
	m *util.ShardedMap[string, base.Ballot]
}

func NewDummyBallotPool() *DummyBallotPool {
	m, _ := util.NewShardedMap[string, base.Ballot](1 << 4)

	return &DummyBallotPool{m: m}
}

func (d *DummyBallotPool) Ballot(point base.Point, stage base.Stage, isSuffrageConfirm bool) (base.Ballot, bool, error) {
	bl, found := d.m.Value(d.key(point, stage, isSuffrageConfirm))

	return bl, found, nil
}

func (d *DummyBallotPool) SetBallot(bl base.Ballot) (bool, error) {
	key := d.key(bl.Point().Point, bl.Point().Stage(), isaac.IsSuffrageConfirmBallotFact(bl.SignFact().Fact()))

	_, err := d.m.Set(key, func(i base.Ballot, found bool) (base.Ballot, error) {
		if found {
			return nil, util.ErrFound.WithStack()
		}

		return bl, nil
	})

	return err == nil, nil
}

func (d *DummyBallotPool) key(point base.Point, stage base.Stage, isSuffrageConfirm bool) string {
	k := base.NewStagePoint(point, stage).String()
	if isSuffrageConfirm {
		k += "s"
	}

	return k
}

func NewDummyBallotBroadcaster(
	local base.Address,
	broadcastf func(base.Ballot) error,
) *DefaultBallotBroadcaster {
	if broadcastf == nil {
		broadcastf = func(base.Ballot) error { return nil }
	}

	return NewDefaultBallotBroadcaster(local, NewDummyBallotPool(), broadcastf)
}
