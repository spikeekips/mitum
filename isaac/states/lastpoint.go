package isaacstates

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
)

type lastPoint struct {
	base.StagePoint
	isMajority        bool
	isSuffrageConfirm bool
}

func newLastPoint( //revive:disable-line:flag-parameter
	point base.StagePoint,
	isMajority, isSuffrageConfirm bool,
) (lastPoint, error) {
	if isSuffrageConfirm && point.Stage() != base.StageINIT {
		return lastPoint{}, errors.Errorf("isSuffrageConfirm should be from INIT stage")
	}

	return lastPoint{
		StagePoint:        point,
		isMajority:        isMajority,
		isSuffrageConfirm: isSuffrageConfirm,
	}, nil
}

func newLastPointFromVoteproof(vp base.Voteproof) (lastPoint, error) {
	var isSuffrageConfirm bool
	if vp.Majority() != nil {
		isSuffrageConfirm = isSuffrageConfirmBallotFact(vp.Majority())
	}

	return newLastPoint(
		vp.Point(),
		vp.Result() == base.VoteResultMajority,
		isSuffrageConfirm,
	)
}

func (l lastPoint) before( //revive:disable-line:flag-parameter
	point base.StagePoint,
	isSuffrageConfirm bool,
) bool {
	if l.IsZero() {
		return true
	}

	if point.Height() != l.Height() {
		return point.Height() > l.Height()
	}

	if point.Point.Equal(l.Point) && point.Stage().Compare(l.Stage()) >= 0 {
		return l.beforeSamePoint(point, isSuffrageConfirm)
	}

	return l.beforeNotSamePoint(point, isSuffrageConfirm)
}

func (l lastPoint) beforeSamePoint( //revive:disable-line:flag-parameter
	point base.StagePoint,
	isSuffrageConfirm bool,
) bool {
	switch {
	case isSuffrageConfirm:
		// NOTE suffrage confirm ballot should be passed under same height and
		// round.
		return !l.isSuffrageConfirm
	case !l.isMajority:
		// NOTE if last is not majority, moves to next round, so higher stage is
		// avoided.
		return false
	case point.Stage() == l.Stage():
		return false
	default:
		return true
	}
}

func (l lastPoint) beforeNotSamePoint( //revive:disable-line:flag-parameter
	point base.StagePoint,
	isSuffrageConfirm bool,
) bool {
	// NOTE by default, lower StagePoint will be ignored.
	switch {
	case point.Compare(l.StagePoint) > 0:
		return true
	case isSuffrageConfirm && !l.isMajority:
		// NOTE if last is not marjoity, suffrage confirms of same height will
		// be passed.
		return true
	default:
		return false
	}
}

func isNewVoteproofbyPoint( // revive:disable-line:flag-parameter
	last lastPoint,
	point base.StagePoint,
	isMajority, isSuffrageConfirm bool,
) bool {
	if last.before(point, isSuffrageConfirm) {
		return true
	}

	if !last.isMajority && isMajority && point.Point.Equal(last.Point) && point.Stage().Compare(last.Stage()) >= 0 {
		return true
	}

	return false
}

func isNewVoteproof(last lastPoint, vp base.Voteproof) bool {
	return isNewVoteproofbyPoint(
		last,
		vp.Point(),
		vp.Result() == base.VoteResultMajority,
		isSuffrageConfirmBallotFact(vp.Majority()),
	)
}

func isNewBallot(last lastPoint, point base.StagePoint, isSuffrageConfirm bool) bool {
	return last.before(point, isSuffrageConfirm)
}

func isSuffrageConfirmBallotFact(fact base.Fact) bool {
	_, ok := fact.(isaac.SuffrageConfirmBallotFact)

	return ok
}
