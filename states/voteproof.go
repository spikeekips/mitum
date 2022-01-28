package states

import (
	"fmt"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
)

var (
	INITVoteproofHint   = hint.MustNewHint("init-voteproof-v0.0.1")
	ACCEPTVoteproofHint = hint.MustNewHint("accept-voteproof-v0.0.1")
)

type baseVoteproof struct {
	hint.BaseHinter
	finishedAt time.Time
	majority   base.BallotFact
	point      base.Point
	result     base.VoteResult
	stage      base.Stage
	threshold  base.Threshold
	sfs        []base.BallotSignedFact
	id         string
}

func newBaseVoteproof(
	ht hint.Hint,
	point base.Point,
	stage base.Stage,
) baseVoteproof {
	return baseVoteproof{
		BaseHinter: hint.NewBaseHinter(ht),
		point:      point,
		stage:      stage,
		id:         fmt.Sprintf("%d-%d-%s-%s", point.Height(), point.Round(), stage, util.UUID().String()),
	}
}

func (baseVoteproof) IsValid([]byte) error {
	// NOTE basically isValidVoteproof() will check

	return nil
}

func (vp baseVoteproof) HashBytes() []byte {
	bs := make([]util.Byter, len(vp.sfs)+6)
	bs[0] = util.DummyByter(func() []byte {
		if vp.majority == nil {
			return nil
		}

		return vp.majority.Hash().Bytes()
	})
	bs[1] = vp.point
	bs[2] = vp.result
	bs[3] = vp.stage
	bs[4] = vp.threshold
	bs[5] = localtime.NewTime(vp.finishedAt)

	for i := range vp.sfs {
		v := vp.sfs[i]
		bs[6+i] = util.DummyByter(func() []byte {
			return v.HashBytes()
		})
	}

	return util.ConcatByters(bs...)
}

func (vp baseVoteproof) FinishedAt() time.Time {
	return vp.finishedAt
}

func (vp *baseVoteproof) Finish() baseVoteproof {
	vp.finishedAt = localtime.Now()

	return *vp
}

func (vp baseVoteproof) Majority() base.BallotFact {
	return vp.majority
}

func (vp *baseVoteproof) SetMajority(fact base.BallotFact) baseVoteproof {
	vp.majority = fact

	return *vp
}

func (vp baseVoteproof) Point() base.Point {
	return vp.point
}

func (vp baseVoteproof) Result() base.VoteResult {
	return vp.result
}

func (vp *baseVoteproof) SetResult(r base.VoteResult) baseVoteproof {
	vp.result = r

	return *vp
}

func (vp baseVoteproof) Stage() base.Stage {
	return vp.stage
}

func (vp baseVoteproof) Threshold() base.Threshold {
	return vp.threshold
}

func (vp *baseVoteproof) SetThreshold(s base.Threshold) baseVoteproof {
	vp.threshold = s

	return *vp
}

func (vp baseVoteproof) SignedFacts() []base.BallotSignedFact {
	return vp.sfs
}

func (vp *baseVoteproof) SetSignedFacts(sfs []base.BallotSignedFact) baseVoteproof {
	vp.sfs = sfs

	return *vp
}

func (vp baseVoteproof) ID() string {
	return vp.id
}

type INITVoteproof struct {
	baseVoteproof
}

func NewINITVoteproof(point base.Point) INITVoteproof {
	return INITVoteproof{
		baseVoteproof: newBaseVoteproof(INITVoteproofHint, point, base.StageINIT),
	}
}

func (vp INITVoteproof) IsValid(networkID []byte) error {
	if err := vp.BaseHinter.IsValid(INITVoteproofHint.Type().Bytes()); err != nil {
		return util.InvalidError.Wrapf(err, "invalid INITVoteproof")
	}

	if err := base.IsValidINITVoteproof(vp, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid INITVoteproof")
	}

	return nil
}

func (vp INITVoteproof) BallotMajority() base.INITBallotFact {
	return vp.majority.(base.INITBallotFact)
}

func (vp INITVoteproof) BallotSignedFacts() []base.INITBallotSignedFact {
	vs := make([]base.INITBallotSignedFact, len(vp.sfs))
	for i := range vp.sfs {
		vs[i] = vp.sfs[i].(base.INITBallotSignedFact)
	}

	return vs
}

type ACCEPTVoteproof struct {
	baseVoteproof
}

func NewACCEPTVoteproof(point base.Point) ACCEPTVoteproof {
	return ACCEPTVoteproof{
		baseVoteproof: newBaseVoteproof(ACCEPTVoteproofHint, point, base.StageACCEPT),
	}
}

func (vp ACCEPTVoteproof) IsValid(networkID []byte) error {
	if err := vp.BaseHinter.IsValid(ACCEPTVoteproofHint.Type().Bytes()); err != nil {
		return util.InvalidError.Wrapf(err, "invalid ACCEPTVoteproof")
	}

	if err := base.IsValidACCEPTVoteproof(vp, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid ACCEPTVoteproof")
	}

	return nil
}

func (vp ACCEPTVoteproof) BallotMajority() base.ACCEPTBallotFact {
	return vp.majority.(base.ACCEPTBallotFact)
}

func (vp ACCEPTVoteproof) BallotSignedFacts() []base.ACCEPTBallotSignedFact {
	vs := make([]base.ACCEPTBallotSignedFact, len(vp.sfs))
	for i := range vp.sfs {
		vs[i] = vp.sfs[i].(base.ACCEPTBallotSignedFact)
	}

	return vs
}
