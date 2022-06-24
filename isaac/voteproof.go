package isaac

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
	finishedAt time.Time
	majority   base.BallotFact
	hint.BaseHinter
	result base.VoteResult
	id     string
	util.DefaultJSONMarshaled
	sfs       []base.BallotSignedFact
	point     base.StagePoint
	threshold base.Threshold
}

func newBaseVoteproof(
	ht hint.Hint,
	point base.Point,
	stage base.Stage,
) baseVoteproof {
	return baseVoteproof{
		BaseHinter: hint.NewBaseHinter(ht),
		point:      base.NewStagePoint(point, stage),
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
	bs[4] = vp.threshold
	bs[5] = localtime.New(vp.finishedAt)

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
	vp.finishedAt = localtime.UTCNow()

	return *vp
}

func (vp baseVoteproof) Majority() base.BallotFact {
	return vp.majority
}

func (vp *baseVoteproof) SetMajority(fact base.BallotFact) *baseVoteproof {
	vp.majority = fact

	return vp
}

func (vp baseVoteproof) Point() base.StagePoint {
	return vp.point
}

func (vp *baseVoteproof) SetPoint(p base.StagePoint) *baseVoteproof {
	vp.point = p

	return vp
}

func (vp baseVoteproof) Result() base.VoteResult {
	return vp.result
}

func (vp *baseVoteproof) SetResult(r base.VoteResult) *baseVoteproof {
	vp.result = r

	if r == base.VoteResultDraw {
		vp.majority = nil
	}

	return vp
}

func (vp baseVoteproof) Threshold() base.Threshold {
	return vp.threshold
}

func (vp *baseVoteproof) SetThreshold(s base.Threshold) *baseVoteproof {
	vp.threshold = s

	return vp
}

func (vp baseVoteproof) SignedFacts() []base.BallotSignedFact {
	return vp.sfs
}

func (vp *baseVoteproof) SetSignedFacts(sfs []base.BallotSignedFact) *baseVoteproof {
	vp.sfs = sfs

	return vp
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
		return util.ErrInvalid.Wrapf(err, "invalid INITVoteproof")
	}

	if err := base.IsValidINITVoteproof(vp, networkID); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid INITVoteproof")
	}

	return nil
}

func (vp INITVoteproof) BallotMajority() base.INITBallotFact {
	if vp.majority == nil {
		return nil
	}

	return vp.majority.(base.INITBallotFact) //nolint:forcetypeassert //...
}

func (vp INITVoteproof) BallotSignedFacts() []base.INITBallotSignedFact {
	vs := make([]base.INITBallotSignedFact, len(vp.sfs))

	for i := range vp.sfs {
		vs[i] = vp.sfs[i].(base.INITBallotSignedFact) //nolint:forcetypeassert //...
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
		return util.ErrInvalid.Wrapf(err, "invalid ACCEPTVoteproof")
	}

	if err := base.IsValidACCEPTVoteproof(vp, networkID); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid ACCEPTVoteproof")
	}

	return nil
}

func (vp ACCEPTVoteproof) BallotMajority() base.ACCEPTBallotFact {
	if vp.majority == nil {
		return nil
	}

	return vp.majority.(base.ACCEPTBallotFact) //nolint:forcetypeassert //...
}

func (vp ACCEPTVoteproof) BallotSignedFacts() []base.ACCEPTBallotSignedFact {
	vs := make([]base.ACCEPTBallotSignedFact, len(vp.sfs))

	for i := range vp.sfs {
		vs[i] = vp.sfs[i].(base.ACCEPTBallotSignedFact) //nolint:forcetypeassert //...
	}

	return vs
}
