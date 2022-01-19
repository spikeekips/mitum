package base

import (
	"fmt"
	"time"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
)

type BaseVoteproof struct {
	hint.BaseHinter
	finishedAt time.Time
	majority   BallotFact
	point      Point
	result     VoteResult
	stage      Stage
	suffrage   SuffrageInfo
	sfs        []BallotSignedFact
	id         string
}

func NewBaseVoteproof(
	ht hint.Hint,
	point Point,
	stage Stage,
) BaseVoteproof {
	return BaseVoteproof{
		BaseHinter: hint.NewBaseHinter(ht),
		point:      point,
		stage:      stage,
		id:         fmt.Sprintf("%d-%d-%s-%s", point.Height(), point.Round(), stage, util.UUID().String()),
	}
}

func (BaseVoteproof) IsValid([]byte) error {
	// NOTE basically isValidVoteproof() will check

	return nil
}

func (vp BaseVoteproof) HashBytes() []byte {
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
	bs[4] = util.DummyByter(func() []byte {
		if vp.suffrage == nil {
			return nil
		}
		return vp.suffrage.HashBytes()
	})
	bs[5] = localtime.NewTime(vp.finishedAt)

	for i := range vp.sfs {
		v := vp.sfs[i]
		bs[6+i] = util.DummyByter(func() []byte {
			return v.HashBytes()
		})
	}

	return util.ConcatByters(bs...)
}

func (vp BaseVoteproof) FinishedAt() time.Time {
	return vp.finishedAt
}

func (vp *BaseVoteproof) Finish() BaseVoteproof {
	vp.finishedAt = localtime.Now()

	return *vp
}

func (vp BaseVoteproof) Majority() BallotFact {
	return vp.majority
}

func (vp *BaseVoteproof) SetMajority(fact BallotFact) BaseVoteproof {
	vp.majority = fact

	return *vp
}

func (vp BaseVoteproof) Point() Point {
	return vp.point
}

func (vp BaseVoteproof) Result() VoteResult {
	return vp.result
}

func (vp *BaseVoteproof) SetResult(r VoteResult) BaseVoteproof {
	vp.result = r

	return *vp
}

func (vp BaseVoteproof) Stage() Stage {
	return vp.stage
}

func (vp BaseVoteproof) Suffrage() SuffrageInfo {
	return vp.suffrage
}

func (vp *BaseVoteproof) SetSuffrage(s SuffrageInfo) BaseVoteproof {
	vp.suffrage = s

	return *vp
}

func (vp BaseVoteproof) SignedFacts() []BallotSignedFact {
	return vp.sfs
}

func (vp *BaseVoteproof) SetSignedFacts(sfs []BallotSignedFact) BaseVoteproof {
	vp.sfs = sfs

	return *vp
}

func (vp BaseVoteproof) ID() string {
	return vp.id
}

type BaseINITVoteproof struct {
	BaseVoteproof
}

func NewBaseINITVoteproof(ht hint.Hint, point Point) BaseINITVoteproof {
	return BaseINITVoteproof{
		BaseVoteproof: NewBaseVoteproof(ht, point, StageINIT),
	}
}

func (vp BaseINITVoteproof) IsValid(networkID []byte) error {
	if err := IsValidINITVoteproof(vp, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid BaseINITVoteproof")
	}

	return nil
}

func (vp BaseINITVoteproof) BallotMajority() INITBallotFact {
	return vp.majority.(INITBallotFact)
}

func (vp BaseINITVoteproof) BallotSignedFacts() []INITBallotSignedFact {
	vs := make([]INITBallotSignedFact, len(vp.sfs))
	for i := range vp.sfs {
		vs[i] = vp.sfs[i].(INITBallotSignedFact)
	}

	return vs
}

type BaseACCEPTVoteproof struct {
	BaseVoteproof
}

func NewBaseACCEPTVoteproof(ht hint.Hint, point Point) BaseACCEPTVoteproof {
	return BaseACCEPTVoteproof{
		BaseVoteproof: NewBaseVoteproof(ht, point, StageACCEPT),
	}
}

func (vp BaseACCEPTVoteproof) IsValid(networkID []byte) error {
	if err := IsValidACCEPTVoteproof(vp, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid BaseACCEPTVoteproof")
	}

	return nil
}

func (vp BaseACCEPTVoteproof) BallotMajority() ACCEPTBallotFact {
	return vp.majority.(ACCEPTBallotFact)
}

func (vp BaseACCEPTVoteproof) BallotSignedFacts() []ACCEPTBallotSignedFact {
	vs := make([]ACCEPTBallotSignedFact, len(vp.sfs))
	for i := range vp.sfs {
		vs[i] = vp.sfs[i].(ACCEPTBallotSignedFact)
	}

	return vs
}
