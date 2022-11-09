package isaac

import (
	"fmt"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
)

// FIXME remove withdraw nodes from sync sources

type WithdrawVoteproof interface {
	Withdraws() []base.SuffrageWithdrawOperation
}

var (
	INITVoteproofHint   = hint.MustNewHint("init-voteproof-v0.0.1")
	ACCEPTVoteproofHint = hint.MustNewHint("accept-voteproof-v0.0.1")
)

type baseVoteproof struct {
	finishedAt time.Time
	majority   base.BallotFact
	hint.BaseHinter
	id string
	util.DefaultJSONMarshaled
	sfs       []base.BallotSignFact
	withdraws []base.SuffrageWithdrawOperation
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

func (vp baseVoteproof) IsValid(networkID []byte) error {
	if err := base.IsValidVoteproof(vp, networkID); err != nil {
		return err
	}

	if err := util.CheckIsValiderSlice(networkID, false, vp.withdraws); err != nil {
		return err
	}

	return vp.isValidWithdraws()
}

func (vp baseVoteproof) isValidWithdraws() error {
	if len(vp.withdraws) < 1 {
		return nil
	}

	if vp.Result() == base.VoteResultMajority {
		if _, ok := vp.Majority().(BallotWithdrawFacts); !ok {
			return util.ErrInvalid.Errorf("majority should be BallotWithdrawFacts, not %q", vp.Majority())
		}
	}

	withdrawnodes := make([]string, len(vp.withdraws))

	if _, found := util.CheckSliceDuplicated(vp.withdraws, func(_ interface{}, i int) string {
		node := vp.withdraws[i].Fact().(base.SuffrageWithdrawFact).Node() //nolint:forcetypeassert //...

		withdrawnodes[i] = node.String()

		return node.String()
	}); found {
		return util.ErrInvalid.Errorf("duplicated withdraw node found")
	}

	for i := range vp.sfs {
		if util.InSlice(withdrawnodes, vp.sfs[i].Node().String()) >= 0 {
			return util.ErrInvalid.Errorf("withdraw node voted")
		}
	}

	if wf, ok := vp.majority.(BallotWithdrawFacts); ok {
		switch withdrawfacts := wf.WithdrawFacts(); { //nolint:forcetypeassert //...
		case len(withdrawfacts) != len(vp.withdraws):
			return util.ErrInvalid.Errorf("withdraws not matched")
		default:
			for i := range withdrawfacts {
				if !withdrawfacts[i].Equal(vp.withdraws[i].Fact().Hash()) {
					return util.ErrInvalid.Errorf("unknown withdraws found")
				}
			}
		}
	}

	return nil
}

func (vp baseVoteproof) HashBytes() []byte {
	bs := make([]util.Byter, len(vp.sfs)+4+len(vp.withdraws))
	bs[0] = util.DummyByter(func() []byte {
		if vp.majority == nil {
			return nil
		}

		return vp.majority.Hash().Bytes()
	})
	bs[1] = vp.point
	bs[2] = vp.threshold
	bs[3] = localtime.New(vp.finishedAt)

	for i := range vp.sfs {
		sf := vp.sfs[i]
		bs[4+i] = util.DummyByter(func() []byte {
			return sf.HashBytes()
		})
	}

	for i := range vp.withdraws {
		withdraw := vp.withdraws[i]
		bs[4+len(vp.sfs)+i] = util.DummyByter(func() []byte {
			return withdraw.Hash().Bytes()
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

func (vp baseVoteproof) Withdraws() []base.SuffrageWithdrawOperation {
	return vp.withdraws
}

func (vp *baseVoteproof) SetWithdraws(withdraws []base.SuffrageWithdrawOperation) *baseVoteproof {
	sortWithdraws(withdraws)

	vp.withdraws = withdraws

	return vp
}

func (vp baseVoteproof) Result() base.VoteResult {
	switch {
	case vp.finishedAt.IsZero():
		return base.VoteResultNotYet
	case vp.majority != nil:
		return base.VoteResultMajority
	default:
		return base.VoteResultDraw
	}
}

func (vp baseVoteproof) Threshold() base.Threshold {
	return vp.threshold
}

func (vp *baseVoteproof) SetThreshold(s base.Threshold) *baseVoteproof {
	vp.threshold = s

	return vp
}

func (vp baseVoteproof) SignFacts() []base.BallotSignFact {
	return vp.sfs
}

func (vp *baseVoteproof) SetSignFacts(sfs []base.BallotSignFact) *baseVoteproof {
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
	e := util.ErrInvalid.Errorf("invalid INITVoteproof")

	if err := vp.BaseHinter.IsValid(INITVoteproofHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := vp.baseVoteproof.IsValid(networkID); err != nil {
		return e.Wrap(err)
	}

	if err := base.IsValidINITVoteproof(vp, networkID); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (vp INITVoteproof) BallotMajority() base.INITBallotFact {
	if vp.majority == nil {
		return nil
	}

	return vp.majority.(INITBallotFact) //nolint:forcetypeassert //...
}

func (vp INITVoteproof) BallotSignFacts() []base.INITBallotSignFact {
	vs := make([]base.INITBallotSignFact, len(vp.sfs))

	for i := range vp.sfs {
		vs[i] = vp.sfs[i].(base.INITBallotSignFact) //nolint:forcetypeassert //...
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
	e := util.ErrInvalid.Errorf("invalid ACCEPTVoteproof")

	if err := vp.BaseHinter.IsValid(ACCEPTVoteproofHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := vp.baseVoteproof.IsValid(networkID); err != nil {
		return e.Wrap(err)
	}

	if err := base.IsValidACCEPTVoteproof(vp, networkID); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (vp ACCEPTVoteproof) BallotMajority() base.ACCEPTBallotFact {
	if vp.majority == nil {
		return nil
	}

	return vp.majority.(ACCEPTBallotFact) //nolint:forcetypeassert //...
}

func (vp ACCEPTVoteproof) BallotSignFacts() []base.ACCEPTBallotSignFact {
	vs := make([]base.ACCEPTBallotSignFact, len(vp.sfs))

	for i := range vp.sfs {
		vs[i] = vp.sfs[i].(base.ACCEPTBallotSignFact) //nolint:forcetypeassert //...
	}

	return vs
}
