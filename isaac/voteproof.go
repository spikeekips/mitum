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

var (
	INITVoteproofHint           = hint.MustNewHint("init-voteproof-v0.0.1")
	INITWithdrawVoteproofHint   = hint.MustNewHint("init-withdraw-voteproof-v0.0.1")
	INITStuckVoteproofHint      = hint.MustNewHint("init-stuck-voteproof-v0.0.1")
	ACCEPTVoteproofHint         = hint.MustNewHint("accept-voteproof-v0.0.1")
	ACCEPTWithdrawVoteproofHint = hint.MustNewHint("accept-withdraw-voteproof-v0.0.1")
	ACCEPTStuckVoteproofHint    = hint.MustNewHint("accept-stuck-voteproof-v0.0.1")
)

type baseVoteproof struct {
	finishedAt time.Time
	majority   base.BallotFact
	hint.BaseHinter
	id string
	util.DefaultJSONMarshaled
	sfs       []base.BallotSignFact
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
	return base.IsValidVoteproof(vp, networkID)
}

func (vp baseVoteproof) HashBytes() []byte {
	bs := make([]util.Byter, len(vp.sfs)+4)
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

	return util.ConcatByters(bs...)
}

func (vp baseVoteproof) FinishedAt() time.Time {
	return vp.finishedAt
}

func (vp *baseVoteproof) Finish() baseVoteproof {
	vp.finishedAt = localtime.Now().UTC()

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

	if err := vp.isValid(networkID); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (vp INITVoteproof) isValid(networkID []byte) error {
	if err := vp.baseVoteproof.IsValid(networkID); err != nil {
		return err
	}

	return base.IsValidINITVoteproof(vp, networkID)
}

func (vp INITVoteproof) BallotMajority() base.INITBallotFact {
	if vp.majority == nil {
		return nil
	}

	return vp.majority.(base.INITBallotFact) //nolint:forcetypeassert //...
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

	if err := vp.isValid(networkID); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (vp ACCEPTVoteproof) isValid(networkID []byte) error {
	if err := vp.baseVoteproof.IsValid(networkID); err != nil {
		return err
	}

	return base.IsValidACCEPTVoteproof(vp, networkID)
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

type INITWithdrawVoteproof struct {
	baseWithdrawVoteproof
	INITVoteproof
}

func NewINITWithdrawVoteproof(point base.Point) INITWithdrawVoteproof {
	vp := INITWithdrawVoteproof{
		INITVoteproof: NewINITVoteproof(point),
	}

	vp.BaseHinter = vp.SetHint(INITWithdrawVoteproofHint).(hint.BaseHinter) //nolint:forcetypeassert //...

	return vp
}

func (vp INITWithdrawVoteproof) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid INITWithdrawVoteproof")

	if err := vp.BaseHinter.IsValid(INITWithdrawVoteproofHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := vp.INITVoteproof.isValid(networkID); err != nil {
		return e.Wrap(err)
	}

	if err := vp.baseWithdrawVoteproof.isValid(networkID, vp.baseVoteproof); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (INITWithdrawVoteproof) IsWithdrawVoteproof() bool {
	return true
}

func (vp INITWithdrawVoteproof) HashBytes() []byte {
	return util.ConcatBytesSlice(vp.baseVoteproof.HashBytes(), vp.baseWithdrawVoteproof.hashBytes())
}

type ACCEPTWithdrawVoteproof struct {
	baseWithdrawVoteproof
	ACCEPTVoteproof
}

func NewACCEPTWithdrawVoteproof(point base.Point) ACCEPTWithdrawVoteproof {
	vp := ACCEPTWithdrawVoteproof{
		ACCEPTVoteproof: NewACCEPTVoteproof(point),
	}

	vp.BaseHinter = vp.SetHint(ACCEPTWithdrawVoteproofHint).(hint.BaseHinter) //nolint:forcetypeassert //...

	return vp
}

func (ACCEPTWithdrawVoteproof) IsWithdrawVoteproof() bool {
	return true
}

func (vp ACCEPTWithdrawVoteproof) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid ACCEPTWithdrawVoteproof")

	if err := vp.BaseHinter.IsValid(ACCEPTWithdrawVoteproofHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := vp.ACCEPTVoteproof.isValid(networkID); err != nil {
		return e.Wrap(err)
	}

	if err := vp.baseWithdrawVoteproof.isValid(networkID, vp.baseVoteproof); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (vp ACCEPTWithdrawVoteproof) HashBytes() []byte {
	return util.ConcatBytesSlice(vp.baseVoteproof.HashBytes(), vp.baseWithdrawVoteproof.hashBytes())
}

type INITStuckVoteproof struct {
	baseStuckVoteproof
	INITVoteproof
}

func NewINITStuckVoteproof(point base.Point) INITStuckVoteproof {
	vp := INITStuckVoteproof{
		INITVoteproof: NewINITVoteproof(point),
	}

	vp.BaseHinter = vp.SetHint(INITStuckVoteproofHint).(hint.BaseHinter) //nolint:forcetypeassert //...

	return vp
}

func (vp INITStuckVoteproof) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid INITStuckVoteproof")

	if err := vp.BaseHinter.IsValid(INITStuckVoteproofHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if vp.threshold != base.MaxThreshold {
		return e.Errorf("wrong threshold for stuck voteproof; should be 100.0, not %v", vp.threshold)
	}

	if err := vp.INITVoteproof.isValid(networkID); err != nil {
		return e.Wrap(err)
	}

	if err := vp.baseStuckVoteproof.isValid(networkID, vp.baseVoteproof); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (vp *INITStuckVoteproof) Finish() *INITStuckVoteproof {
	_ = vp.baseStuckVoteproof.finish(&vp.baseVoteproof)

	return vp
}

type ACCEPTStuckVoteproof struct {
	baseStuckVoteproof
	ACCEPTVoteproof
}

func NewACCEPTStuckVoteproof(point base.Point) ACCEPTStuckVoteproof {
	vp := ACCEPTStuckVoteproof{
		ACCEPTVoteproof: NewACCEPTVoteproof(point),
	}

	vp.BaseHinter = vp.SetHint(ACCEPTStuckVoteproofHint).(hint.BaseHinter) //nolint:forcetypeassert //...

	return vp
}

func (vp ACCEPTStuckVoteproof) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid ACCEPTStuckVoteproof")

	if err := vp.BaseHinter.IsValid(ACCEPTStuckVoteproofHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if vp.threshold != base.MaxThreshold {
		return e.Errorf("wrong threshold for stuck voteproof; should be 100.0, not %v", vp.threshold)
	}

	if err := vp.ACCEPTVoteproof.isValid(networkID); err != nil {
		return e.Wrap(err)
	}

	if err := vp.baseStuckVoteproof.isValid(networkID, vp.baseVoteproof); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (vp *ACCEPTStuckVoteproof) Finish() *ACCEPTStuckVoteproof {
	_ = vp.baseStuckVoteproof.finish(&vp.baseVoteproof)

	return vp
}

type baseWithdrawVoteproof struct {
	withdraws []base.SuffrageWithdrawOperation
}

func (vp baseWithdrawVoteproof) isValid(networkID []byte, ovp baseVoteproof) error {
	if err := isValidithdrawVoteproof(networkID, vp.withdraws, ovp); err != nil {
		return util.ErrInvalid.Wrap(err)
	}

	if wf, ok := ovp.majority.(WithdrawBallotFact); ok {
		withdrawfacts := wf.WithdrawFacts()
		if len(withdrawfacts) > 0 {
			switch { //nolint:forcetypeassert //...
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
	}

	return nil
}

func (vp baseWithdrawVoteproof) Withdraws() []base.SuffrageWithdrawOperation {
	return vp.withdraws
}

func (vp *baseWithdrawVoteproof) SetWithdraws(withdraws []base.SuffrageWithdrawOperation) *baseWithdrawVoteproof {
	sortWithdraws(withdraws)

	vp.withdraws = withdraws

	return vp
}

func (vp baseWithdrawVoteproof) hashBytes() []byte {
	bs := make([]util.Byter, len(vp.withdraws))

	for i := range vp.withdraws {
		withdraw := vp.withdraws[i]
		bs[i] = util.DummyByter(func() []byte {
			return withdraw.Hash().Bytes()
		})
	}

	return util.ConcatByters(bs...)
}

type baseStuckVoteproof struct {
	baseWithdrawVoteproof
}

func (baseStuckVoteproof) IsStuckVoteproof() bool {
	return true
}

func (vp baseStuckVoteproof) isValid(networkID []byte, ovp baseVoteproof) error {
	if len(vp.withdraws) < 1 {
		return util.ErrInvalid.Errorf("empty withdraws")
	}

	return isValidithdrawVoteproof(networkID, vp.withdraws, ovp)
}

func isValidithdrawVoteproof(networkID []byte, withdraws []base.SuffrageWithdrawOperation, ovp baseVoteproof) error {
	if len(withdraws) < 1 {
		return util.ErrInvalid.Errorf("empty withdraws")
	}

	if err := util.CheckIsValiderSlice(networkID, false, withdraws); err != nil {
		return err
	}

	withdrawnodes := make([]string, len(withdraws))

	var n int

	if _, found := util.IsDuplicatedSlice(withdraws, func(i base.SuffrageWithdrawOperation) (bool, string) {
		node := i.Fact().(base.SuffrageWithdrawFact).Node() //nolint:forcetypeassert //...

		withdrawnodes[n] = node.String()
		n++

		return true, node.String()
	}); found {
		return util.ErrInvalid.Errorf("duplicated withdraw node found")
	}

	for i := range ovp.sfs {
		if util.InSlice(withdrawnodes, ovp.sfs[i].Node().String()) >= 0 {
			return util.ErrInvalid.Errorf("withdraw node voted")
		}
	}

	return nil
}

func (vp *baseStuckVoteproof) finish(bvp *baseVoteproof) *baseStuckVoteproof {
	_ = bvp.SetMajority(nil).
		SetThreshold(base.MaxThreshold).
		Finish()

	return vp
}
