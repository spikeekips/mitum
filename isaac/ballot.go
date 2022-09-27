package isaac

import (
	"sort"
	"strings"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	INITBallotHint   = hint.MustNewHint("init-ballot-v0.0.1")
	ACCEPTBallotHint = hint.MustNewHint("accept-ballot-v0.0.1")
)

type baseBallot struct {
	vp       base.Voteproof
	signFact base.BallotSignFact
	util.DefaultJSONMarshaled
	hint.BaseHinter
}

func newBaseBallot(ht hint.Hint, vp base.Voteproof, signFact base.BallotSignFact) baseBallot {
	return baseBallot{
		BaseHinter: hint.NewBaseHinter(ht),
		vp:         vp,
		signFact:   signFact,
	}
}

func (bl baseBallot) Point() base.StagePoint {
	bf := bl.ballotFact()
	if bf == nil {
		return base.ZeroStagePoint
	}

	return bf.Point()
}

func (bl baseBallot) SignFact() base.BallotSignFact {
	return bl.signFact
}

func (bl baseBallot) Voteproof() base.Voteproof {
	return bl.vp
}

func (bl baseBallot) IsValid(networkID []byte) error {
	if err := base.IsValidBallot(bl, networkID); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid baseBallot")
	}

	return nil
}

func (bl baseBallot) HashBytes() []byte {
	return util.ConcatByters(
		bl.Hint(),
		util.DummyByter(func() []byte {
			if bl.vp == nil {
				return nil
			}

			return bl.vp.HashBytes()
		}),
		util.DummyByter(func() []byte {
			if bl.signFact == nil {
				return nil
			}

			return bl.signFact.HashBytes()
		}),
	)
}

func (bl baseBallot) ballotFact() base.BallotFact {
	if bl.signFact == nil || bl.signFact.Fact() == nil {
		return nil
	}

	bf, ok := bl.signFact.Fact().(base.BallotFact)
	if !ok {
		return nil
	}

	return bf
}

func (bl *baseBallot) Sign(priv base.Privatekey, networkID base.NetworkID) error {
	e := util.StringErrorFunc("failed to sign ballot")

	signer, ok := bl.signFact.(base.Signer)
	if !ok {
		return e(nil, "invalid sign fact; missing Sign()")
	}

	if err := signer.Sign(priv, networkID); err != nil {
		return e(err, "")
	}

	bl.signFact = signer.(base.BallotSignFact) //nolint:forcetypeassert //...

	return nil
}

type INITBallot struct {
	withdraws []SuffrageWithdraw
	baseBallot
}

func NewINITBallot(
	vp base.Voteproof,
	signfact INITBallotSignFact,
	withdraws []SuffrageWithdraw,
) INITBallot {
	if len(withdraws) > 0 {
		sort.Slice(withdraws, func(i, j int) bool {
			return strings.Compare(withdraws[i].Fact().Hash().String(), withdraws[j].Fact().Hash().String()) < 0
		})
	}

	return INITBallot{
		baseBallot: newBaseBallot(INITBallotHint, vp, signfact),
		withdraws:  withdraws,
	}
}

func (bl INITBallot) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid INITBallot")

	if err := bl.BaseHinter.IsValid(INITBallotHint.Type().Bytes()); err != nil {
		return e.Wrapf(err, "invalid INITBallot")
	}

	if err := base.IsValidINITBallot(bl, networkID); err != nil {
		return e.Wrapf(err, "invalid INITBallot")
	}

	switch fact, ok := bl.signFact.Fact().(INITBallotFact); {
	case !ok:
		return e.Errorf("expected isaac.INITBallotFact, not %T", bl.signFact)
	case len(fact.withdrawfacts) != len(bl.withdraws):
		return e.Errorf("number of withdraws not matched")
	case len(bl.withdraws) < 1:
	default:
		if err := util.CheckIsValiderSlice(networkID, false, bl.withdraws); err != nil {
			return e.Wrap(err)
		}

		for i := range fact.withdrawfacts {
			if !fact.withdrawfacts[i].Hash().Equal(bl.withdraws[i].Fact().Hash()) {
				return e.Errorf("withdraw fact hash not matched")
			}
		}
	}

	return nil
}

func (bl INITBallot) BallotSignFact() base.INITBallotSignFact {
	if bl.signFact == nil {
		return nil
	}

	return bl.signFact.(base.INITBallotSignFact) //nolint:forcetypeassert //...
}

func (bl INITBallot) Withdraws() []SuffrageWithdraw {
	return bl.withdraws
}

type ACCEPTBallot struct {
	baseBallot
}

func NewACCEPTBallot(
	ivp base.INITVoteproof,
	signfact ACCEPTBallotSignFact,
) ACCEPTBallot {
	return ACCEPTBallot{
		baseBallot: newBaseBallot(ACCEPTBallotHint, ivp, signfact),
	}
}

func (bl ACCEPTBallot) IsValid(networkID []byte) error {
	if err := bl.BaseHinter.IsValid(ACCEPTBallotHint.Type().Bytes()); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid ACCEPTBallot")
	}

	if err := base.IsValidACCEPTBallot(bl, networkID); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid ACCEPTBallot")
	}

	return nil
}

func (bl ACCEPTBallot) BallotSignFact() base.ACCEPTBallotSignFact {
	if bl.signFact == nil {
		return nil
	}

	return bl.signFact.(base.ACCEPTBallotSignFact) //nolint:forcetypeassert //...
}
