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

type ballotWithdraws interface {
	Withdraws() []SuffrageWithdrawOperation
}

type baseBallot struct {
	withdraws []SuffrageWithdrawOperation
	vp        base.Voteproof
	signFact  base.BallotSignFact
	util.DefaultJSONMarshaled
	hint.BaseHinter
}

func newBaseBallot(
	ht hint.Hint,
	vp base.Voteproof,
	signFact base.BallotSignFact,
	withdraws []SuffrageWithdrawOperation,
) baseBallot {
	sortWithdraws(withdraws)

	return baseBallot{
		BaseHinter: hint.NewBaseHinter(ht),
		vp:         vp,
		signFact:   signFact,
		withdraws:  withdraws,
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

func (bl baseBallot) Withdraws() []SuffrageWithdrawOperation {
	return bl.withdraws
}

func (bl baseBallot) IsValid(networkID []byte) error {
	if err := base.IsValidBallot(bl, networkID); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid baseBallot")
	}

	switch fact, ok := bl.signFact.Fact().(ballotWithdrawFacts); {
	case !ok:
		return util.ErrInvalid.Errorf("expected isaac.INITBallotFact, not %T", bl.signFact)
	case len(fact.WithdrawFacts()) != len(bl.withdraws):
		return util.ErrInvalid.Errorf("number of withdraws not matched")
	case len(bl.withdraws) < 1:
	default:
		if err := util.CheckIsValiderSlice(networkID, false, bl.withdraws); err != nil {
			return util.ErrInvalid.Wrap(err)
		}

		withdrawfacts := fact.WithdrawFacts()

		for i := range withdrawfacts {
			if !withdrawfacts[i].Hash().Equal(bl.withdraws[i].Fact().Hash()) {
				return util.ErrInvalid.Errorf("withdraw fact hash not matched")
			}
		}
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
	baseBallot
}

func NewINITBallot(
	vp base.Voteproof,
	signfact INITBallotSignFact,
	withdraws []SuffrageWithdrawOperation,
) INITBallot {
	return INITBallot{
		baseBallot: newBaseBallot(INITBallotHint, vp, signfact, withdraws),
	}
}

func (bl INITBallot) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid INITBallot")

	if err := bl.BaseHinter.IsValid(INITBallotHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := bl.baseBallot.IsValid(networkID); err != nil {
		return e.Wrap(err)
	}

	if err := base.IsValidINITBallot(bl, networkID); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (bl INITBallot) BallotSignFact() base.INITBallotSignFact {
	if bl.signFact == nil {
		return nil
	}

	return bl.signFact.(base.INITBallotSignFact) //nolint:forcetypeassert //...
}

type ACCEPTBallot struct {
	baseBallot
}

func NewACCEPTBallot(
	ivp base.INITVoteproof,
	signfact ACCEPTBallotSignFact,
	withdraws []SuffrageWithdrawOperation,
) ACCEPTBallot {
	return ACCEPTBallot{
		baseBallot: newBaseBallot(ACCEPTBallotHint, ivp, signfact, withdraws),
	}
}

func (bl ACCEPTBallot) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid ACCEPTBallot")

	if err := bl.BaseHinter.IsValid(ACCEPTBallotHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := bl.baseBallot.IsValid(networkID); err != nil {
		return e.Wrap(err)
	}

	if err := base.IsValidACCEPTBallot(bl, networkID); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (bl ACCEPTBallot) BallotSignFact() base.ACCEPTBallotSignFact {
	if bl.signFact == nil {
		return nil
	}

	return bl.signFact.(base.ACCEPTBallotSignFact) //nolint:forcetypeassert //...
}

func sortWithdrawFacts(withdrawfacts []SuffrageWithdrawFact) {
	if len(withdrawfacts) < 1 {
		return
	}

	sort.Slice(withdrawfacts, func(i, j int) bool {
		return strings.Compare(withdrawfacts[i].Hash().String(), withdrawfacts[j].Hash().String()) < 0
	})
}

func sortWithdraws(withdraws []SuffrageWithdrawOperation) {
	if len(withdraws) < 1 {
		return
	}

	sort.Slice(withdraws, func(i, j int) bool {
		return strings.Compare(withdraws[i].Fact().Hash().String(), withdraws[j].Fact().Hash().String()) < 0
	})
}

func IsValidBallotWithSuffrage(
	bl base.Ballot,
	suf base.Suffrage,
	checkValid func(base.Voteproof, base.Suffrage) error,
) (base.Suffrage, bool, error) {
	if !suf.ExistsPublickey(bl.SignFact().Node(), bl.SignFact().Signer()) {
		return nil, false, nil
	}

	if err := IsValidVoteproofWithSuffrage(bl.Voteproof(), suf, checkValid); err != nil {
		return suf, false, nil
	}

	return suf, true, nil
}

func IsValidVoteproofWithSuffrage(
	vp base.Voteproof,
	suf base.Suffrage,
	checkValid func(base.Voteproof, base.Suffrage) error,
) error {
	e := util.ErrInvalid.Errorf("invalid sign facts in voteproof with suffrage")

	cf := checkValid
	if cf == nil {
		cf = func(base.Voteproof, base.Suffrage) error { return nil }
	}

	if err := cf(vp, suf); err != nil {
		return e.Wrapf(err, "invalid voteproof")
	}

	return nil
}

func ValidateBallotBeforeVoting(
	bl base.Ballot,
	networkID base.NetworkID,
	getSuffrage GetSuffrageByBlockHeight,
	isValidVoteproofWithSuffrage func(base.Voteproof, base.Suffrage) error,
) error {
	if isValidVoteproofWithSuffrage == nil {
		isValidVoteproofWithSuffrage = base.IsValidVoteproofWithSuffrage // revive:disable-line:modifies-parameter
	}

	if err := bl.IsValid(networkID); err != nil {
		return err
	}

	var suf base.Suffrage

	switch i, found, err := getSuffrage(bl.Point().Height()); {
	case err != nil:
		return err
	case !found:
		return nil
	default:
		suf = i
	}

	switch i, found, err := IsValidBallotWithSuffrage(bl, suf, isValidVoteproofWithSuffrage); {
	case err != nil:
		return err
	case !found:
		return nil
	default:
		suf = i
	}

	switch wbl, ok := bl.(ballotWithdraws); {
	case !ok:
		return nil
	default:
		if err := IsValidWithdrawsWithSuffrage(wbl.Withdraws(), suf); err != nil {
			return err
		}
	}

	return nil
}

func IsValidWithdrawsWithSuffrage(withdraws []SuffrageWithdrawOperation, suf base.Suffrage) error {
	e := util.ErrInvalid.Errorf("invalid withdraws with suffrage")

	if len(withdraws) < 1 {
		return nil
	}

	for i := range withdraws {
		w := withdraws[i]

		fact := w.WithdrawFact()

		if !suf.Exists(fact.Node()) {
			return e.Errorf("unknown withdraw node found, %q", fact.Node())
		}

		signs := w.NodeSigns()

		for i := range signs {
			sign := signs[i]

			if !suf.ExistsPublickey(sign.Node(), sign.Signer()) {
				return e.Errorf("unknown withdraw node found, %q", sign.Node())
			}
		}
	}

	return nil
}
