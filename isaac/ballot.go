package isaac

import (
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	INITBallotHint   = hint.MustNewHint("init-ballot-v0.0.1")
	ACCEPTBallotHint = hint.MustNewHint("accept-ballot-v0.0.1")
)

type baseBallot struct {
	withdraws []base.SuffrageWithdrawOperation
	vp        base.Voteproof
	signFact  base.BallotSignFact
	util.DefaultJSONMarshaled
	hint.BaseHinter
}

func newBaseBallot(
	ht hint.Hint,
	vp base.Voteproof,
	signFact base.BallotSignFact,
	withdraws []base.SuffrageWithdrawOperation,
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

func (bl baseBallot) Withdraws() []base.SuffrageWithdrawOperation {
	return bl.withdraws
}

func (bl baseBallot) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid baseBallot")

	if err := base.IsValidBallot(bl, networkID); err != nil {
		return e.Wrap(err)
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

func (bl *baseBallot) isValidWithdraws(networkID []byte, withdraws []base.SuffrageWithdrawOperation) error {
	fact, ok := bl.signFact.Fact().(WithdrawBallotFact)
	if !ok {
		return nil
	}

	withdrawfacts := fact.WithdrawFacts()

	switch {
	case len(fact.WithdrawFacts()) != len(withdraws):
		return errors.Errorf("number of withdraws not matched")
	case len(withdraws) < 1:
	default:
		if err := util.CheckIsValiderSlice(networkID, false, withdraws); err != nil {
			return err
		}

		var werr error

		if _, found := util.IsDuplicatedSlice(withdraws, func(i base.SuffrageWithdrawOperation) (bool, string) {
			if i == nil {
				return true, ""
			}

			fact := i.WithdrawFact()

			if werr == nil && fact.WithdrawStart() > bl.Point().Height() {
				werr = util.ErrInvalid.Errorf("wrong start height in withdraw")
			}

			return werr == nil, fact.Node().String()
		}); werr == nil && found {
			return util.ErrInvalid.Errorf("duplicated withdraw node found")
		}

		if werr != nil {
			return werr
		}

		for i := range withdrawfacts {
			if !withdrawfacts[i].Equal(withdraws[i].Fact().Hash()) {
				return errors.Errorf("withdraw fact hash not matched")
			}
		}
	}

	withdrawnodes := make([]base.Address, len(withdraws))

	for i := range withdraws {
		signs := withdraws[i].NodeSigns()

		filtered := util.FilterSlice(signs, func(sign base.NodeSign) bool {
			return util.InSliceFunc(withdrawnodes, func(addr base.Address) bool {
				return sign.Node().Equal(addr)
			}) < 0
		})

		if len(filtered) < 1 {
			return errors.Errorf("valid node signs not found")
		}
	}

	return nil
}

type INITBallot struct {
	baseBallot
}

func NewINITBallot(
	vp base.Voteproof,
	signfact INITBallotSignFact,
	withdraws []base.SuffrageWithdrawOperation,
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

	switch _, ok := bl.signFact.Fact().(SuffrageConfirmBallotFact); {
	case ok:
		if err := bl.isvalidSuffrageConfirmBallotFact(networkID); err != nil {
			return e.Wrap(err)
		}
	default:
		if err := bl.isValidWithdraws(networkID, bl.withdraws); err != nil {
			return e.Wrap(err)
		}

		if err := base.IsValidINITBallot(bl, networkID); err != nil {
			return e.Wrap(err)
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

func (bl INITBallot) Withdraws() []base.SuffrageWithdrawOperation {
	if bl.signFact != nil {
		if _, ok := bl.signFact.Fact().(SuffrageConfirmBallotFact); ok {
			switch w, ok := bl.Voteproof().(base.WithdrawVoteproof); {
			case !ok:
				return nil
			default:
				return w.Withdraws()
			}
		}
	}

	return bl.baseBallot.Withdraws()
}

func (bl INITBallot) isvalidSuffrageConfirmBallotFact(networkID base.NetworkID) error {
	e := util.ErrInvalid.Errorf("invalid suffrage confirm ballot")

	vp := bl.Voteproof()

	if s := vp.Point().Stage(); s != base.StageINIT {
		return e.Errorf("wrong voteproof; stage should be INIT, not %q", s)
	}

	// NOTE if fact is sign fact, voteproof result should be majority
	if r := vp.Result(); r != base.VoteResultMajority {
		return e.Errorf("wrong voteproof; vote result should be %q, not %q", base.VoteResultMajority, r)
	}

	if _, ok := vp.Majority().(base.INITBallotFact); !ok {
		return e.Errorf("wrong voteproof; majority should be INITBallotFact, not %q", vp.Majority())
	}

	// NOTE SuffrageConfirm INIT ballot does use withdraws from voteproof
	// instead of from it's body.
	switch w, ok := vp.(base.WithdrawVoteproof); {
	case !ok:
		return e.Errorf("wrong voteproof; expected WithdrawVoteproof, but %T", vp)
	case len(w.Withdraws()) < 1:
		return e.Errorf("wrong voteproof; empty withdraws")
	case len(bl.withdraws) > 0:
		return e.Errorf("not empty withdraws")
	default:
		if err := bl.isValidWithdraws(networkID, w.Withdraws()); err != nil {
			return e.Wrap(err)
		}
	}

	fact := bl.signFact.Fact().(SuffrageConfirmBallotFact) //nolint:forcetypeassert //...

	bfacts := fact.WithdrawFacts()                               //nolint:forcetypeassert //...
	vfacts := vp.Majority().(WithdrawBallotFact).WithdrawFacts() //nolint:forcetypeassert //...

	// NOTE compare withdraws in fact with ballot's
	if len(bfacts) != len(vfacts) {
		return e.Errorf("wrong voteproof; not matched with suffrage confirm ballot fact")
	}

	for i := range bfacts {
		if !bfacts[i].Equal(vfacts[i]) {
			return e.Errorf("wrong voteproof; not matched hash with suffrage confirm ballot fact")
		}
	}

	// NOTE compare previous block and proposal of suffrage confirm ballot fact
	// with voteproof's
	switch mfact := vp.Majority().(base.INITBallotFact); { //nolint:forcetypeassert //...
	case !mfact.PreviousBlock().Equal(fact.PreviousBlock()):
		return e.Errorf("wrong voteproof; wrong previous block with suffrage confirm ballot fact")
	case !mfact.Proposal().Equal(fact.Proposal()):
		return e.Errorf("wrong voteproof; wrong proposal with suffrage confirm ballot fact")
	}

	return nil
}

type ACCEPTBallot struct {
	baseBallot
}

func NewACCEPTBallot(
	ivp base.INITVoteproof,
	signfact ACCEPTBallotSignFact,
	withdraws []base.SuffrageWithdrawOperation,
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

	if err := bl.isValidWithdraws(networkID, bl.withdraws); err != nil {
		return e.Wrap(err)
	}

	if err := base.IsValidACCEPTBallot(bl, networkID); err != nil {
		return e.Wrap(err)
	}

	return bl.isValidACCEPTWithdraws()
}

func (bl ACCEPTBallot) BallotSignFact() base.ACCEPTBallotSignFact {
	if bl.signFact == nil {
		return nil
	}

	return bl.signFact.(base.ACCEPTBallotSignFact) //nolint:forcetypeassert //...
}

func (bl ACCEPTBallot) isValidACCEPTWithdraws() error {
	e := util.ErrInvalid.Errorf("invalid withdraws in accept ballot")

	if len(bl.withdraws) < 1 {
		return nil
	}

	wvp, ok := bl.vp.(base.HasWithdraws)
	if !ok {
		return e.Errorf("invalid init voteproof; withdraws not found")
	}

	withdraws := wvp.Withdraws()

	if len(bl.withdraws) != len(withdraws) {
		return e.Errorf("invalid init voteproof; insufficient withdraws")
	}

	for i := range bl.withdraws {
		bw := bl.withdraws[i]
		vw := withdraws[i]

		if !bw.Fact().Hash().Equal(vw.Fact().Hash()) {
			return e.Errorf("invalid init voteproof; withdraw not match")
		}
	}

	return nil
}

func sortWithdrawFacts(withdrawfacts []util.Hash) {
	if len(withdrawfacts) < 1 {
		return
	}

	sort.Slice(withdrawfacts, func(i, j int) bool {
		return strings.Compare(withdrawfacts[i].String(), withdrawfacts[j].String()) < 0
	})
}

func sortWithdraws[T base.SuffrageWithdrawOperation](withdraws []T) {
	if len(withdraws) < 1 {
		return
	}

	sort.Slice(withdraws, func(i, j int) bool {
		return strings.Compare(withdraws[i].Fact().Hash().String(), withdraws[j].Fact().Hash().String()) < 0
	})
}
