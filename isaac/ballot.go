package isaac

import (
	"sort"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"golang.org/x/exp/slices"
)

var (
	INITBallotHint   = hint.MustNewHint("init-ballot-v0.0.1")
	ACCEPTBallotHint = hint.MustNewHint("accept-ballot-v0.0.1")
)

type baseBallot struct {
	expels   []base.SuffrageExpelOperation
	vp       base.Voteproof
	signFact base.BallotSignFact
	hint.BaseHinter
}

func newBaseBallot(
	ht hint.Hint,
	vp base.Voteproof,
	signFact base.BallotSignFact,
	expels []base.SuffrageExpelOperation,
) baseBallot {
	sortExpels(expels)

	return baseBallot{
		BaseHinter: hint.NewBaseHinter(ht),
		vp:         vp,
		signFact:   signFact,
		expels:     expels,
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

func (bl baseBallot) Expels() []base.SuffrageExpelOperation {
	return bl.expels
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

func (bl *baseBallot) isValidExpels(networkID []byte, expels []base.SuffrageExpelOperation) error {
	fact, ok := bl.signFact.Fact().(ExpelBallotFact)
	if !ok {
		return nil
	}

	expelfacts := fact.ExpelFacts()

	switch {
	case len(fact.ExpelFacts()) != len(expels):
		return errors.Errorf("number of expels not matched")
	case len(expels) < 1:
	default:
		if err := util.CheckIsValiderSlice(networkID, false, expels); err != nil {
			return err
		}

		var werr error

		if found := util.IsDuplicatedSlice(expels, func(i base.SuffrageExpelOperation) (bool, string) {
			if i == nil {
				return true, ""
			}

			fact := i.ExpelFact()

			if werr == nil && fact.ExpelStart() > bl.Point().Height() {
				werr = util.ErrInvalid.Errorf("wrong start height in expel")
			}

			return werr == nil, fact.Node().String()
		}); werr == nil && found {
			return util.ErrInvalid.Errorf("duplicated expel node found")
		}

		if werr != nil {
			return werr
		}

		for i := range expelfacts {
			if !expelfacts[i].Equal(expels[i].Fact().Hash()) {
				return errors.Errorf("expel fact hash not matched")
			}
		}
	}

	expelnodes := make([]base.Address, len(expels))

	for i := range expels {
		signs := expels[i].NodeSigns()

		filtered := util.FilterSlice(signs, func(sign base.NodeSign) bool {
			return slices.IndexFunc(expelnodes, func(addr base.Address) bool {
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
	expels []base.SuffrageExpelOperation,
) INITBallot {
	return INITBallot{
		baseBallot: newBaseBallot(INITBallotHint, vp, signfact, expels),
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
		if err := bl.isValidExpels(networkID, bl.expels); err != nil {
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

func (bl INITBallot) Expels() []base.SuffrageExpelOperation {
	if bl.signFact != nil {
		if _, ok := bl.signFact.Fact().(SuffrageConfirmBallotFact); ok {
			switch w, ok := bl.Voteproof().(base.ExpelVoteproof); {
			case !ok:
				return nil
			default:
				return w.Expels()
			}
		}
	}

	return bl.baseBallot.Expels()
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

	if _, err := util.AssertInterfaceValue[base.INITBallotFact](vp.Majority()); err != nil {
		return e.Wrap(err)
	}

	// NOTE SuffrageConfirm INIT ballot does use expels from voteproof
	// instead of from it's body.
	switch w, err := util.AssertInterfaceValue[base.ExpelVoteproof](vp); {
	case err != nil:
		return e.Wrap(err)
	case len(w.Expels()) < 1:
		return e.Errorf("wrong voteproof; empty expels")
	case len(bl.expels) > 0:
		return e.Errorf("not empty expels")
	default:
		if err := bl.isValidExpels(networkID, w.Expels()); err != nil {
			return e.Wrap(err)
		}
	}

	fact := bl.signFact.Fact().(SuffrageConfirmBallotFact) //nolint:forcetypeassert //...

	bfacts := fact.ExpelFacts()                            //nolint:forcetypeassert //...
	vfacts := vp.Majority().(ExpelBallotFact).ExpelFacts() //nolint:forcetypeassert //...

	// NOTE compare expels in fact with ballot's
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
	expels []base.SuffrageExpelOperation,
) ACCEPTBallot {
	return ACCEPTBallot{
		baseBallot: newBaseBallot(ACCEPTBallotHint, ivp, signfact, expels),
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

	if err := bl.isValidExpels(networkID, bl.expels); err != nil {
		return e.Wrap(err)
	}

	if err := base.IsValidACCEPTBallot(bl, networkID); err != nil {
		return e.Wrap(err)
	}

	return bl.isValidACCEPTExpels()
}

func (bl ACCEPTBallot) BallotSignFact() base.ACCEPTBallotSignFact {
	if bl.signFact == nil {
		return nil
	}

	return bl.signFact.(base.ACCEPTBallotSignFact) //nolint:forcetypeassert //...
}

func (bl ACCEPTBallot) isValidACCEPTExpels() error {
	e := util.ErrInvalid.Errorf("invalid expels in accept ballot")

	if len(bl.expels) < 1 {
		return nil
	}

	var expels []base.SuffrageExpelOperation

	switch wvp, err := util.AssertInterfaceValue[base.HasExpels](bl.vp); {
	case err != nil:
		return e.Wrap(err)
	default:
		expels = wvp.Expels()
	}

	if len(bl.expels) != len(expels) {
		return e.Errorf("invalid init voteproof; insufficient expels")
	}

	for i := range bl.expels {
		bw := bl.expels[i]
		vw := expels[i]

		if !bw.Fact().Hash().Equal(vw.Fact().Hash()) {
			return e.Errorf("invalid init voteproof; expel not match")
		}
	}

	return nil
}

func sortExpelFacts(expelfacts []util.Hash) {
	if len(expelfacts) < 1 {
		return
	}

	sort.Slice(expelfacts, func(i, j int) bool {
		return expelfacts[i].String() < expelfacts[j].String()
	})
}

func sortExpels[T base.SuffrageExpelOperation](expels []T) {
	if len(expels) < 1 {
		return
	}

	sort.Slice(expels, func(i, j int) bool {
		return expels[i].Fact().Hash().String() < expels[j].Fact().Hash().String()
	})
}
