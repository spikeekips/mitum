package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	INITBallotFactHint            = hint.MustNewHint("init-ballot-fact-v0.0.1")
	ACCEPTBallotFactHint          = hint.MustNewHint("accept-ballot-fact-v0.0.1")
	SuffrageConfirmBallotFactHint = hint.MustNewHint("suffrage-confirm-ballot-fact-v0.0.1")
)

type WithdrawBallotFact interface {
	WithdrawFacts() []util.Hash
}

type baseBallotFact struct {
	withdrawfacts []util.Hash
	util.DefaultJSONMarshaled
	base.BaseFact
	point base.StagePoint
}

func newBaseBallotFact(
	ht hint.Hint,
	stage base.Stage,
	point base.Point,
	withdrawfacts []util.Hash,
) baseBallotFact {
	sp := base.NewStagePoint(point, stage)

	sortWithdrawFacts(withdrawfacts)

	return baseBallotFact{
		BaseFact:      base.NewBaseFact(ht, base.Token(sp.Bytes())),
		point:         sp,
		withdrawfacts: withdrawfacts,
	}
}

func (fact baseBallotFact) IsValid([]byte) error {
	if err := fact.BaseFact.IsValid(nil); err != nil {
		return err
	}

	if err := base.IsValidBallotFact(fact); err != nil {
		return err
	}

	if len(fact.withdrawfacts) > 0 {
		if err := util.CheckIsValiderSlice(nil, false, fact.withdrawfacts); err != nil {
			return util.ErrInvalid.Wrapf(err, "wrong withdrawfacts")
		}

		if _, found := util.IsDuplicatedSlice(fact.withdrawfacts, func(i util.Hash) (bool, string) {
			return true, i.String()
		}); found {
			return util.ErrInvalid.Errorf("duplicated withdraw fact found")
		}
	}

	return nil
}

func (fact baseBallotFact) Stage() base.Stage {
	return fact.point.Stage()
}

func (fact baseBallotFact) Point() base.StagePoint {
	return fact.point
}

func (fact baseBallotFact) WithdrawFacts() []util.Hash {
	return fact.withdrawfacts
}

func (fact baseBallotFact) hashBytes() []byte {
	return util.ConcatByters(
		fact.point,
		util.BytesToByter(fact.Token()),
		util.DummyByter(func() []byte {
			if len(fact.withdrawfacts) < 1 {
				return nil
			}

			hs := make([]util.Hash, len(fact.withdrawfacts))
			for i := range hs {
				hs[i] = fact.withdrawfacts[i]
			}

			return util.ConcatByterSlice(hs)
		}),
	)
}

type INITBallotFact struct {
	previousBlock util.Hash
	proposal      util.Hash
	baseBallotFact
}

func NewINITBallotFact(
	point base.Point,
	previousBlock, proposal util.Hash,
	withdrawfacts []util.Hash,
) INITBallotFact {
	fact := newINITBallotFact(INITBallotFactHint, point, previousBlock, proposal, withdrawfacts)

	fact.SetHash(fact.generateHash())

	return fact
}

func newINITBallotFact(
	ht hint.Hint,
	point base.Point,
	previousBlock, proposal util.Hash,
	withdrawfacts []util.Hash,
) INITBallotFact {
	return INITBallotFact{
		baseBallotFact: newBaseBallotFact(ht, base.StageINIT, point, withdrawfacts),
		previousBlock:  previousBlock,
		proposal:       proposal,
	}
}

func (fact INITBallotFact) PreviousBlock() util.Hash {
	return fact.previousBlock
}

func (fact INITBallotFact) Proposal() util.Hash {
	return fact.proposal
}

func (fact INITBallotFact) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid INITBallotFact")

	if err := fact.baseBallotFact.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := base.IsValidINITBallotFact(fact); err != nil {
		return e.Wrap(err)
	}

	if !fact.Hash().Equal(fact.generateHash()) {
		return e.Errorf("wrong hash of INITBallotFact")
	}

	return nil
}

func (fact INITBallotFact) hashBytes() []byte {
	return util.ConcatByters(
		util.DummyByter(fact.baseBallotFact.hashBytes),
		fact.previousBlock,
		fact.proposal,
	)
}

func (fact INITBallotFact) generateHash() util.Hash {
	return valuehash.NewSHA256(fact.hashBytes())
}

type ACCEPTBallotFact struct {
	proposal util.Hash
	newBlock util.Hash
	baseBallotFact
}

func NewACCEPTBallotFact(
	point base.Point,
	proposal, newBlock util.Hash,
	withdrawfacts []util.Hash,
) ACCEPTBallotFact {
	fact := ACCEPTBallotFact{
		baseBallotFact: newBaseBallotFact(ACCEPTBallotFactHint, base.StageACCEPT, point, withdrawfacts),
		proposal:       proposal,
		newBlock:       newBlock,
	}

	fact.SetHash(fact.generateHash())

	return fact
}

func (fact ACCEPTBallotFact) Proposal() util.Hash {
	return fact.proposal
}

func (fact ACCEPTBallotFact) NewBlock() util.Hash {
	return fact.newBlock
}

func (fact ACCEPTBallotFact) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid ACCEPTBallotFact")

	if err := fact.baseBallotFact.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := base.IsValidACCEPTBallotFact(fact); err != nil {
		return e.Wrap(err)
	}

	if !fact.Hash().Equal(fact.generateHash()) {
		return e.Errorf("wrong hash of ACCEPTBallotFact")
	}

	return nil
}

func (fact ACCEPTBallotFact) generateHash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.DummyByter(fact.baseBallotFact.hashBytes),
		fact.proposal,
		fact.newBlock,
	))
}

type SuffrageConfirmBallotFact struct {
	INITBallotFact
}

func NewSuffrageConfirmBallotFact(
	point base.Point,
	previousBlock, proposal util.Hash,
	withdrawfacts []util.Hash,
) SuffrageConfirmBallotFact {
	fact := SuffrageConfirmBallotFact{
		INITBallotFact: newINITBallotFact(SuffrageConfirmBallotFactHint, point, previousBlock, proposal, withdrawfacts),
	}

	fact.SetHash(fact.generateHash())

	return fact
}

func (fact SuffrageConfirmBallotFact) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid SuffrageConfirmBallotFact")

	if len(fact.withdrawfacts) < 1 {
		return e.Errorf("empty withdraw facts")
	}

	if err := fact.baseBallotFact.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := base.IsValidINITBallotFact(fact); err != nil {
		return e.Wrap(err)
	}

	if !fact.Hash().Equal(fact.generateHash()) {
		return e.Errorf("wrong hash of INITBallotFact")
	}

	return nil
}

func IsSuffrageConfirmBallotFact(fact base.Fact) bool {
	_, ok := fact.(SuffrageConfirmBallotFact)

	return ok
}
