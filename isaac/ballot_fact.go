package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	INITBallotFactHint                  = hint.MustNewHint("init-ballot-fact-v0.0.1")
	ACCEPTBallotFactHint                = hint.MustNewHint("accept-ballot-fact-v0.0.1")
	SuffrageConfirmBallotFactHint       = hint.MustNewHint("suffrage-confirm-ballot-fact-v0.0.1")
	EmptyProposalINITBallotFactHint     = hint.MustNewHint("empty-proposal-init-ballot-fact-v0.0.1")
	EmptyOperationsACCEPTBallotFactHint = hint.MustNewHint("empty-operations-accept-ballot-fact-v0.0.1")
	NotProcessedACCEPTBallotFactHint    = hint.MustNewHint("not-processed-accept-ballot-fact-v0.0.1")
)

type ExpelBallotFact interface {
	ExpelFacts() []util.Hash
}

type baseBallotFact struct {
	expelfacts []util.Hash
	point      base.StagePoint
	base.BaseFact
}

func newBaseBallotFact(
	ht hint.Hint,
	stage base.Stage,
	point base.Point,
	expelfacts []util.Hash,
) baseBallotFact {
	sp := base.NewStagePoint(point, stage)

	sortExpelFacts(expelfacts)

	return baseBallotFact{
		BaseFact:   base.NewBaseFact(ht, base.Token(sp.Bytes())),
		point:      sp,
		expelfacts: expelfacts,
	}
}

func (fact baseBallotFact) IsValid([]byte) error {
	if err := fact.BaseFact.IsValid(nil); err != nil {
		return err
	}

	if err := base.IsValidBallotFact(fact); err != nil {
		return err
	}

	if len(fact.expelfacts) > 0 {
		if err := util.CheckIsValiderSlice(nil, false, fact.expelfacts); err != nil {
			return util.ErrInvalid.WithMessage(err, "wrong expelfacts")
		}

		if util.IsDuplicatedSlice(fact.expelfacts, func(i util.Hash) (bool, string) {
			if i == nil {
				return true, ""
			}

			return true, i.String()
		}) {
			return util.ErrInvalid.Errorf("duplicated expel fact found")
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

func (fact baseBallotFact) ExpelFacts() []util.Hash {
	return fact.expelfacts
}

func (fact baseBallotFact) hashBytes() []byte {
	return util.ConcatByters(
		fact.point,
		util.BytesToByter(fact.Token()),
		util.DummyByter(func() []byte {
			if len(fact.expelfacts) < 1 {
				return nil
			}

			hs := make([]util.Hash, len(fact.expelfacts))
			for i := range hs {
				hs[i] = fact.expelfacts[i]
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
	expelfacts []util.Hash,
) INITBallotFact {
	fact := newINITBallotFact(INITBallotFactHint, point, previousBlock, proposal, expelfacts)

	fact.SetHash(fact.generateHash())

	return fact
}

func newINITBallotFact(
	ht hint.Hint,
	point base.Point,
	previousBlock, proposal util.Hash,
	expelfacts []util.Hash,
) INITBallotFact {
	return INITBallotFact{
		baseBallotFact: newBaseBallotFact(ht, base.StageINIT, point, expelfacts),
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
	expelfacts []util.Hash,
) ACCEPTBallotFact {
	return newACCEPTBallotFact(ACCEPTBallotFactHint, point, proposal, newBlock, expelfacts)
}

func newACCEPTBallotFact(
	ht hint.Hint,
	point base.Point,
	proposal, newBlock util.Hash,
	expelfacts []util.Hash,
) ACCEPTBallotFact {
	fact := ACCEPTBallotFact{
		baseBallotFact: newBaseBallotFact(ht, base.StageACCEPT, point, expelfacts),
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
	expelfacts []util.Hash,
) SuffrageConfirmBallotFact {
	fact := SuffrageConfirmBallotFact{
		INITBallotFact: newINITBallotFact(SuffrageConfirmBallotFactHint, point, previousBlock, proposal, expelfacts),
	}

	fact.SetHash(fact.generateHash())

	return fact
}

func (fact SuffrageConfirmBallotFact) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid SuffrageConfirmBallotFact")

	if len(fact.expelfacts) < 1 {
		return e.Errorf("empty expel facts")
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

type EmptyProposalINITBallotFact struct {
	r string
	INITBallotFact
}

func NewEmptyProposalINITBallotFact(
	point base.Point,
	previousBlock, proposal util.Hash,
) EmptyProposalINITBallotFact {
	fact := EmptyProposalINITBallotFact{
		INITBallotFact: newINITBallotFact(EmptyProposalINITBallotFactHint, point, previousBlock, proposal, nil),
		r:              util.UUID().String(),
	}

	fact.SetHash(fact.generateHash())

	return fact
}

func (fact EmptyProposalINITBallotFact) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid EmptyProposalINITBallotFact")

	if err := fact.baseBallotFact.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := base.IsValidINITBallotFact(fact); err != nil {
		return e.Wrap(err)
	}

	if len(fact.r) < 1 {
		return e.Errorf("empty r")
	}

	return nil
}

func (fact EmptyProposalINITBallotFact) generateHash() util.Hash {
	return valuehash.NewSHA256(util.ConcatBytesSlice(
		fact.INITBallotFact.hashBytes(),
		[]byte(fact.r),
	))
}

type EmptyOperationsACCEPTBallotFact struct {
	ACCEPTBallotFact
}

func NewEmptyOperationsACCEPTBallotFact(
	point base.Point,
	proposal util.Hash,
) EmptyOperationsACCEPTBallotFact {
	return EmptyOperationsACCEPTBallotFact{
		ACCEPTBallotFact: newACCEPTBallotFact(
			EmptyOperationsACCEPTBallotFactHint, point, proposal, valuehash.RandomSHA256(), nil),
	}
}

type NotProcessedACCEPTBallotFact struct {
	ACCEPTBallotFact
}

func NewNotProcessedACCEPTBallotFact(
	point base.Point,
	proposal util.Hash,
) NotProcessedACCEPTBallotFact {
	return NotProcessedACCEPTBallotFact{
		ACCEPTBallotFact: newACCEPTBallotFact(
			NotProcessedACCEPTBallotFactHint, point, proposal, valuehash.RandomSHA256(), nil),
	}
}
