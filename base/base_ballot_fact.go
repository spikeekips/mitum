package base

import (
	"time"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

type BaseBallotFact struct {
	hint.BaseHinter
	h     util.Hash
	stage Stage
	point Point
}

func newBaseBallotFact(ht hint.Hint, stage Stage, point Point) BaseBallotFact {
	return BaseBallotFact{
		BaseHinter: hint.NewBaseHinter(ht),
		stage:      stage,
		point:      point,
	}
}

func (fact BaseBallotFact) Token() Token {
	if fact.h == nil {
		return nil
	}

	return Token(fact.h.Bytes())
}

func (fact BaseBallotFact) Hash() util.Hash {
	return fact.h
}

func (fact BaseBallotFact) Stage() Stage {
	return fact.stage
}

func (fact BaseBallotFact) Point() Point {
	return fact.point
}

func (fact BaseBallotFact) hashBytes() []byte {
	return util.ConcatByters(fact.stage, fact.point)
}

type BaseINITBallotFact struct {
	BaseBallotFact
	previousBlock util.Hash
}

func NewBaseINITBallotFact(ht hint.Hint, point Point, previousBlock util.Hash) BaseINITBallotFact {
	fact := BaseINITBallotFact{
		BaseBallotFact: newBaseBallotFact(ht, StageINIT, point),
		previousBlock:  previousBlock,
	}

	fact.h = fact.hash()

	return fact
}

func (fact BaseINITBallotFact) PreviousBlock() util.Hash {
	return fact.previousBlock
}

func (fact BaseINITBallotFact) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid BaseINITBallotFact")

	if err := IsValidINITBallotFact(fact); err != nil {
		return e(err, "")
	}

	if !fact.h.Equal(fact.hash()) {
		return util.InvalidError.Errorf("wrong hash of BaseINITBallotFact")
	}

	return nil
}

func (fact BaseINITBallotFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.DummyByter(fact.BaseBallotFact.hashBytes),
		fact.previousBlock,
	))
}

type BaseProposalFact struct {
	BaseBallotFact
	operations []util.Hash
	proposedAt time.Time
}

func NewBaseProposalFact(
	ht hint.Hint,
	point Point,
	operations []util.Hash,
) BaseProposalFact {
	fact := BaseProposalFact{
		BaseBallotFact: newBaseBallotFact(ht, StageProposal, point),
		operations:     operations,
		proposedAt:     localtime.Now(),
	}

	fact.h = fact.hash()

	return fact
}

func (fact BaseProposalFact) Operations() []util.Hash {
	return fact.operations
}

func (fact BaseProposalFact) ProposedAt() time.Time {
	return fact.proposedAt
}

func (fact BaseProposalFact) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid BaseProposalFact")

	if err := IsValidProposalFact(fact); err != nil {
		return e(err, "")
	}

	if !fact.h.Equal(fact.hash()) {
		return util.InvalidError.Errorf("wrong hash of BaseProposalFact")
	}

	return nil
}

func (fact BaseProposalFact) hash() util.Hash {
	bs := make([]util.Byter, len(fact.operations)+3)
	bs[0] = util.DummyByter(fact.BaseBallotFact.hashBytes)
	bs[1] = localtime.NewTime(fact.proposedAt)
	for i := range fact.operations {
		bs[i+2] = fact.operations[i]
	}

	return valuehash.NewSHA256(util.ConcatByters(bs...))
}

type BaseACCEPTBallotFact struct {
	BaseBallotFact
	proposal util.Hash
	newBlock util.Hash
}

func NewBaseACCEPTBallotFact(ht hint.Hint, point Point, proposal, newBlock util.Hash) BaseACCEPTBallotFact {
	fact := BaseACCEPTBallotFact{
		BaseBallotFact: newBaseBallotFact(ht, StageACCEPT, point),
		proposal:       proposal,
		newBlock:       newBlock,
	}

	fact.h = fact.hash()

	return fact
}

func (fact BaseACCEPTBallotFact) Proposal() util.Hash {
	return fact.proposal
}

func (fact BaseACCEPTBallotFact) NewBlock() util.Hash {
	return fact.newBlock
}

func (fact BaseACCEPTBallotFact) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid BaseACCEPTBallotFact")

	if err := IsValidACCEPTBallotFact(fact); err != nil {
		return e(err, "")
	}

	if !fact.h.Equal(fact.hash()) {
		return util.InvalidError.Errorf("wrong hash of BaseACCEPTBallotFact")
	}

	return nil
}

func (fact BaseACCEPTBallotFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.DummyByter(fact.BaseBallotFact.hashBytes),
		fact.proposal,
		fact.newBlock,
	))
}
