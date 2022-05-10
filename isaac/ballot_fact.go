package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	INITBallotFactHint   = hint.MustNewHint("init-ballot-fact-v0.0.1")
	ACCEPTBallotFactHint = hint.MustNewHint("accept-ballot-fact-v0.0.1")
)

type baseBallotFact struct {
	util.DefaultJSONMarshaled
	base.BaseFact
	point base.StagePoint
}

func newBaseBallotFact(ht hint.Hint, stage base.Stage, point base.Point) baseBallotFact {
	sp := base.NewStagePoint(point, stage)
	return baseBallotFact{
		BaseFact: base.NewBaseFact(ht, base.Token(util.ConcatByters(ht, sp))),
		point:    sp,
	}
}

func (fact baseBallotFact) Stage() base.Stage {
	return fact.point.Stage()
}

func (fact baseBallotFact) Point() base.StagePoint {
	return fact.point
}

func (fact baseBallotFact) hashBytes() []byte {
	return util.ConcatByters(fact.point, util.BytesToByter(fact.Token()))
}

type INITBallotFact struct {
	previousBlock util.Hash
	proposal      util.Hash
	baseBallotFact
}

func NewINITBallotFact(point base.Point, previousBlock, proposal util.Hash) INITBallotFact {
	fact := INITBallotFact{
		baseBallotFact: newBaseBallotFact(INITBallotFactHint, base.StageINIT, point),
		previousBlock:  previousBlock,
		proposal:       proposal,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact INITBallotFact) PreviousBlock() util.Hash {
	return fact.previousBlock
}

func (fact INITBallotFact) Proposal() util.Hash {
	return fact.proposal
}

func (fact INITBallotFact) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid INITBallotFact")

	if fact.point.Stage() != base.StageINIT {
		return e(util.InvalidError.Errorf("invalid stage, %q", fact.point.Stage()), "")
	}

	if err := fact.BaseFact.IsValid(nil); err != nil {
		return e(err, "")
	}

	if err := base.IsValidINITBallotFact(fact); err != nil {
		return e(err, "")
	}

	if !fact.Hash().Equal(fact.hash()) {
		return util.InvalidError.Errorf("wrong hash of INITBallotFact")
	}

	return nil
}

func (fact INITBallotFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.DummyByter(fact.baseBallotFact.hashBytes),
		fact.previousBlock,
		fact.proposal,
	))
}

type ACCEPTBallotFact struct {
	proposal util.Hash
	newBlock util.Hash
	baseBallotFact
}

func NewACCEPTBallotFact(point base.Point, proposal, newBlock util.Hash) ACCEPTBallotFact {
	fact := ACCEPTBallotFact{
		baseBallotFact: newBaseBallotFact(ACCEPTBallotFactHint, base.StageACCEPT, point),
		proposal:       proposal,
		newBlock:       newBlock,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact ACCEPTBallotFact) Proposal() util.Hash {
	return fact.proposal
}

func (fact ACCEPTBallotFact) NewBlock() util.Hash {
	return fact.newBlock
}

func (fact ACCEPTBallotFact) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid ACCEPTBallotFact")

	if fact.point.Stage() != base.StageACCEPT {
		return e(util.InvalidError.Errorf("invalid stage, %q", fact.point.Stage()), "")
	}

	if err := base.IsValidACCEPTBallotFact(fact); err != nil {
		return e(err, "")
	}

	if !fact.Hash().Equal(fact.hash()) {
		return util.InvalidError.Errorf("wrong hash of ACCEPTBallotFact")
	}

	return nil
}

func (fact ACCEPTBallotFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.DummyByter(fact.baseBallotFact.hashBytes),
		fact.proposal,
		fact.newBlock,
	))
}
