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

type ballotWithdrawFacts interface {
	WithdrawFacts() []SuffrageWithdrawFact
}

type baseBallotFact struct {
	withdrawfacts []SuffrageWithdrawFact
	util.DefaultJSONMarshaled
	base.BaseFact
	point base.StagePoint
}

func newBaseBallotFact(
	ht hint.Hint,
	stage base.Stage,
	point base.Point,
	withdrawfacts []SuffrageWithdrawFact,
) baseBallotFact {
	sp := base.NewStagePoint(point, stage)

	sortWithdrawFacts(withdrawfacts)

	return baseBallotFact{
		BaseFact:      base.NewBaseFact(ht, base.Token(util.ConcatByters(ht, sp))),
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
			return util.ErrInvalid.Errorf("wrong withdrawfacts")
		}

		var err error
		if _, found := util.CheckSliceDuplicated(fact.withdrawfacts, func(_ interface{}, i int) string {
			wfact := fact.withdrawfacts[i]

			// FIXME check start with lifespan
			if err == nil && wfact.Start() >= fact.point.Height() {
				err = util.ErrInvalid.Errorf("wrong start height in withdraw fact")
			}

			return wfact.Node().String()
		}); found {
			return util.ErrInvalid.Errorf("duplicated withdraw node found")
		}

		if err != nil {
			return err
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

func (fact baseBallotFact) WithdrawFacts() []SuffrageWithdrawFact {
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
				hs[i] = fact.withdrawfacts[i].Hash()
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
	withdrawfacts []SuffrageWithdrawFact,
) INITBallotFact {
	fact := INITBallotFact{
		baseBallotFact: newBaseBallotFact(INITBallotFactHint, base.StageINIT, point, withdrawfacts),
		previousBlock:  previousBlock,
		proposal:       proposal,
	}

	fact.SetHash(fact.generateHash())

	return fact
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

func (fact INITBallotFact) generateHash() util.Hash {
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

func NewACCEPTBallotFact(
	point base.Point,
	proposal, newBlock util.Hash,
	withdrawfacts []SuffrageWithdrawFact,
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
