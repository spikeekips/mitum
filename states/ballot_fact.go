package states

import (
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	INITBallotFactHint   = hint.MustNewHint("init-ballot-fact-v0.0.1")
	ProposalFactHint     = hint.MustNewHint("proposalt-fact-v0.0.1")
	ACCEPTBallotFactHint = hint.MustNewHint("accept-ballot-fact-v0.0.1")
)

type baseBallotFact struct {
	hint.BaseHinter
	h     util.Hash
	point base.StagePoint
}

func newBaseBallotFact(ht hint.Hint, stage base.Stage, point base.Point) baseBallotFact {
	return baseBallotFact{
		BaseHinter: hint.NewBaseHinter(ht),
		point:      base.NewStagePoint(point, stage),
	}
}

func (fact baseBallotFact) Token() base.Token {
	if fact.h == nil {
		return nil
	}

	return base.Token(fact.h.Bytes())
}

func (fact baseBallotFact) Hash() util.Hash {
	return fact.h
}

func (fact baseBallotFact) Stage() base.Stage {
	return fact.point.Stage()
}

func (fact baseBallotFact) Point() base.StagePoint {
	return fact.point
}

func (fact baseBallotFact) hashBytes() []byte {
	return fact.point.Bytes()
}

type INITBallotFact struct {
	baseBallotFact
	previousBlock util.Hash
	proposal      util.Hash
}

func NewINITBallotFact(point base.Point, previousBlock, proposal util.Hash) INITBallotFact {
	fact := INITBallotFact{
		baseBallotFact: newBaseBallotFact(INITBallotFactHint, base.StageINIT, point),
		previousBlock:  previousBlock,
		proposal:       proposal,
	}

	fact.h = fact.hash()

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

	if err := base.IsValidINITBallotFact(fact); err != nil {
		return e(err, "")
	}

	if !fact.h.Equal(fact.hash()) {
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

type ProposalFact struct {
	baseBallotFact
	operations []util.Hash
	proposedAt time.Time
}

func NewProposalFact(point base.Point, operations []util.Hash) ProposalFact {
	fact := ProposalFact{
		baseBallotFact: newBaseBallotFact(ProposalFactHint, base.StageProposal, point),
		operations:     operations,
		proposedAt:     localtime.Now(),
	}

	fact.h = fact.hash()

	return fact
}

func (fact ProposalFact) Operations() []util.Hash {
	return fact.operations
}

func (fact ProposalFact) ProposedAt() time.Time {
	return fact.proposedAt
}

func (fact ProposalFact) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid ProposalFact")

	if fact.point.Stage() != base.StageProposal {
		return e(util.InvalidError.Errorf("invalid stage, %q", fact.point.Stage()), "")
	}

	if err := base.IsValidProposalFact(fact); err != nil {
		return e(err, "")
	}

	if !fact.h.Equal(fact.hash()) {
		return util.InvalidError.Errorf("wrong hash of ProposalFact")
	}

	return nil
}

func (fact ProposalFact) hash() util.Hash {
	bs := make([]util.Byter, len(fact.operations)+3)
	bs[0] = util.DummyByter(fact.baseBallotFact.hashBytes)
	bs[1] = localtime.NewTime(fact.proposedAt)
	for i := range fact.operations {
		bs[i+2] = fact.operations[i]
	}

	return valuehash.NewSHA256(util.ConcatByters(bs...))
}

type ACCEPTBallotFact struct {
	baseBallotFact
	proposal util.Hash
	newBlock util.Hash
}

func NewACCEPTBallotFact(point base.Point, proposal, newBlock util.Hash) ACCEPTBallotFact {
	fact := ACCEPTBallotFact{
		baseBallotFact: newBaseBallotFact(ACCEPTBallotFactHint, base.StageACCEPT, point),
		proposal:       proposal,
		newBlock:       newBlock,
	}

	fact.h = fact.hash()

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

	if !fact.h.Equal(fact.hash()) {
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
