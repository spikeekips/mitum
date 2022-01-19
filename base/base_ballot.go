package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type BaseBallot struct {
	hint.BaseHinter
	ivp        INITVoteproof
	avp        ACCEPTVoteproof
	signedFact BallotSignedFact
}

func NewBaseBallot(ht hint.Hint, ivp INITVoteproof, avp ACCEPTVoteproof, signedFact BallotSignedFact) BaseBallot {
	return BaseBallot{
		BaseHinter: hint.NewBaseHinter(ht),
		ivp:        ivp,
		avp:        avp,
		signedFact: signedFact,
	}
}

func (bl BaseBallot) Point() Point {
	bf := bl.ballotFact()
	if bf == nil {
		return Point{}
	}

	return bf.Point()
}

func (bl BaseBallot) Stage() Stage {
	bf := bl.ballotFact()
	if bf == nil {
		return StageUnknown
	}

	return bf.Stage()
}

func (bl BaseBallot) SignedFact() BallotSignedFact {
	return bl.signedFact
}

func (bl BaseBallot) INITVoteproof() INITVoteproof {
	return bl.ivp
}

func (bl BaseBallot) ACCEPTVoteproof() ACCEPTVoteproof {
	return bl.avp
}

func (bl BaseBallot) IsValid(networkID []byte) error {
	if err := IsValidBallot(bl, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid BaseBallot")
	}

	return nil
}

func (bl BaseBallot) HashBytes() []byte {
	return util.ConcatByters(
		bl.Hint(),
		util.DummyByter(func() []byte {
			if bl.ivp == nil {
				return nil
			}

			return bl.ivp.HashBytes()
		}),
		util.DummyByter(func() []byte {
			if bl.avp == nil {
				return nil
			}

			return bl.avp.HashBytes()
		}),
		util.DummyByter(func() []byte {
			if bl.signedFact == nil {
				return nil
			}

			return bl.signedFact.HashBytes()
		}),
	)
}

func (bl BaseBallot) ballotFact() BallotFact {
	if bl.signedFact == nil || bl.signedFact.Fact() == nil {
		return nil
	}

	bf, ok := bl.signedFact.Fact().(BallotFact)
	if !ok {
		return nil
	}

	return bf
}

type BaseINITBallot struct {
	BaseBallot
}

func NewBaseINITBallot(
	ht hint.Hint,
	ivp INITVoteproof,
	avp ACCEPTVoteproof,
	signedFact BallotSignedFact,
) BaseINITBallot {
	return BaseINITBallot{
		BaseBallot: NewBaseBallot(ht, ivp, avp, signedFact),
	}
}

func (bl BaseINITBallot) IsValid(networkID []byte) error {
	if err := IsValidINITBallot(bl, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid BaseINITBallot")
	}

	return nil
}

func (bl BaseINITBallot) BallotSignedFact() INITBallotSignedFact {
	if bl.signedFact == nil {
		return nil
	}

	return bl.signedFact.(INITBallotSignedFact)
}

type BaseProposal struct {
	BaseBallot
}

func NewBaseProposal(
	ht hint.Hint,
	ivp INITVoteproof,
	avp ACCEPTVoteproof,
	signedFact BallotSignedFact,
) BaseProposal {
	return BaseProposal{
		BaseBallot: NewBaseBallot(ht, ivp, avp, signedFact),
	}
}

func (bl BaseProposal) IsValid(networkID []byte) error {
	if err := IsValidProposal(bl, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid BaseProposal")
	}

	return nil
}

func (bl BaseProposal) BallotSignedFact() ProposalSignedFact {
	if bl.signedFact == nil {
		return nil
	}

	return bl.signedFact.(ProposalSignedFact)
}

type BaseACCEPTBallot struct {
	BaseBallot
}

func NewBaseACCEPTBallot(
	ht hint.Hint,
	ivp INITVoteproof,
	avp ACCEPTVoteproof,
	signedFact BallotSignedFact,
) BaseACCEPTBallot {
	return BaseACCEPTBallot{
		BaseBallot: NewBaseBallot(ht, ivp, avp, signedFact),
	}
}

func (bl BaseACCEPTBallot) IsValid(networkID []byte) error {
	if err := IsValidACCEPTBallot(bl, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid BaseACCEPTBallot")
	}

	return nil
}

func (bl BaseACCEPTBallot) BallotSignedFact() ACCEPTBallotSignedFact {
	if bl.signedFact == nil {
		return nil
	}

	return bl.signedFact.(ACCEPTBallotSignedFact)
}
