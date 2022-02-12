package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	INITBallotHint   = hint.MustNewHint("init-ballot-v0.0.1")
	ProposalHint     = hint.MustNewHint("proposalt-v0.0.1")
	ACCEPTBallotHint = hint.MustNewHint("accept-ballot-v0.0.1")
)

type baseBallot struct {
	hint.BaseHinter
	vp         base.Voteproof
	signedFact base.BallotSignedFact
}

func newBaseBallot(ht hint.Hint, vp base.Voteproof, signedFact base.BallotSignedFact) baseBallot {
	return baseBallot{
		BaseHinter: hint.NewBaseHinter(ht),
		vp:         vp,
		signedFact: signedFact,
	}
}

func (bl baseBallot) Point() base.StagePoint {
	bf := bl.ballotFact()
	if bf == nil {
		return base.ZeroStagePoint
	}

	return bf.Point()
}

func (bl baseBallot) SignedFact() base.BallotSignedFact {
	return bl.signedFact
}

func (bl baseBallot) Voteproof() base.Voteproof {
	return bl.vp
}

func (bl baseBallot) IsValid(networkID []byte) error {
	if err := base.IsValidBallot(bl, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid baseBallot")
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
			if bl.signedFact == nil {
				return nil
			}

			return bl.signedFact.HashBytes()
		}),
	)
}

func (bl baseBallot) ballotFact() base.BallotFact {
	if bl.signedFact == nil || bl.signedFact.Fact() == nil {
		return nil
	}

	bf, ok := bl.signedFact.Fact().(base.BallotFact)
	if !ok {
		return nil
	}

	return bf
}

func (bl *baseBallot) Sign(priv base.Privatekey, networkID base.NetworkID) error {
	e := util.StringErrorFunc("failed to sign ballot")

	signer, ok := bl.signedFact.(base.Signer)
	if !ok {
		return e(nil, "invalid signed fact; missing Sign()")
	}

	if err := signer.Sign(priv, networkID); err != nil {
		return e(err, "")
	}

	bl.signedFact = signer.(base.BallotSignedFact)

	return nil
}

type INITBallot struct {
	baseBallot
}

func NewINITBallot(
	vp base.Voteproof,
	signedFact INITBallotSignedFact,
) INITBallot {
	return INITBallot{
		baseBallot: newBaseBallot(INITBallotHint, vp, signedFact),
	}
}

func (bl INITBallot) IsValid(networkID []byte) error {
	if err := bl.BaseHinter.IsValid(INITBallotHint.Type().Bytes()); err != nil {
		return util.InvalidError.Wrapf(err, "invalid INITBallot")
	}

	if err := base.IsValidINITBallot(bl, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid INITBallot")
	}

	return nil
}

func (bl INITBallot) BallotSignedFact() base.INITBallotSignedFact {
	if bl.signedFact == nil {
		return nil
	}

	return bl.signedFact.(base.INITBallotSignedFact)
}

type Proposal struct {
	baseBallot
}

func NewProposal(signedFact ProposalSignedFact) Proposal {
	return Proposal{
		baseBallot: newBaseBallot(ProposalHint, nil, signedFact),
	}
}

func (bl Proposal) IsValid(networkID []byte) error {
	if err := bl.BaseHinter.IsValid(ProposalHint.Type().Bytes()); err != nil {
		return util.InvalidError.Wrapf(err, "invalid Proposal")
	}

	if err := base.IsValidProposal(bl, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid Proposal")
	}

	return nil
}

func (bl Proposal) BallotSignedFact() base.ProposalSignedFact {
	if bl.signedFact == nil {
		return nil
	}

	return bl.signedFact.(base.ProposalSignedFact)
}

type ACCEPTBallot struct {
	baseBallot
}

func NewACCEPTBallot(
	ivp base.INITVoteproof,
	signedFact ACCEPTBallotSignedFact,
) ACCEPTBallot {
	return ACCEPTBallot{
		baseBallot: newBaseBallot(ACCEPTBallotHint, ivp, signedFact),
	}
}

func (bl ACCEPTBallot) IsValid(networkID []byte) error {
	if err := bl.BaseHinter.IsValid(ACCEPTBallotHint.Type().Bytes()); err != nil {
		return util.InvalidError.Wrapf(err, "invalid ACCEPTBallot")
	}

	if err := base.IsValidACCEPTBallot(bl, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid ACCEPTBallot")
	}

	return nil
}

func (bl ACCEPTBallot) BallotSignedFact() base.ACCEPTBallotSignedFact {
	if bl.signedFact == nil {
		return nil
	}

	return bl.signedFact.(base.ACCEPTBallotSignedFact)
}
