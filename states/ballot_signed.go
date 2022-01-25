package states

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	INITBallotSignedFactHint   = hint.MustNewHint("init-ballot-signed-fact-v0.0.1")
	ProposalSignedFactHint     = hint.MustNewHint("proposalt-signed-fact-v0.0.1")
	ACCEPTBallotSignedFactHint = hint.MustNewHint("accept-ballot-signed-fact-v0.0.1")
)

type baseBallotSignedFact struct {
	hint.BaseHinter
	fact   base.BallotFact
	node   base.Address
	signed base.BaseSigned
}

func newBaseBallotSignedFact(ht hint.Hint, node base.Address, fact base.BallotFact) baseBallotSignedFact {
	return baseBallotSignedFact{
		BaseHinter: hint.NewBaseHinter(ht),
		fact:       fact,
		node:       node,
	}
}

func (sf baseBallotSignedFact) Node() base.Address {
	return sf.node
}

func (sf baseBallotSignedFact) Signed() []base.Signed {
	return []base.Signed{sf.signed}
}

func (sf baseBallotSignedFact) Fact() base.Fact {
	return sf.fact
}

func (sf baseBallotSignedFact) IsValid(networkID []byte) error {
	return nil
}

func (sf *baseBallotSignedFact) Sign(priv base.Privatekey, networkID base.NetworkID) error {
	sign, err := base.BaseSignedFromFact(
		priv,
		networkID,
		sf.fact,
	)
	if err != nil {
		return errors.Wrap(err, "failed to sign BaseSeal")
	}

	sf.signed = sign

	return nil
}

func (sf baseBallotSignedFact) HashBytes() []byte {
	return util.ConcatByters(sf.BaseHinter, sf.signed, sf.node)
}

type INITBallotSignedFact struct {
	baseBallotSignedFact
}

func NewINITBallotSignedFact(node base.Address, fact base.INITBallotFact) INITBallotSignedFact {
	return INITBallotSignedFact{
		baseBallotSignedFact: newBaseBallotSignedFact(INITBallotSignedFactHint, node, fact),
	}
}

func (sf INITBallotSignedFact) BallotFact() base.INITBallotFact {
	if sf.fact == nil {
		return nil
	}

	return sf.fact.(base.INITBallotFact)
}

func (sf INITBallotSignedFact) IsValid(networkID []byte) error {
	if err := base.IsValidINITBallotSignedFact(sf, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid INITBallotSignedFact")
	}

	return nil
}

type ProposalSignedFact struct {
	baseBallotSignedFact
}

func NewProposalSignedFact(node base.Address, fact base.ProposalFact) ProposalSignedFact {
	return ProposalSignedFact{
		baseBallotSignedFact: newBaseBallotSignedFact(ProposalSignedFactHint, node, fact),
	}
}

func (sf ProposalSignedFact) BallotFact() base.ProposalFact {
	if sf.fact == nil {
		return nil
	}

	return sf.fact.(base.ProposalFact)
}

func (sf ProposalSignedFact) Proposer() base.Address {
	return sf.node
}

func (sf ProposalSignedFact) IsValid(networkID []byte) error {
	if err := base.IsValidProposalSignedFact(sf, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid ProposalSignedFact")
	}

	return nil
}

type ACCEPTBallotSignedFact struct {
	baseBallotSignedFact
}

func NewACCEPTBallotSignedFact(node base.Address, fact base.ACCEPTBallotFact) ACCEPTBallotSignedFact {
	return ACCEPTBallotSignedFact{
		baseBallotSignedFact: newBaseBallotSignedFact(ACCEPTBallotSignedFactHint, node, fact),
	}
}

func (sf ACCEPTBallotSignedFact) BallotFact() base.ACCEPTBallotFact {
	if sf.fact == nil {
		return nil
	}

	return sf.fact.(base.ACCEPTBallotFact)
}

func (sf ACCEPTBallotSignedFact) IsValid(networkID []byte) error {
	if err := base.IsValidACCEPTBallotSignedFact(sf, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid ACCEPTBallotSignedFact")
	}

	return nil
}
