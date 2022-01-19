package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type BaseBallotSignedFact struct {
	hint.BaseHinter
	fact   BallotFact
	node   Address
	signed BaseSigned
}

func NewBaseBallotSignedFact(ht hint.Hint, node Address, fact BallotFact) BaseBallotSignedFact {
	return BaseBallotSignedFact{
		BaseHinter: hint.NewBaseHinter(ht),
		fact:       fact,
		node:       node,
	}
}

func (sf BaseBallotSignedFact) Node() Address {
	return sf.node
}

func (sf BaseBallotSignedFact) Signed() []Signed {
	return []Signed{sf.signed}
}

func (sf BaseBallotSignedFact) Fact() Fact {
	return sf.fact
}

func (sf BaseBallotSignedFact) IsValid(networkID []byte) error {
	return nil
}

func (sf *BaseBallotSignedFact) Sign(priv Privatekey, networkID NetworkID) error {
	sign, err := BaseSignedFromFact(
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

func (sf BaseBallotSignedFact) HashBytes() []byte {
	return util.ConcatByters(sf.BaseHinter, sf.signed, sf.node)
}

type BaseINITBallotSignedFact struct {
	BaseBallotSignedFact
}

func NewBaseINITBallotSignedFact(ht hint.Hint, node Address, fact INITBallotFact) BaseINITBallotSignedFact {
	return BaseINITBallotSignedFact{
		BaseBallotSignedFact: NewBaseBallotSignedFact(ht, node, fact),
	}
}

func (sf BaseINITBallotSignedFact) BallotFact() INITBallotFact {
	if sf.fact == nil {
		return nil
	}

	return sf.fact.(INITBallotFact)
}

func (sf BaseINITBallotSignedFact) IsValid(networkID []byte) error {
	if err := IsValidINITBallotSignedFact(sf, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid BaseINITBallotSignedFact")
	}

	return nil
}

type BaseProposalSignedFact struct {
	BaseBallotSignedFact
}

func NewBaseProposalSignedFact(ht hint.Hint, node Address, fact ProposalFact) BaseProposalSignedFact {
	return BaseProposalSignedFact{
		BaseBallotSignedFact: NewBaseBallotSignedFact(ht, node, fact),
	}
}

func (sf BaseProposalSignedFact) BallotFact() ProposalFact {
	if sf.fact == nil {
		return nil
	}

	return sf.fact.(ProposalFact)
}

func (sf BaseProposalSignedFact) Proposer() Address {
	return sf.node
}

func (sf BaseProposalSignedFact) IsValid(networkID []byte) error {
	if err := IsValidProposalSignedFact(sf, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid BaseProposalSignedFact")
	}

	return nil
}

type BaseACCEPTBallotSignedFact struct {
	BaseBallotSignedFact
}

func NewBaseACCEPTBallotSignedFact(ht hint.Hint, node Address, fact ACCEPTBallotFact) BaseACCEPTBallotSignedFact {
	return BaseACCEPTBallotSignedFact{
		BaseBallotSignedFact: NewBaseBallotSignedFact(ht, node, fact),
	}
}

func (sf BaseACCEPTBallotSignedFact) BallotFact() ACCEPTBallotFact {
	if sf.fact == nil {
		return nil
	}

	return sf.fact.(ACCEPTBallotFact)
}

func (sf BaseACCEPTBallotSignedFact) IsValid(networkID []byte) error {
	if err := IsValidACCEPTBallotSignedFact(sf, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid BaseACCEPTBallotSignedFact")
	}

	return nil
}
