package isaac

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	INITBallotSignedFactHint   = hint.MustNewHint("init-ballot-signed-fact-v0.0.1")
	ACCEPTBallotSignedFactHint = hint.MustNewHint("accept-ballot-signed-fact-v0.0.1")
)

type baseBallotSignedFact struct {
	fact   base.BallotFact
	node   base.Address
	signed base.BaseSigned
	util.DefaultJSONMarshaled
	hint.BaseHinter
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

func (sf baseBallotSignedFact) Signer() base.Publickey {
	return sf.signed.Signer()
}

func (sf baseBallotSignedFact) Signed() []base.Signed {
	return []base.Signed{sf.signed}
}

func (sf baseBallotSignedFact) Fact() base.Fact {
	return sf.fact
}

func (baseBallotSignedFact) IsValid([]byte) error {
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

func NewINITBallotSignedFact(node base.Address, fact INITBallotFact) INITBallotSignedFact {
	return INITBallotSignedFact{
		baseBallotSignedFact: newBaseBallotSignedFact(INITBallotSignedFactHint, node, fact),
	}
}

func (sf INITBallotSignedFact) BallotFact() base.INITBallotFact {
	if sf.fact == nil {
		return nil
	}

	return sf.fact.(base.INITBallotFact) //nolint:forcetypeassert //...
}

func (sf INITBallotSignedFact) IsValid(networkID []byte) error {
	if err := base.IsValidINITBallotSignedFact(sf, networkID); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid INITBallotSignedFact")
	}

	return nil
}

type ACCEPTBallotSignedFact struct {
	baseBallotSignedFact
}

func NewACCEPTBallotSignedFact(node base.Address, fact ACCEPTBallotFact) ACCEPTBallotSignedFact {
	return ACCEPTBallotSignedFact{
		baseBallotSignedFact: newBaseBallotSignedFact(ACCEPTBallotSignedFactHint, node, fact),
	}
}

func (sf ACCEPTBallotSignedFact) BallotFact() base.ACCEPTBallotFact {
	if sf.fact == nil {
		return nil
	}

	return sf.fact.(base.ACCEPTBallotFact) //nolint:forcetypeassert //...
}

func (sf ACCEPTBallotSignedFact) IsValid(networkID []byte) error {
	if err := base.IsValidACCEPTBallotSignedFact(sf, networkID); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid ACCEPTBallotSignedFact")
	}

	return nil
}
