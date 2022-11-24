package isaac

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	INITBallotSignFactHint   = hint.MustNewHint("init-ballot-sign-fact-v0.0.1")
	ACCEPTBallotSignFactHint = hint.MustNewHint("accept-ballot-sign-fact-v0.0.1")
)

type baseBallotSignFact struct {
	fact base.BallotFact
	sign base.BaseNodeSign
	util.DefaultJSONMarshaled
	hint.BaseHinter
}

func newBaseBallotSignFact(ht hint.Hint, fact base.BallotFact) baseBallotSignFact {
	return baseBallotSignFact{
		BaseHinter: hint.NewBaseHinter(ht),
		fact:       fact,
	}
}

func (sf baseBallotSignFact) Node() base.Address {
	return sf.sign.Node()
}

func (sf baseBallotSignFact) Signer() base.Publickey {
	return sf.sign.Signer()
}

func (sf baseBallotSignFact) Signs() []base.Sign {
	return []base.Sign{sf.sign}
}

func (sf baseBallotSignFact) Fact() base.Fact {
	return sf.fact
}

func (sf baseBallotSignFact) NodeSigns() []base.NodeSign {
	return []base.NodeSign{sf.sign}
}

func (baseBallotSignFact) IsValid([]byte) error {
	return nil
}

func (sf *baseBallotSignFact) NodeSign(priv base.Privatekey, networkID base.NetworkID, node base.Address) error {
	sign, err := base.NewBaseNodeSignFromFact(node, priv, networkID, sf.fact)
	if err != nil {
		return errors.Wrap(err, "failed to sign base ballot sign fact")
	}

	sf.sign = sign

	return nil
}

func (sf baseBallotSignFact) HashBytes() []byte {
	return util.ConcatByters(sf.BaseHinter, sf.sign)
}

type INITBallotSignFact struct {
	baseBallotSignFact
}

func NewINITBallotSignFact(fact base.INITBallotFact) INITBallotSignFact {
	return INITBallotSignFact{
		baseBallotSignFact: newBaseBallotSignFact(INITBallotSignFactHint, fact),
	}
}

func (sf INITBallotSignFact) BallotFact() base.INITBallotFact {
	if sf.fact == nil {
		return nil
	}

	return sf.fact.(base.INITBallotFact) //nolint:forcetypeassert //...
}

func (sf INITBallotSignFact) IsValid(networkID []byte) error {
	if err := base.IsValidINITBallotSignFact(sf, networkID); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid INITBallotSignFact")
	}

	return nil
}

type ACCEPTBallotSignFact struct {
	baseBallotSignFact
}

func NewACCEPTBallotSignFact(fact ACCEPTBallotFact) ACCEPTBallotSignFact {
	return ACCEPTBallotSignFact{
		baseBallotSignFact: newBaseBallotSignFact(ACCEPTBallotSignFactHint, fact),
	}
}

func (sf ACCEPTBallotSignFact) BallotFact() base.ACCEPTBallotFact {
	if sf.fact == nil {
		return nil
	}

	return sf.fact.(base.ACCEPTBallotFact) //nolint:forcetypeassert //...
}

func (sf ACCEPTBallotSignFact) IsValid(networkID []byte) error {
	if err := base.IsValidACCEPTBallotSignFact(sf, networkID); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid ACCEPTBallotSignFact")
	}

	return nil
}
