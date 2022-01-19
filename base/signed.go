package base

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/localtime"
)

type Signed interface {
	util.Byter
	util.IsValider
	Signer() Publickey
	Signature() Signature
	SignedAt() time.Time
	Verify(NetworkID, []byte) error
}

type BaseSigned struct {
	signer    Publickey
	signature Signature
	signedAt  time.Time
}

func NewBaseSigned(signer Publickey, signature Signature, signedAt time.Time) BaseSigned {
	return BaseSigned{
		signer:    signer,
		signature: signature,
		signedAt:  signedAt,
	}
}

func BaseSignedFromFact(priv Privatekey, networkID NetworkID, fact Fact) (BaseSigned, error) {
	if fact == nil || fact.Hash() == nil {
		return BaseSigned{}, util.InvalidError.Errorf("failed to make BaseSigned; empty fact")
	}

	return BaseSignedFromBytes(priv, networkID, fact.Hash().Bytes())
}

func BaseSignedFromBytes(priv Privatekey, networkID NetworkID, b []byte) (BaseSigned, error) {
	sig, err := priv.Sign(util.ConcatBytesSlice(networkID, b))
	if err != nil {
		return BaseSigned{}, errors.Wrap(err, "failed to generate BaseSign")
	}

	return BaseSigned{
		signer:    priv.Publickey(),
		signature: sig,
		signedAt:  localtime.Now(),
	}, nil
}

func (si BaseSigned) Signer() Publickey {
	return si.signer
}

func (si BaseSigned) Signature() Signature {
	return si.signature
}

func (si BaseSigned) SignedAt() time.Time {
	return si.signedAt
}

func (si BaseSigned) Bytes() []byte {
	return util.ConcatByters(si.signer, si.signature, localtime.NewTime(si.signedAt))
}

func (si BaseSigned) IsValid([]byte) error {
	if err := util.CheckIsValid(nil, false,
		si.signer,
		si.signature,
		util.DummyIsValider(func([]byte) error {
			if si.signedAt.IsZero() {
				return util.InvalidError.Errorf("empty signedAt in BaseSign")
			}

			return nil
		}),
	); err != nil {
		return errors.Wrap(err, "invalid BaseSign")
	}

	return nil
}

func (si BaseSigned) Verify(networkID NetworkID, input []byte) error {
	if err := si.signer.Verify(util.ConcatBytesSlice(networkID, input), si.signature); err != nil {
		return errors.Wrap(err, "failed to verfiy sign")
	}

	return nil
}
