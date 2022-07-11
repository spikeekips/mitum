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

type NodeSigned interface {
	Signed
	Node() Address
}

type BaseSigned struct {
	signedAt  time.Time
	signer    Publickey
	signature Signature
}

func NewBaseSigned(signer Publickey, signature Signature, signedAt time.Time) BaseSigned {
	return BaseSigned{
		signer:    signer,
		signature: signature,
		signedAt:  signedAt,
	}
}

func NewBaseSignedFromFact(priv Privatekey, networkID NetworkID, fact Fact) (BaseSigned, error) {
	if fact == nil || fact.Hash() == nil {
		return BaseSigned{}, util.ErrInvalid.Errorf("failed to make BaseSigned; empty fact")
	}

	return NewBaseSignedFromBytes(priv, networkID, fact.Hash().Bytes())
}

func NewBaseSignedFromBytes(priv Privatekey, networkID NetworkID, b []byte) (BaseSigned, error) {
	now := localtime.New(localtime.Now())

	sig, err := priv.Sign(util.ConcatBytesSlice(networkID, b, now.Bytes()))
	if err != nil {
		return BaseSigned{}, errors.Wrap(err, "failed to generate BaseSign")
	}

	return NewBaseSigned(priv.Publickey(), sig, now.Time), nil
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
	return util.ConcatByters(si.signer, si.signature, localtime.New(si.signedAt))
}

func (si BaseSigned) IsValid([]byte) error {
	if err := util.CheckIsValid(nil, false,
		si.signer,
		si.signature,
		util.DummyIsValider(func([]byte) error {
			if si.signedAt.IsZero() {
				return util.ErrInvalid.Errorf("empty signedAt in BaseSign")
			}

			return nil
		}),
	); err != nil {
		return errors.Wrap(err, "invalid BaseSign")
	}

	return nil
}

func (si BaseSigned) Verify(networkID NetworkID, b []byte) error {
	if err := si.signer.Verify(util.ConcatBytesSlice(
		networkID,
		b,
		localtime.New(si.signedAt).Bytes(),
	), si.signature); err != nil {
		return errors.Wrap(err, "failed to verfiy sign")
	}

	return nil
}

type BaseNodeSigned struct {
	node Address
	BaseSigned
}

func NewBaseNodeSigned(node Address, signer Publickey, signature Signature, signedAt time.Time) BaseNodeSigned {
	return BaseNodeSigned{
		BaseSigned: NewBaseSigned(signer, signature, signedAt),
		node:       node,
	}
}

func BaseNodeSignedFromFact(node Address, priv Privatekey, networkID NetworkID, fact Fact) (BaseNodeSigned, error) {
	if fact == nil || fact.Hash() == nil {
		return BaseNodeSigned{}, util.ErrInvalid.Errorf("failed to make BaseSigned; empty fact")
	}

	return BaseNodeSignedFromBytes(node, priv, networkID, fact.Hash().Bytes())
}

func BaseNodeSignedFromBytes(node Address, priv Privatekey, networkID NetworkID, b []byte) (BaseNodeSigned, error) {
	si, err := NewBaseSignedFromBytes(priv, networkID, util.ConcatByters(node, util.BytesToByter(b)))
	if err != nil {
		return BaseNodeSigned{}, errors.Wrap(err, "failed to create BaseNodeSigned from bytes")
	}

	return BaseNodeSigned{
		BaseSigned: si,
		node:       node,
	}, nil
}

func (si BaseNodeSigned) Node() Address {
	return si.node
}

func (si BaseNodeSigned) Bytes() []byte {
	return util.ConcatByters(si.node, si.BaseSigned)
}

func (si BaseNodeSigned) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid BaseNodeSigned")

	if err := util.CheckIsValid(nil, false, si.node); err != nil {
		return e(err, "invalid node")
	}

	if err := si.BaseSigned.IsValid(nil); err != nil {
		return e(err, "")
	}

	return nil
}

func (si BaseNodeSigned) Verify(networkID NetworkID, b []byte) error {
	return si.BaseSigned.Verify(networkID, util.ConcatByters(si.node, util.BytesToByter(b)))
}

func CheckFactSignsBySuffrage(suf Suffrage, threshold Threshold, signs []NodeSigned) error {
	var signed float64

	for i := range signs {
		s := signs[i]

		if suf.ExistsPublickey(s.Node(), s.Signer()) {
			signed++
		}
	}

	if (signed/float64(suf.Len()))*100 < threshold.Float64() {
		return errors.Errorf("not enough signs")
	}

	return nil
}
