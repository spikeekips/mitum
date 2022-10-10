package base

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/localtime"
)

type Signer interface {
	Sign(Privatekey, NetworkID /* or with additional data */) error
}

type NodeSigner interface {
	NodeSign(Privatekey, NetworkID, Address) error
	AddNodeSigns([]NodeSign) (added bool, _ error)
	SetNodeSigns([]NodeSign) error
}

type Sign interface {
	util.Byter
	util.IsValider
	Signer() Publickey
	Signature() Signature
	SignedAt() time.Time
	Verify(NetworkID, []byte) error
}

type NodeSign interface {
	Sign
	Node() Address
}

type BaseSign struct {
	signedAt  time.Time
	signer    Publickey
	signature Signature
}

func NewBaseSign(signer Publickey, signature Signature, signedAt time.Time) BaseSign {
	return BaseSign{
		signer:    signer,
		signature: signature,
		signedAt:  signedAt,
	}
}

func NewBaseSignFromFact(priv Privatekey, networkID NetworkID, fact Fact) (BaseSign, error) {
	if fact == nil || fact.Hash() == nil {
		return BaseSign{}, util.ErrInvalid.Errorf("failed to make BaseSign; empty fact")
	}

	return NewBaseSignFromBytes(priv, networkID, fact.Hash().Bytes())
}

func NewBaseSignFromBytes(priv Privatekey, networkID NetworkID, b []byte) (BaseSign, error) {
	now := localtime.New(localtime.UTCNow())

	sig, err := priv.Sign(util.ConcatBytesSlice(networkID, b, now.Bytes()))
	if err != nil {
		return BaseSign{}, errors.Wrap(err, "failed to generate BaseSign")
	}

	return NewBaseSign(priv.Publickey(), sig, now.Time), nil
}

func (si BaseSign) Signer() Publickey {
	return si.signer
}

func (si BaseSign) Signature() Signature {
	return si.signature
}

func (si BaseSign) SignedAt() time.Time {
	return si.signedAt
}

func (si BaseSign) Bytes() []byte {
	return util.ConcatByters(si.signer, si.signature, localtime.New(si.signedAt))
}

func (si BaseSign) IsValid([]byte) error {
	if err := util.CheckIsValiders(nil, false,
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

func (si BaseSign) Verify(networkID NetworkID, b []byte) error {
	if err := si.signer.Verify(util.ConcatBytesSlice(
		networkID,
		b,
		localtime.New(si.signedAt).Bytes(),
	), si.signature); err != nil {
		return errors.Wrap(err, "failed to verfiy sign")
	}

	return nil
}

type BaseNodeSign struct {
	node Address
	BaseSign
}

func NewBaseNodeSign(node Address, signer Publickey, signature Signature, signedAt time.Time) BaseNodeSign {
	return BaseNodeSign{
		BaseSign: NewBaseSign(signer, signature, signedAt),
		node:     node,
	}
}

func BaseNodeSignFromFact(node Address, priv Privatekey, networkID NetworkID, fact Fact) (BaseNodeSign, error) {
	if fact == nil || fact.Hash() == nil {
		return BaseNodeSign{}, util.ErrInvalid.Errorf("failed to make BaseSign; empty fact")
	}

	return BaseNodeSignFromBytes(node, priv, networkID, fact.Hash().Bytes())
}

func BaseNodeSignFromBytes(node Address, priv Privatekey, networkID NetworkID, b []byte) (BaseNodeSign, error) {
	si, err := NewBaseSignFromBytes(priv, networkID, util.ConcatByters(node, util.BytesToByter(b)))
	if err != nil {
		return BaseNodeSign{}, errors.Wrap(err, "failed to create BaseNodeSign from bytes")
	}

	return BaseNodeSign{
		BaseSign: si,
		node:     node,
	}, nil
}

func (si BaseNodeSign) Node() Address {
	return si.node
}

func (si BaseNodeSign) Bytes() []byte {
	return util.ConcatByters(si.node, si.BaseSign)
}

func (si BaseNodeSign) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid BaseNodeSign")

	if err := util.CheckIsValiders(nil, false, si.node); err != nil {
		return e.Wrapf(err, "invalid node")
	}

	if err := si.BaseSign.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (si BaseNodeSign) Verify(networkID NetworkID, b []byte) error {
	return si.BaseSign.Verify(networkID, util.ConcatByters(si.node, util.BytesToByter(b)))
}

func CheckFactSignsBySuffrage(suf Suffrage, threshold Threshold, signs []NodeSign) error {
	var sign float64

	for i := range signs {
		s := signs[i]

		if suf.ExistsPublickey(s.Node(), s.Signer()) {
			sign++
		}
	}

	if (sign/float64(suf.Len()))*100 < threshold.Float64() {
		return errors.Errorf("not enough signs")
	}

	return nil
}
