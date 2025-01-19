package main

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	accountOpFactHint = hint.MustNewHint("test-fact-v0.0.1")
	accountOpHint     = hint.MustNewHint("test-operation-v0.0.1")
)

type operation interface {
	base.Operation
	StateMergeValues(context.Context) (
		stateMergeValue, []stateMergeValue, string, error,
	)
}

type accountOpFact struct {
	Issuer   base.Publickey
	Receiver base.Publickey
	base.BaseFact
	Amount int64
}

func newAccountOpFact(token base.Token, amount int64, issuer, receiver base.Publickey) *accountOpFact {
	fact := &accountOpFact{
		BaseFact: base.NewBaseFact(accountOpFactHint, token),
		Issuer:   issuer,
		Receiver: receiver,
		Amount:   amount,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact *accountOpFact) IsValid([]byte) error {
	if err := fact.BaseFact.IsValid(nil); err != nil {
		return err
	}

	if err := util.CheckIsValiders(nil, false, fact.Receiver); err != nil {
		return err
	}

	if fact.Amount < 1 {
		return util.ErrInvalid.Errorf("under 1")
	}

	if fact.Issuer != nil {
		if err := fact.Issuer.IsValid(nil); err != nil {
			return err
		}
	}

	if !fact.Hash().Equal(fact.hash()) {
		return errors.Errorf("hash does not match")
	}

	return nil
}

func (fact *accountOpFact) hash() util.Hash {
	var ib []byte
	if fact.Issuer != nil {
		ib = fact.Issuer.Bytes()
	}

	return valuehash.NewSHA256(slices.Concat(
		fact.Token(),
		ib,
		fact.Receiver.Bytes(),
		util.Int64ToBytes(fact.Amount),
	))
}

type accountOpFactJSONMarshaler struct {
	Issuer   base.Publickey `json:"issuer"`
	Receiver base.Publickey `json:"receiver"`
	base.BaseFactJSONMarshaler
	Amount int64 `json:"amount"`
}

func (fact *accountOpFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(accountOpFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Issuer:                fact.Issuer,
		Receiver:              fact.Receiver,
		Amount:                fact.Amount,
	})
}

type accountOpFactJSONUnmarshaler struct {
	Issuer   string `json:"issuer"`
	Receiver string `json:"receiver"`
	base.BaseFactJSONUnmarshaler
	Amount int64 `json:"amount"`
}

func (fact *accountOpFact) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u accountOpFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	fact.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)

	if u.Issuer != "" {
		switch i, err := base.DecodePublickeyFromString(u.Issuer, enc); {
		case err != nil:
			return err
		default:
			fact.Issuer = i
		}
	}

	switch i, err := base.DecodePublickeyFromString(u.Receiver, enc); {
	case err != nil:
		return err
	default:
		fact.Receiver = i
	}

	fact.Amount = u.Amount

	return nil
}

type accountOp struct {
	base.BaseOperation
}

func newAccountOp(fact *accountOpFact) *accountOp {
	return &accountOp{
		BaseOperation: base.NewBaseOperation(accountOpHint, fact),
	}
}

func (op *accountOp) IsValid(networkID []byte) error {
	if err := op.BaseOperation.IsValid(networkID); err != nil {
		return err
	}

	if _, ok := op.Fact().(*accountOpFact); !ok {
		return errors.Errorf("not Fact, %T", op.Fact())
	}

	return nil
}

func (op *accountOp) StateMergeValues(context.Context) (
	stateMergeValue, []stateMergeValue, string, error,
) {
	var fact *accountOpFact

	if err := util.SetInterfaceValue(op.Fact(), &fact); err != nil {
		return nil, nil, "", err
	}

	var subv stateMergeValue
	var addvs []stateMergeValue

	if fact.Issuer != nil {
		switch i, err := newAccountStateMergeValue(fact.Issuer, fact.Amount*-1); {
		case err != nil:
			return nil, nil, "", err
		default:
			subv = i
		}
	}

	switch i, err := newAccountStateMergeValue(fact.Receiver, fact.Amount); {
	case err != nil:
		return nil, nil, "", err
	default:
		addvs = []stateMergeValue{i}
	}

	return subv, addvs, "", nil
}
