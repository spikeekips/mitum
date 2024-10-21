//go:build test
// +build test

package isaac

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	DummyOperationFactHint = hint.MustNewHint("dummy-operation-fact-v0.0.1")
	DummyOperationHint     = hint.MustNewHint("dummy-operation-v0.0.1")
)

type DummyOperationFact struct {
	hint.BaseHinter
	h     util.Hash
	token base.Token
	v     util.Byter
}

func NewDummyOperationFact(token base.Token, v util.Byter) DummyOperationFact {
	fact := DummyOperationFact{
		BaseHinter: hint.NewBaseHinter(DummyOperationFactHint),
		token:      token,
		v:          v,
	}
	fact.h = fact.generateHash()

	return fact
}

func (fact DummyOperationFact) IsValid([]byte) error {
	if err := util.CheckIsValiders(nil, false, fact.h, fact.token); err != nil {
		return util.ErrInvalid.WithMessage(err, "invalid DummyOperationFact")
	}

	if !fact.h.Equal(fact.generateHash()) {
		return util.ErrInvalid.Errorf("DummyOperationFact hash does not match")
	}

	return nil
}

func (fact DummyOperationFact) Hash() util.Hash {
	return fact.h
}

func (fact DummyOperationFact) Token() base.Token {
	return fact.token
}

func (fact *DummyOperationFact) UpdateHint(ht hint.Hint) {
	fact.BaseHinter = fact.BaseHinter.SetHint(ht).(hint.BaseHinter)
}

func (fact DummyOperationFact) generateHash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(fact.v, util.BytesToByter(fact.token)))
}

func (fact DummyOperationFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		hint.BaseHinter
		H     util.Hash
		Token base.Token
		V     []byte
	}{
		BaseHinter: fact.BaseHinter,
		H:          fact.h,
		Token:      fact.token,
		V:          fact.v.Bytes(),
	})
}

func (fact *DummyOperationFact) DecodeJSON(b []byte, _ encoder.Encoder) error {
	var u struct {
		H     valuehash.HashDecoder
		Token base.Token
		V     []byte
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	fact.h = u.H.Hash()
	fact.token = u.Token
	fact.v = util.BytesToByter(u.V)

	return nil
}

type DummyOperation struct {
	hint.BaseHinter
	util.DefaultExtensibleJSON
	h          util.Hash
	fact       DummyOperationFact
	sign       base.BaseSign
	preprocess func(context.Context, base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error)
	process    func(context.Context, base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error)
}

func NewDummyOperation(fact DummyOperationFact, priv base.Privatekey, networkID base.NetworkID) (DummyOperation, error) {
	sign, err := base.NewBaseSignFromFact(
		priv,
		networkID,
		fact,
	)
	if err != nil {
		return DummyOperation{}, errors.Wrap(err, "failed to sign DummyOperation")
	}

	return DummyOperation{
		BaseHinter: hint.NewBaseHinter(DummyOperationHint),
		h:          valuehash.RandomSHA256(), fact: fact, sign: sign,
	}, nil
}

func (op DummyOperation) Hash() util.Hash {
	return op.h
}

func (op DummyOperation) Signs() []base.Sign {
	return []base.Sign{op.sign}
}

func (op DummyOperation) Fact() base.Fact {
	return op.fact
}

func (op DummyOperation) HashBytes() []byte {
	return op.fact.h.Bytes()
}

func (op DummyOperation) IsValid([]byte) error {
	if err := op.fact.IsValid(nil); err != nil {
		return err
	}

	return nil
}

func (op DummyOperation) MarshalJSON() ([]byte, error) {
	if b, ok := op.MarshaledJSON(); ok {
		return b, nil
	}

	return util.MarshalJSON(struct {
		hint.HinterJSONHead
		H    util.Hash
		Fact DummyOperationFact
		Sign base.BaseSign
	}{
		HinterJSONHead: hint.NewHinterJSONHead(op.Hint()),
		H:              op.h,
		Fact:           op.fact,
		Sign:           op.sign,
	})
}

func (op *DummyOperation) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u struct {
		H    valuehash.HashDecoder
		Fact json.RawMessage
		Sign json.RawMessage
	}
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	op.h = u.H.Hash()

	if err := encoder.Decode(enc, u.Fact, &op.fact); err != nil {
		return err
	}

	var bs base.BaseSign
	switch err := bs.DecodeJSON(u.Sign, enc); {
	case err != nil:
		return err
	default:
		op.sign = bs
	}

	return nil
}

func (op DummyOperation) PreProcess(ctx context.Context, getStateFunc base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
	if op.preprocess == nil {
		return ctx, base.NewBaseOperationProcessReason("nil preprocess"), nil
	}

	return op.preprocess(ctx, getStateFunc)
}

func (op DummyOperation) Process(ctx context.Context, getStateFunc base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
	if op.process == nil {
		return nil, base.NewBaseOperationProcessReason("empty process"), nil
	}

	return op.process(ctx, getStateFunc)
}
