//go:build test
// +build test

package isaac

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	DummyOperationFactHint = hint.MustNewHint("dummy-operation-fact-v0.0.1")
	DummyOperationHint     = hint.MustNewHint("dummy-operation-v0.0.1")
)

type DummyOperationFact struct {
	h     util.Hash
	token base.Token
	v     util.Byter
}

func NewDummyOperationFact(token base.Token, v util.Byter) DummyOperationFact {
	fact := DummyOperationFact{
		token: token,
		v:     v,
	}
	fact.h = fact.hash()

	return fact
}

func (fact DummyOperationFact) Hint() hint.Hint {
	return DummyOperationFactHint
}

func (fact DummyOperationFact) IsValid([]byte) error {
	if err := util.CheckIsValid(nil, false, fact.h, fact.token); err != nil {
		return util.InvalidError.Wrapf(err, "invalid DummyOperationFact")
	}

	if !fact.h.Equal(fact.hash()) {
		return util.InvalidError.Errorf("DummyOperationFact hash does not match")
	}

	return nil
}

func (fact DummyOperationFact) Hash() util.Hash {
	return fact.h
}

func (fact DummyOperationFact) Token() base.Token {
	return fact.token
}

func (fact DummyOperationFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(fact.v, util.BytesToByter(fact.token)))
}

func (fact DummyOperationFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		hint.HintedJSONHead
		H     util.Hash
		Token base.Token
		V     []byte
	}{
		HintedJSONHead: hint.NewHintedJSONHead(fact.Hint()),
		H:              fact.h,
		Token:          fact.token,
		V:              fact.v.Bytes(),
	})
}

func (fact *DummyOperationFact) UnmarshalJSON(b []byte) error {
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
	fact   DummyOperationFact
	signed base.BaseSigned
}

func NewDummyOperation(fact DummyOperationFact, priv base.Privatekey, networkID base.NetworkID) (DummyOperation, error) {
	signed, err := base.BaseSignedFromFact(
		priv,
		networkID,
		fact,
	)
	if err != nil {
		return DummyOperation{}, errors.Wrap(err, "failed to sign DummyOperation")
	}

	return DummyOperation{fact: fact, signed: signed}, nil
}

func (op DummyOperation) Hint() hint.Hint {
	return DummyOperationHint
}

func (op DummyOperation) Signed() []base.Signed {
	return []base.Signed{op.signed}
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
	return util.MarshalJSON(struct {
		hint.HintedJSONHead
		Fact   DummyOperationFact
		Signed base.BaseSigned
	}{
		HintedJSONHead: hint.NewHintedJSONHead(op.Hint()),
		Fact:           op.fact,
		Signed:         op.signed,
	})
}

func (op *DummyOperation) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	var u struct {
		Fact   DummyOperationFact
		Signed json.RawMessage
	}
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	op.fact = u.Fact

	var bs base.BaseSigned
	switch err := bs.DecodeJSON(u.Signed, enc); {
	case err != nil:
		return err
	default:
		op.signed = bs
	}

	return nil
}

type DummyOperationProcessable struct {
	DummyOperation
	preprocess func(context.Context, base.StatePool) (base.OperationProcessReasonError, error)
	process    func(context.Context, base.StatePool) ([]base.State, base.OperationProcessReasonError, error)
}

func NewDummyOperationProcessable(fact DummyOperationFact, priv base.Privatekey, networkID base.NetworkID) (DummyOperationProcessable, error) {
	op, err := NewDummyOperation(fact, priv, networkID)
	if err != nil {
		return DummyOperationProcessable{}, err
	}

	return DummyOperationProcessable{
		DummyOperation: op,
	}, nil
}

func (op DummyOperationProcessable) PreProcess(ctx context.Context, sp base.StatePool) (base.OperationProcessReasonError, error) {
	if op.preprocess == nil {
		return base.NewBaseOperationProcessReasonError("nil preprocess"), nil
	}

	return op.preprocess(ctx, sp)
}

func (op DummyOperationProcessable) Process(ctx context.Context, sp base.StatePool) ([]base.State, base.OperationProcessReasonError, error) {
	if op.process == nil {
		return nil, base.NewBaseOperationProcessReasonError("empty process"), nil
	}

	return op.process(ctx, sp)
}
