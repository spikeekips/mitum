package testopsworkers

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	OpFactHint = hint.MustNewHint("test-fact-v0.0.1")
	OpHint     = hint.MustNewHint("test-operation-v0.0.1")
)

type OpFact struct {
	Issuer   base.Publickey
	Receiver base.Publickey
	base.BaseFact
	Amount int64
}

func NewOpFact(token base.Token, amount int64, issuer, receiver base.Publickey) OpFact {
	fact := OpFact{
		BaseFact: base.NewBaseFact(OpFactHint, token),
		Issuer:   issuer,
		Receiver: receiver,
		Amount:   amount,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact OpFact) IsValid([]byte) error {
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

func (fact OpFact) hash() util.Hash {
	var ib []byte
	if fact.Issuer != nil {
		ib = fact.Issuer.Bytes()
	}

	return valuehash.NewSHA256(util.ConcatBytesSlice(
		fact.Token(),
		ib,
		fact.Receiver.Bytes(),
		util.Int64ToBytes(fact.Amount),
	))
}

type opFactJSONMarshaler struct {
	Issuer   base.Publickey `json:"issuer"`
	Receiver base.Publickey `json:"receiver"`
	base.BaseFactJSONMarshaler
	Amount int64 `json:"amount"`
}

func (fact OpFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(opFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Issuer:                fact.Issuer,
		Receiver:              fact.Receiver,
		Amount:                fact.Amount,
	})
}

type opFactJSONUnmarshaler struct {
	Issuer   string `json:"issuer"`
	Receiver string `json:"receiver"`
	base.BaseFactJSONUnmarshaler
	Amount int64 `json:"amount"`
}

func (fact *OpFact) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u opFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	fact.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)

	if len(u.Issuer) > 0 {
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

type Op struct {
	base.BaseOperation
}

func NewOp(fact OpFact) Op {
	return Op{
		BaseOperation: base.NewBaseOperation(OpHint, fact),
	}
}

func (op Op) IsValid(networkID []byte) error {
	if err := op.BaseOperation.IsValid(networkID); err != nil {
		return err
	}

	if _, ok := op.Fact().(OpFact); !ok {
		return errors.Errorf("not Fact, %T", op.Fact())
	}

	return nil
}

func (op Op) ProcessNew(context.Context) (
	[]StateMergeValue, base.OperationProcessReasonError, error,
) {
	var fact OpFact

	if err := util.SetInterfaceValue(op.Fact(), &fact); err != nil {
		return nil, nil, err
	}

	var values []StateMergeValue

	switch {
	case fact.Issuer != nil:
		values = make([]StateMergeValue, 2)
	default:
		values = make([]StateMergeValue, 1)
	}

	values[0], _ = NewAccountStateMergeValue(fact.Receiver, fact.Amount)

	if fact.Issuer != nil {
		values[1], _ = NewAccountStateMergeValue(fact.Issuer, fact.Amount*-1)
	}

	return values, nil, nil
}
