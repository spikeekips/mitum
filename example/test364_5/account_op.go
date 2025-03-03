package dop5

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

var AccountOpFactHint = hint.MustNewHint("test-fact-v0.0.1")

type OperationFact interface {
	base.Fact
	SMVs(context.Context) (SMV, []SMV, string, error)
}

type AccountOpFact struct {
	Issuer   base.Publickey
	Receiver base.Publickey
	base.BaseFact
	Amount int64
}

var _ = interface{}(new(AccountOpFact)).(OperationFact)

func NewAccountOpFact(token base.Token, amount int64, issuer, receiver base.Publickey) *AccountOpFact {
	fact := &AccountOpFact{
		BaseFact: base.NewBaseFact(AccountOpFactHint, token),
		Issuer:   issuer,
		Receiver: receiver,
		Amount:   amount,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact *AccountOpFact) SMVs(context.Context) (SMV, []SMV, string, error) {
	var subv SMV
	var addvs []SMV

	if fact.Issuer != nil {
		switch i, err := NewAccountSMV(fact.Issuer, fact.Amount*-1); {
		case err != nil:
			return nil, nil, "", err
		default:
			subv = i
		}
	}

	switch i, err := NewAccountSMV(fact.Receiver, fact.Amount); {
	case err != nil:
		return nil, nil, "", err
	default:
		addvs = []SMV{i}
	}

	return subv, addvs, "", nil
}

func (fact *AccountOpFact) IsValid([]byte) error {
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

func (fact *AccountOpFact) hash() util.Hash {
	w, sum := dutil.NewSHA256HashWriter()

	w(fact.Token())

	if fact.Issuer != nil {
		w(fact.Issuer.Bytes())
	}

	w(fact.Receiver.Bytes())
	w(util.Int64ToBytes(fact.Amount))

	return sum()
}

type accountOpFactJSONMarshaler struct {
	Issuer   base.Publickey `json:"issuer"`
	Receiver base.Publickey `json:"receiver"`
	base.BaseFactJSONMarshaler
	Amount int64 `json:"amount"`
}

func (fact *AccountOpFact) MarshalJSON() ([]byte, error) {
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

func (fact *AccountOpFact) DecodeJSON(b []byte, enc encoder.Encoder) error {
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
