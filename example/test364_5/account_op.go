//go:generate go run -v -race tool/set_base_hinter.go - $GOFILE

package dop5

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var AccountOpFactHint = hint.MustNewHint("test-fact-v0.0.1")

type OperationFact interface {
	base.Fact
	SMVs(context.Context) (SMV, []SMV, string, error)
}

type AccountOpFact struct {
	h        util.Hash
	Issuer   base.Publickey
	Receiver base.Publickey
	token    base.Token
	hint.BaseHinter
	Amount int64
}

var _ = interface{}(new(AccountOpFact)).(OperationFact)

func NewAccountOpFact(token base.Token, amount int64, issuer, receiver base.Publickey) *AccountOpFact {
	fact := &AccountOpFact{
		BaseHinter: hint.NewBaseHinter(AccountOpFactHint),
		token:      token,
		Issuer:     issuer,
		Receiver:   receiver,
		Amount:     amount,
	}

	fact.h = fact.hash()

	return fact
}

func (fact *AccountOpFact) Hash() util.Hash {
	return fact.h
}

func (fact *AccountOpFact) Token() base.Token {
	return fact.token
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
	if err := fact.BaseHinter.IsValid(fact.Hint().Type().Bytes()); err != nil {
		return err
	}

	if err := base.IsValidFact(fact, nil); err != nil {
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
	b, _ := dutil.EncodeInt(fact.Amount)
	w(b)

	return sum()
}

type accountOpFactJSONMarshaler struct {
	Hash     util.Hash      `json:"hash"`
	Issuer   base.Publickey `json:"issuer"`
	Receiver base.Publickey `json:"receiver"`
	Token    base.Token     `json:"token"`
	hint.BaseHinter
	Amount int64 `json:"amount"`
}

func (fact *AccountOpFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(accountOpFactJSONMarshaler{
		BaseHinter: fact.BaseHinter,
		Hash:       fact.h,
		Token:      fact.token,
		Issuer:     fact.Issuer,
		Receiver:   fact.Receiver,
		Amount:     fact.Amount,
	})
}

func AccountOpFactDecodeJSON(enc *jsonenc.Encoder, m map[string]any) (any, error) {
	fact := &AccountOpFact{}

	var token []byte

	if err := dutil.FuncChain(
		dutil.AssignJSONHash(m["hash"], &fact.h),
		dutil.AssignJSONBytes(m["token"], &token),
		AssignJSONPublickey(m["issuer"], &fact.Issuer, enc),
		AssignJSONPublickey(m["receiver"], &fact.Receiver, enc),
		dutil.AssignJSONInts(m["amount"], &fact.Amount),
	); err != nil {
		return nil, err
	}

	fact.token = token

	return fact, nil
}
