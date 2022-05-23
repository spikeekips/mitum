package base

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

// BaseOperation is basic form to make new Operation.
type BaseOperation struct {
	h      util.Hash
	fact   Fact
	signed []Signed
	hint.BaseHinter
}

func NewBaseOperation(ht hint.Hint, fact Fact) BaseOperation {
	return BaseOperation{
		BaseHinter: hint.NewBaseHinter(ht),
		fact:       fact,
	}
}

func (op BaseOperation) Hash() util.Hash {
	return op.h
}

func (op BaseOperation) Signed() []Signed {
	return op.signed
}

func (op BaseOperation) Fact() Fact {
	return op.fact
}

func (op BaseOperation) HashBytes() []byte {
	bs := make([]util.Byter, len(op.signed)+1)
	bs[0] = op.fact.Hash()

	for i := range op.signed {
		bs[i+1] = op.signed[i]
	}

	return util.ConcatByters(bs...)
}

func (op BaseOperation) IsValid(networkID []byte) error {
	e := util.StringErrorFunc("invalid BaseOperation")

	if len(op.signed) < 1 {
		return util.ErrInvalid.Errorf("empty signed")
	}

	vs := make([]util.IsValider, len(op.signed)+2)
	vs[0] = op.h
	vs[1] = op.fact

	duplicated := map[string]struct{}{}

	for i := range op.signed {
		s := op.signed[i]
		if s == nil {
			return e(util.ErrInvalid.Errorf("empty signed found"), "")
		}

		if _, found := duplicated[s.Signer().String()]; found {
			return e(util.ErrInvalid.Errorf("duplicated signed found"), "")
		}

		vs[i+2] = s
	}

	if err := util.CheckIsValid(networkID, false, vs...); err != nil {
		return e(err, "")
	}

	for i := range op.signed {
		if err := op.signed[i].Verify(networkID, op.fact.Hash().Bytes()); err != nil {
			return e(util.ErrInvalid.Wrap(err), "")
		}
	}

	if !op.h.Equal(op.hash()) {
		return e(util.ErrInvalid.Errorf("hash does not match"), "")
	}

	return nil
}

func (op *BaseOperation) Sign(priv Privatekey, networkID NetworkID) error {
	if err := op.sign(priv, networkID); err != nil {
		return err
	}

	op.h = op.hash()

	return nil
}

func (op *BaseOperation) sign(priv Privatekey, networkID NetworkID) error {
	e := util.StringErrorFunc("failed to sign BaseOperation")

	found := -1

	for i := range op.signed {
		s := op.signed[i]
		if s == nil {
			continue
		}

		if s.Signer().Equal(priv.Publickey()) {
			found = i

			break
		}
	}

	newsigned, err := NewBaseSignedFromFact(priv, networkID, op.fact)
	if err != nil {
		return e(err, "")
	}

	if found >= 0 {
		op.signed[found] = newsigned

		return nil
	}

	op.signed = append(op.signed, newsigned)

	return nil
}

func (BaseOperation) PreProcess(context.Context, GetStateFunc) (OperationProcessReasonError, error) {
	return nil, errors.Wrap(util.ErrNotImplemented, "")
}

func (BaseOperation) Process(context.Context, GetStateFunc) ([]StateMergeValue, OperationProcessReasonError, error) {
	return nil, nil, errors.Wrap(util.ErrNotImplemented, "")
}

func (op BaseOperation) hash() util.Hash {
	return valuehash.NewSHA256(op.HashBytes())
}
