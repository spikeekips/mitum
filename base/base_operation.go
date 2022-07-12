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

func (op *BaseOperation) SetFact(fact Fact) {
	op.fact = fact
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
	e := util.ErrInvalid.Errorf("invalid BaseOperation")

	if len(op.signed) < 1 {
		return e.Errorf("empty signed")
	}

	if err := util.CheckIsValid(networkID, false, op.h); err != nil {
		return e.Wrap(err)
	}

	if err := IsValidSignedFact(op, networkID); err != nil {
		return e.Wrap(err)
	}

	if !op.h.Equal(op.hash()) {
		return e.Errorf("hash does not match")
	}

	return nil
}

func (op *BaseOperation) Sign(priv Privatekey, networkID NetworkID) error {
	switch index, signed, err := op.sign(priv, networkID); {
	case err != nil:
		return err
	case index < 0:
		op.signed = append(op.signed, signed)
	default:
		op.signed[index] = signed
	}

	op.h = op.hash()

	return nil
}

func (op *BaseOperation) sign(priv Privatekey, networkID NetworkID) (found int, signed BaseSigned, _ error) {
	e := util.StringErrorFunc("failed to sign BaseOperation")

	found = -1

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
		return found, signed, e(err, "")
	}

	return found, newsigned, nil
}

func (BaseOperation) PreProcess(context.Context, GetStateFunc) (OperationProcessReasonError, error) {
	return nil, errors.WithStack(util.ErrNotImplemented)
}

func (BaseOperation) Process(context.Context, GetStateFunc) ([]StateMergeValue, OperationProcessReasonError, error) {
	return nil, nil, errors.WithStack(util.ErrNotImplemented)
}

func (op BaseOperation) hash() util.Hash {
	return valuehash.NewSHA256(op.HashBytes())
}

type BaseNodeOperation struct {
	BaseOperation
}

func NewBaseNodeOperation(ht hint.Hint, fact Fact) BaseNodeOperation {
	return BaseNodeOperation{
		BaseOperation: NewBaseOperation(ht, fact),
	}
}

func (op BaseNodeOperation) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid BaseNodeOperation")

	if err := op.BaseOperation.IsValid(networkID); err != nil {
		return e.Wrap(err)
	}

	sfs := op.Signed()

	var duplicatederr error

	switch _, duplicated := util.CheckSliceDuplicated(sfs, func(_ interface{}, i int) string {
		if duplicatederr != nil {
			return ""
		}

		switch ns, ok := sfs[i].(NodeSigned); {
		case !ok:
			duplicatederr = errors.Errorf("not NodeSigned, %T", sfs[i])

			return ""
		default:
			return ns.Node().String()
		}
	}); {
	case duplicatederr != nil:
		return e.Wrap(duplicatederr)
	case duplicated:
		return e.Errorf("duplicated signed found")
	}

	for i := range sfs {
		if _, ok := sfs[i].(NodeSigned); !ok {
			return e.Errorf("not NodeSigned, %T", sfs[i])
		}
	}

	return nil
}

func (op *BaseNodeOperation) Sign(priv Privatekey, networkID NetworkID, node Address) error {
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

	ns, err := BaseNodeSignedFromFact(node, priv, networkID, op.fact)
	if err != nil {
		return err
	}

	switch {
	case found < 0:
		op.signed = append(op.signed, ns)
	default:
		op.signed[found] = ns
	}

	op.h = op.hash()

	return nil
}

func (op BaseNodeOperation) NodeSigned() []NodeSigned {
	ss := op.Signed()
	signeds := make([]NodeSigned, len(ss))

	for i := range ss {
		signeds[i] = ss[i].(NodeSigned) //nolint:forcetypeassert //...
	}

	return signeds
}
