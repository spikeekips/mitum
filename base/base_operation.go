package base

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"golang.org/x/exp/slices"
)

// BaseOperation is basic form to make new Operation.
type BaseOperation struct {
	h     util.Hash
	fact  Fact
	signs []Sign
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

func (op BaseOperation) Signs() []Sign {
	return op.signs
}

func (op BaseOperation) Fact() Fact {
	return op.fact
}

func (op *BaseOperation) SetFact(fact Fact) {
	op.fact = fact
}

func (op BaseOperation) HashBytes() []byte {
	bs := make([]util.Byter, len(op.signs)+1)
	bs[0] = op.fact.Hash()

	for i := range op.signs {
		bs[i+1] = op.signs[i]
	}

	return util.ConcatByters(bs...)
}

func (op BaseOperation) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid BaseOperation")

	if len(op.signs) < 1 {
		return e.Errorf("empty signs")
	}

	if err := util.CheckIsValiders(networkID, false, op.h); err != nil {
		return e.Wrap(err)
	}

	if err := IsValidSignFact(op, networkID); err != nil {
		return e.Wrap(err)
	}

	if !op.h.Equal(op.hash()) {
		return e.Errorf("hash does not match")
	}

	return nil
}

func (op *BaseOperation) Sign(priv Privatekey, networkID NetworkID) error {
	switch index, sign, err := op.sign(priv, networkID); {
	case err != nil:
		return err
	case index < 0:
		op.signs = append(op.signs, sign)
	default:
		op.signs[index] = sign
	}

	op.h = op.hash()

	return nil
}

func (op *BaseOperation) sign(priv Privatekey, networkID NetworkID) (found int, sign BaseSign, _ error) {
	e := util.StringError("sign BaseOperation")

	found = -1

	for i := range op.signs {
		s := op.signs[i]
		if s == nil {
			continue
		}

		if s.Signer().Equal(priv.Publickey()) {
			found = i

			break
		}
	}

	newsign, err := NewBaseSignFromFact(priv, networkID, op.fact)
	if err != nil {
		return found, sign, e.Wrap(err)
	}

	return found, newsign, nil
}

func (BaseOperation) PreProcess(ctx context.Context, _ GetStateFunc) (
	context.Context, OperationProcessReasonError, error,
) {
	return ctx, nil, errors.WithStack(util.ErrNotImplemented)
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

	sfs := op.Signs()

	var duplicatederr error

	switch duplicated := util.IsDuplicatedSlice(sfs, func(i Sign) (bool, string) {
		if i == nil {
			return true, ""
		}

		switch ns, err := util.AssertInterfaceValue[NodeSign](i); {
		case err != nil:
			duplicatederr = err

			return false, ""
		default:
			return true, ns.Node().String()
		}
	}); {
	case duplicatederr != nil:
		return e.Wrap(duplicatederr)
	case duplicated:
		return e.Errorf("duplicated signs found")
	}

	for i := range sfs {
		if _, err := util.AssertInterfaceValue[NodeSign](sfs[i]); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func (op *BaseNodeOperation) NodeSign(priv Privatekey, networkID NetworkID, node Address) error {
	found := -1

	for i := range op.signs {
		s := op.signs[i].(NodeSign) //nolint:forcetypeassert //...
		if s == nil {
			continue
		}

		if s.Node().Equal(node) {
			found = i

			break
		}
	}

	ns, err := NewBaseNodeSignFromFact(node, priv, networkID, op.fact)
	if err != nil {
		return err
	}

	switch {
	case found < 0:
		op.signs = append(op.signs, ns)
	default:
		op.signs[found] = ns
	}

	op.h = op.hash()

	return nil
}

func (op *BaseNodeOperation) SetNodeSigns(signs []NodeSign) error {
	if util.IsDuplicatedSlice(signs, func(i NodeSign) (bool, string) {
		if i == nil {
			return true, ""
		}

		return true, i.Node().String()
	}) {
		return errors.Errorf("duplicated signs found")
	}

	op.signs = make([]Sign, len(signs))
	for i := range signs {
		op.signs[i] = signs[i]
	}

	op.h = op.hash()

	return nil
}

func (op *BaseNodeOperation) AddNodeSigns(signs []NodeSign) (added bool, _ error) {
	updates := util.FilterSlice(signs, func(sign NodeSign) bool {
		return slices.IndexFunc(op.signs, func(s Sign) bool {
			nodesign, ok := s.(NodeSign)
			if !ok {
				return false
			}

			return sign.Node().Equal(nodesign.Node())
		}) < 0
	})

	if len(updates) < 1 {
		return false, nil
	}

	mergedsigns := make([]Sign, len(op.signs)+len(updates))
	copy(mergedsigns, op.signs)

	for i := range updates {
		mergedsigns[len(op.signs)+i] = updates[i]
	}

	op.signs = mergedsigns
	op.h = op.hash()

	return true, nil
}

func (op BaseNodeOperation) NodeSigns() []NodeSign {
	ss := op.Signs()
	signs := make([]NodeSign, len(ss))

	for i := range ss {
		signs[i] = ss[i].(NodeSign) //nolint:forcetypeassert //...
	}

	return signs
}
