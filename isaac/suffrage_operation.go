package isaac

import (
	"bytes"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	SuffrageWithdrawFactHint = hint.MustNewHint("suffrage-withdraw-fact-v0.0.1")
	SuffrageWithdrawHint     = hint.MustNewHint("suffrage-withdraw-v0.0.1")
)

type SuffrageWithdrawFact struct {
	node base.Address
	base.BaseFact
	start base.Height
}

func NewSuffrageWithdrawFact(
	node base.Address,
	start base.Height, // FIXME check start height is behind last block
) SuffrageWithdrawFact {
	fact := SuffrageWithdrawFact{
		// NOTE token should be node + start
		BaseFact: base.NewBaseFact(SuffrageWithdrawFactHint, base.Token(util.ConcatByters(node, start))),
		node:     node,
		start:    start,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact SuffrageWithdrawFact) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid SuffrageWithdrawFact")

	switch {
	case fact.start <= base.GenesisHeight:
		return e.Errorf("invalid start height; should be over genesis height")
	case !bytes.Equal(fact.Token(), base.Token(util.ConcatByters(fact.node, fact.start))):
		return e.Errorf("invalid token; should be node + start")
	}

	if err := util.CheckIsValiders(nil, false, fact.BaseFact, fact.node); err != nil {
		return e.Wrap(err)
	}

	if !fact.Hash().Equal(fact.hash()) {
		return e.Errorf("hash does not match")
	}

	return nil
}

func (fact SuffrageWithdrawFact) Node() base.Address {
	return fact.node
}

func (fact SuffrageWithdrawFact) WithdrawStart() base.Height {
	return fact.start
}

func (fact SuffrageWithdrawFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.BytesToByter(fact.Token()),
		fact.node,
		fact.start,
	))
}

type SuffrageWithdrawOperation struct {
	base.BaseNodeOperation
}

func NewSuffrageWithdrawOperation(fact SuffrageWithdrawFact) SuffrageWithdrawOperation {
	return SuffrageWithdrawOperation{
		BaseNodeOperation: base.NewBaseNodeOperation(SuffrageWithdrawHint, fact),
	}
}

func (op *SuffrageWithdrawOperation) SetToken(t base.Token) error {
	fact := op.Fact().(SuffrageWithdrawFact) //nolint:forcetypeassert //...

	if err := fact.SetToken(t); err != nil {
		return err
	}

	op.BaseNodeOperation.SetFact(fact)

	return nil
}

func (op SuffrageWithdrawOperation) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid SuffrageWithdrawOperation")

	if _, ok := op.Fact().(SuffrageWithdrawFact); !ok {
		return e.Errorf("not SuffrageWithdrawFact, %T", op.Fact())
	}

	if err := op.BaseNodeOperation.IsValid(networkID); err != nil {
		return e.Wrap(err)
	}

	if len(op.NodeSigns()) < 1 {
		return e.Errorf("empty signs; withdraw target node sign found")
	}

	return nil
}

func (op SuffrageWithdrawOperation) NodeSigns() []base.NodeSign {
	signs := op.BaseNodeOperation.NodeSigns()
	if len(signs) < 1 {
		return nil
	}

	fact, ok := op.Fact().(SuffrageWithdrawFact)
	if !ok {
		return nil
	}

	return util.FilterSlices(signs, func(_ interface{}, i int) bool {
		return !fact.Node().Equal(signs[i].Node())
	})
}

func (op SuffrageWithdrawOperation) WithdrawFact() base.SuffrageWithdrawFact {
	return op.Fact().(SuffrageWithdrawFact) //nolint:forcetypeassert //...
}
