package isaac

import (
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
	token base.Token,
	node base.Address,
	start base.Height, // FIXME check start height is behind last block
) SuffrageWithdrawFact {
	fact := SuffrageWithdrawFact{
		BaseFact: base.NewBaseFact(SuffrageWithdrawFactHint, token),
		node:     node,
		start:    start,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact SuffrageWithdrawFact) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid SuffrageWithdrawFact")

	if fact.start <= base.GenesisHeight {
		return e(util.ErrInvalid.Errorf("invalid start height; should be over genesis height"), "")
	}

	if err := util.CheckIsValiders(nil, false, fact.BaseFact, fact.node); err != nil {
		return e(err, "")
	}

	if !fact.Hash().Equal(fact.hash()) {
		return e(util.ErrInvalid.Errorf("hash does not match"), "")
	}

	return nil
}

func (fact SuffrageWithdrawFact) Node() base.Address {
	return fact.node
}

func (fact SuffrageWithdrawFact) Start() base.Height {
	return fact.start
}

func (fact SuffrageWithdrawFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.BytesToByter(fact.Token()),
		fact.node,
		fact.start,
	))
}

type SuffrageWithdraw struct {
	base.BaseNodeOperation
}

func NewSuffrageWithdraw(fact SuffrageWithdrawFact) SuffrageWithdraw {
	return SuffrageWithdraw{
		BaseNodeOperation: base.NewBaseNodeOperation(SuffrageWithdrawHint, fact),
	}
}

func (op *SuffrageWithdraw) SetToken(t base.Token) error {
	fact := op.Fact().(SuffrageWithdrawFact) //nolint:forcetypeassert //...

	if err := fact.SetToken(t); err != nil {
		return err
	}

	op.BaseNodeOperation.SetFact(fact)

	return nil
}

func (op SuffrageWithdraw) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid SuffrageWithdraw")

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

func (op SuffrageWithdraw) NodeSigns() []base.NodeSign {
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
