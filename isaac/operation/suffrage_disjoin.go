package isaacoperation

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	SuffrageDisjoinFactHint = hint.MustNewHint("suffrage-disjoin-fact-v0.0.1")
	SuffrageDisjoinHint     = hint.MustNewHint("suffrage-disjoin-operation-v0.0.1")
)

type SuffrageDisjoinFact struct {
	node base.Address
	base.BaseFact
	start base.Height
}

func NewSuffrageDisjoinFact(
	token base.Token,
	node base.Address,
	start base.Height,
) SuffrageDisjoinFact {
	fact := SuffrageDisjoinFact{
		BaseFact: base.NewBaseFact(SuffrageDisjoinFactHint, token),
		node:     node,
		start:    start,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact SuffrageDisjoinFact) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid SuffrageDisjoinFact")

	if err := util.CheckIsValiders(nil, false, fact.BaseFact, fact.node, fact.start); err != nil {
		return e.Wrap(err)
	}

	if !fact.Hash().Equal(fact.hash()) {
		return e.Errorf("hash does not match")
	}

	return nil
}

func (fact SuffrageDisjoinFact) Node() base.Address {
	return fact.node
}

func (fact SuffrageDisjoinFact) Start() base.Height {
	return fact.start
}

func (fact SuffrageDisjoinFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.BytesToByter(fact.Token()),
		fact.node,
		fact.start,
	))
}

type SuffrageDisjoin struct {
	base.BaseNodeOperation
}

func NewSuffrageDisjoin(fact SuffrageDisjoinFact) SuffrageDisjoin {
	return SuffrageDisjoin{
		BaseNodeOperation: base.NewBaseNodeOperation(SuffrageDisjoinHint, fact),
	}
}

func (op *SuffrageDisjoin) SetToken(t base.Token) error {
	fact := op.Fact().(SuffrageDisjoinFact) //nolint:forcetypeassert //...

	if err := fact.SetToken(t); err != nil {
		return err
	}

	fact.SetHash(fact.hash())

	op.BaseNodeOperation.SetFact(fact)

	return nil
}

func (op SuffrageDisjoin) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid SuffrageDisjoin")

	if err := op.BaseNodeOperation.IsValid(networkID); err != nil {
		return e.Wrap(err)
	}

	var fact SuffrageDisjoinFact
	if err := util.SetInterfaceValue(op.Fact(), &fact); err != nil {
		return e.Wrap(err)
	}

	switch sfs := op.Signs(); {
	case len(sfs) > 1:
		return e.Errorf("multiple signs found")
	case !sfs[0].(base.NodeSign).Node().Equal(fact.Node()): //nolint:forcetypeassert //...
		return e.Errorf("not signed by node")
	}

	return nil
}
