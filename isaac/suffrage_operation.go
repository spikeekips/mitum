package isaac

import (
	"bytes"

	"github.com/pkg/errors"
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
	end   base.Height
}

func NewSuffrageWithdrawFact(
	node base.Address,
	start base.Height,
	end base.Height,
) SuffrageWithdrawFact {
	fact := SuffrageWithdrawFact{
		// NOTE token should be node + start
		BaseFact: base.NewBaseFact(SuffrageWithdrawFactHint, base.Token(util.ConcatByters(node, start, end))),
		node:     node,
		start:    start,
		end:      end,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact SuffrageWithdrawFact) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid SuffrageWithdrawFact")

	switch {
	case fact.start <= base.GenesisHeight:
		return e.Errorf("invalid start height; should be over genesis height")
	case fact.start >= fact.end:
		return e.Errorf("invalid start and end height; end should be over start")
	case !bytes.Equal(fact.Token(), base.Token(util.ConcatByters(fact.node, fact.start, fact.end))):
		return e.Errorf("invalid token; should be node + start + end")
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

func (fact SuffrageWithdrawFact) WithdrawEnd() base.Height {
	return fact.end
}

func (fact SuffrageWithdrawFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.BytesToByter(fact.Token()),
		fact.node,
		fact.start,
		fact.end,
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

// IsValidWithdrawWithSuffrageLifespan checks withdraw operation itself with
// suffrage and lifespan.
func IsValidWithdrawWithSuffrageLifespan(
	height base.Height,
	withdraw base.SuffrageWithdrawOperation,
	suf base.Suffrage,
	lifespan base.Height,
) error {
	fact := withdraw.WithdrawFact()

	if fact.WithdrawEnd() > fact.WithdrawStart()+lifespan {
		return util.ErrInvalid.Errorf("invalid withdraw; wrong withdraw end")
	}

	return IsValidWithdrawWithSuffrage(height, withdraw, suf)
}

func IsValidWithdrawWithSuffrage(
	height base.Height,
	withdraw base.SuffrageWithdrawOperation,
	suf base.Suffrage,
) error {
	e := util.ErrInvalid.Errorf("invalid withdraw with suffrage")

	fact := withdraw.WithdrawFact()

	if height > fact.WithdrawEnd() {
		return errors.Errorf("withdraw expired")
	}

	if !suf.Exists(fact.Node()) {
		return e.Errorf("unknown withdraw node found, %q", fact.Node())
	}

	signs := withdraw.NodeSigns()

	for i := range signs {
		sign := signs[i]

		if !suf.ExistsPublickey(sign.Node(), sign.Signer()) {
			return e.Errorf("unknown node signed, %q", sign.Node())
		}
	}

	return nil
}
