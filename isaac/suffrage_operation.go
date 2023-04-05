package isaac

import (
	"bytes"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	SuffrageExpelFactHint      = hint.MustNewHint("suffrage-expel-fact-v0.0.1")
	SuffrageExpelOperationHint = hint.MustNewHint("suffrage-expel-operation-v0.0.1")
)

type SuffrageExpelFact struct {
	reason string
	node   base.Address
	base.BaseFact
	start base.Height
	end   base.Height
}

func NewSuffrageExpelFact(
	node base.Address,
	start base.Height,
	end base.Height,
	reason string,
) SuffrageExpelFact {
	fact := SuffrageExpelFact{
		// NOTE token is <node + start + end>
		BaseFact: base.NewBaseFact(SuffrageExpelFactHint, base.Token(util.ConcatByters(node, start, end))),
		node:     node,
		start:    start,
		end:      end,
		reason:   reason,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact SuffrageExpelFact) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid SuffrageExpelFact")

	switch {
	case fact.start <= base.GenesisHeight:
		return e.Errorf("invalid start height; should be over genesis height")
	case fact.start > fact.end:
		return e.Errorf("invalid start and end height; end should be equal or over start")
	case !bytes.Equal(fact.Token(), base.Token(util.ConcatByters(fact.node, fact.start, fact.end))):
		return e.Errorf("invalid token; should be node + start + end")
	case len(strings.TrimSpace(fact.reason)) < 1:
		return e.Errorf("empty reason")
	}

	if err := util.CheckIsValiders(nil, false, fact.BaseFact, fact.node); err != nil {
		return e.Wrap(err)
	}

	if !fact.Hash().Equal(fact.hash()) {
		return e.Errorf("hash does not match")
	}

	return nil
}

func (fact SuffrageExpelFact) Node() base.Address {
	return fact.node
}

func (fact SuffrageExpelFact) ExpelStart() base.Height {
	return fact.start
}

func (fact SuffrageExpelFact) ExpelEnd() base.Height {
	return fact.end
}

func (fact SuffrageExpelFact) Reason() string {
	return fact.reason
}

func (fact SuffrageExpelFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.BytesToByter(fact.Token()),
		fact.node,
		fact.start,
		fact.end,
	))
}

type SuffrageExpelOperation struct {
	base.BaseNodeOperation
}

func NewSuffrageExpelOperation(fact SuffrageExpelFact) SuffrageExpelOperation {
	return SuffrageExpelOperation{
		BaseNodeOperation: base.NewBaseNodeOperation(SuffrageExpelOperationHint, fact),
	}
}

func (op *SuffrageExpelOperation) SetToken(base.Token) error {
	fact := op.Fact().(SuffrageExpelFact) //nolint:forcetypeassert //...

	// NOTE ignore given token
	t := base.Token(util.ConcatByters(fact.node, fact.start, fact.end))

	if err := fact.SetToken(t); err != nil {
		return err
	}

	fact.SetHash(fact.hash())

	op.BaseNodeOperation.SetFact(fact)

	return nil
}

func (op SuffrageExpelOperation) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid SuffrageExpelOperation")

	if _, ok := op.Fact().(SuffrageExpelFact); !ok {
		return e.Errorf("not SuffrageExpelFact, %T", op.Fact())
	}

	if err := op.BaseNodeOperation.IsValid(networkID); err != nil {
		return e.Wrap(err)
	}

	if len(op.NodeSigns()) < 1 {
		return e.Errorf("empty signs; valid node signs not found")
	}

	return nil
}

func (op SuffrageExpelOperation) NodeSigns() []base.NodeSign {
	signs := op.BaseNodeOperation.NodeSigns()
	if len(signs) < 1 {
		return nil
	}

	fact, ok := op.Fact().(SuffrageExpelFact)
	if !ok {
		return nil
	}

	return util.FilterSlice(signs, func(i base.NodeSign) bool {
		return !fact.Node().Equal(i.Node())
	})
}

func (op SuffrageExpelOperation) ExpelFact() base.SuffrageExpelFact {
	return op.Fact().(SuffrageExpelFact) //nolint:forcetypeassert //...
}

// IsValidExpelWithSuffrageLifespan checks expel operation itself with
// suffrage and lifespan.
func IsValidExpelWithSuffrageLifespan(
	height base.Height,
	expel base.SuffrageExpelOperation,
	suf base.Suffrage,
	lifespan base.Height,
) error {
	fact := expel.ExpelFact()

	if fact.ExpelEnd() > fact.ExpelStart()+lifespan {
		return util.ErrInvalid.Errorf("invalid expel; wrong expel end")
	}

	return IsValidExpelWithSuffrage(height, expel, suf)
}

func IsValidExpelWithSuffrage(
	height base.Height,
	expel base.SuffrageExpelOperation,
	suf base.Suffrage,
) error {
	e := util.ErrInvalid.Errorf("invalid expel with suffrage")

	fact := expel.ExpelFact()

	if height > fact.ExpelEnd() {
		return errors.Errorf("expel expired")
	}

	if !suf.Exists(fact.Node()) {
		return e.Errorf("unknown expel node found, %q", fact.Node())
	}

	signs := expel.NodeSigns()

	for i := range signs {
		sign := signs[i]

		if !suf.ExistsPublickey(sign.Node(), sign.Signer()) {
			return e.Errorf("unknown node signed, %q", sign.Node())
		}
	}

	return nil
}
