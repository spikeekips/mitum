package isaacoperation

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	SuffrageUpdateFactHint = hint.MustNewHint("suffrage-update-operation-fact-v0.0.1")
	SuffrageUpdateHint     = hint.MustNewHint("suffrage-update-operation-v0.0.1")
)

type SuffrageUpdateFact struct {
	base.BaseFact
	news []base.Node
	outs []base.Address
}

func NewSuffrageUpdateFact(t base.Token, news []base.Node, outs []base.Address) SuffrageUpdateFact {
	fact := SuffrageUpdateFact{
		BaseFact: base.NewBaseFact(SuffrageUpdateFactHint, t),
		news:     news,
		outs:     outs,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact SuffrageUpdateFact) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid SuffrageUpdateFact")

	if err := fact.BaseFact.IsValid(nil); err != nil {
		return e(err, "")
	}

	if len(fact.news) < 1 && len(fact.outs) < 1 {
		return e(util.InvalidError.Errorf("empty new members and out members"), "")
	}

	sl := make([]base.Address, len(fact.news)+len(fact.outs))
	for i := range fact.news {
		n := fact.news[i]
		if n == nil {
			continue
		}

		sl[i] = n.Address()
	}

	copy(sl[len(fact.news):], fact.outs)

	if util.CheckSliceDuplicated(sl, func(i interface{}) string {
		return i.(base.Address).String()
	}) {
		return e(util.InvalidError.Errorf("duplciated member found in new members and out members"), "")
	}

	if !fact.Hash().Equal(fact.hash()) {
		return e(util.InvalidError.Errorf("hash does not match"), "")
	}

	return nil
}

func (fact SuffrageUpdateFact) NewMembers() []base.Node {
	return fact.news
}

func (fact SuffrageUpdateFact) OutMembers() []base.Address {
	return fact.outs
}

func (fact SuffrageUpdateFact) hash() util.Hash {
	bs := make([]util.Byter, len(fact.news)+len(fact.outs)+1)
	bs[0] = util.BytesToByter([]byte(fact.Token()))
	for i := range fact.news {
		n := fact.news[i]
		if n == nil {
			continue
		}

		bs[i+1] = util.DummyByter(n.HashBytes)
	}
	for i := range fact.outs {
		bs[i+len(fact.news)+1] = fact.outs[i]
	}

	return valuehash.NewSHA256(util.ConcatByters(bs...))
}

func NewSuffrageUpdate(fact SuffrageUpdateFact) base.BaseOperation {
	return base.NewBaseOperation(SuffrageUpdateHint, fact)
}
