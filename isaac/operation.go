package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type BaseOperationFact struct {
	hint.BaseHinter
	h util.Hash
	t base.Token
}

func NewBaseOperationFact(ht hint.Hint, t base.Token) BaseOperationFact {
	return BaseOperationFact{
		BaseHinter: hint.NewBaseHinter(ht),
		t:          t,
	}
}

func (fact BaseOperationFact) Hash() util.Hash {
	return fact.h
}

func (fact BaseOperationFact) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid BaseOperationFact")
	if err := util.CheckIsValid(nil, false,
		fact.h,
		fact.t,
	); err != nil {
		return e(err, "")
	}

	return nil
}
