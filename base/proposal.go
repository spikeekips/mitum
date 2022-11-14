package base

import (
	"time"

	"github.com/spikeekips/mitum/util"
)

type ProposalFact interface {
	Fact
	Point() Point
	Proposer() Address
	Operations() []util.Hash // NOTE operation hash
	ProposedAt() time.Time
}

type ProposalSignFact interface {
	util.HashByter
	util.IsValider
	SignFact
	Point() Point
	ProposalFact() ProposalFact
}

func IsValidProposalFact(fact ProposalFact) error {
	e := util.StringErrorFunc("invalid ProposalFact")
	if err := IsValidFact(fact, nil); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValiders(nil, false,
		fact.Point(),
		fact.Proposer(),
		util.DummyIsValider(func([]byte) error {
			if fact.ProposedAt().IsZero() {
				return util.ErrInvalid.Errorf("zero propsed at time")
			}

			return nil
		}),
	); err != nil {
		return e(err, "")
	}

	ops := fact.Operations()

	if _, found := util.IsDuplicatedSlice(ops, func(_ interface{}, i int) (bool, string) {
		op := ops[i]

		if op == nil {
			return true, ""
		}

		return true, op.String()
	}); found {
		return util.ErrInvalid.Errorf("duplicated operation found")
	}

	if err := util.CheckIsValiderSlice(nil, false, fact.Operations()); err != nil {
		return e(err, "")
	}

	return nil
}

func IsValidProposalSignFact(sf ProposalSignFact, networkID []byte) error {
	e := util.StringErrorFunc("invalid ProposalSignFact")

	if err := IsValidSignFact(sf, networkID); err != nil {
		return e(err, "")
	}

	if _, ok := sf.Fact().(ProposalFact); !ok {
		return e(util.ErrInvalid.Errorf("not ProposalFact, %T", sf.Fact()), "")
	}

	return nil
}
