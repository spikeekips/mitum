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

type ProposalSignedFact interface {
	util.HashByter
	util.IsValider
	SignedFact
	Point() Point
	ProposalFact() ProposalFact
}

func IsValidProposalFact(fact ProposalFact) error {
	e := util.StringErrorFunc("invalid ProposalFact")
	if err := IsValidFact(fact, nil); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false,
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
	vs := make([]util.IsValider, len(ops))

	if _, found := util.CheckSliceDuplicated(ops, func(_ interface{}, i int) string {
		op := ops[i]

		if op == nil {
			return ""
		}

		vs[i] = ops[i]

		return op.String()
	}); found {
		return util.ErrInvalid.Errorf("duplicated operation found")
	}

	if err := util.CheckIsValid(nil, false, vs...); err != nil {
		return e(err, "")
	}

	return nil
}

func IsValidProposalSignedFact(sf ProposalSignedFact, networkID []byte) error {
	e := util.StringErrorFunc("invalid ProposalSignedFact")

	if err := IsValidSignedFact(sf, networkID); err != nil {
		return e(err, "")
	}

	if _, ok := sf.Fact().(ProposalFact); !ok {
		return e(util.ErrInvalid.Errorf("not ProposalFact, %T", sf.Fact()), "")
	}

	return nil
}
