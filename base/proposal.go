package base

import (
	"time"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type ProposalFact interface {
	Fact
	Point() Point
	Proposer() Address
	Operations() []util.Hash
	ProposedAt() time.Time
}

type ProposalSignedFact interface {
	hint.Hinter
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
				return util.InvalidError.Errorf("zero propsed at time")
			}

			return nil
		}),
	); err != nil {
		return e(err, "")
	}

	ops := fact.Operations()
	vs := make([]util.IsValider, len(ops))

	var c int
	if util.CheckSliceDuplicated(ops, func(i interface{}) string {
		j, ok := i.(util.Hash)
		if !ok {
			return ""
		}

		vs[c] = ops[c]
		c++

		return j.String()
	}) {
		return util.InvalidError.Errorf("duplicated operation found")
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

	// BLOCK check signed publickey is real proposer publickey
	if _, ok := sf.Fact().(ProposalFact); !ok {
		return e(util.InvalidError.Errorf("not ProposalFact, %T", sf.Fact()), "")
	}

	return nil
}
