package base

import (
	"time"

	"github.com/spikeekips/mitum/util"
)

type ProposalFact interface {
	Fact
	Point() Point
	Proposer() Address
	Operations() [][2]util.Hash // NOTE []util.Hash{operation hash, fact hash}
	ProposedAt() time.Time
	PreviousBlock() util.Hash
}

type ProposalSignFact interface {
	util.HashByter
	util.IsValider
	SignFact
	Point() Point
	ProposalFact() ProposalFact
}

func IsValidProposalFact(fact ProposalFact) error {
	e := util.ErrInvalid.Errorf("invalid ProposalFact")
	if err := IsValidFact(fact, nil); err != nil {
		return e.Wrap(err)
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
		return e.Wrap(err)
	}

	switch h := fact.Point().Height(); {
	case h == GenesisHeight && fact.PreviousBlock() != nil:
		return e.Errorf("previous block should be nil for genesis proposal")
	case h == GenesisHeight:
	case fact.PreviousBlock() == nil:
		return e.Errorf("nil previous block")
	default:
		if err := fact.PreviousBlock().IsValid(nil); err != nil {
			return e.WithMessage(err, "invalid previous block")
		}
	}

	ops := fact.Operations()

	if util.IsDuplicatedSlice(ops, func(hs [2]util.Hash) (bool, string) {
		if hs[0] == nil {
			return true, ""
		}

		return true, hs[0].String()
	}) {
		return e.Errorf("duplicated operation found")
	}

	if util.IsDuplicatedSlice(ops, func(hs [2]util.Hash) (bool, string) {
		if hs[1] == nil {
			return true, ""
		}

		return true, hs[1].String()
	}) {
		return e.Errorf("duplicated operation fact found")
	}

	for i := range ops {
		if err := util.CheckIsValiders(nil, false, ops[i][0], ops[i][1]); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func IsValidProposalSignFact(sf ProposalSignFact, networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid ProposalSignFact")

	if err := IsValidSignFact(sf, networkID); err != nil {
		return e.Wrap(err)
	}

	if _, err := util.AssertInterfaceValue[ProposalFact](sf.Fact()); err != nil {
		return e.Wrap(err)
	}

	return nil
}
