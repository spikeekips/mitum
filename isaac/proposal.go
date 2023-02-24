package isaac

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	ProposalFactHint     = hint.MustNewHint("proposal-fact-v0.0.1")
	ProposalSignFactHint = hint.MustNewHint("proposal-sign-fact-v0.0.1")
)

type ProposalFact struct {
	proposedAt time.Time
	proposer   base.Address
	operations []util.Hash
	base.BaseFact
	point base.Point
}

func NewProposalFact(point base.Point, proposer base.Address, operations []util.Hash) ProposalFact {
	fact := ProposalFact{
		BaseFact:   base.NewBaseFact(ProposalFactHint, base.Token(util.ConcatByters(ProposalFactHint, point))),
		point:      point,
		proposer:   proposer,
		operations: operations,
		proposedAt: localtime.Now().UTC(),
	}

	fact.SetHash(fact.generateHash())

	return fact
}

func (fact ProposalFact) Point() base.Point {
	return fact.point
}

func (fact ProposalFact) Proposer() base.Address {
	return fact.proposer
}

func (fact ProposalFact) Operations() []util.Hash {
	return fact.operations
}

func (fact ProposalFact) ProposedAt() time.Time {
	return fact.proposedAt
}

func (fact ProposalFact) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid ProposalFact")

	if err := fact.BaseFact.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := base.IsValidProposalFact(fact); err != nil {
		return e.Wrap(err)
	}

	if !fact.Hash().Equal(fact.generateHash()) {
		return e.Errorf("wrong hash")
	}

	return nil
}

func (fact ProposalFact) generateHash() util.Hash {
	bs := make([]util.Byter, len(fact.operations)+4)
	bs[0] = util.BytesToByter(fact.Token())
	bs[1] = fact.point
	bs[2] = fact.proposer
	bs[3] = localtime.New(fact.proposedAt)

	for i := range fact.operations {
		bs[i+3] = fact.operations[i]
	}

	return valuehash.NewSHA256(util.ConcatByters(bs...))
}

type ProposalSignFact struct {
	fact base.ProposalFact
	sign base.BaseSign
	hint.BaseHinter
}

func NewProposalSignFact(fact ProposalFact) ProposalSignFact {
	return ProposalSignFact{
		BaseHinter: hint.NewBaseHinter(ProposalSignFactHint),
		fact:       fact,
	}
}

func (sf ProposalSignFact) IsValid(networkID []byte) error {
	if err := base.IsValidProposalSignFact(sf, networkID); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid ProposalSignFact")
	}

	return nil
}

func (sf ProposalSignFact) Fact() base.Fact {
	return sf.fact
}

func (sf ProposalSignFact) ProposalFact() base.ProposalFact {
	return sf.fact
}

func (sf ProposalSignFact) Point() base.Point {
	if sf.fact == nil {
		return base.ZeroPoint
	}

	return sf.fact.Point()
}

func (sf ProposalSignFact) Signs() []base.Sign {
	return []base.Sign{sf.sign}
}

func (sf ProposalSignFact) HashBytes() []byte {
	return util.ConcatByters(sf.BaseHinter, sf.sign)
}

func (sf *ProposalSignFact) Sign(priv base.Privatekey, networkID base.NetworkID) error {
	sign, err := base.NewBaseSignFromFact(
		priv,
		networkID,
		sf.fact,
	)
	if err != nil {
		return errors.Wrap(err, "failed to sign ProposalSignFact")
	}

	sf.sign = sign

	return nil
}
