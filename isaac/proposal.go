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
	ProposalFactHint       = hint.MustNewHint("proposal-fact-v0.0.1")
	ProposalSignedFactHint = hint.MustNewHint("proposal-signed-fact-v0.0.1")
)

type ProposalFact struct {
	base.BaseFact
	point      base.Point
	proposer   base.Address
	operations []util.Hash
	proposedAt time.Time
}

func NewProposalFact(point base.Point, proposer base.Address, operations []util.Hash) ProposalFact {
	fact := ProposalFact{
		BaseFact:   base.NewBaseFact(ProposalFactHint, base.Token(util.ConcatByters(ProposalFactHint, point))),
		point:      point,
		proposer:   proposer,
		operations: operations,
		proposedAt: localtime.UTCNow(),
	}

	fact.SetHash(fact.hash())

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
	e := util.StringErrorFunc("invalid ProposalFact")

	if err := base.IsValidProposalFact(fact); err != nil {
		return e(err, "")
	}

	if !fact.Hash().Equal(fact.hash()) {
		return util.InvalidError.Errorf("wrong hash of ProposalFact")
	}

	return nil
}

func (fact ProposalFact) hash() util.Hash {
	bs := make([]util.Byter, len(fact.operations)+3)
	bs[0] = fact.point
	bs[1] = fact.proposer
	bs[2] = localtime.New(fact.proposedAt)
	for i := range fact.operations {
		bs[i+3] = fact.operations[i]
	}

	return valuehash.NewSHA256(util.ConcatByters(bs...))
}

type ProposalSignedFact struct {
	util.DefaultJSONMarshaled
	hint.BaseHinter
	fact   base.ProposalFact
	signed base.BaseSigned
}

func NewProposalSignedFact(fact ProposalFact) ProposalSignedFact {
	return ProposalSignedFact{
		BaseHinter: hint.NewBaseHinter(ProposalSignedFactHint),
		fact:       fact,
	}
}

func (sf ProposalSignedFact) IsValid(networkID []byte) error {
	if err := base.IsValidProposalSignedFact(sf, networkID); err != nil {
		return util.InvalidError.Wrapf(err, "invalid ProposalSignedFact")
	}

	return nil
}

func (sf ProposalSignedFact) Fact() base.Fact {
	return sf.fact
}

func (sf ProposalSignedFact) ProposalFact() base.ProposalFact {
	return sf.fact
}

func (sf ProposalSignedFact) Point() base.Point {
	if sf.fact == nil {
		return base.ZeroPoint
	}

	return sf.fact.Point()
}

func (sf ProposalSignedFact) Signed() []base.Signed {
	return []base.Signed{sf.signed}
}

func (sf ProposalSignedFact) HashBytes() []byte {
	return util.ConcatByters(sf.BaseHinter, sf.signed)
}

func (sf *ProposalSignedFact) Sign(priv base.Privatekey, networkID base.NetworkID) error {
	signed, err := base.BaseSignedFromFact(
		priv,
		networkID,
		sf.fact,
	)
	if err != nil {
		return errors.Wrap(err, "failed to sign ProposalSignedFact")
	}

	sf.signed = signed

	return nil
}
