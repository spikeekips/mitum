package base

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

var (
	dummyINITBallotHint   = hint.MustNewHint("dummy-init-ballot-v3.2.1")
	dummyProposalHint     = hint.MustNewHint("dummy-proposal-v3.2.1")
	dummyACCEPTBallotHint = hint.MustNewHint("dummy-accept-ballot-v3.2.1")
)

type dummyINITBallot struct {
	BaseINITBallot
}

func (bl dummyINITBallot) IsValid(networkID []byte) error {
	e := util.StringErrorFunc("invalid dummyINITBallot")

	if err := bl.BaseHinter.IsValid(dummyINITBallotHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := bl.BaseINITBallot.IsValid(networkID); err != nil {
		return e(err, "")
	}

	return nil
}

type dummyProposal struct {
	BaseProposal
}

func (bl dummyProposal) IsValid(networkID []byte) error {
	e := util.StringErrorFunc("invalid dummyProposal")

	if err := bl.BaseHinter.IsValid(dummyProposalHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := bl.BaseProposal.IsValid(networkID); err != nil {
		return e(err, "")
	}

	return nil
}

type dummyACCEPTBallot struct {
	BaseACCEPTBallot
}

func (bl dummyACCEPTBallot) IsValid(networkID []byte) error {
	e := util.StringErrorFunc("invalid dummyACCEPTBallot")

	if err := bl.BaseHinter.IsValid(dummyACCEPTBallotHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := bl.BaseACCEPTBallot.IsValid(networkID); err != nil {
		return e(err, "")
	}

	return nil
}

type testBaseBallot struct {
	suite.Suite
	priv      Privatekey
	networkID NetworkID
	ballot    func() Ballot
}

func (t *testBaseBallot) SetupTest() {
	t.priv = NewMPrivatekey()
	t.networkID = NetworkID(util.UUID().Bytes())
}

func (t *testBaseBallot) TestNew() {
	bl := t.ballot()
	t.NoError(bl.IsValid(t.networkID))
}

func (t *testBaseBallot) TestEmptyINITVoteproof() {
	bl := t.ballot()
	t.NoError(bl.IsValid(t.networkID))

	if bl.Stage() == StageINIT {
		fact := bl.(INITBallot).BallotSignedFact().BallotFact()
		if fact.Point().Round() == 0 {
			t.T().Skip("0 round init ballot")

			return
		}
	}

	switch bt := bl.(type) {
	case dummyINITBallot:
		bt.ivp = nil
		bl = bt
	case dummyProposal:
		bt.ivp = nil
		bl = bt
	case dummyACCEPTBallot:
		bt.ivp = nil
		bl = bt
	}
	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func (t *testBaseBallot) TestEmptyACCEPTVoteproof() {
	bl := t.ballot()
	t.NoError(bl.IsValid(t.networkID))

	switch bt := bl.(type) {
	case dummyINITBallot:
		bt.avp = nil
		bl = bt
	case dummyProposal:
		bt.avp = nil
		bl = bt
	case dummyACCEPTBallot:
		bt.avp = nil
		bl = bt
	}
	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func (t *testBaseBallot) TestEmptySignedFact() {
	bl := t.ballot()
	t.NoError(bl.IsValid(t.networkID))

	switch bt := bl.(type) {
	case dummyINITBallot:
		bt.signedFact = nil
		bl = bt
	case dummyProposal:
		bt.signedFact = nil
		bl = bt
	case dummyACCEPTBallot:
		bt.signedFact = nil
		bl = bt
	}
	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func TestBaseINITBallot(tt *testing.T) {
	t := new(testBaseBallot)
	t.ballot = func() Ballot {
		block := valuehash.RandomSHA256()
		afact := newDummyACCEPTBallotFact(NewPoint(Height(33), Round(44)),
			valuehash.RandomSHA256(),
			block,
			util.UUID().String())

		asignedFact := dummyACCEPTBallotSignedFact{
			BaseACCEPTBallotSignedFact: NewBaseACCEPTBallotSignedFact(
				dummyACCEPTBallotSignedFactHint,
				RandomAddress(""),
				afact,
			),
		}
		t.NoError(asignedFact.Sign(t.priv, t.networkID))

		avp := NewDummyACCEPTVoteproof(afact.Point())
		avp.Finish()
		avp.SetResult(VoteResultMajority)
		avp.SetMajority(afact)
		avp.SetSignedFacts([]BallotSignedFact{asignedFact})
		avp.SetSuffrage(DummySuffrageInfo{})

		fact := newDummyINITBallotFact(NewPoint(afact.Point().Height()+1, Round(0)),
			block,
			util.UUID().String())

		signedFact := dummyINITBallotSignedFact{
			BaseINITBallotSignedFact: NewBaseINITBallotSignedFact(
				dummyINITBallotSignedFactHint,
				RandomAddress(""),
				fact,
			),
		}
		t.NoError(signedFact.Sign(t.priv, t.networkID))

		return dummyINITBallot{
			BaseINITBallot: NewBaseINITBallot(
				dummyINITBallotHint,
				nil, avp,
				signedFact,
			),
		}
	}

	suite.Run(tt, t)
}

func TestBaseProposalBallot(tt *testing.T) {
	t := new(testBaseBallot)
	t.ballot = func() Ballot {
		afact := newDummyACCEPTBallotFact(NewPoint(Height(32), Round(44)),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			util.UUID().String())

		asignedFact := dummyACCEPTBallotSignedFact{
			BaseACCEPTBallotSignedFact: NewBaseACCEPTBallotSignedFact(
				dummyACCEPTBallotSignedFactHint,
				RandomAddress(""),
				afact,
			),
		}
		t.NoError(asignedFact.Sign(t.priv, t.networkID))

		avp := NewDummyACCEPTVoteproof(afact.Point())
		avp.Finish()
		avp.SetResult(VoteResultMajority)
		avp.SetMajority(afact)
		avp.SetSignedFacts([]BallotSignedFact{asignedFact})
		avp.SetSuffrage(DummySuffrageInfo{})

		ifact := newDummyINITBallotFact(NewPoint(afact.Point().Height()+1, Round(55)),
			valuehash.RandomSHA256(),
			util.UUID().String())

		isignedFact := dummyINITBallotSignedFact{
			BaseINITBallotSignedFact: NewBaseINITBallotSignedFact(
				dummyINITBallotSignedFactHint,
				RandomAddress(""),
				ifact,
			),
		}
		t.NoError(isignedFact.Sign(t.priv, t.networkID))

		ivp := NewDummyINITVoteproof(ifact.Point())
		ivp.Finish()
		ivp.SetResult(VoteResultMajority)
		ivp.SetMajority(ifact)
		ivp.SetSignedFacts([]BallotSignedFact{isignedFact})
		ivp.SetSuffrage(DummySuffrageInfo{})

		fact := newDummyProposalFact(ifact.Point(),
			[]util.Hash{
				valuehash.RandomSHA256(),
				valuehash.RandomSHA256(),
			},
			util.UUID().String())

		signedFact := dummyProposalSignedFact{
			BaseProposalSignedFact: NewBaseProposalSignedFact(
				dummyProposalSignedFactHint,
				RandomAddress(""),
				fact,
			),
		}
		t.NoError(signedFact.Sign(t.priv, t.networkID))

		return dummyProposal{
			BaseProposal: NewBaseProposal(
				dummyProposalHint,
				ivp, avp,
				signedFact,
			),
		}
	}

	suite.Run(tt, t)
}

func TestBaseACCEPTBallot(tt *testing.T) {
	t := new(testBaseBallot)
	t.ballot = func() Ballot {
		afact := newDummyACCEPTBallotFact(NewPoint(Height(32), Round(44)),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			util.UUID().String())

		asignedFact := dummyACCEPTBallotSignedFact{
			BaseACCEPTBallotSignedFact: NewBaseACCEPTBallotSignedFact(
				dummyACCEPTBallotSignedFactHint,
				RandomAddress(""),
				afact,
			),
		}
		t.NoError(asignedFact.Sign(t.priv, t.networkID))

		avp := NewDummyACCEPTVoteproof(afact.Point())
		avp.Finish()
		avp.SetResult(VoteResultMajority)
		avp.SetMajority(afact)
		avp.SetSignedFacts([]BallotSignedFact{asignedFact})
		avp.SetSuffrage(DummySuffrageInfo{})

		ifact := newDummyINITBallotFact(NewPoint(afact.Point().Height()+1, Round(55)),
			valuehash.RandomSHA256(),
			util.UUID().String())

		isignedFact := dummyINITBallotSignedFact{
			BaseINITBallotSignedFact: NewBaseINITBallotSignedFact(
				dummyINITBallotSignedFactHint,
				RandomAddress(""),
				ifact,
			),
		}
		t.NoError(isignedFact.Sign(t.priv, t.networkID))

		ivp := NewDummyINITVoteproof(ifact.Point())
		ivp.Finish()
		ivp.SetResult(VoteResultMajority)
		ivp.SetMajority(ifact)
		ivp.SetSignedFacts([]BallotSignedFact{isignedFact})
		ivp.SetSuffrage(DummySuffrageInfo{})

		fact := newDummyACCEPTBallotFact(ifact.Point(),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			util.UUID().String())

		signedFact := dummyACCEPTBallotSignedFact{
			BaseACCEPTBallotSignedFact: NewBaseACCEPTBallotSignedFact(
				dummyACCEPTBallotSignedFactHint,
				RandomAddress(""),
				fact,
			),
		}
		t.NoError(signedFact.Sign(t.priv, t.networkID))

		return dummyACCEPTBallot{
			BaseACCEPTBallot: NewBaseACCEPTBallot(
				dummyACCEPTBallotHint,
				ivp, avp,
				signedFact,
			),
		}
	}

	suite.Run(tt, t)
}

type testBaseINITBallotWithVoteproof struct {
	suite.Suite
	priv      Privatekey
	networkID NetworkID
}

func (t *testBaseINITBallotWithVoteproof) SetupTest() {
	t.priv = NewMPrivatekey()
	t.networkID = NetworkID(util.UUID().Bytes())
}

func (t *testBaseINITBallotWithVoteproof) TestNoneNilINITVoteproof0Round() {
	block := valuehash.RandomSHA256()
	afact := newDummyACCEPTBallotFact(NewPoint(Height(33), Round(44)),
		valuehash.RandomSHA256(),
		block,
		util.UUID().String())

	asignedFact := dummyACCEPTBallotSignedFact{
		BaseACCEPTBallotSignedFact: NewBaseACCEPTBallotSignedFact(
			dummyACCEPTBallotSignedFactHint,
			RandomAddress(""),
			afact,
		),
	}
	t.NoError(asignedFact.Sign(t.priv, t.networkID))

	avp := NewDummyACCEPTVoteproof(afact.Point())
	avp.Finish()
	avp.SetResult(VoteResultMajority)
	avp.SetMajority(afact)
	avp.SetSignedFacts([]BallotSignedFact{asignedFact})
	avp.SetSuffrage(DummySuffrageInfo{})

	ifact := newDummyINITBallotFact(NewPoint(avp.Point().Height(), Round(55)), // same height with previous accept voteproof
		valuehash.RandomSHA256(),
		util.UUID().String())

	isignedFact := dummyINITBallotSignedFact{
		BaseINITBallotSignedFact: NewBaseINITBallotSignedFact(
			dummyINITBallotSignedFactHint,
			RandomAddress(""),
			ifact,
		),
	}
	t.NoError(isignedFact.Sign(t.priv, t.networkID))

	ivp := NewDummyINITVoteproof(ifact.Point())
	ivp.Finish()
	ivp.SetResult(VoteResultMajority)
	ivp.SetMajority(ifact)
	ivp.SetSignedFacts([]BallotSignedFact{isignedFact})
	ivp.SetSuffrage(DummySuffrageInfo{})

	fact := newDummyINITBallotFact(NewPoint(afact.Point().Height()+1, Round(0)),
		block,
		util.UUID().String())

	signedFact := dummyINITBallotSignedFact{
		BaseINITBallotSignedFact: NewBaseINITBallotSignedFact(
			dummyINITBallotSignedFactHint,
			RandomAddress(""),
			fact,
		),
	}
	t.NoError(signedFact.Sign(t.priv, t.networkID))

	bl := dummyINITBallot{
		BaseINITBallot: NewBaseINITBallot(
			dummyINITBallotHint,
			ivp, avp,
			signedFact,
		),
	}

	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "nit voteproof should be nil in 0 round init ballot")
}

func (t *testBaseINITBallotWithVoteproof) TestValidINITVoteproofNone0Round() {
	block := valuehash.RandomSHA256()
	afact := newDummyACCEPTBallotFact(NewPoint(Height(33), Round(44)),
		valuehash.RandomSHA256(),
		block,
		util.UUID().String())

	asignedFact := dummyACCEPTBallotSignedFact{
		BaseACCEPTBallotSignedFact: NewBaseACCEPTBallotSignedFact(
			dummyACCEPTBallotSignedFactHint,
			RandomAddress(""),
			afact,
		),
	}
	t.NoError(asignedFact.Sign(t.priv, t.networkID))

	avp := NewDummyACCEPTVoteproof(afact.Point())
	avp.Finish()
	avp.SetResult(VoteResultMajority)
	avp.SetMajority(afact)
	avp.SetSignedFacts([]BallotSignedFact{asignedFact})
	avp.SetSuffrage(DummySuffrageInfo{})

	ifact := newDummyINITBallotFact(NewPoint(avp.Point().Height()+1, Round(55)),
		valuehash.RandomSHA256(),
		util.UUID().String())

	isignedFact := dummyINITBallotSignedFact{
		BaseINITBallotSignedFact: NewBaseINITBallotSignedFact(
			dummyINITBallotSignedFactHint,
			RandomAddress(""),
			ifact,
		),
	}
	t.NoError(isignedFact.Sign(t.priv, t.networkID))

	ivp := NewDummyINITVoteproof(ifact.Point())
	ivp.Finish()
	ivp.SetResult(VoteResultMajority)
	ivp.SetMajority(ifact)
	ivp.SetSignedFacts([]BallotSignedFact{isignedFact})
	ivp.SetSuffrage(DummySuffrageInfo{})

	fact := newDummyINITBallotFact(NewPoint(ifact.Point().Height(), ifact.Point().Round()+1),
		block,
		util.UUID().String())

	signedFact := dummyINITBallotSignedFact{
		BaseINITBallotSignedFact: NewBaseINITBallotSignedFact(
			dummyINITBallotSignedFactHint,
			RandomAddress(""),
			fact,
		),
	}
	t.NoError(signedFact.Sign(t.priv, t.networkID))

	bl := dummyINITBallot{
		BaseINITBallot: NewBaseINITBallot(
			dummyINITBallotHint,
			ivp, avp,
			signedFact,
		),
	}

	t.NoError(bl.IsValid(t.networkID))
}

func (t *testBaseINITBallotWithVoteproof) TestWrongHeightINITVoteproofNone0Round() {
	block := valuehash.RandomSHA256()
	afact := newDummyACCEPTBallotFact(NewPoint(Height(33), Round(44)),
		valuehash.RandomSHA256(),
		block,
		util.UUID().String())

	asignedFact := dummyACCEPTBallotSignedFact{
		BaseACCEPTBallotSignedFact: NewBaseACCEPTBallotSignedFact(
			dummyACCEPTBallotSignedFactHint,
			RandomAddress(""),
			afact,
		),
	}
	t.NoError(asignedFact.Sign(t.priv, t.networkID))

	avp := NewDummyACCEPTVoteproof(afact.Point())
	avp.Finish()
	avp.SetResult(VoteResultMajority)
	avp.SetMajority(afact)
	avp.SetSignedFacts([]BallotSignedFact{asignedFact})
	avp.SetSuffrage(DummySuffrageInfo{})

	ifact := newDummyINITBallotFact(NewPoint(avp.Point().Height()+2, Round(55)),
		valuehash.RandomSHA256(),
		util.UUID().String())

	isignedFact := dummyINITBallotSignedFact{
		BaseINITBallotSignedFact: NewBaseINITBallotSignedFact(
			dummyINITBallotSignedFactHint,
			RandomAddress(""),
			ifact,
		),
	}
	t.NoError(isignedFact.Sign(t.priv, t.networkID))

	ivp := NewDummyINITVoteproof(ifact.Point())
	ivp.Finish()
	ivp.SetResult(VoteResultMajority)
	ivp.SetMajority(ifact)
	ivp.SetSignedFacts([]BallotSignedFact{isignedFact})
	ivp.SetSuffrage(DummySuffrageInfo{})

	fact := newDummyINITBallotFact(NewPoint(afact.Point().Height()+1, ifact.Point().Round()+1),
		block,
		util.UUID().String())

	signedFact := dummyINITBallotSignedFact{
		BaseINITBallotSignedFact: NewBaseINITBallotSignedFact(
			dummyINITBallotSignedFactHint,
			RandomAddress(""),
			fact,
		),
	}
	t.NoError(signedFact.Sign(t.priv, t.networkID))

	bl := dummyINITBallot{
		BaseINITBallot: NewBaseINITBallot(
			dummyINITBallotHint,
			ivp, avp,
			signedFact,
		),
	}

	err := bl.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "wrong height of init voteproof")
}

func TestBaseINITBallotWithVoteproof(t *testing.T) {
	suite.Run(t, new(testBaseINITBallotWithVoteproof))
}

type baseTestDummyBallotEncode struct {
	*encoder.BaseTestEncode
	enc       encoder.Encoder
	priv      Privatekey
	networkID NetworkID
}

func (t *baseTestDummyBallotEncode) SetupTest() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: StringAddressHint, Instance: StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyINITBallotFactHint, Instance: dummyINITBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyProposalFactHint, Instance: dummyProposalFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyACCEPTBallotFactHint, Instance: dummyACCEPTBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyINITVoteproofHint, Instance: DummyINITVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyACCEPTVoteproofHint, Instance: DummyACCEPTVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyINITBallotSignedFactHint, Instance: dummyINITBallotSignedFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyProposalSignedFactHint, Instance: dummyProposalSignedFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyACCEPTBallotSignedFactHint, Instance: dummyACCEPTBallotSignedFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyINITBallotHint, Instance: dummyINITBallot{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyProposalHint, Instance: dummyProposal{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyACCEPTBallotHint, Instance: dummyACCEPTBallot{}}))
}

func testDummyBallotEncode() *baseTestDummyBallotEncode {
	t := new(baseTestDummyBallotEncode)
	t.BaseTestEncode = new(encoder.BaseTestEncode)

	t.priv = NewMPrivatekey()
	t.networkID = NetworkID(util.UUID().Bytes())

	t.Compare = func(a, b interface{}) {
		as, ok := a.(Ballot)
		t.True(ok)
		bs, ok := b.(Ballot)
		t.True(ok)

		t.NoError(bs.IsValid(t.networkID))

		CompareBallot(t.Assert(), as, bs)
	}

	return t
}

func TestDummyINITBallotJSON(tt *testing.T) {
	t := testDummyBallotEncode()

	t.Encode = func() (interface{}, []byte) {
		block := valuehash.RandomSHA256()
		afact := newDummyACCEPTBallotFact(NewPoint(Height(33), Round(44)),
			valuehash.RandomSHA256(),
			block,
			util.UUID().String())

		asignedFact := dummyACCEPTBallotSignedFact{
			BaseACCEPTBallotSignedFact: NewBaseACCEPTBallotSignedFact(
				dummyACCEPTBallotSignedFactHint,
				RandomAddress(""),
				afact,
			),
		}
		t.NoError(asignedFact.Sign(t.priv, t.networkID))

		avp := NewDummyACCEPTVoteproof(afact.Point())
		avp.Finish()
		avp.SetResult(VoteResultMajority)
		avp.SetMajority(afact)
		avp.SetSignedFacts([]BallotSignedFact{asignedFact})
		avp.SetSuffrage(DummySuffrageInfo{})

		fact := newDummyINITBallotFact(NewPoint(afact.Point().Height()+1, Round(0)),
			block,
			util.UUID().String())

		signedFact := dummyINITBallotSignedFact{
			BaseINITBallotSignedFact: NewBaseINITBallotSignedFact(
				dummyINITBallotSignedFactHint,
				RandomAddress(""),
				fact,
			),
		}
		t.NoError(signedFact.Sign(t.priv, t.networkID))

		bl := dummyINITBallot{
			BaseINITBallot: NewBaseINITBallot(
				dummyINITBallotHint,
				nil, avp,
				signedFact,
			),
		}

		b, err := t.enc.Marshal(bl)
		t.NoError(err)

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(dummyINITBallot)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}

func TestDummyProposalBallotJSON(tt *testing.T) {
	t := testDummyBallotEncode()

	t.Encode = func() (interface{}, []byte) {
		afact := newDummyACCEPTBallotFact(NewPoint(Height(32), Round(44)),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			util.UUID().String())

		asignedFact := dummyACCEPTBallotSignedFact{
			BaseACCEPTBallotSignedFact: NewBaseACCEPTBallotSignedFact(
				dummyACCEPTBallotSignedFactHint,
				RandomAddress(""),
				afact,
			),
		}
		t.NoError(asignedFact.Sign(t.priv, t.networkID))

		avp := NewDummyACCEPTVoteproof(afact.Point())
		avp.Finish()
		avp.SetResult(VoteResultMajority)
		avp.SetMajority(afact)
		avp.SetSignedFacts([]BallotSignedFact{asignedFact})
		avp.SetSuffrage(DummySuffrageInfo{})

		ifact := newDummyINITBallotFact(NewPoint(afact.Point().Height()+1, Round(55)),
			valuehash.RandomSHA256(),
			util.UUID().String())

		isignedFact := dummyINITBallotSignedFact{
			BaseINITBallotSignedFact: NewBaseINITBallotSignedFact(
				dummyINITBallotSignedFactHint,
				RandomAddress(""),
				ifact,
			),
		}
		t.NoError(isignedFact.Sign(t.priv, t.networkID))

		ivp := NewDummyINITVoteproof(ifact.Point())
		ivp.Finish()
		ivp.SetResult(VoteResultMajority)
		ivp.SetMajority(ifact)
		ivp.SetSignedFacts([]BallotSignedFact{isignedFact})
		ivp.SetSuffrage(DummySuffrageInfo{})

		fact := newDummyProposalFact(ifact.Point(),
			[]util.Hash{
				valuehash.RandomSHA256(),
				valuehash.RandomSHA256(),
			},
			util.UUID().String())

		signedFact := dummyProposalSignedFact{
			BaseProposalSignedFact: NewBaseProposalSignedFact(
				dummyProposalSignedFactHint,
				RandomAddress(""),
				fact,
			),
		}
		t.NoError(signedFact.Sign(t.priv, t.networkID))

		bl := dummyProposal{
			BaseProposal: NewBaseProposal(
				dummyProposalHint,
				ivp, avp,
				signedFact,
			),
		}

		b, err := t.enc.Marshal(bl)
		t.NoError(err)

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(dummyProposal)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}

func TestDummyACCEPTBallotJSON(tt *testing.T) {
	t := testDummyBallotEncode()

	t.Encode = func() (interface{}, []byte) {
		afact := newDummyACCEPTBallotFact(NewPoint(Height(32), Round(44)),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			util.UUID().String())

		asignedFact := dummyACCEPTBallotSignedFact{
			BaseACCEPTBallotSignedFact: NewBaseACCEPTBallotSignedFact(
				dummyACCEPTBallotSignedFactHint,
				RandomAddress(""),
				afact,
			),
		}
		t.NoError(asignedFact.Sign(t.priv, t.networkID))

		avp := NewDummyACCEPTVoteproof(afact.Point())
		avp.Finish()
		avp.SetResult(VoteResultMajority)
		avp.SetMajority(afact)
		avp.SetSignedFacts([]BallotSignedFact{asignedFact})
		avp.SetSuffrage(DummySuffrageInfo{})

		ifact := newDummyINITBallotFact(NewPoint(afact.Point().Height()+1, Round(55)),
			valuehash.RandomSHA256(),
			util.UUID().String())

		isignedFact := dummyINITBallotSignedFact{
			BaseINITBallotSignedFact: NewBaseINITBallotSignedFact(
				dummyINITBallotSignedFactHint,
				RandomAddress(""),
				ifact,
			),
		}
		t.NoError(isignedFact.Sign(t.priv, t.networkID))

		ivp := NewDummyINITVoteproof(ifact.Point())
		ivp.Finish()
		ivp.SetResult(VoteResultMajority)
		ivp.SetMajority(ifact)
		ivp.SetSignedFacts([]BallotSignedFact{isignedFact})
		ivp.SetSuffrage(DummySuffrageInfo{})

		fact := newDummyACCEPTBallotFact(ifact.Point(),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			util.UUID().String())

		signedFact := dummyACCEPTBallotSignedFact{
			BaseACCEPTBallotSignedFact: NewBaseACCEPTBallotSignedFact(
				dummyACCEPTBallotSignedFactHint,
				RandomAddress(""),
				fact,
			),
		}
		t.NoError(signedFact.Sign(t.priv, t.networkID))

		bl := dummyACCEPTBallot{
			BaseACCEPTBallot: NewBaseACCEPTBallot(
				dummyACCEPTBallotHint,
				ivp, avp,
				signedFact,
			),
		}

		b, err := t.enc.Marshal(bl)
		t.NoError(err)

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(dummyACCEPTBallot)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}
