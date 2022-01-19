package base

import (
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testBaseVoteproof struct {
	suite.Suite
	priv      Privatekey
	networkID NetworkID
}

func (t *testBaseVoteproof) SetupTest() {
	t.priv = NewMPrivatekey()
	t.networkID = NetworkID(util.UUID().Bytes())
}

func (t *testBaseVoteproof) TestNewINIT() {
	ifact := newDummyINITBallotFact(NewPoint(Height(33), Round(55)),
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

	t.NoError(ivp.IsValid(t.networkID))
}

func (t *testBaseVoteproof) TestNewACCEPT() {
	afact := newDummyACCEPTBallotFact(NewPoint(Height(33), Round(55)),
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

	t.NoError(avp.IsValid(t.networkID))
}

func TestBaseVoteproof(t *testing.T) {
	suite.Run(t, new(testBaseVoteproof))
}

type baseTestBaseVoteproofEncode struct {
	*encoder.BaseTestEncode
	enc       encoder.Encoder
	priv      Privatekey
	networkID NetworkID
}

func (t *baseTestBaseVoteproofEncode) SetupTest() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: StringAddressHint, Instance: StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyINITBallotFactHint, Instance: dummyINITBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyACCEPTBallotFactHint, Instance: dummyACCEPTBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyINITVoteproofHint, Instance: DummyINITVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyACCEPTVoteproofHint, Instance: DummyACCEPTVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyINITBallotSignedFactHint, Instance: dummyINITBallotSignedFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyACCEPTBallotSignedFactHint, Instance: dummyACCEPTBallotSignedFact{}}))
}

func testTestBaseVoteproof() *baseTestBaseVoteproofEncode {
	t := new(baseTestBaseVoteproofEncode)
	t.BaseTestEncode = new(encoder.BaseTestEncode)

	t.priv = NewMPrivatekey()
	t.networkID = NetworkID(util.UUID().Bytes())

	t.Compare = func(a, b interface{}) {
		avp, ok := a.(Voteproof)
		t.True(ok)
		bvp, ok := b.(Voteproof)
		t.True(ok)

		t.NoError(bvp.IsValid(t.networkID))

		CompareVoteproof(t.Assert(), avp, bvp)
	}

	return t
}

func TestINITVoteproofJSON(tt *testing.T) {
	t := testTestBaseVoteproof()

	t.Encode = func() (interface{}, []byte) {
		ifact := newDummyINITBallotFact(NewPoint(Height(32), Round(44)),
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

		b, err := t.enc.Marshal(ivp)
		t.NoError(err)

		return ivp, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(DummyINITVoteproof)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}

func TestACCEPTVoteproofJSON(tt *testing.T) {
	t := testTestBaseVoteproof()

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

		b, err := t.enc.Marshal(avp)
		t.NoError(err)

		return avp, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(DummyACCEPTVoteproof)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}
