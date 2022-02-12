package isaac

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testBaseVoteproof struct {
	suite.Suite
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *testBaseVoteproof) SetupTest() {
	t.priv = base.NewMPrivatekey()
	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testBaseVoteproof) validVoteproof() INITVoteproof {
	node := base.RandomAddress("")
	ifact := NewINITBallotFact(base.NewPoint(base.Height(33), base.Round(55)), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	isignedFact := NewINITBallotSignedFact(node, ifact)
	t.NoError(isignedFact.Sign(t.priv, t.networkID))

	ivp := NewINITVoteproof(ifact.Point().Point)
	ivp.finish()
	ivp.SetResult(base.VoteResultMajority)
	ivp.SetMajority(ifact)
	ivp.SetSignedFacts([]base.BallotSignedFact{isignedFact})

	ivp.SetThreshold(base.Threshold(100))

	return ivp
}

func (t *testBaseVoteproof) TestNewINIT() {
	ivp := t.validVoteproof()

	t.NoError(ivp.IsValid(t.networkID))
}

func (t *testBaseVoteproof) TestEmptyID() {
	ivp := t.validVoteproof()
	ivp.id = ""

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "empty id")
}

func (t *testBaseVoteproof) TestInvalidStage() {
	ivp := t.validVoteproof()
	ivp.point = base.NewStagePoint(ivp.point.Point, base.StageProposal)

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "wrong stage")
}

func (t *testBaseVoteproof) TestInvalidVoteResult() {
	ivp := t.validVoteproof()
	ivp.SetResult(base.VoteResultNotYet)

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "not yet finished")
}

func (t *testBaseVoteproof) TestZeroFinishedAt() {
	ivp := t.validVoteproof()
	ivp.finishedAt = time.Time{}

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "zero finished time")
}

func (t *testBaseVoteproof) TestEmptySignedFacts() {
	ivp := t.validVoteproof()
	ivp.SetSignedFacts(nil)

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "empty signed facts")
}

func (t *testBaseVoteproof) TestNilMajority() {
	ivp := t.validVoteproof()
	ivp.SetMajority(nil)

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "empty majority")
}

func (t *testBaseVoteproof) TestNotNilMajorityOfDraw() {
	ivp := t.validVoteproof()
	ivp.SetResult(base.VoteResultDraw)

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "empty majority")
}

func (t *testBaseVoteproof) TestInvalidPoint() {
	ivp := t.validVoteproof()
	ivp.point = base.NewStagePoint(base.NewPoint(base.NilHeight, base.Round(0)), ivp.point.Stage())

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "invalid point")
}

func (t *testBaseVoteproof) TestDuplicatedNodeInSignedFact() {
	ivp := t.validVoteproof()

	sfs := ivp.SignedFacts()
	isf := sfs[0].(INITBallotSignedFact)

	isignedFact := NewINITBallotSignedFact(isf.Node(), isf.Fact().(INITBallotFact))
	t.NoError(isignedFact.Sign(t.priv, t.networkID))

	sfs = append(sfs, isignedFact)

	ivp.SetSignedFacts(sfs)

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "duplicated node found")
}

func (t *testBaseVoteproof) TestInvalidSignedFact() {
	ivp := t.validVoteproof()

	ifact := NewINITBallotFact(base.NewPoint(base.Height(33), base.Round(55)), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	isignedFact := NewINITBallotSignedFact(base.RandomAddress(""), ifact)
	t.NoError(isignedFact.Sign(t.priv, util.UUID().Bytes())) // wrong network id

	ivp.SetMajority(ifact)
	ivp.SetSignedFacts([]base.BallotSignedFact{isignedFact})

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "failed to verify signed")
}

func (t *testBaseVoteproof) TestWrongPointOfSignedFact() {
	ivp := t.validVoteproof()

	ifact := NewINITBallotFact(base.NewPoint(base.Height(33), base.Round(55)), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	wfact := NewINITBallotFact(base.NewPoint(base.Height(34), base.Round(55)), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	isignedFact := NewINITBallotSignedFact(base.RandomAddress(""), ifact)
	t.NoError(isignedFact.Sign(t.priv, t.networkID))

	wsignedFact := NewINITBallotSignedFact(base.RandomAddress(""), wfact)
	t.NoError(wsignedFact.Sign(t.priv, t.networkID))

	ivp.finish()
	ivp.SetMajority(ifact)
	ivp.SetSignedFacts([]base.BallotSignedFact{isignedFact, wsignedFact})

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "point does not match")
	t.Contains(err.Error(), "invalid signed fact")
}

func (t *testBaseVoteproof) TestWrongPointOfMajority() {
	ivp := t.validVoteproof()

	ifact := NewINITBallotFact(base.NewPoint(ivp.Point().Height()+1, ivp.Point().Round()), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	isignedFact := NewINITBallotSignedFact(base.RandomAddress(""), ifact)

	t.NoError(isignedFact.Sign(t.priv, t.networkID))

	ivp.SetMajority(ifact)
	ivp.SetSignedFacts([]base.BallotSignedFact{isignedFact})

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "point does not match")
	t.Contains(err.Error(), "invalid majority")
}

func (t *testBaseVoteproof) TestMajorityNotFoundInSignedFacts() {
	ivp := t.validVoteproof()

	fact := NewINITBallotFact(base.NewPoint(base.Height(33), base.Round(55)), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	ivp.SetMajority(fact)

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "majoirty not found in signed facts")
}

func (t *testBaseVoteproof) TestWrongMajorityWithSuffrage() {
	ivp := t.validVoteproof()

	n0 := base.RandomAddress("n0-")
	n1 := base.RandomAddress("n1-")
	n2 := base.RandomAddress("n2-")

	fact := NewINITBallotFact(base.NewPoint(base.Height(33), base.Round(55)), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	newsignedfact := func(node base.Address) INITBallotSignedFact {
		signedFact := NewINITBallotSignedFact(node, fact)

		t.NoError(signedFact.Sign(t.priv, t.networkID))

		return signedFact
	}

	sfs := ivp.SignedFacts()
	sfs = append(sfs, newsignedfact(n0), newsignedfact(n1), newsignedfact(n2))
	ivp.SetSignedFacts(sfs)

	ivp.SetThreshold(base.Threshold(67))

	t.NoError(ivp.IsValid(t.networkID))

	nodes := make([]base.Address, len(ivp.SignedFacts()))
	oldsfs := ivp.SignedFacts()
	for i := range oldsfs {
		nodes[i] = oldsfs[i].Node()
	}

	suf, err := newSuffrage(nodes)
	t.NoError(err)

	err = base.IsValidVoteproofWithSuffrage(ivp, suf)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "wrong majority")
}

func (t *testBaseVoteproof) TestUnknownNode() {
	ivp := t.validVoteproof()

	ivp.SetThreshold(base.Threshold(100))

	t.NoError(ivp.IsValid(t.networkID))

	suf, err := newSuffrage([]base.Address{base.RandomAddress("n0-")})
	t.NoError(err)

	err = base.IsValidVoteproofWithSuffrage(ivp, suf)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "unknown node found")
}

func (t *testBaseVoteproof) TestNewACCEPT() {
	afact := NewACCEPTBallotFact(base.NewPoint(base.Height(33), base.Round(55)), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	node := base.RandomAddress("")
	asignedFact := NewACCEPTBallotSignedFact(node, afact)

	t.NoError(asignedFact.Sign(t.priv, t.networkID))

	avp := NewACCEPTVoteproof(afact.Point().Point)
	avp.finish()
	avp.SetResult(base.VoteResultMajority)
	avp.SetMajority(afact)
	avp.SetSignedFacts([]base.BallotSignedFact{asignedFact})
	avp.SetThreshold(base.Threshold(100))

	t.NoError(avp.IsValid(t.networkID))
}

func TestBaseVoteproof(t *testing.T) {
	suite.Run(t, new(testBaseVoteproof))
}

type baseTestBaseVoteproofEncode struct {
	encoder.BaseTestEncode
	enc       encoder.Encoder
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *baseTestBaseVoteproofEncode) SetupTest() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotFactHint, Instance: INITBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotFactHint, Instance: ACCEPTBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITVoteproofHint, Instance: INITVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTVoteproofHint, Instance: ACCEPTVoteproof{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotSignedFactHint, Instance: INITBallotSignedFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotSignedFactHint, Instance: ACCEPTBallotSignedFact{}}))
}

func testBaseVoteproofEncode() *baseTestBaseVoteproofEncode {
	t := new(baseTestBaseVoteproofEncode)

	t.priv = base.NewMPrivatekey()
	t.networkID = base.NetworkID(util.UUID().Bytes())

	t.Compare = func(a, b interface{}) {
		avp, ok := a.(base.Voteproof)
		t.True(ok)
		bvp, ok := b.(base.Voteproof)
		t.True(ok)

		t.NoError(bvp.IsValid(t.networkID))

		base.CompareVoteproof(t.Assert(), avp, bvp)
	}

	return t
}

func TestINITVoteproofJSON(tt *testing.T) {
	t := testBaseVoteproofEncode()

	t.Encode = func() (interface{}, []byte) {
		node := base.RandomAddress("")

		ifact := NewINITBallotFact(base.NewPoint(base.Height(32), base.Round(44)), valuehash.RandomSHA256(), valuehash.RandomSHA256())

		isignedFact := NewINITBallotSignedFact(node, ifact)

		t.NoError(isignedFact.Sign(t.priv, t.networkID))

		ivp := NewINITVoteproof(ifact.Point().Point)
		ivp.finish()
		ivp.SetResult(base.VoteResultMajority)
		ivp.SetMajority(ifact)
		ivp.SetSignedFacts([]base.BallotSignedFact{isignedFact})
		ivp.SetThreshold(base.Threshold(100))

		b, err := t.enc.Marshal(ivp)
		t.NoError(err)

		return ivp, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(INITVoteproof)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}

func TestACCEPTVoteproofJSON(tt *testing.T) {
	t := testBaseVoteproofEncode()

	t.Encode = func() (interface{}, []byte) {
		node := base.RandomAddress("")

		afact := NewACCEPTBallotFact(base.NewPoint(base.Height(32), base.Round(44)), valuehash.RandomSHA256(), valuehash.RandomSHA256())

		asignedFact := NewACCEPTBallotSignedFact(node, afact)

		t.NoError(asignedFact.Sign(t.priv, t.networkID))

		avp := NewACCEPTVoteproof(afact.Point().Point)
		avp.finish()
		avp.SetResult(base.VoteResultMajority)
		avp.SetMajority(afact)
		avp.SetSignedFacts([]base.BallotSignedFact{asignedFact})
		avp.SetThreshold(base.Threshold(100))

		b, err := t.enc.Marshal(avp)
		t.NoError(err)

		return avp, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(ACCEPTVoteproof)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}
