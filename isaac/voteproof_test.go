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
	local     LocalNode
	networkID base.NetworkID
}

func (t *testBaseVoteproof) SetupTest() {
	t.local = RandomLocalNode()
	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testBaseVoteproof) validVoteproof() INITVoteproof {
	ifact := NewINITBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	isignedFact := NewINITBallotSignedFact(t.local.Address(), ifact)
	t.NoError(isignedFact.Sign(t.local.Privatekey(), t.networkID))

	ivp := NewINITVoteproof(ifact.Point().Point)
	ivp.SetResult(base.VoteResultMajority).
		SetMajority(ifact).
		SetSignedFacts([]base.BallotSignedFact{isignedFact}).
		SetThreshold(base.Threshold(100)).
		finish()

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
	ivp.point = base.NewStagePoint(ivp.point.Point, base.StageUnknown)

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "wrong stage")
}

func (t *testBaseVoteproof) TestInvalidVoteResult() {
	ivp := t.validVoteproof()
	ivp.SetResult(base.VoteResultNotYet).finish()

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
	ivp.SetSignedFacts(nil).finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "empty signed facts")
}

func (t *testBaseVoteproof) TestNilMajority() {
	ivp := t.validVoteproof()
	ivp.SetMajority(nil).finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "empty majority")
}

func (t *testBaseVoteproof) TestNotNilMajorityOfDraw() {
	ivp := t.validVoteproof()
	m := ivp.Majority()
	ivp.SetResult(base.VoteResultDraw).SetMajority(m).finish()

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
	t.NoError(isignedFact.Sign(t.local.Privatekey(), t.networkID))

	sfs = append(sfs, isignedFact)

	ivp.SetSignedFacts(sfs).finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "duplicated node found")
}

func (t *testBaseVoteproof) TestInvalidSignedFact() {
	ivp := t.validVoteproof()

	ifact := NewINITBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	isignedFact := NewINITBallotSignedFact(base.RandomAddress(""), ifact)
	t.NoError(isignedFact.Sign(t.local.Privatekey(), util.UUID().Bytes())) // wrong network id

	ivp.SetMajority(ifact).SetSignedFacts([]base.BallotSignedFact{isignedFact}).finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "failed to verify signed")
}

func (t *testBaseVoteproof) TestWrongPointOfSignedFact() {
	ivp := t.validVoteproof()

	ifact := NewINITBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	wfact := NewINITBallotFact(base.RawPoint(34, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	isignedFact := NewINITBallotSignedFact(base.RandomAddress(""), ifact)
	t.NoError(isignedFact.Sign(t.local.Privatekey(), t.networkID))

	wsignedFact := NewINITBallotSignedFact(base.RandomAddress(""), wfact)
	t.NoError(wsignedFact.Sign(t.local.Privatekey(), t.networkID))

	ivp.SetMajority(ifact).SetSignedFacts([]base.BallotSignedFact{isignedFact, wsignedFact}).finish()

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

	t.NoError(isignedFact.Sign(t.local.Privatekey(), t.networkID))

	ivp.SetMajority(ifact).SetSignedFacts([]base.BallotSignedFact{isignedFact}).finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "point does not match")
	t.Contains(err.Error(), "invalid majority")
}

func (t *testBaseVoteproof) TestMajorityNotFoundInSignedFacts() {
	ivp := t.validVoteproof()

	fact := NewINITBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	ivp.SetMajority(fact).finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "majoirty not found in signed facts")
}

func (t *testBaseVoteproof) TestWrongMajorityWithSuffrage() {
	suf, nodes := NewTestSuffrage(4)
	t.local = nodes[0]

	ivp := t.validVoteproof()

	fact := NewINITBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	newsignedfact := func(node LocalNode) INITBallotSignedFact {
		signedFact := NewINITBallotSignedFact(node.Address(), fact)

		t.NoError(signedFact.Sign(node.Privatekey(), t.networkID))

		return signedFact
	}

	sfs := ivp.SignedFacts()
	sfs = append(sfs, newsignedfact(nodes[1]), newsignedfact(nodes[2]), newsignedfact(nodes[3]))
	ivp.SetSignedFacts(sfs).SetThreshold(base.Threshold(67)).finish()

	t.NoError(ivp.IsValid(t.networkID))

	err := base.IsValidVoteproofWithSuffrage(ivp, suf)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "wrong majority")
}

func (t *testBaseVoteproof) TestUnknownNode() {
	ivp := t.validVoteproof()

	ivp.SetThreshold(base.Threshold(100))

	t.NoError(ivp.IsValid(t.networkID))

	suf, _ := NewTestSuffrage(1)

	err := base.IsValidVoteproofWithSuffrage(ivp, suf)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "unknown node found")
}

func (t *testBaseVoteproof) TestNewACCEPT() {
	afact := NewACCEPTBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	node := base.RandomAddress("")
	asignedFact := NewACCEPTBallotSignedFact(node, afact)

	t.NoError(asignedFact.Sign(t.local.Privatekey(), t.networkID))

	avp := NewACCEPTVoteproof(afact.Point().Point)
	avp.SetResult(base.VoteResultMajority).
		SetMajority(afact).
		SetSignedFacts([]base.BallotSignedFact{asignedFact}).
		SetThreshold(base.Threshold(100)).
		finish()

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

		base.EqualVoteproof(t.Assert(), avp, bvp)
	}

	return t
}

func TestINITVoteproofJSON(tt *testing.T) {
	t := testBaseVoteproofEncode()

	t.Encode = func() (interface{}, []byte) {
		node := base.RandomAddress("")

		ifact := NewINITBallotFact(base.RawPoint(32, 44), valuehash.RandomSHA256(), valuehash.RandomSHA256())

		isignedFact := NewINITBallotSignedFact(node, ifact)

		t.NoError(isignedFact.Sign(t.priv, t.networkID))

		ivp := NewINITVoteproof(ifact.Point().Point)
		ivp.SetResult(base.VoteResultMajority).
			SetMajority(ifact).
			SetSignedFacts([]base.BallotSignedFact{isignedFact}).
			SetThreshold(base.Threshold(100)).
			finish()

		b, err := t.enc.Marshal(&ivp)
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

		afact := NewACCEPTBallotFact(base.RawPoint(32, 44), valuehash.RandomSHA256(), valuehash.RandomSHA256())

		asignedFact := NewACCEPTBallotSignedFact(node, afact)

		t.NoError(asignedFact.Sign(t.priv, t.networkID))

		avp := NewACCEPTVoteproof(afact.Point().Point)
		avp.SetResult(base.VoteResultMajority).
			SetMajority(afact).
			SetSignedFacts([]base.BallotSignedFact{asignedFact}).
			SetThreshold(base.Threshold(100)).
			finish()

		b, err := t.enc.Marshal(&avp)
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
