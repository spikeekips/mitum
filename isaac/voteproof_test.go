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
	ifact := NewINITBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	isignfact := NewINITBallotSignFact(t.local.Address(), ifact)
	t.NoError(isignfact.Sign(t.local.Privatekey(), t.networkID))

	ivp := NewINITVoteproof(ifact.Point().Point)
	ivp.SetResult(base.VoteResultMajority).
		SetMajority(ifact).
		SetSignFacts([]base.BallotSignFact{isignfact}).
		SetThreshold(base.Threshold(100)).
		Finish()

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
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "empty id")
}

func (t *testBaseVoteproof) TestInvalidStage() {
	ivp := t.validVoteproof()
	ivp.point = base.NewStagePoint(ivp.point.Point, base.StageUnknown)

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "wrong stage")
}

func (t *testBaseVoteproof) TestInvalidVoteResult() {
	ivp := t.validVoteproof()
	ivp.SetResult(base.VoteResultNotYet)

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "not yet finished")
}

func (t *testBaseVoteproof) TestZeroFinishedAt() {
	ivp := t.validVoteproof()
	ivp.finishedAt = time.Time{}

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.ErrorContains(err, "not yet finished")
}

func (t *testBaseVoteproof) TestEmptySignFacts() {
	ivp := t.validVoteproof()
	ivp.SetSignFacts(nil).Finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "empty sign facts")
}

func (t *testBaseVoteproof) TestNilMajority() {
	ivp := t.validVoteproof()
	ivp.SetMajority(nil)

	err := ivp.IsValid(t.networkID)
	t.NoError(err)

	t.Equal(base.VoteResultDraw, ivp.Result())
}

func (t *testBaseVoteproof) TestInvalidPoint() {
	ivp := t.validVoteproof()
	ivp.point = base.NewStagePoint(base.NewPoint(base.NilHeight, base.Round(0)), ivp.point.Stage())

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "invalid point")
}

func (t *testBaseVoteproof) TestDuplicatedNodeInSignFact() {
	ivp := t.validVoteproof()

	sfs := ivp.SignFacts()
	isf := sfs[0].(INITBallotSignFact)

	isignfact := NewINITBallotSignFact(isf.Node(), isf.Fact().(INITBallotFact))
	t.NoError(isignfact.Sign(t.local.Privatekey(), t.networkID))

	sfs = append(sfs, isignfact)

	ivp.SetSignFacts(sfs).Finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "duplicated node found")
}

func (t *testBaseVoteproof) TestInvalidSignFact() {
	ivp := t.validVoteproof()

	ifact := NewINITBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	isignfact := NewINITBallotSignFact(base.RandomAddress(""), ifact)
	t.NoError(isignfact.Sign(t.local.Privatekey(), util.UUID().Bytes())) // wrong network id

	ivp.SetMajority(ifact).SetSignFacts([]base.BallotSignFact{isignfact}).Finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "failed to verify sign")
}

func (t *testBaseVoteproof) TestWrongPointOfSignFact() {
	ivp := t.validVoteproof()

	ifact := NewINITBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	wfact := NewINITBallotFact(base.RawPoint(34, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	isignfact := NewINITBallotSignFact(base.RandomAddress(""), ifact)
	t.NoError(isignfact.Sign(t.local.Privatekey(), t.networkID))

	wsignfact := NewINITBallotSignFact(base.RandomAddress(""), wfact)
	t.NoError(wsignfact.Sign(t.local.Privatekey(), t.networkID))

	ivp.SetMajority(ifact).SetSignFacts([]base.BallotSignFact{isignfact, wsignfact}).Finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "point does not match")
	t.ErrorContains(err, "invalid sign fact")
}

func (t *testBaseVoteproof) TestWrongPointOfMajority() {
	ivp := t.validVoteproof()

	ifact := NewINITBallotFact(base.NewPoint(ivp.Point().Height()+1, ivp.Point().Round()), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	isignfact := NewINITBallotSignFact(base.RandomAddress(""), ifact)

	t.NoError(isignfact.Sign(t.local.Privatekey(), t.networkID))

	ivp.SetMajority(ifact).SetSignFacts([]base.BallotSignFact{isignfact}).Finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "point does not match")
	t.ErrorContains(err, "invalid majority")
}

func (t *testBaseVoteproof) TestMajorityNotFoundInSignFacts() {
	ivp := t.validVoteproof()

	fact := NewINITBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	ivp.SetMajority(fact).Finish()

	err := ivp.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "majoirty not found in sign facts")
}

func (t *testBaseVoteproof) TestWrongMajorityWithSuffrage() {
	suf, nodes := NewTestSuffrage(4)
	t.local = nodes[0]

	ivp := t.validVoteproof()

	fact := NewINITBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

	newsignfact := func(node LocalNode) INITBallotSignFact {
		signfact := NewINITBallotSignFact(node.Address(), fact)

		t.NoError(signfact.Sign(node.Privatekey(), t.networkID))

		return signfact
	}

	sfs := ivp.SignFacts()
	sfs = append(sfs, newsignfact(nodes[1]), newsignfact(nodes[2]), newsignfact(nodes[3]))
	ivp.SetSignFacts(sfs).SetThreshold(base.Threshold(67)).Finish()

	t.NoError(ivp.IsValid(t.networkID))

	err := base.IsValidVoteproofWithSuffrage(ivp, suf)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "wrong majority")
}

func (t *testBaseVoteproof) TestUnknownNode() {
	ivp := t.validVoteproof()

	ivp.SetThreshold(base.Threshold(100))

	t.NoError(ivp.IsValid(t.networkID))

	suf, _ := NewTestSuffrage(1)

	err := base.IsValidVoteproofWithSuffrage(ivp, suf)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "unknown node found")
}

func (t *testBaseVoteproof) TestNewACCEPT() {
	afact := NewACCEPTBallotFact(base.RawPoint(33, 55), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	node := base.RandomAddress("")
	asignfact := NewACCEPTBallotSignFact(node, afact)

	t.NoError(asignfact.Sign(t.local.Privatekey(), t.networkID))

	avp := NewACCEPTVoteproof(afact.Point().Point)
	avp.SetResult(base.VoteResultMajority).
		SetMajority(afact).
		SetSignFacts([]base.BallotSignFact{asignfact}).
		SetThreshold(base.Threshold(100)).
		Finish()

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
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotSignFactHint, Instance: INITBallotSignFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotSignFactHint, Instance: ACCEPTBallotSignFact{}}))
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
		point := base.RawPoint(32, 44)

		sfs := make([]base.BallotSignFact, 2)
		for i := range sfs {
			node := RandomLocalNode()
			fact := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)
			sf := NewINITBallotSignFact(node.Address(), fact)
			t.NoError(sf.Sign(node.Privatekey(), t.networkID))

			sfs[i] = sf
		}

		ivp := NewINITVoteproof(point)
		ivp.SetResult(base.VoteResultMajority).
			SetMajority(sfs[0].Fact().(base.BallotFact)).
			SetSignFacts(sfs).
			SetThreshold(base.Threshold(100)).
			Finish()

		b, err := t.enc.Marshal(&ivp)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

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

		asignfact := NewACCEPTBallotSignFact(node, afact)

		t.NoError(asignfact.Sign(t.priv, t.networkID))

		avp := NewACCEPTVoteproof(afact.Point().Point)
		avp.SetResult(base.VoteResultMajority).
			SetMajority(afact).
			SetSignFacts([]base.BallotSignFact{asignfact}).
			SetThreshold(base.Threshold(100)).
			Finish()

		t.NoError(avp.IsValid(t.networkID))

		b, err := t.enc.Marshal(&avp)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return avp, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		avp, ok := i.(ACCEPTVoteproof)
		t.True(ok)

		t.NoError(avp.IsValid(t.networkID))

		return i
	}

	suite.Run(tt, t)
}
